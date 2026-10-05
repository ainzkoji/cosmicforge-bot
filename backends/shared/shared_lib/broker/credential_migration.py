"""Explicit, transactional rotation; never enables runtime legacy reads."""
import json
from datetime import datetime, timezone
from cryptography.fernet import Fernet, InvalidToken
from shared_lib.core.security.broker_security import _primary_key, _legacy_keys


def classify(blob, broker):
    for index, key in enumerate([_primary_key(), *_legacy_keys()]):
        try:
            data = json.loads(Fernet(key).decrypt(blob.encode()).decode())
        except (InvalidToken, ValueError, TypeError, AttributeError):
            continue
        valid = isinstance(data, dict) and bool(data)
        if valid and broker in {"binance", "bybit", "bingx"}:
            valid = all(isinstance(v, str) and bool(v.strip()) for v in
                        (data.get("api_key") or data.get("key"), data.get("api_secret") or data.get("secret")))
        return ("PRIMARY" if index == 0 else "LEGACY", data) if valid else ("UNREADABLE", None)
    return "UNREADABLE", None


def rotate(conn, *, apply=False):
    """Atomic scan. Advance active version; rewrap history; leave orphans alone."""
    results = []
    conn.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        accounts = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM broker_accounts")}
        now = datetime.now(timezone.utc).isoformat()
        for table in ("broker_credentials_v2", "broker_credentials"):
            if table not in tables:
                continue
            for row in conn.execute(f"SELECT * FROM {table}").fetchall():
                record = dict(row)
                account = accounts.get(record["account_id"])
                if not account:
                    results.append((record["account_id"], "ORPHAN_SKIPPED"))
                    continue
                status, data = classify(record["encrypted_blob"], account["broker_id"].lower())
                results.append((account["id"], status))
                if not apply or status == "PRIMARY":
                    continue
                active = table.endswith("v2") and record["version"] == account.get("active_credential_version")
                if status == "UNREADABLE":
                    if active or (table == "broker_credentials" and not account.get("active_credential_version")):
                        conn.execute("UPDATE broker_accounts SET last_error_code=?, last_error_message=?, validation_error=? WHERE id=? AND user_id=?",
                            ("CREDENTIAL_RECONNECT_REQUIRED",)*3 + (account["id"], account["user_id"]))
                    continue
                token = Fernet(_primary_key()).encrypt(json.dumps(data).encode()).decode()
                if json.loads(Fernet(_primary_key()).decrypt(token.encode())) != data:
                    raise ValueError("CREDENTIAL_MIGRATION_VERIFICATION_FAILED")
                where = "account_id=?" + (" AND version=?" if table.endswith("v2") else "")
                args = (account["id"], record["version"]) if table.endswith("v2") else (account["id"],)
                conn.execute(f"UPDATE {table} SET encrypted_blob=?,key_metadata=?,updated_at=? WHERE {where}",
                             (token, "fernet_primary", now, *args))
                if active:
                    next_version = conn.execute("SELECT MAX(version)+1 FROM broker_credentials_v2 WHERE account_id=?", (account["id"],)).fetchone()[0]
                    copy = {**record, "version": next_version, "encrypted_blob": token,
                            "key_metadata": "fernet_primary", "created_at": now, "updated_at": now}
                    copy.pop("id", None)
                    columns = list(copy)
                    conn.execute(f"INSERT INTO broker_credentials_v2 ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", tuple(copy.values()))
                    conn.execute("UPDATE broker_credentials_v2 SET status='superseded',superseded_at=? WHERE account_id=? AND version=?", (now, account["id"], record["version"]))
                    conn.execute("UPDATE broker_accounts SET active_credential_version=?,updated_at=?,last_error_code=NULL,last_error_message=NULL,validation_error=NULL WHERE id=? AND user_id=? AND active_credential_version=?",
                                 (next_version, now, account["id"], account["user_id"], record["version"]))
                    if "broker_audit_log" in tables:
                        conn.execute("INSERT INTO broker_audit_log(broker_account_id,user_id,event_type,details_json,timestamp_utc) VALUES(?,?,?,?,?)",
                                     (account["id"], account["user_id"], "credential_encryption_migrated", json.dumps({"from_version": record["version"], "to_version": next_version, "primary_verified": True}), now))
                stored = conn.execute(f"SELECT encrypted_blob FROM {table} WHERE {where}", args).fetchone()[0]
                if classify(stored, account["broker_id"].lower())[0] != "PRIMARY":
                    raise ValueError("CREDENTIAL_MIGRATION_VERIFICATION_FAILED")
        conn.commit() if apply else conn.rollback()
    except BaseException:
        conn.rollback()
        raise
    return results
