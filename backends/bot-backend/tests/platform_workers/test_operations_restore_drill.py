"""Section H Step 8.5: backup / encrypted off-site / restore drill (scripts/restore_drill.py) and the secret scan
that refuses to ship plaintext credentials.

Every run builds a throwaway synthetic database in ``tmp_path``; the "remote" is a local directory. Nothing is
uploaded anywhere. The PostgreSQL leg runs when ``PLATFORM_TEST_POSTGRES_URL`` is set (pg_dump/pg_restore from
``PG_BIN`` or PATH).
"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sqlite3
from pathlib import Path

import pytest

from shared_lib.operations.secrets_scan import (CIPHERTEXT, EMPTY, HASH, MASKED, PLAINTEXT, classify_stored_secret,
                                                find_secrets)

ROOT = Path(__file__).resolve().parents[4]
PG_URL = os.environ.get("PLATFORM_TEST_POSTGRES_URL", "")


def _drill():
    spec = importlib.util.spec_from_file_location("restore_drill_under_test", ROOT / "scripts" / "restore_drill.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def drill(monkeypatch):
    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    monkeypatch.delenv("BACKUP_ENCRYPTION_RECIPIENT", raising=False)
    monkeypatch.delenv("BACKUP_GPG_RECIPIENT", raising=False)
    monkeypatch.setenv("DATABASE_URL", os.environ.get("DATABASE_URL", ""))   # restored after the test
    return _drill()


def _pg_ok() -> bool:
    if not PG_URL:
        return False
    d = _drill()
    return bool(shutil.which(d._pg_tool("pg_dump")) or Path(d._pg_tool("pg_dump.exe")).exists()
                or Path(d._pg_tool("pg_dump")).exists())


def test_full_drill_passes_and_evidence_holds_no_secret(drill, tmp_path):
    remote = tmp_path / "offsite"
    result = drill.run(tmp_path / "work", rows=2, remote_dir=remote,
                       postgres_url=PG_URL if _pg_ok() else None)
    failed = [k for k, v in result["checks"].items() if v is not True]
    assert result["ok"] and not failed, failed
    assert result["outcome"] == "PASSED"
    assert result["encryption"]["method"] == "fernet-TEST-ONLY" and "TEST ONLY" in result["encryption"]["key_source"]
    for group in ("research_governance", "account_configuration", "trading_intents"):
        assert result["verification"][group]
    assert "cati_family_events" in result["verification"]["research_governance"]
    # recovery restores account ownership and financial state ...
    assert result["checks"]["account_ownership_and_financial_state_equal"] is True
    owned = result["verification"]["ownership_and_financial_state"]
    assert {"users", "broker_accounts", "billing_events", "subscriptions", "invoices"} <= set(owned)
    # ... but never a live permission revoked after the backup
    assert result["checks"]["revoked_live_permission_not_restored"] is True
    rev = result["golive_revocation"]
    assert rev["consent_active_at_backup"] and not rev["consent_active_after_revocation"]
    assert rev["restored_database_alone_says_consent_active"] is True        # the DB alone would bring it back
    assert rev["ledger_verified_after_restore"] is False and rev["gate_authorized"] is False
    assert rev["gate_ledger_condition"].startswith("LEDGER_BEHIND_REVOCATION_ANCHOR")
    assert rev["anchored_revocations"] == ["CONSENT_REVOKED"] and rev["account_ownership_restored"] is True
    assert result["rpo"]["rpo_ms"] > 0 and result["rto"]["rto_ms"] > 0
    assert result["rpo"]["target"].startswith("UNSET") and result["rto"]["target"].startswith("UNSET")
    if PG_URL and _pg_ok():
        assert result["postgres"]["status"] == "PASSED" and result["postgres"]["restore_equal"]
    text = json.dumps(result, default=str)
    assert "SYNTHETIC-SECRET" not in text and "postgresql://" not in text
    # The remote copy is ciphertext: neither the SQLite header nor any synthetic credential is readable there.
    for f in remote.rglob("*"):
        if f.is_file():
            raw = f.read_bytes()
            assert b"SQLite format 3" not in raw[:16]
            assert b"SYNTHETIC-SECRET" not in raw


def _set(path, sql, params=()):
    conn = sqlite3.connect(str(path))
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall():
        conn.execute(f'DROP TRIGGER "{name}"')
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def _assert_refused(drill, tmp_path, tamper, expected_kind):
    remote = tmp_path / "offsite"
    with pytest.raises(drill.DrillRefused) as refused:
        drill.run(tmp_path / "work", rows=2, remote_dir=remote, tamper=tamper)
    evidence = json.loads(str(refused.value))
    assert evidence["outcome"] == "REFUSED_SECRET_SCAN" and evidence["ok"] is False
    kinds = [f["kind"] for f in evidence["secret_scan"]["findings"]] + \
            [f["kind"] for f in evidence["secret_scan"]["foreign_files"]]
    assert any(expected_kind in k for k in kinds), kinds
    assert not remote.exists() or not any(remote.rglob("*"))        # nothing was uploaded
    assert not list((tmp_path / "work").glob("*.enc"))              # nothing was encrypted
    return evidence


def test_plaintext_broker_credential_refuses_the_drill(drill, tmp_path):
    secret = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"

    def tamper(backup, _dir):
        _set(backup, "UPDATE broker_credentials SET encrypted_blob=?",
             (json.dumps({"api_key": "plainKEY123", "api_secret": secret}),))
    evidence = _assert_refused(drill, tmp_path, tamper, "PLAINTEXT_CREDENTIAL")
    assert secret not in json.dumps(evidence)                       # findings never carry the value


def test_plaintext_totp_secret_refuses_the_drill(drill, tmp_path):
    _assert_refused(drill, tmp_path, lambda b, d: _set(b, "UPDATE users SET totp_secret='JBSWY3DPEHPK3PXP'"),
                    "PLAINTEXT_CREDENTIAL")


def test_secret_inside_a_config_cell_refuses_the_drill(drill, tmp_path):
    def tamper(backup, _dir):
        conn = sqlite3.connect(str(backup))
        col = [c[1] for c in conn.execute("PRAGMA table_info(bot_instances)").fetchall()
               if (c[2] or "").upper() in ("TEXT", "JSON") and not c[5]][0]
        conn.close()
        _set(backup, f'UPDATE bot_instances SET "{col}"=?', ('{"exchange": "binance", "api_secret": "Qw3rTy-9988-xx"}',))
    _assert_refused(drill, tmp_path, tamper, "API_KEY_ASSIGNMENT")


def test_env_file_in_the_backup_set_refuses_the_drill(drill, tmp_path):
    def tamper(_backup, directory):
        (directory / ".env").write_text("BROKER_SECRET_KEY=abcdefghijklmnopqrstuvwxyz012345\n")
    _assert_refused(drill, tmp_path, tamper, "ENV_FILE")


def test_main_exit_codes(drill, tmp_path):
    assert drill.main(["--workdir", str(tmp_path / "w"), "--out", str(tmp_path / "o.json"),
                       "--source", str(tmp_path / "missing.db")]) == drill.EXIT_USAGE
    assert drill.main(["--workdir", str(tmp_path / "w"), "--out", str(tmp_path / "o.json"), "--rows", "0"]) == 1


def test_offsite_uploader_still_refuses_plaintext(drill):
    assert drill.offsite_refuses_plaintext() is True


def test_revocation_check_fails_when_revocations_are_not_anchored_outside_the_database(drill, tmp_path, monkeypatch):
    """The check is not vacuous: were a revocation kept only inside the database, the restore would resurrect the
    live consent, the ledger would verify, and the drill check must FAIL."""
    from app.golive.ledger import GoLiveLedger
    monkeypatch.setattr(GoLiveLedger, "_anchor", lambda self, ev: None)
    leg = drill.revocation_leg(tmp_path)
    assert leg["status"] == "FAILED"
    assert leg["ledger_verified_after_restore"] is True and leg["anchored_revocations"] == []


# ── backup failure markers (Step 8.5: failures, not only age, reach the alerting) ──────────────────────────
def test_backup_and_offsite_scripts_record_durable_failure_markers(drill, tmp_path):
    bt, off = drill._load("backup_trading_db"), drill._load("offsite_backup")
    out = tmp_path / "backups"
    out.mkdir()
    assert bt.main(["--database", str(tmp_path / "missing.db"), "--output-dir", str(out)]) == bt.EXIT_USAGE
    failed = json.loads((out / "backup-status.json").read_text(encoding="utf-8"))
    assert (failed["outcome"], failed["exit_code"], failed["last_ok_at"]) == ("FAILED", bt.EXIT_USAGE, None)
    src = tmp_path / "src.db"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (x)")
    conn.commit()
    conn.close()
    assert bt.main(["--database", str(src), "--output-dir", str(out)]) == bt.EXIT_OK
    ok = json.loads((out / "backup-status.json").read_text(encoding="utf-8"))
    assert ok["outcome"] == "OK" and ok["last_ok_at"] == ok["finished_at"] and ok["file"].startswith("cosmicforge-")
    # off-site: no destination configured -> exit 1, recorded; a dry run records nothing
    assert off.main(["--backup-dir", str(out), "--env-file", str(tmp_path / "none.env")], environ={}) == off.EXIT_CONFIG
    marker = json.loads((out / "offsite-status.json").read_text(encoding="utf-8"))
    assert marker["outcome"] == "FAILED" and marker["exit_code"] == off.EXIT_CONFIG and "destination" in marker["message"]
    (out / "offsite-status.json").unlink()
    off.main(["--backup-dir", str(out), "--env-file", str(tmp_path / "none.env"), "--dry-run"], environ={})
    assert not (out / "offsite-status.json").exists()
    # the marker files never disturb backup selection, pruning or the drill's backup-set scan
    assert [p.name for p in out.iterdir() if bt.NAME.match(p.name)] == [ok["file"]]


def test_backup_status_marker_failure_is_never_fatal(drill, tmp_path, monkeypatch):
    bt = drill._load("backup_trading_db")
    monkeypatch.setattr(bt.os, "replace", lambda *a, **k: (_ for _ in ()).throw(PermissionError("read-only")))
    bt.write_status(tmp_path, tool="backup_trading_db", code=2, file=None, message="FAILED x")   # does not raise


# ── classification units ───────────────────────────────────────────────────────────────────────────────────
def test_classify_stored_secret():
    from cryptography.fernet import Fernet
    import base64
    token = Fernet(Fernet.generate_key()).encrypt(b'{"api_key": "x"}').decode()
    assert classify_stored_secret(token) == CIPHERTEXT
    assert classify_stored_secret("v2:" + token) == CIPHERTEXT
    assert classify_stored_secret("enc:v2:" + token) == CIPHERTEXT
    assert classify_stored_secret(base64.urlsafe_b64encode(token.encode()).decode()) == CIPHERTEXT
    assert classify_stored_secret("$2b$12$" + "a" * 53) == HASH
    assert classify_stored_secret("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") == HASH
    assert classify_stored_secret("abcd****wxyz") == MASKED
    assert classify_stored_secret(None) == EMPTY and classify_stored_secret("") == EMPTY
    assert classify_stored_secret("JBSWY3DPEHPK3PXP") == PLAINTEXT
    assert classify_stored_secret('{"api_secret": "abc"}') == PLAINTEXT
    assert classify_stored_secret("gAAAAAnotreallyfernet") == PLAINTEXT


def test_find_secrets_examples():
    assert "PRIVATE_KEY_BLOCK" in find_secrets("-----BEGIN OPENSSH PRIVATE KEY-----")
    assert "CLOUD_ACCESS_KEY" in find_secrets("AKIAIOSFODNN7EXAMPLE")
    assert "PAYMENT_PROVIDER_KEY" in find_secrets("sk_" + "live_" + "x" * 24)  # built at run time: no key-shaped literal in the repository
    assert "BOT_TOKEN" in find_secrets("123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawq")
    assert "ENV_FILE_SECRET" in find_secrets("BINANCE_API_SECRET=abcdef123456")
    assert "CONNECTION_STRING_PASSWORD" in find_secrets("postgresql://app:hunter2@db/prod")
    assert find_secrets("order 123456 failed; tx 0x" + "ab" * 32 + " uuid 3fa85f64-5717-4562-b3fc-2c963f66afa6") == []
    assert find_secrets("I forgot my password") == []
