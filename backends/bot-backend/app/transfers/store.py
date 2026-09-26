"""Persistence for broker-internal transfers.

* Idempotency: (user_id, broker_account_id, idempotency_key) is UNIQUE. A
  repeat with the same fingerprint returns the existing transfer; a repeat
  with a different fingerprint is an ``IdempotencyConflict``.
* Every state change is a compare-and-set on the current status inside
  ``BEGIN IMMEDIATE`` and appends one ``broker_transfer_events`` row
  (append-only, enforced by triggers).
* Every read is scoped by user_id AND broker_account_id.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from app.transfers.models import (
    ALLOWED_TRANSITIONS, IN_FLIGHT, RECONCILABLE, IdempotencyConflict, TransferIntent, TransferStatus,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row(r: Any) -> Dict[str, Any]:
    d = dict(r)
    d["metadata"] = json.loads(d.pop("metadata_json") or "{}")
    return d


class TransferStore:
    def __init__(self, db: Any):
        self.db = db

    # -- create (idempotent) --------------------------------------------------------
    def create_or_get(self, intent: TransferIntent, *, broker: str, environment: str,
                      credential_version: Optional[int]) -> tuple[Dict[str, Any], bool]:
        """(row, created). Raises IdempotencyConflict on key reuse with other params."""
        now = _now()
        meta = {**dict(intent.metadata), "fingerprint": intent.fingerprint()}
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM broker_transfer_requests WHERE user_id=? AND broker_account_id=? AND idempotency_key=?",
                (intent.user_id, intent.broker_account_id, intent.idempotency_key)).fetchone()
            if existing is not None:
                row = _row(existing)
                if row["metadata"].get("fingerprint") != intent.fingerprint():
                    raise IdempotencyConflict("idempotency_key already used for a different transfer")
                return row, False
            tid = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO broker_transfer_requests (id, user_id, broker_account_id, broker, environment,
                   credential_version, asset, amount, source_wallet, destination_wallet, idempotency_key, status,
                   origin, requested_at, metadata_json, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (tid, intent.user_id, intent.broker_account_id, broker, environment, credential_version,
                 intent.asset.upper(), format(intent.amount, "f"), intent.source_wallet.upper(),
                 intent.destination_wallet.upper(), intent.idempotency_key, TransferStatus.REQUESTED.value,
                 intent.origin.value, now, json.dumps(meta, default=str), now, now))
            self._event(conn, tid, intent.user_id, intent.broker_account_id, "CREATED", None,
                        TransferStatus.REQUESTED.value, {"origin": intent.origin.value})
            row = conn.execute("SELECT * FROM broker_transfer_requests WHERE id=?", (tid,)).fetchone()
            return _row(row), True

    # -- transitions ----------------------------------------------------------------
    def transition(self, transfer_id: str, *, expect: TransferStatus, to: TransferStatus,
                   event: str, detail: Optional[Dict[str, Any]] = None, **fields: Any) -> bool:
        """Compare-and-set. False when the row is no longer in ``expect``."""
        if to not in ALLOWED_TRANSITIONS[expect]:
            raise ValueError(f"illegal transfer transition {expect.value} -> {to.value}")
        now = _now()
        sets = ["status=?", "updated_at=?"]
        vals: List[Any] = [to.value, now]
        for k, v in fields.items():
            sets.append(f"{k}=?")
            vals.append(json.dumps(v, default=str) if k == "metadata_json" and not isinstance(v, str) else v)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT user_id, broker_account_id, status FROM broker_transfer_requests WHERE id=?",
                               (transfer_id,)).fetchone()
            if row is None or row["status"] != expect.value:
                return False
            conn.execute(f"UPDATE broker_transfer_requests SET {', '.join(sets)} WHERE id=? AND status=?",
                         (*vals, transfer_id, expect.value))
            self._event(conn, transfer_id, row["user_id"], row["broker_account_id"], event, expect.value, to.value,
                        detail or {})
            return True

    def note(self, transfer_id: str, event: str, detail: Dict[str, Any], **fields: Any) -> None:
        """Append an event without a status change (e.g. a reconciliation attempt)."""
        with self.db.connect() as conn:
            row = conn.execute("SELECT user_id, broker_account_id, status FROM broker_transfer_requests WHERE id=?",
                               (transfer_id,)).fetchone()
            if row is None:
                return
            if fields:
                sets = ", ".join(f"{k}=?" for k in fields)
                conn.execute(f"UPDATE broker_transfer_requests SET {sets}, updated_at=? WHERE id=?",
                             (*fields.values(), _now(), transfer_id))
            self._event(conn, transfer_id, row["user_id"], row["broker_account_id"], event, row["status"],
                        row["status"], detail)

    @staticmethod
    def _event(conn, transfer_id, user_id, account_id, event, from_status, to_status, detail) -> None:
        conn.execute(
            "INSERT INTO broker_transfer_events (transfer_id, user_id, broker_account_id, event_type, from_status, "
            "to_status, detail_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (transfer_id, user_id, account_id, event, from_status, to_status, json.dumps(detail, default=str), _now()))

    # -- reads (always user + account scoped) ---------------------------------------
    def get(self, *, user_id: str, broker_account_id: str, transfer_id: str) -> Optional[Dict[str, Any]]:
        with self.db.connect() as conn:
            r = conn.execute("SELECT * FROM broker_transfer_requests WHERE id=? AND user_id=? AND broker_account_id=?",
                             (transfer_id, user_id, broker_account_id)).fetchone()
        return _row(r) if r else None

    def get_any(self, transfer_id: str) -> Optional[Dict[str, Any]]:
        """Internal (worker) read by id; never exposed through the API."""
        with self.db.connect() as conn:
            r = conn.execute("SELECT * FROM broker_transfer_requests WHERE id=?", (transfer_id,)).fetchone()
        return _row(r) if r else None

    def list(self, *, user_id: str, broker_account_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        with self.db.connect() as conn:
            rows = conn.execute("SELECT * FROM broker_transfer_requests WHERE user_id=? AND broker_account_id=? "
                                "ORDER BY requested_at DESC LIMIT ?", (user_id, broker_account_id, int(limit))).fetchall()
        return [_row(r) for r in rows]

    def events(self, *, user_id: str, broker_account_id: str, transfer_id: str) -> List[Dict[str, Any]]:
        with self.db.connect() as conn:
            rows = conn.execute("SELECT event_type, from_status, to_status, detail_json, created_at FROM "
                                "broker_transfer_events WHERE transfer_id=? AND user_id=? AND broker_account_id=? "
                                "ORDER BY id", (transfer_id, user_id, broker_account_id)).fetchall()
        return [{**dict(r), "detail": json.loads(r["detail_json"] or "{}")} for r in rows]

    def in_flight(self, broker_account_id: str, exclude_id: Optional[str] = None) -> List[Dict[str, Any]]:
        marks = ",".join("?" for _ in IN_FLIGHT)
        with self.db.connect() as conn:
            rows = conn.execute(f"SELECT * FROM broker_transfer_requests WHERE broker_account_id=? AND status IN ({marks})",
                                (broker_account_id, *[s.value for s in IN_FLIGHT])).fetchall()
        return [_row(r) for r in rows if r["id"] != exclude_id]

    def reconcilable(self, broker_account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        marks = ",".join("?" for _ in RECONCILABLE)
        sql = f"SELECT * FROM broker_transfer_requests WHERE status IN ({marks})"
        args: List[Any] = [s.value for s in RECONCILABLE]
        if broker_account_id:
            sql += " AND broker_account_id=?"
            args.append(broker_account_id)
        with self.db.connect() as conn:
            rows = conn.execute(sql + " ORDER BY requested_at", args).fetchall()
        return [_row(r) for r in rows]

    def claimed_broker_ids(self, broker_account_id: str) -> set:
        with self.db.connect() as conn:
            rows = conn.execute("SELECT broker_transfer_id FROM broker_transfer_requests WHERE broker_account_id=? "
                                "AND broker_transfer_id IS NOT NULL", (broker_account_id,)).fetchall()
        return {str(r[0]) for r in rows}

    def completed_amount_since(self, broker_account_id: str, asset: str, hours: int = 24) -> Decimal:
        """Daily-limit usage: COMPLETED plus everything that may still complete."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        counted = [TransferStatus.COMPLETED] + list(IN_FLIGHT)
        marks = ",".join("?" for _ in counted)
        with self.db.connect() as conn:
            rows = conn.execute(f"SELECT amount FROM broker_transfer_requests WHERE broker_account_id=? AND asset=? "
                                f"AND requested_at>=? AND status IN ({marks})",
                                (broker_account_id, asset.upper(), since, *[s.value for s in counted])).fetchall()
        return sum((Decimal(r[0]) for r in rows), Decimal("0"))

    # -- settings -----------------------------------------------------------------
    #: Auto Capital Routing policy (Section 9.8). Auto routing is distinct from auto trading: it needs
    #: mode=AUTOMATED_INTERNAL_REALLOCATION + an explicit authorization + auto_rebalance_enabled, and
    #: ``emergency_disabled`` stops it immediately (manual transfers stay possible).
    #: ``allowed_routes``: ["FUND->CONTRACT", ...] (native types or purposes); None = any declared route.
    #: ``max_transfer_pct``: max share (0-1] of the source's transferable balance per transfer.
    #: ``max_destination_balance``: the destination's transferable balance may not exceed it afterwards.
    #: ``manual_approval_threshold``: an AUTOMATED transfer above it is refused (MANUAL_APPROVAL_REQUIRED).
    DEFAULT_SETTINGS = {"mode": "MANUAL_TRANSFER", "auto_rebalance_enabled": False, "asset_allowlist": None,
                        "wallet_allowlist": None, "max_transfer_amount": None, "min_funding_balance": None,
                        "min_derivatives_reserve": None, "min_free_margin": None, "daily_transfer_limit": None,
                        "authorized_at": None, "allowed_routes": None, "max_transfer_pct": None,
                        "max_destination_balance": None, "manual_approval_threshold": None,
                        "emergency_disabled": False}

    def settings(self, *, user_id: str, broker_account_id: str) -> Dict[str, Any]:
        with self.db.connect() as conn:
            r = conn.execute("SELECT * FROM broker_transfer_settings WHERE broker_account_id=? AND user_id=?",
                             (broker_account_id, user_id)).fetchone()
        if r is None:
            return dict(self.DEFAULT_SETTINGS)
        d = {**self.DEFAULT_SETTINGS, **dict(r)}
        d["auto_rebalance_enabled"] = bool(d["auto_rebalance_enabled"])
        d["emergency_disabled"] = bool(d.get("emergency_disabled"))
        d["asset_allowlist"] = json.loads(d.pop("asset_allowlist_json", None) or "null")
        d["wallet_allowlist"] = json.loads(d.pop("wallet_allowlist_json", None) or "null")
        d["allowed_routes"] = json.loads(d.pop("allowed_routes_json", None) or "null")
        return d

    def save_settings(self, *, user_id: str, broker_account_id: str, values: Dict[str, Any]) -> Dict[str, Any]:
        cur = self.settings(user_id=user_id, broker_account_id=broker_account_id)
        cur.update({k: v for k, v in values.items() if k in cur})
        automated = cur["mode"] == "AUTOMATED_INTERNAL_REALLOCATION"
        authorized_at = cur.get("authorized_at") or (_now() if automated else None)
        if not automated:
            authorized_at = None
        with self.db.connect() as conn:
            conn.execute(
                """INSERT INTO broker_transfer_settings (broker_account_id, user_id, mode, auto_rebalance_enabled,
                   max_transfer_amount, min_funding_balance, min_derivatives_reserve, min_free_margin,
                   asset_allowlist_json, wallet_allowlist_json, daily_transfer_limit, authorized_at, updated_at,
                   allowed_routes_json, max_transfer_pct, max_destination_balance, manual_approval_threshold,
                   emergency_disabled)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(broker_account_id) DO UPDATE SET mode=excluded.mode,
                   auto_rebalance_enabled=excluded.auto_rebalance_enabled,
                   max_transfer_amount=excluded.max_transfer_amount, min_funding_balance=excluded.min_funding_balance,
                   min_derivatives_reserve=excluded.min_derivatives_reserve, min_free_margin=excluded.min_free_margin,
                   asset_allowlist_json=excluded.asset_allowlist_json, wallet_allowlist_json=excluded.wallet_allowlist_json,
                   daily_transfer_limit=excluded.daily_transfer_limit, authorized_at=excluded.authorized_at,
                   updated_at=excluded.updated_at, allowed_routes_json=excluded.allowed_routes_json,
                   max_transfer_pct=excluded.max_transfer_pct, max_destination_balance=excluded.max_destination_balance,
                   manual_approval_threshold=excluded.manual_approval_threshold,
                   emergency_disabled=excluded.emergency_disabled
                   WHERE broker_transfer_settings.user_id=excluded.user_id""",
                (broker_account_id, user_id, cur["mode"], int(bool(cur["auto_rebalance_enabled"])),
                 _s(cur["max_transfer_amount"]), _s(cur["min_funding_balance"]), _s(cur["min_derivatives_reserve"]),
                 _s(cur["min_free_margin"]), json.dumps(cur["asset_allowlist"]), json.dumps(cur["wallet_allowlist"]),
                 _s(cur["daily_transfer_limit"]), authorized_at, _now(), json.dumps(cur["allowed_routes"]),
                 _s(cur["max_transfer_pct"]), _s(cur["max_destination_balance"]),
                 _s(cur["manual_approval_threshold"]), int(bool(cur["emergency_disabled"]))))
        return self.settings(user_id=user_id, broker_account_id=broker_account_id)


def _s(v: Any) -> Optional[str]:
    return None if v in (None, "") else str(Decimal(str(v)))


__all__ = ["TransferStore"]
