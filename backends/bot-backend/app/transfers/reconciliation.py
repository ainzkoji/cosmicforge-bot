"""Broker-internal transfer reconciliation (Phase 2I).

For every transfer the broker may still be processing (SUBMITTED,
CONFIRMATION_PENDING, UNKNOWN, RECONCILIATION_REQUIRED) ask the broker's own
transfer history what happened:

* found  -> COMPLETED / FAILED / CONFIRMATION_PENDING (broker-authoritative)
* not found:
    - broker de-duplicates on OUR id (Bybit transferId) and the settlement
      window has passed with enough attempts -> FAILED
      (NOT_FOUND_AT_BROKER): the broker has provably never accepted it;
    - otherwise -> RECONCILIATION_REQUIRED, and it stays in flight so no
      dependent transfer or trade proceeds. It is NEVER re-submitted.

History rows are also normalised into ``broker_transfers_cache`` with
``classification='INTERNAL_TRANSFER'`` (never DEPOSIT / WITHDRAWAL).
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

from shared_lib.broker import BrokerResolverError, resolve_broker_auth
from shared_lib.broker.resolver import BrokerAuth
from shared_lib.core.security.redaction import redact_exception

from app.transfers.adapters import TransferAdapter, adapter_for
from app.transfers.models import TransferStatus as S
from app.transfers.store import TransferStore

logger = logging.getLogger(__name__)

SETTLEMENT_WINDOW_MS = 30 * 60_000
MIN_ATTEMPTS_BEFORE_NOT_FOUND = 3


def _ms(iso: Optional[str]) -> int:
    if not iso:
        return 0
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


class TransferReconciler:
    def __init__(self, db: Any, *, adapter_factory: Callable[[BrokerAuth], Optional[TransferAdapter]] = adapter_for,
                 resolver: Callable[..., BrokerAuth] = resolve_broker_auth, now_ms: Optional[Callable[[], int]] = None):
        self.db = db
        self.store = TransferStore(db)
        self._adapter_factory = adapter_factory
        self._resolve = resolver
        self._now = now_ms or (lambda: int(datetime.now(timezone.utc).timestamp() * 1000))

    def reconcile_account(self, *, user_id: str, broker_account_id: str) -> Dict[str, Any]:
        summary = {"history_rows": 0, "matched": 0, "completed": 0, "failed": 0, "still_unresolved": 0,
                   "status": "OK", "errors": []}
        try:
            auth = self._resolve(broker_account_id, user_id, self.db)
        except BrokerResolverError as exc:
            summary.update(status="CREDENTIALS_UNAVAILABLE", errors=[exc.reason_code])
            return self._record(user_id, broker_account_id, "unknown", summary)
        adapter = self._adapter_factory(auth)
        if adapter is None:
            summary["status"] = "UNSUPPORTED"
            return self._record(user_id, broker_account_id, auth.broker_type, summary)

        claimed = self.store.claimed_broker_ids(broker_account_id)
        now = self._now()
        for row in self.store.reconcilable(broker_account_id):
            if row["user_id"] != user_id:
                continue
            current = S(row["status"])
            attempts = int(row.get("reconcile_attempts") or 0) + 1
            submitted_ms = _ms(row.get("submitted_at") or row.get("requested_at"))
            try:
                found = adapter.lookup(request_id=row["id"], broker_transfer_id=row.get("broker_transfer_id"),
                                       route_code=(row.get("metadata") or {}).get("route_code") or "",
                                       asset=row["asset"], amount=Decimal(row["amount"]),
                                       submitted_at_ms=submitted_ms, claimed_ids=claimed)
            except Exception as exc:
                summary["errors"].append(redact_exception(exc)[:160])
                self.store.note(row["id"], "RECONCILE_ERROR", {"error": redact_exception(exc)[:200]},
                                reconcile_attempts=attempts, last_reconciled_at=_iso(now))
                summary["still_unresolved"] += 1
                continue
            stamp = {"reconcile_attempts": attempts, "last_reconciled_at": _iso(now)}
            if found.found and found.status in (S.COMPLETED, S.FAILED):
                summary["matched"] += 1
                fields = dict(stamp, broker_transfer_id=found.broker_transfer_id or row.get("broker_transfer_id"))
                if found.status == S.COMPLETED:
                    fields["confirmed_at"] = _iso(now)
                    summary["completed"] += 1
                else:
                    fields["failure_reason"] = f"BROKER_STATUS_{found.raw_status or 'FAILED'}"
                    summary["failed"] += 1
                self.store.transition(row["id"], expect=current, to=found.status, event="RECONCILED",
                                      detail={"raw_status": found.raw_status}, **fields)
                if found.broker_transfer_id:
                    claimed.add(str(found.broker_transfer_id))
                continue
            if found.found:  # still pending at the broker
                summary["matched"] += 1
                summary["still_unresolved"] += 1
                if current in (S.UNKNOWN, S.RECONCILIATION_REQUIRED, S.SUBMITTED):
                    self.store.transition(row["id"], expect=current, to=S.CONFIRMATION_PENDING,
                                          event="RECONCILED_PENDING", detail={"raw_status": found.raw_status},
                                          broker_transfer_id=found.broker_transfer_id, **stamp)
                else:
                    self.store.note(row["id"], "RECONCILE_PENDING", {"raw_status": found.raw_status}, **stamp)
                continue
            # Not found in broker history.
            aged = now - submitted_ms >= SETTLEMENT_WINDOW_MS
            if adapter.client_supplied_id and aged and attempts >= MIN_ATTEMPTS_BEFORE_NOT_FOUND:
                self.store.transition(row["id"], expect=current,
                                      to=S.FAILED, event="NOT_FOUND_AT_BROKER",
                                      detail={"attempts": attempts}, failure_reason="NOT_FOUND_AT_BROKER", **stamp)
                summary["failed"] += 1
                continue
            summary["still_unresolved"] += 1
            if current in (S.UNKNOWN, S.SUBMITTED, S.CONFIRMATION_PENDING):
                self.store.transition(row["id"], expect=current, to=S.RECONCILIATION_REQUIRED,
                                      event="RECONCILIATION_REQUIRED", detail={"attempts": attempts}, **stamp)
            else:
                self.store.note(row["id"], "RECONCILE_NOT_FOUND", {"attempts": attempts}, **stamp)

        # Normalise broker history into the transfer cache.
        try:
            start = now - 7 * 86_400_000
            rows = adapter.history(start, now)
            summary["history_rows"] = len(rows)
            self._cache_history(user_id, auth, rows)
        except Exception as exc:
            summary["errors"].append(f"history: {redact_exception(exc)[:160]}")
        if summary["still_unresolved"]:
            summary["status"] = "UNRESOLVED_REMAIN"
        return self._record(user_id, broker_account_id, auth.broker_type, summary)

    def _cache_history(self, user_id: str, auth: BrokerAuth, rows) -> None:
        now = _iso(self._now())
        with self.db.connect() as conn:
            linked = {r[0]: r[1] for r in conn.execute(
                "SELECT broker_transfer_id, id FROM broker_transfer_requests WHERE broker_account_id=? "
                "AND broker_transfer_id IS NOT NULL", (auth.account_id,)).fetchall()}
            for r in rows:
                conn.execute(
                    """INSERT INTO broker_transfers_cache (user_id, broker_account_id, broker_id, ts_utc, type, asset,
                       amount, status, raw_id, raw_json, created_at, classification, direction, source_wallet,
                       destination_wallet, transfer_request_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(broker_account_id, type, raw_id) DO UPDATE SET status=excluded.status,
                       transfer_request_id=COALESCE(excluded.transfer_request_id, broker_transfers_cache.transfer_request_id)""",
                    (user_id, auth.account_id, auth.broker_type, _iso(r.timestamp_ms), "INTERNAL_TRANSFER",
                     r.asset.upper(), float(r.amount), r.status.value, r.broker_transfer_id,
                     json.dumps(dict(r.raw), default=str), now, "INTERNAL_TRANSFER",
                     f"{r.source_native}->{r.destination_native}", r.source_native, r.destination_native,
                     linked.get(r.broker_transfer_id)))

    def _record(self, user_id: str, account_id: str, broker: str, summary: Dict[str, Any]) -> Dict[str, Any]:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO broker_transfer_reconciliations (id, user_id, broker_account_id, broker, run_at, "
                "history_rows, matched, completed, failed, still_unresolved, status, detail_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), user_id, account_id, broker, _iso(self._now()), summary["history_rows"],
                 summary["matched"], summary["completed"], summary["failed"], summary["still_unresolved"],
                 summary["status"], json.dumps({"errors": summary["errors"]})))
        return summary

    def reconcile_all(self) -> Dict[str, Any]:
        """Worker entry point: every account that has something to resolve."""
        results = {}
        pairs = {(r["user_id"], r["broker_account_id"]) for r in self.store.reconcilable()}
        for user_id, account_id in sorted(pairs):
            try:
                results[account_id] = self.reconcile_account(user_id=user_id, broker_account_id=account_id)
            except Exception as exc:
                logger.error("transfer_reconcile_failed account=%s error=%s", account_id, redact_exception(exc))
                results[account_id] = {"status": "ERROR"}
        return results


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


__all__ = ["MIN_ATTEMPTS_BEFORE_NOT_FOUND", "SETTLEMENT_WINDOW_MS", "TransferReconciler"]


async def reconciliation_loop(db: Any, *, interval_s: float = 60.0) -> None:
    """Background worker: resolve in-flight transfers from broker history.

    Only accounts with a reconcilable transfer are contacted. Runs the
    blocking broker calls in a worker thread.
    """
    import asyncio

    reconciler = TransferReconciler(db)
    while True:
        try:
            if reconciler.store.reconcilable():
                await asyncio.to_thread(reconciler.reconcile_all)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("transfer_reconciliation_loop error=%s", redact_exception(exc))
        await asyncio.sleep(interval_s)
