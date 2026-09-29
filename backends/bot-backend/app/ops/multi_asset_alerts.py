"""Multi-asset operational alerts (Section 26.4) on the EXISTING alert store (``alerts`` table, the same rows
``shared_lib.persistence.alert_manager.AlertManager`` writes and the monitoring API / acknowledgement flow reads).

Conditions (read-only evaluation over recorded state; nothing is mutated except the alert rows):

    TRANSFER_RESOLUTION_STUCK     an internal transfer still SUBMITTED / CONFIRMATION_PENDING / UNKNOWN /
                                  RECONCILIATION_REQUIRED past ``transfer_stuck_ms``
    ORDER_RESOLUTION_STUCK        a CATI reservation RESOLUTION_PENDING past its resolution deadline
    RECONCILIATION_MISMATCH       the latest transfer reconciliation run for an account left transfers unresolved
                                  or did not complete
    CAPABILITY_REGRESSION         an open position or active reservation's instrument is delisted / no longer
                                  API-tradable in the venue catalog
    INSTRUMENT_METADATA_STALE     a venue catalog with connected accounts is older than the discovery max age
    UNEXPLAINED_DATA_GAP          research acquisition has FAILED (retryable, unexplained) periods
    DATASET_ACQUISITION_STALLED   a dataset still ACQUIRING has recorded nothing for ``acquisition_stall_ms``
    REFERENCE_VENUE_DIVERGENCE    ``divergence_alert``: an FX venue price diverges from the reference beyond the
                                  threshold (evaluated where an FX context is built)

Rows carry reason code, tenant / account lineage where the condition is account-scoped (global dataset alerts
carry none), and a ``trace_id`` de-duplication key: an unacknowledged alert with the same key is never re-emitted,
so a persisting condition cannot flood the store. No secret or raw broker payload is ever written.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)
ALERTS_VERSION = "multi-asset-alerts-v1"

TRANSFER_RESOLUTION_STUCK = "TRANSFER_RESOLUTION_STUCK"
ORDER_RESOLUTION_STUCK = "ORDER_RESOLUTION_STUCK"
RECONCILIATION_MISMATCH = "RECONCILIATION_MISMATCH"
CAPABILITY_REGRESSION = "CAPABILITY_REGRESSION"
INSTRUMENT_METADATA_STALE = "INSTRUMENT_METADATA_STALE"
UNEXPLAINED_DATA_GAP = "UNEXPLAINED_DATA_GAP"
DATASET_ACQUISITION_STALLED = "DATASET_ACQUISITION_STALLED"
REFERENCE_VENUE_DIVERGENCE = "REFERENCE_VENUE_DIVERGENCE"


@dataclass(frozen=True)
class AlertPolicy:
    transfer_stuck_ms: int = 15 * 60_000
    acquisition_stall_ms: int = 6 * 3_600_000
    divergence_bps: float = 50.0


@dataclass(frozen=True)
class MultiAssetAlert:
    alert_type: str
    severity: str
    reason_code: str
    dedupe_key: str
    message: str
    user_id: Optional[str] = None
    broker_account_id: Optional[str] = None
    symbol: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)


def _iso_ms(value: Any) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(str(value))
        return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp() * 1000)
    except (TypeError, ValueError):
        return None


def _rows(db: Any, sql: str, params=()) -> List[Any]:
    try:
        with db.connect() as conn:
            return conn.execute(sql, params).fetchall()
    except Exception:
        return []  # a missing table is simply no evidence for that condition


def divergence_alert(venue: str, symbol: str, bps: Optional[float], *,
                     policy: AlertPolicy = AlertPolicy()) -> Optional[MultiAssetAlert]:
    if bps is None or abs(float(bps)) <= policy.divergence_bps:
        return None
    return MultiAssetAlert(REFERENCE_VENUE_DIVERGENCE, "MEDIUM", "REFERENCE_VENUE_DIVERGENCE_ABOVE_THRESHOLD",
                           f"div:{venue}:{symbol}", f"{venue} {symbol} diverges {float(bps):.1f} bps from reference",
                           symbol=symbol, details={"venue": venue, "bps": round(float(bps), 3),
                                                   "threshold_bps": policy.divergence_bps})


def evaluate(db: Any, *, now_ms: int, policy: AlertPolicy = AlertPolicy(), research: bool = True
             ) -> List[MultiAssetAlert]:
    from app.transfers.models import RECONCILABLE

    out: List[MultiAssetAlert] = []
    marks = ",".join("?" for _ in RECONCILABLE)
    for r in _rows(db, f"SELECT id, user_id, broker_account_id, status, updated_at FROM broker_transfer_requests "
                       f"WHERE status IN ({marks})", tuple(s.value for s in RECONCILABLE)):
        age = now_ms - (_iso_ms(r[4]) or now_ms)
        if age >= policy.transfer_stuck_ms:
            out.append(MultiAssetAlert(TRANSFER_RESOLUTION_STUCK, "HIGH", f"TRANSFER_{r[3]}_UNRESOLVED",
                                       f"xfer:{r[0]}", f"internal transfer unresolved ({r[3]}) for {age // 60_000} min",
                                       user_id=r[1], broker_account_id=r[2],
                                       details={"transfer_id": r[0], "status": r[3], "age_ms": age}))
    for r in _rows(db, "SELECT reservation_id, broker_account_id, trade_plan_id, resolution_deadline FROM "
                       "cati_portfolio_reservations WHERE status='RESOLUTION_PENDING' AND resolution_deadline<=?",
                   (now_ms,)):
        out.append(MultiAssetAlert(ORDER_RESOLUTION_STUCK, "CRITICAL", "SUBMIT_OUTCOME_UNRESOLVED", f"resv:{r[0]}",
                                   "entry submission outcome still unresolved past its deadline",
                                   broker_account_id=r[1], details={"reservation_id": r[0], "trade_plan_id": r[2]}))
    for acct, user, status, unresolved, run_at in _rows(
            db, "SELECT r.broker_account_id, r.user_id, r.status, r.still_unresolved, r.run_at FROM "
                "broker_transfer_reconciliations r WHERE r.run_at = (SELECT MAX(run_at) FROM "
                "broker_transfer_reconciliations x WHERE x.broker_account_id = r.broker_account_id)"):
        if (unresolved or 0) > 0 or str(status or "").upper() not in ("OK", "COMPLETED"):
            out.append(MultiAssetAlert(RECONCILIATION_MISMATCH, "HIGH", f"RECONCILIATION_{status}",
                                       f"recon:{acct}:{run_at}", f"transfer reconciliation left {unresolved} unresolved",
                                       user_id=user, broker_account_id=acct,
                                       details={"status": status, "still_unresolved": unresolved, "run_at": run_at}))
    out += _capability_regressions(db)
    out += _stale_catalogs(db, now_ms)
    if research:
        out += _research_alerts(now_ms, policy)
    return out


def _capability_regressions(db: Any) -> List[MultiAssetAlert]:
    from app.activation.account_status import VENUE_KEY, _catalog_env

    out = []
    held = _rows(db, "SELECT DISTINCT p.symbol, b.broker_account_id, a.user_id, a.broker_id, a.environment FROM positions p "
                     "JOIN bot_instances b ON b.id = p.bot_instance_id JOIN broker_accounts a ON a.id = b.broker_account_id "
                     "WHERE p.status='OPEN'")
    for symbol, account, user, broker, env in held:
        venue = VENUE_KEY.get(str(broker or "").lower())
        if venue is None:
            continue
        for e in _catalog_env(str(env or "").upper()):
            row = _rows(db, "SELECT api_tradable, delisted_at_ms FROM venue_instruments WHERE venue=? AND environment=? "
                            "AND venue_symbol=?", (venue, e, str(symbol).upper()))
            if row:
                tradable, delisted = row[0]
                if delisted is not None or not tradable:
                    out.append(MultiAssetAlert(
                        CAPABILITY_REGRESSION, "HIGH", "INSTRUMENT_DELISTED" if delisted is not None else
                        "INSTRUMENT_NOT_API_TRADABLE", f"cap:{account}:{venue}:{symbol}",
                        f"held instrument {symbol} lost execution capability on {venue}", user_id=user,
                        broker_account_id=account, symbol=str(symbol), details={"venue": venue, "environment": e}))
                break
    return out


def _stale_catalogs(db: Any, now_ms: int) -> List[MultiAssetAlert]:
    from app.activation.account_status import DISCOVERY_MAX_AGE_MS, VENUE_KEY, _connected_pairs, discovery_freshness

    out = []
    try:
        pairs = _connected_pairs(db)
    except Exception:
        return out
    for broker, env in sorted(pairs):
        f = discovery_freshness(db, broker, env, now_ms=now_ms)
        if f["status"] != "SYNCED":
            out.append(MultiAssetAlert(INSTRUMENT_METADATA_STALE, "MEDIUM", f"DISCOVERY_{f['status']}",
                                       f"stale:{VENUE_KEY[broker]}:{env}",
                                       f"{VENUE_KEY[broker]}/{env} instrument catalog is {f['status']}",
                                       details={"venue": VENUE_KEY[broker], "environment": env,
                                                "age_ms": f.get("age_ms"), "max_age_ms": DISCOVERY_MAX_AGE_MS}))
    return out


def _research_alerts(now_ms: int, policy: AlertPolicy) -> List[MultiAssetAlert]:
    from app.market_data import research_status as rs

    out = []
    try:
        acq = {"FX_REFERENCE": rs.fx_acquisition(rs.load_manifest("FX_REFERENCE")["manifest"]),
               "CRYPTO_DEEP": rs.crypto_deep_acquisition(rs.load_manifest("CRYPTO_DEEP")["manifest"])}
    except Exception:
        return out
    for name, a in acq.items():
        one = a.get("1m") or {}
        failed = one.get("failed_retryable_periods") or 0
        if failed:
            out.append(MultiAssetAlert(UNEXPLAINED_DATA_GAP, "MEDIUM", "INGEST_FAILURE_PERIODS", f"gap:{name}",
                                       f"{name}: {failed} failed (retryable) acquisition periods",
                                       details={"dataset": name, "failed_periods": failed}))
        last = one.get("last_recorded_at_ms") or a.get("last_recorded_at_ms")
        if a.get("state") == "ACQUIRING" and last is not None and now_ms - int(last) >= policy.acquisition_stall_ms:
            out.append(MultiAssetAlert(DATASET_ACQUISITION_STALLED, "LOW", "NO_PROGRESS_RECORDED", f"stall:{name}",
                                       f"{name} acquisition recorded nothing for {(now_ms - int(last)) // 3_600_000} h",
                                       details={"dataset": name, "last_recorded_at_ms": last,
                                                "remaining_periods": one.get("remaining_periods")}))
    return out


def emit(db: Any, alerts: List[MultiAssetAlert]) -> int:
    """Insert new alerts into the existing ``alerts`` store; an unacknowledged alert with the same key is kept,
    not duplicated. Returns the number written. Never raises."""
    written = 0
    try:
        from app.trading_intelligence.observability.metrics import METRICS
    except Exception:  # pragma: no cover
        METRICS = None
    for a in alerts:
        try:
            with db.connect() as conn:
                dup = conn.execute("SELECT 1 FROM alerts WHERE trace_id=? AND alert_type=? AND "
                                   "COALESCE(acknowledged,0)=0 LIMIT 1", (a.dedupe_key, a.alert_type)).fetchone()
                if dup:
                    continue
                cols = {r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
                row = {"ts": datetime.now(timezone.utc).isoformat(), "alert_type": a.alert_type,
                       "severity": a.severity, "trace_id": a.dedupe_key, "symbol": a.symbol, "message": a.message,
                       "details_json": json.dumps({**dict(a.details), "reason_code": a.reason_code,
                                                   "broker_account_id": a.broker_account_id,
                                                   "version": ALERTS_VERSION}, sort_keys=True, default=str)}
                if "user_id" in cols:
                    row["user_id"] = a.user_id
                conn.execute(f"INSERT INTO alerts ({', '.join(row)}) VALUES ({', '.join('?' for _ in row)})",
                             tuple(row.values()))
            written += 1
            if METRICS is not None:
                METRICS.inc("cati_multi_asset_alert_total", component=a.alert_type, status=a.severity)
        except Exception as exc:
            logger.debug("multi_asset_alert_dropped %s: %s", a.alert_type, type(exc).__name__)
    return written


async def multi_asset_alert_loop(db: Any, *, interval_s: float = 300.0) -> None:
    import asyncio
    import time

    while True:
        try:
            alerts = await asyncio.to_thread(evaluate, db, now_ms=int(time.time() * 1000))
            n = await asyncio.to_thread(emit, db, alerts)
            if n:
                logger.warning("[MULTI_ASSET_ALERTS] %d new alert(s): %s", n,
                               sorted({a.alert_type for a in alerts}))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            from shared_lib.core.security.redaction import redact_exception

            logger.error("[MULTI_ASSET_ALERTS] loop error=%s", redact_exception(exc))
        await asyncio.sleep(interval_s)


__all__ = ["ALERTS_VERSION", "AlertPolicy", "CAPABILITY_REGRESSION", "DATASET_ACQUISITION_STALLED",
           "INSTRUMENT_METADATA_STALE", "MultiAssetAlert", "ORDER_RESOLUTION_STUCK", "RECONCILIATION_MISMATCH",
           "REFERENCE_VENUE_DIVERGENCE", "TRANSFER_RESOLUTION_STUCK", "UNEXPLAINED_DATA_GAP", "divergence_alert",
           "emit", "evaluate", "multi_asset_alert_loop"]
