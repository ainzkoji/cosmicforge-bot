"""Protection state of an open position: CONFIRMED, ABSENT or UNKNOWN.

A failed read of the venue's open conditional orders is not evidence that a
position is unprotected. The maintenance path used to turn ANY exception from
the protection step into the reduce-only fail-safe close, so a timeout, a
5xx, a rate limit or a malformed answer while reading the stop legs closed a
healthy protected position. This module is the single place that decides
which failure means what:

* ``ABSENT``  -- the venue answered and the evidence shows the leg is not
  there and cannot be established: a definitive 4xx refusal of the CREATE, an
  acknowledged leg that two consecutive successful reads no longer list, every
  bounded attempt proven absent, or a leg found with the wrong geometry. The
  caller keeps its fail-safe close.
* ``UNKNOWN`` -- the venue did not answer (transport failure, 5xx, 429/418,
  malformed body), a CREATE's outcome is still ambiguous inside its
  resolution window, or an acknowledged CREATE has not propagated to the open
  list yet. The position is preserved, the failure is recorded durably, the
  next cycle retries, and the operator is alerted once the uncertainty has
  lasted ``UNKNOWN_ALERT_CYCLES`` consecutive cycles. No entry is evaluated
  for the account while any of its positions is in this state.

The operator emergency flatten never consults this module: it closes through
``production_close.close_position`` directly.
"""
from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

CONFIRMED = "CONFIRMED"
ABSENT = "ABSENT"
UNKNOWN = "UNKNOWN"

#: Reason codes that PROVE protection is not in place (fail-safe close applies).
ABSENT_CODES = frozenset({
    "PROTECTION_CONFIRMED_ABSENT",
    "PROTECTION_LEG_ATTEMPTS_EXHAUSTED",
    "PROTECTION_READ_BACK_GEOMETRY_MISMATCH",
    "PROTECTION_VENUE_REFUSED",
})
#: Reason codes that leave the state unknown (position preserved, retried).
UNKNOWN_CODES = frozenset({
    "PROTECTION_READ_UNAVAILABLE",
    "PROTECTION_READ_BACK_UNAVAILABLE",
    "PROTECTION_READ_BACK_UNCONFIRMED",
    "PROTECTION_SUBMIT_OUTCOME_UNKNOWN",
    "NATIVE_PROTECTION_READ_UNAVAILABLE",
    "ALGO_ORDERS_RESPONSE_MALFORMED",
})
#: Raised by the maintenance path while a position's protection is unknown.
STATE_UNKNOWN = "PROTECTION_STATE_UNKNOWN"
#: Consecutive maintenance cycles of unknown protection before the operator is alerted.
UNKNOWN_ALERT_CYCLES = 4
#: Alert type recorded on the existing alert store once the bound is reached.
UNCERTAINTY_ALERT = "PROTECTION_STATE_UNKNOWN_OPERATOR_REVIEW"

TABLE = "cati_production_protection_uncertainty"
_SCHEMA = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
    account_id TEXT NOT NULL, symbol TEXT NOT NULL, trade_plan_id TEXT,
    reason TEXT NOT NULL, first_seen_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL,
    cycles INTEGER NOT NULL DEFAULT 1, alerted INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(account_id, symbol))"""


def reason_code(exc) -> str:
    """The stable reason code of ``exc`` (never its possibly signed text)."""
    code = getattr(exc, "reason_code", None)
    if not code and isinstance(exc, ValueError):
        text = str(exc)
        if text and text.replace("_", "").isalnum() and text.upper() == text:
            code = text
    return code or type(exc).__name__


def failure_state(exc) -> str:
    """``ABSENT`` or ``UNKNOWN`` for an exception raised by the protection step.

    Only positive venue evidence yields ``ABSENT``. A definitive 4xx refusal of
    a protection CREATE counts: the venue answered, no leg exists and it will
    not create one (e.g. the trigger price is already crossed). Everything the
    venue did not answer -- timeouts, resets, 5xx, rate limits, unparseable
    bodies and any unlisted exception -- is ``UNKNOWN``.
    """
    code = reason_code(exc)
    if code in ABSENT_CODES:
        return ABSENT
    if code in UNKNOWN_CODES:
        return UNKNOWN
    try:
        from app.execution.fill_resolution import TRANSIENT_NOT_PROCESSED_CODES, definitive_rejection
        venue_code = definitive_rejection(exc)
        # A refusal ABOUT THE REQUEST (clock skew, rate) is not about the leg:
        # the identical CREATE may be accepted a moment later, so it is retried,
        # never closed on.
        if venue_code is not None and venue_code not in TRANSIENT_NOT_PROCESSED_CODES:
            return ABSENT
    except Exception:  # classification must never raise
        pass
    return UNKNOWN


def initialize(conn) -> None:
    conn.execute(_SCHEMA)


def _now_ms(now_ms):
    return int(time.time() * 1000) if now_ms is None else int(now_ms)


def record_unknown(db, account_id: str, symbol: str, trade_plan_id, reason: str, now_ms=None) -> dict:
    """Durably note one more cycle of unknown protection for ``symbol``.

    Returns the ledger row. Once ``UNKNOWN_ALERT_CYCLES`` consecutive cycles
    are reached a CRITICAL operator alert is recorded exactly once per
    uncertainty episode (the alert store de-duplicates while unacknowledged).
    """
    now = _now_ms(now_ms)
    with db.connect() as c:
        initialize(c)
        row = c.execute(f"SELECT * FROM {TABLE} WHERE account_id=? AND symbol=?", (account_id, symbol)).fetchone()
        if row is None:
            c.execute(f"INSERT INTO {TABLE} VALUES(?,?,?,?,?,?,1,0)",
                      (account_id, symbol, trade_plan_id, reason, now, now))
        else:
            c.execute(f"UPDATE {TABLE} SET reason=?, last_seen_at=?, cycles=cycles+1, "
                      "trade_plan_id=COALESCE(?,trade_plan_id) WHERE account_id=? AND symbol=?",
                      (reason, now, trade_plan_id, account_id, symbol))
        row = dict(c.execute(f"SELECT * FROM {TABLE} WHERE account_id=? AND symbol=?", (account_id, symbol)).fetchone())
    if row["cycles"] >= UNKNOWN_ALERT_CYCLES and not row["alerted"]:
        _alert(db, row)
        with db.connect() as c:
            c.execute(f"UPDATE {TABLE} SET alerted=1 WHERE account_id=? AND symbol=?", (account_id, symbol))
        row["alerted"] = 1
    return row


def _alert(db, row: dict) -> None:
    """Record the operator alert on the existing store. Never raises."""
    logger.critical("[%s] account=%s symbol=%s protection unknown for %s cycles (%s)", UNCERTAINTY_ALERT,
                    row["account_id"], row["symbol"], row["cycles"], row["reason"])
    try:
        from app.ops.multi_asset_alerts import MultiAssetAlert, emit
        emit(db, [MultiAssetAlert(UNCERTAINTY_ALERT, "CRITICAL", UNCERTAINTY_ALERT,
                                  f"protection-unknown:{row['account_id']}:{row['symbol']}",
                                  f"{row['symbol']}: exchange-side protection could not be verified for "
                                  f"{row['cycles']} cycles; position preserved, operator review required",
                                  broker_account_id=row["account_id"], symbol=row["symbol"],
                                  details={k: row[k] for k in ("reason", "first_seen_at", "last_seen_at", "cycles",
                                                                "trade_plan_id")})])
    except Exception:
        logger.exception("[%s] alert could not be recorded", UNCERTAINTY_ALERT)


def clear(db, account_id: str, symbol: str) -> bool:
    """Forget the uncertainty of ``symbol`` (protection confirmed, or the
    position is confirmed flat / closed by the fail-safe)."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
            return False
        return c.execute(f"DELETE FROM {TABLE} WHERE account_id=? AND symbol=?", (account_id, symbol)).rowcount > 0


def uncertain_positions(db, account_id: str | None = None) -> list[dict]:
    """Every position whose protection is currently unknown (for monitoring
    and the user-facing read model). Local read only."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
            return []
        sql, args = f"SELECT * FROM {TABLE}", ()
        if account_id is not None:
            sql, args = sql + " WHERE account_id=?", (account_id,)
        return [dict(r) for r in c.execute(sql + " ORDER BY first_seen_at", args)]


def describe(exc) -> dict:
    """The ``protection`` document recorded for a failed protection step."""
    return {"status": "UNCONFIRMED", "state": failure_state(exc), "reason": reason_code(exc)}


__all__ = ["CONFIRMED", "ABSENT", "UNKNOWN", "ABSENT_CODES", "UNKNOWN_CODES", "STATE_UNKNOWN", "UNKNOWN_ALERT_CYCLES",
           "UNCERTAINTY_ALERT", "TABLE", "failure_state", "reason_code", "initialize", "record_unknown", "clear",
           "uncertain_positions", "describe"]
