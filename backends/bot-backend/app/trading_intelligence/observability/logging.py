"""Structured, sanitized CATI stage logs + latency + failure evidence
(Sections 21.16, 21.21, 21.22).

``log_stage`` emits ONE ``[CATI_STAGE] {json}`` line per major stage with
runtime_session_id / bot_run_id / cycle_id / component / status /
duration_ms / reason codes, plus OPAQUE tenant ids where the stage is
tenant-specific. The payload passes through ``sanitize_payload`` -- API
keys, secrets, Authorization headers, JWTs and passwords can never be
logged, even if a caller puts one in ``extra``.

``timed_stage`` measures latency into ``cati_stage_latency_ms{stage}``,
logs the stage, and turns any exception into structured
``CATI_COMPONENT_ERROR`` evidence (the canonical record in
``integration/errors``, plus the ``cati_component_errors`` row when a db is
given) and RE-RAISES: callers fail closed for new entries; nothing is
silently swallowed.
"""
from __future__ import annotations

import contextlib
import json
import logging
import time
from typing import Any, Dict, Iterable, Iterator, Optional

from app.trading_intelligence.observability.metrics import METRICS
from app.trading_intelligence.observability.sanitize import sanitize_payload
from app.trading_intelligence.versions import STAGE_LOG_SCHEMA_VERSION

logger = logging.getLogger("app.trading_intelligence.stage")

#: Every stage whose latency is recorded (Section 21.22).
STAGES = ("MARKET_STATE", "REGIME", "SETUP_DISCOVERY", "FORECAST", "VENUE_ECONOMICS", "VETO", "RANKING", "PORTFOLIO",
          "TRADE_PLAN", "POSITION_FORECAST", "EXIT_DECISION", "RISK", "EXECUTION")


def stage_payload(*, component: str, status: str, duration_ms: Optional[float] = None,
                  reason_codes: Iterable[str] = (), runtime_session_id: Optional[str] = None,
                  bot_run_id: Optional[str] = None, cycle_id: Optional[str] = None, user_id: Optional[str] = None,
                  broker_account_id: Optional[str] = None, bot_instance_id: Optional[str] = None,
                  extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {
        "schema_version": STAGE_LOG_SCHEMA_VERSION, "component": component, "status": status,
        "duration_ms": None if duration_ms is None else round(float(duration_ms), 3),
        "reason_codes": list(reason_codes or ()), "runtime_session_id": runtime_session_id, "bot_run_id": bot_run_id,
        "cycle_id": cycle_id, "user_id": user_id, "broker_account_id": broker_account_id,
        "bot_instance_id": bot_instance_id,
    }
    if extra:
        payload["extra"] = dict(extra)
    return sanitize_payload(payload)


def log_stage(**kwargs: Any) -> Dict[str, Any]:
    """Emit one structured stage line. Never raises."""
    try:
        payload = stage_payload(**kwargs)
        logger.info("[CATI_STAGE] %s", json.dumps(payload, sort_keys=True, default=str))
        return payload
    except Exception:  # pragma: no cover - observability must never break the pipeline
        return {}


def record_stage_error(component: str, stage: str, exc: BaseException, *, db: Any = None, cycle_id=None,
                       user_id=None, broker_account_id=None, bot_instance_id=None, symbol=None):
    """CATI_COMPONENT_ERROR evidence: canonical structured log/ring record,
    plus an append-only ``cati_component_errors`` row when ``db`` is given."""
    from app.trading_intelligence.integration.errors import record_component_error

    rec = record_component_error(component, exc, cycle_id=cycle_id, bot_instance_id=bot_instance_id,
                                 broker_account_id=broker_account_id, symbol=symbol, stage=stage, user_id=user_id)
    METRICS.inc("cati_component_errors_total", stage=stage, outcome=type(exc).__name__[:40])
    if db is not None:
        try:
            from app.trading_intelligence.evidence.stores import ComponentErrorStore

            ComponentErrorStore(db).append(rec)
        except Exception:
            pass
    return rec


@contextlib.contextmanager
def timed_stage(stage: str, component: str, *, db: Any = None, **ids: Any) -> Iterator[Dict[str, Any]]:
    """``with timed_stage("RISK", "boundary.process_trade_plan", cycle_id=...) as st:``
    -- set ``st["status"]`` / ``st["reason_codes"]`` inside the block."""
    state: Dict[str, Any] = {"status": "OK", "reason_codes": ()}
    t0 = time.perf_counter()
    try:
        yield state
    except Exception as exc:
        ms = (time.perf_counter() - t0) * 1000.0
        METRICS.observe("cati_stage_latency_ms", ms, stage=stage)
        record_stage_error(component, stage, exc, db=db, cycle_id=ids.get("cycle_id"), user_id=ids.get("user_id"),
                           broker_account_id=ids.get("broker_account_id"),
                           bot_instance_id=ids.get("bot_instance_id"), symbol=ids.get("symbol"))
        log_stage(component=component, status="CATI_COMPONENT_ERROR", duration_ms=ms,
                  reason_codes=("CATI_COMPONENT_ERROR",), **{k: v for k, v in ids.items() if k != "symbol"})
        raise
    ms = (time.perf_counter() - t0) * 1000.0
    METRICS.observe("cati_stage_latency_ms", ms, stage=stage)
    log_stage(component=component, status=str(state.get("status")), duration_ms=ms,
              reason_codes=tuple(state.get("reason_codes") or ()), **{k: v for k, v in ids.items() if k != "symbol"})


__all__ = ["STAGES", "stage_payload", "log_stage", "record_stage_error", "timed_stage"]
