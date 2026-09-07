"""TradingView external signal queue processor.

The webhook layer only authenticates, validates, stores, and queues alerts.
This processor is called from the runner after a normal cycle and delegates
BUY/SELL candidates back into the runner-owned protected execution path.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from app.core.config import settings
from shared_lib.persistence.db import DB
from shared_lib.persistence.tradingview import (
    QUEUE_STATUS_EXPIRED,
    QUEUE_STATUS_FAILED,
    QUEUE_STATUS_PROCESSED,
    QUEUE_STATUS_REJECTED,
    SUPPORTED_STAGE1_ACTIONS,
    get_tradingview_safety_lockout,
    mark_queue_status,
    set_tradingview_safety_lockout,
    update_signal_decision_processing_result,
    utc_now_iso,
)

if TYPE_CHECKING:
    from app.runner.runner import PaperRunner

logger = logging.getLogger(__name__)

_SAFE_ENVS = frozenset({"testnet", "demo", "paper", "sandbox"})
_LIVE_ENVS = frozenset({"live", "mainnet", "production", "prod"})
STALE_CLAIM_TIMEOUT_MINUTES = 10


def _bool_setting(name: str, default: bool = False) -> bool:
    """Read bool settings safely when tests patch settings with MagicMock."""
    value = getattr(settings, name, default)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if type(value).__module__.startswith("unittest.mock"):
        return default
    return bool(value)


def _csv_setting(name: str, default: str = "") -> set[str]:
    raw = getattr(settings, name, default)
    if raw is None or type(raw).__module__.startswith("unittest.mock"):
        raw = default
    if isinstance(raw, (list, tuple, set)):
        return {str(v).strip().upper() for v in raw if str(v).strip()}
    return {p.strip().upper() for p in str(raw or "").split(",") if p.strip()}


def _int_setting(name: str, default: int) -> int:
    raw = getattr(settings, name, default)
    if raw is None or type(raw).__module__.startswith("unittest.mock"):
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _float_setting(name: str, default: float) -> float:
    raw = getattr(settings, name, default)
    if raw is None or type(raw).__module__.startswith("unittest.mock"):
        return default
    try:
        return float(raw)
    except Exception:
        return default


class ExternalSignalProcessor:
    """Processes durable external_signal_queue rows for one bot."""

    def __init__(self, db: DB) -> None:
        self._db = db

    def _env_gate_reason(self) -> str | None:
        if not _bool_setting("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False):
            return "TRADINGVIEW_EXTERNAL_SIGNALS_DISABLED"
        if not _bool_setting("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED", False):
            return "TRADINGVIEW_LIVE_MODE_LIMITED_DISABLED"
        if _bool_setting("TRADINGVIEW_SAFETY_LOCKOUT_ENABLED", False):
            return (
                "TRADINGVIEW_SAFETY_LOCKOUT:"
                + str(getattr(settings, "TRADINGVIEW_SAFETY_LOCKOUT_REASON", "") or "configured lockout active")
            )

        env = (getattr(settings, "BINANCE_ENV", "") or "").lower()
        if env in _SAFE_ENVS:
            return None

        if env in _LIVE_ENVS:
            if _bool_setting("TRADINGVIEW_TESTNET_ONLY", True):
                return (
                    f"TESTNET_ONLY_GATE: BINANCE_ENV={env!r} is a live environment. "
                    "Set TRADINGVIEW_TESTNET_ONLY=false, "
                    "TRADINGVIEW_LIVE_MODE_PROOF_ENABLED=true, and "
                    "TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED=true for live-mode proof runs."
                )

            new_live_ack = (
                _bool_setting("TRADINGVIEW_LIVE_MODE_PROOF_ENABLED", False)
                and _bool_setting("TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED", False)
            )
            legacy_live_ack = (
                _bool_setting("TRADINGVIEW_ALLOW_PAPER_LIVE_MODE", False)
                and _bool_setting("PAPER_TRADING_MODE", False)
            )
            if not (new_live_ack or legacy_live_ack):
                return (
                    f"LIVE_ENV_BLOCKED: BINANCE_ENV={env!r} requires "
                    "TRADINGVIEW_LIVE_MODE_PROOF_ENABLED=true and "
                    "TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED=true"
                )
            return None

        return (
            f"UNKNOWN_ENV_BLOCKED: BINANCE_ENV={env!r} is not recognised. "
            f"Safe envs={sorted(_SAFE_ENVS)} live envs={sorted(_LIVE_ENVS)}"
        )

    def _active_lockout_reason(self, bot_id: str) -> str | None:
        row = get_tradingview_safety_lockout(self._db, bot_id)
        if row and int(row.get("is_locked") or 0):
            return str(row.get("reason") or "TradingView safety lockout active")
        return None

    def _phase6_config(self) -> dict[str, Any]:
        return {
            "allowed_actions": _csv_setting("TRADINGVIEW_ALLOWED_ACTIONS", "BUY,SELL"),
            "allowed_symbols": _csv_setting("TRADINGVIEW_ALLOWED_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT"),
            "max_signals_per_hour": _int_setting("TRADINGVIEW_MAX_SIGNALS_PER_HOUR", 5),
            "max_signals_per_day": _int_setting("TRADINGVIEW_MAX_SIGNALS_PER_DAY", 20),
            "max_executions_per_day": _int_setting("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY", 3),
            "max_trade_usdt_cap": _float_setting("TRADINGVIEW_MAX_TRADE_USDT_CAP", 150.0),
            "require_sltp": _bool_setting("TRADINGVIEW_REQUIRE_SLTP_PROTECTION", True),
            "auto_disable": _bool_setting("TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL", True),
            "allow_close": _bool_setting("TRADINGVIEW_ALLOW_CLOSE", False),
            "allow_reverse": _bool_setting("TRADINGVIEW_ALLOW_REVERSE", False),
            "allow_reduce": _bool_setting("TRADINGVIEW_ALLOW_REDUCE", False),
            "allow_cancel": _bool_setting("TRADINGVIEW_ALLOW_CANCEL", False),
            "allow_external_sltp": _bool_setting("TRADINGVIEW_ALLOW_EXTERNAL_SLTP", False),
            "allow_external_size": _bool_setting("TRADINGVIEW_ALLOW_EXTERNAL_SIZE", False),
            "allow_risk_override": _bool_setting("TRADINGVIEW_ALLOW_RISK_OVERRIDE", False),
        }

    def _count_since(self, bot_id: str, table: str, where: str, since_iso: str) -> int:
        with self._db.connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS c FROM {table} WHERE bot_id = ? AND created_at >= ? AND {where}",
                (bot_id, since_iso),
            ).fetchone()
        return int(row["c"] if row else 0)

    def _phase6_counter_since(self, bot_id: str, since_iso: str) -> str:
        """Apply an audited proof reset watermark without mutating historical rows."""
        with self._db.connect() as conn:
            try:
                row = conn.execute(
                    """
                    SELECT reset_at
                    FROM tradingview_phase6_rate_limit_resets
                    WHERE bot_instance_id = ?
                    ORDER BY reset_at DESC
                    LIMIT 1
                    """,
                    (bot_id,),
                ).fetchone()
            except Exception:
                return since_iso
        reset_at = str(row["reset_at"]) if row and row["reset_at"] else None
        if reset_at and reset_at > since_iso:
            return reset_at
        return since_iso

    def _unprotected_active_count(self, bot_id: str) -> int:
        with self._db.connect() as conn:
            try:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM position_lifecycle_state
                    WHERE bot_instance_id = ?
                      AND COALESCE(exchange_position_active, 0) = 1
                      AND (
                        sl_order_id IS NULL OR tp_order_id IS NULL
                        OR sl_order_id LIKE '%DUPLICATE_4130%'
                        OR tp_order_id LIKE '%DUPLICATE_4130%'
                      )
                    """,
                    (bot_id,),
                ).fetchone()
                return int(row["c"] if row else 0)
            except Exception:
                return 0

    def _phase6_gate_for_row(self, bot_id: str, row: dict[str, Any]) -> tuple[bool, str, str]:
        cfg = self._phase6_config()
        symbol = str(row.get("symbol") or "").upper()
        action = str(row.get("action") or "").upper()

        lockout_reason = self._active_lockout_reason(bot_id)
        if lockout_reason:
            return False, "REJECTED_TV_SAFETY_LOCKOUT", lockout_reason
        if self._unprotected_active_count(bot_id) > 0:
            reason = "Unprotected active position detected before TradingView processing"
            if cfg["auto_disable"]:
                set_tradingview_safety_lockout(self._db, bot_instance_id=bot_id, reason=reason)
            return False, "REJECTED_TV_SAFETY_LOCKOUT", reason

        forbidden_enabled = {
            "CLOSE": cfg["allow_close"],
            "REVERSE": cfg["allow_reverse"],
            "REDUCE": cfg["allow_reduce"],
            "CANCEL": cfg["allow_cancel"],
        }
        if action in forbidden_enabled and not forbidden_enabled[action]:
            return False, "REJECTED_TV_ACTION_NOT_ALLOWED", f"{action} is disabled in Phase 6 limited mode"
        if action not in cfg["allowed_actions"] or action not in {"BUY", "SELL"}:
            return False, "REJECTED_TV_ACTION_NOT_ALLOWED", f"{action} is not in TRADINGVIEW_ALLOWED_ACTIONS"
        if cfg["allowed_symbols"] and symbol not in cfg["allowed_symbols"]:
            return False, "REJECTED_TV_SYMBOL_NOT_ALLOWED", f"{symbol} is not in TRADINGVIEW_ALLOWED_SYMBOLS"
        if not cfg["require_sltp"]:
            return False, "REJECTED_TV_LIMITED_MODE_DISABLED", "TRADINGVIEW_REQUIRE_SLTP_PROTECTION must remain true"
        if cfg["allow_external_sltp"] or cfg["allow_external_size"] or cfg["allow_risk_override"]:
            return False, "REJECTED_TV_LIMITED_MODE_DISABLED", "External SL/TP, sizing, and risk override must remain disabled"

        now = datetime.now(timezone.utc)
        hour_ago = self._phase6_counter_since(bot_id, (now - timedelta(hours=1)).isoformat())
        day_ago = self._phase6_counter_since(bot_id, (now - timedelta(days=1)).isoformat())
        hour_count = self._count_since(bot_id, "external_signal_queue", "source = 'TRADINGVIEW'", hour_ago)
        day_count = self._count_since(bot_id, "external_signal_queue", "source = 'TRADINGVIEW'", day_ago)
        exec_count = self._count_since(
            bot_id,
            "tradingview_signal_decisions",
            "final_status IN ('PROCESSED_EXECUTED','PROCESSED_EXECUTED_PROTECTED')",
            day_ago,
        )
        if hour_count > cfg["max_signals_per_hour"]:
            return False, "REJECTED_TV_RATE_LIMIT", "Hourly TradingView signal limit exceeded"
        if day_count > cfg["max_signals_per_day"]:
            return False, "REJECTED_TV_RATE_LIMIT", "Daily TradingView signal limit exceeded"
        if exec_count >= cfg["max_executions_per_day"]:
            return False, "REJECTED_TV_DAILY_EXECUTION_CAP", "Daily TradingView execution cap reached"

        return True, "PASS", "Phase 6 limited gate passed"

    def _activate_lockout_if_critical(self, bot_id: str, final_status: str, final_reason: str) -> None:
        if not _bool_setting("TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL", True):
            return
        critical = {
            "FAILED_SLTP_PROTECTION",
            "FAILED_EXECUTION",
            "UNSAFE_TRADINGVIEW_INVARIANT",
        }
        if final_status in critical and (
            "SL" in final_reason.upper()
            or "PROTECTION" in final_reason.upper()
            or "UNPROTECTED" in final_reason.upper()
        ):
            set_tradingview_safety_lockout(
                self._db,
                bot_instance_id=bot_id,
                reason=f"{final_status}: {final_reason}",
            )

    def recover_stale_claimed(
        self,
        bot_id: str,
        timeout_minutes: int = STALE_CLAIM_TIMEOUT_MINUTES,
    ) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        ).isoformat()
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id FROM external_signal_queue
                WHERE bot_id = ? AND status = 'CLAIMED' AND claimed_at < ?
                """,
                (bot_id, cutoff),
            ).fetchall()
            stale_ids = [r["id"] for r in rows]

        if not stale_ids:
            return 0

        now = utc_now_iso()
        result_json = json.dumps(
            {
                "reason": "PROCESSOR_STALE_CLAIM_TIMEOUT",
                "timeout_minutes": timeout_minutes,
            }
        )
        final_reason = (
            f"Stale CLAIMED row automatically failed after "
            f"{timeout_minutes}-minute processor timeout"
        )
        with self._db.connect() as conn:
            for qid in stale_ids:
                conn.execute(
                    """
                    UPDATE external_signal_queue
                    SET status = 'FAILED', processed_at = ?, result = ?
                    WHERE id = ? AND status = 'CLAIMED'
                    """,
                    (now, result_json, qid),
                )
                conn.execute(
                    """
                    UPDATE tradingview_signal_decisions
                    SET final_status = 'FAILED',
                        final_reason = ?,
                        execution_result = 'FAILED:PROCESSOR_STALE_CLAIM_TIMEOUT'
                    WHERE queue_id = ?
                    """,
                    (final_reason, qid),
                )
        logger.warning(
            "[ExtSig] bot=%s recovered %d stale CLAIMED row(s)",
            bot_id,
            len(stale_ids),
        )
        return len(stale_ids)

    def _fetch_pending(self, bot_id: str, max_rows: int) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, symbol, side, action, confidence,
                       expires_at, available_at, created_at, result
                FROM external_signal_queue
                WHERE bot_id = ? AND status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (bot_id, max_rows),
            ).fetchall()
        return [dict(r) for r in rows]

    def _atomic_claim(self, queue_id: str, now: str) -> bool:
        with self._db.connect() as conn:
            changed = conn.execute(
                """
                UPDATE external_signal_queue
                SET status = 'CLAIMED', claimed_at = ?
                WHERE id = ? AND status = 'PENDING'
                """,
                (now, queue_id),
            ).rowcount
        return changed == 1

    def _write_rejected(
        self,
        queue_id: str,
        *,
        event_filter: str | None,
        policy: str | None,
        sizing: str | None,
        execution: str | None,
        final_status: str,
        final_reason: str,
    ) -> None:
        mark_queue_status(
            self._db,
            queue_id=queue_id,
            status=QUEUE_STATUS_REJECTED,
            result={"final_status": final_status, "reason": final_reason},
        )
        update_signal_decision_processing_result(
            self._db,
            queue_id=queue_id,
            event_filter_result=event_filter,
            policy_result=policy,
            sizing_result=sizing,
            execution_result=execution,
            final_status=final_status,
            final_reason=final_reason,
        )

    def _persist_runner_result(
        self,
        queue_id: str,
        *,
        runner_result: dict[str, Any],
        event_filter_result: str,
    ) -> None:
        final_status = str(runner_result.get("final_status") or "FAILED_EXECUTION")
        final_reason = str(
            runner_result.get("final_reason")
            or runner_result.get("reason")
            or final_status
        )
        queue_status = str(runner_result.get("queue_status") or "")
        if queue_status not in {
            QUEUE_STATUS_PROCESSED,
            QUEUE_STATUS_REJECTED,
            QUEUE_STATUS_FAILED,
            QUEUE_STATUS_EXPIRED,
        }:
            if final_status.startswith("PROCESSED") or final_status in {
                "EXECUTED",
                "EXECUTED_PAPER",
            }:
                queue_status = QUEUE_STATUS_PROCESSED
            elif final_status.startswith("FAILED"):
                queue_status = QUEUE_STATUS_FAILED
            else:
                queue_status = QUEUE_STATUS_REJECTED

        mark_queue_status(
            self._db,
            queue_id=queue_id,
            status=queue_status,
            result={
                "final_status": final_status,
                "reason": final_reason,
                "runner_result": runner_result,
            },
        )
        update_signal_decision_processing_result(
            self._db,
            queue_id=queue_id,
            event_filter_result=runner_result.get("event_filter_result")
            or event_filter_result,
            policy_result=runner_result.get("policy_result"),
            sizing_result=runner_result.get("sizing_result"),
            execution_result=runner_result.get("execution_result"),
            decision_trace_id=runner_result.get("decision_trace_id")
            or runner_result.get("trace_id"),
            final_status=final_status,
            final_reason=final_reason,
        )

    def process_pending_for_bot(
        self,
        bot_instance_id: str,
        runner: "PaperRunner",
        max_per_cycle: int | None = None,
    ) -> list[dict[str, Any]]:
        gate_reason = self._env_gate_reason()
        if gate_reason:
            logger.debug("[ExtSig] env gate blocked: %s", gate_reason)
            return []

        if max_per_cycle is None:
            max_per_cycle = min(
                int(getattr(settings, "TRADINGVIEW_QUEUE_MAX_PER_CYCLE", 3)),
                _int_setting("TRADINGVIEW_MAX_QUEUE_PER_CYCLE", 1),
            )

        try:
            self.recover_stale_claimed(bot_instance_id)
        except Exception as exc:
            logger.warning("[ExtSig] stale-CLAIMED recovery failed: %s", exc)

        pending = self._fetch_pending(bot_instance_id, max_per_cycle)
        if not pending:
            return []

        now = utc_now_iso()
        results: list[dict[str, Any]] = []

        for row in pending:
            queue_id = str(row["id"])
            symbol = str(row["symbol"] or "").upper()
            action = str(row["action"] or "").upper()
            expires_at = str(row.get("expires_at") or "")
            result_entry: dict[str, Any] = {
                "queue_id": queue_id,
                "symbol": symbol,
                "action": action,
            }

            if expires_at and expires_at < now:
                mark_queue_status(
                    self._db,
                    queue_id=queue_id,
                    status=QUEUE_STATUS_EXPIRED,
                    result={"reason": "EXPIRED_BEFORE_PROCESSING", "expired_at": expires_at},
                )
                update_signal_decision_processing_result(
                    self._db,
                    queue_id=queue_id,
                    event_filter_result=None,
                    policy_result=None,
                    sizing_result=None,
                    execution_result=None,
                    final_status="EXPIRED",
                    final_reason=f"Signal expired at {expires_at} before runner processing",
                )
                result_entry["outcome"] = "EXPIRED"
                results.append(result_entry)
                continue

            if action not in SUPPORTED_STAGE1_ACTIONS:
                self._write_rejected(
                    queue_id,
                    event_filter=None,
                    policy=None,
                    sizing=None,
                    execution=None,
                    final_status="REJECTED_TV_ACTION_NOT_ALLOWED",
                    final_reason=(
                        f"Action {action!r} is not supported for execution. "
                        f"Supported: {sorted(SUPPORTED_STAGE1_ACTIONS)}"
                    ),
                )
                result_entry["outcome"] = "REJECTED_UNSUPPORTED_ACTION"
                results.append(result_entry)
                continue

            gate_ok, gate_status, gate_reason = self._phase6_gate_for_row(bot_instance_id, row)
            if not gate_ok:
                self._write_rejected(
                    queue_id,
                    event_filter=None,
                    policy=None,
                    sizing=None,
                    execution="NOT_CALLED:PHASE6_LIMITED_GATE",
                    final_status=gate_status,
                    final_reason=gate_reason,
                )
                result_entry["outcome"] = gate_status
                results.append(result_entry)
                continue

            if not self._atomic_claim(queue_id, now):
                result_entry["outcome"] = "SKIPPED_ALREADY_CLAIMED"
                results.append(result_entry)
                continue

            blackout = runner.event_blackout_filter.check(symbol)
            if blackout.is_blocked:
                self._write_rejected(
                    queue_id,
                    event_filter=f"BLOCKED:{blackout.reason}",
                    policy=None,
                    sizing=None,
                    execution=None,
                    final_status="REJECTED_EVENT_BLACKOUT",
                    final_reason=blackout.reason or "EVENT_BLACKOUT",
                )
                result_entry["outcome"] = "REJECTED_EVENT_BLACKOUT"
                results.append(result_entry)
                continue

            event_filter_result = "PASS"
            if not hasattr(runner, "process_external_signal_candidate"):
                self._write_rejected(
                    queue_id,
                    event_filter=event_filter_result,
                    policy=None,
                    sizing=None,
                    execution=None,
                    final_status="FAILED_RUNNER_ADAPTER_MISSING",
                    final_reason="Runner does not expose process_external_signal_candidate",
                )
                result_entry["outcome"] = "FAILED_RUNNER_ADAPTER_MISSING"
                results.append(result_entry)
                continue

            try:
                proof_context: dict[str, Any] = {}
                try:
                    proof_context = json.loads(str(row.get("result") or "{}"))
                    if not isinstance(proof_context, dict):
                        proof_context = {}
                except Exception:
                    proof_context = {}
                runner_result = runner.process_external_signal_candidate(
                    {
                        "source": "TRADINGVIEW",
                        "queue_id": queue_id,
                        "bot_id": bot_instance_id,
                        "symbol": symbol,
                        "side": row.get("side"),
                        "action": action,
                        "confidence": row.get("confidence"),
                        "proof_context": proof_context,
                    }
                )
            except Exception as exc:
                logger.exception(
                    "[ExtSig] runner adapter raised for %s queue=%s: %s",
                    symbol,
                    queue_id[:8],
                    exc,
                )
                runner_result = {
                    "queue_status": QUEUE_STATUS_FAILED,
                    "final_status": "FAILED_EXECUTION",
                    "final_reason": f"Runner adapter raised {type(exc).__name__}: {exc}",
                    "execution_result": f"EXCEPTION:{type(exc).__name__}:{exc}",
                }

            if not isinstance(runner_result, dict):
                runner_result = {
                    "queue_status": QUEUE_STATUS_FAILED,
                    "final_status": "FAILED_EXECUTION",
                    "final_reason": "Runner adapter returned a non-dict result",
                    "execution_result": "FAILED:INVALID_RUNNER_RESULT",
                }

            self._persist_runner_result(
                queue_id,
                runner_result=runner_result,
                event_filter_result=event_filter_result,
            )
            self._activate_lockout_if_critical(
                bot_instance_id,
                str(runner_result.get("final_status") or ""),
                str(runner_result.get("final_reason") or runner_result.get("reason") or ""),
            )
            result_entry.update(runner_result)
            result_entry["outcome"] = (
                runner_result.get("final_status") or runner_result.get("outcome")
            )
            results.append(result_entry)

        return results
