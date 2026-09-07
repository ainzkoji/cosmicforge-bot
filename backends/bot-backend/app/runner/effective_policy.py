"""Canonical, immutable per-bot runtime policy resolution.

The resolver is the only place where BotInstance, operator settings, risk-profile
requests and SystemLimits are merged.  Downstream runtime objects consume the
resolved values and must not reinterpret the same setting.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Mapping, Tuple

from app.core.config import settings
from app.risk.system_limits import SystemLimits

logger = logging.getLogger(__name__)

POLICY_VERSION = "effective-bot-policy-v1"

# Timeframes the strategy/data layer can actually fetch and resample.
SUPPORTED_TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d")


class EffectivePolicyError(ValueError):
    """Raised when a bot cannot be resolved without inventing configuration."""

    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


def normalize_execution_mode(value: str | None) -> str:
    """Return the canonical execution dimension: ``paper`` or ``broker``."""
    mode = str(value or "paper").strip().lower()
    if mode in {"paper", "sim", "simulation"}:
        return "paper"
    if mode in {"live", "broker", "testnet", "demo"}:
        return "broker"
    raise EffectivePolicyError("INVALID_EXECUTION_MODE", f"Unsupported execution mode: {value!r}")


def _symbols(value: Any) -> Tuple[str, ...]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            value = parsed if isinstance(parsed, list) else value.split(",")
        except Exception:
            value = value.split(",")
    result = tuple(dict.fromkeys(str(v).strip().upper() for v in (value or []) if str(v).strip()))
    if not result:
        raise EffectivePolicyError("MISSING_SYMBOLS", "At least one configured symbol is required")
    return result


def _clamp(
    name: str,
    requested: float,
    ceiling: float,
    warnings: list[str],
    clamps: list[dict[str, Any]] | None = None,
    reason: str = "SYSTEM_LIMIT_CEILING",
) -> float:
    """Clamp ``requested`` to ``ceiling`` and record the fact that it happened.

    Clamping is never silent: every reduction produces both a human-readable
    warning and a structured record carrying requested/effective/ceiling/reason.
    """
    effective = min(float(requested), float(ceiling))
    if effective != float(requested):
        warnings.append(f"{name}: requested={requested} effective={effective} ceiling={ceiling}")
        if clamps is not None:
            clamps.append(
                {
                    "setting": name,
                    "requested_value": float(requested),
                    "effective_value": float(effective),
                    "hard_ceiling": float(ceiling),
                    "clamp_reason": reason,
                }
            )
    return effective


@dataclass(frozen=True)
class EffectiveBotPolicy:
    bot_instance_id: str
    user_id: str
    broker_account_id: str
    market_type: str

    execution_mode: str
    broker_environment: str
    strategy_id: str
    strategy_version: str
    symbols: Tuple[str, ...]
    timeframe: str
    higher_timeframe: str
    execution_monitor_interval: int

    capital_budget: float
    capital_allocation_type: str
    position_allocation_type: str
    position_allocation_value: float

    risk_level: str
    requested_risk_per_trade: float
    risk_per_trade: float
    risk_per_trade_ceiling: float
    max_daily_loss: float
    max_weekly_drawdown: float
    max_monthly_drawdown: float
    requested_max_daily_trades: int
    max_daily_trades: int
    requested_max_open_positions: int
    max_open_positions: int
    requested_max_leverage: float
    max_leverage: float
    max_leverage_ceiling: float
    min_risk_reward: float
    minimum_notional: float
    stop_loss_fraction: float
    min_stop_loss_fraction: float
    max_stop_loss_fraction: float
    consecutive_loss_soft: int
    consecutive_loss_hard: int

    confidence_absolute_floor: float
    regime_blocked: Tuple[str, ...]
    session_enabled: bool
    session_windows_utc: str
    volatility_filter_enabled: bool

    ml_mode: str
    iofs_mode: str
    event_filter_enabled: bool
    news_mode: str
    external_signal_mode: str

    policy_version: str = POLICY_VERSION
    policy_hash: str = ""
    resolved_at: str = ""
    clamp_warnings: Tuple[str, ...] = field(default_factory=tuple)
    clamps: Tuple[Mapping[str, Any], ...] = field(default_factory=tuple)

    def runtime_payload(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("resolved_at", None)
        data.pop("policy_hash", None)
        data.pop("clamp_warnings", None)
        data.pop("clamps", None)
        return data

    def to_public_dict(self) -> dict[str, Any]:
        data = asdict(self)
        return data


def resolve_effective_bot_policy(
    *,
    instance: Any,
    broker_environment: str,
    risk_params: Mapping[str, Any],
    system_limits: SystemLimits | None = None,
    monitor_interval_seconds: int = 10,
) -> EffectiveBotPolicy:
    limits = system_limits or SystemLimits()
    warnings: list[str] = []
    clamps: list[dict[str, Any]] = []

    capital_raw = getattr(instance, "capital_allocation", None)
    if capital_raw is None or float(capital_raw or 0.0) <= 0:
        raise EffectivePolicyError(
            "CAPITAL_BUDGET_REQUIRED",
            "Auto Pilot capital budget is missing; operator/user correction is required",
        )
    capital_budget = float(capital_raw)

    allocation_type = str(getattr(instance, "allocation_type", "") or "").lower()
    allocation_value = float(getattr(instance, "allocation_value", 0.0) or 0.0)
    if allocation_type not in {"fixed_amount", "percent_balance"} or allocation_value <= 0:
        raise EffectivePolicyError("INVALID_POSITION_ALLOCATION", "A valid position allocation is required")
    if allocation_type == "fixed_amount" and allocation_value > capital_budget:
        raise EffectivePolicyError(
            "POSITION_ALLOCATION_EXCEEDS_CAPITAL",
            f"Position allocation {allocation_value} exceeds capital budget {capital_budget}",
        )

    requested_risk = float(risk_params.get("per_trade_risk_pct", limits.max_risk_per_trade_ceiling))
    effective_risk = _clamp(
        "risk_per_trade", requested_risk, limits.max_risk_per_trade_ceiling, warnings, clamps
    )

    operator_trades = int(getattr(settings, "MAX_TRADES_DAILY", limits.max_trades_per_day))
    requested_trades = int(risk_params.get("max_trades_per_day", operator_trades))
    trades_ceiling = min(operator_trades, limits.max_trades_per_day)
    effective_trades = int(min(requested_trades, trades_ceiling))
    if effective_trades != requested_trades:
        warnings.append(f"max_daily_trades: requested={requested_trades} effective={effective_trades}")
        clamps.append(
            {
                "setting": "max_daily_trades",
                "requested_value": requested_trades,
                "effective_value": effective_trades,
                "hard_ceiling": trades_ceiling,
                "clamp_reason": (
                    "OPERATOR_LIMIT" if operator_trades < limits.max_trades_per_day else "SYSTEM_LIMIT_CEILING"
                ),
            }
        )

    operator_positions = int(getattr(settings, "MAX_OPEN_POSITIONS", limits.max_open_positions))
    requested_positions = int(risk_params.get("max_position_slots", operator_positions))
    positions_ceiling = min(operator_positions, limits.max_open_positions)
    effective_positions = int(min(requested_positions, positions_ceiling))
    if effective_positions != requested_positions:
        warnings.append(f"max_open_positions: requested={requested_positions} effective={effective_positions}")
        clamps.append(
            {
                "setting": "max_open_positions",
                "requested_value": requested_positions,
                "effective_value": effective_positions,
                "hard_ceiling": positions_ceiling,
                "clamp_reason": (
                    "OPERATOR_LIMIT" if operator_positions < limits.max_open_positions else "SYSTEM_LIMIT_CEILING"
                ),
            }
        )

    requested_leverage = float(risk_params.get("max_leverage", 10.0))
    symbol_values = _symbols(getattr(instance, "symbols", None))
    asset_ceiling = limits.max_leverage_major if all(s in {"BTCUSDT", "ETHUSDT"} for s in symbol_values) else limits.max_leverage_alt
    effective_leverage = _clamp(
        "max_leverage", requested_leverage, asset_ceiling, warnings, clamps, reason="ASSET_CLASS_LEVERAGE_CEILING"
    )

    profile_daily_loss = float(risk_params.get("daily_loss_limit_pct", 0.05)) * capital_budget
    operator_daily_loss = float(getattr(settings, "DAILY_MAX_LOSS_USDT", profile_daily_loss) or profile_daily_loss)
    max_daily_loss = min(profile_daily_loss, operator_daily_loss, limits.max_daily_loss_pct * capital_budget)

    weekly_requested = float(getattr(settings, "MAX_WEEKLY_DRAWDOWN_PCT", 5.0))
    monthly_requested = float(getattr(settings, "MAX_MONTHLY_DRAWDOWN_PCT", 10.0))
    max_weekly = min(weekly_requested, limits.max_weekly_drawdown_pct * 100.0)
    max_monthly = min(monthly_requested, limits.emergency_drawdown_halt_pct * 100.0)

    stop_fraction = float(risk_params.get("stop_loss_multiplier", 2.0)) * 0.01
    stop_fraction = max(limits.min_stop_loss_pct, min(stop_fraction, limits.max_stop_loss_pct))
    timeframe_values = getattr(instance, "timeframes", None) or [getattr(settings, "DEFAULT_INTERVAL", "15m")]
    timeframe = str(timeframe_values[0] if isinstance(timeframe_values, (list, tuple)) else timeframe_values)
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise EffectivePolicyError(
            "INVALID_TIMEFRAME",
            f"Timeframe {timeframe!r} is not supported (expected one of {list(SUPPORTED_TIMEFRAMES)})",
        )

    absolute_floor = max(
        float(getattr(settings, "MIN_CONFIDENCE_THRESHOLD", 0.70)),
        float(getattr(settings, "ENSEMBLE_MIN_THRESHOLD_FLOOR", 0.55)),
    )
    execution_mode = normalize_execution_mode(getattr(instance, "mode", "paper"))
    from app.core.strong_trend_guard import evaluate_strong_trend_guard
    strong_trend_guard = evaluate_strong_trend_guard(settings, execution_mode=execution_mode)
    blocked = strong_trend_guard.effective_blocked_regimes
    external_mode = "restricted" if bool(getattr(settings, "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False)) else "disabled"

    kwargs = dict(
        bot_instance_id=str(instance.id), user_id=str(instance.user_id),
        broker_account_id=str(instance.broker_account_id), market_type=str(instance.market_type).upper(),
        execution_mode=execution_mode,
        broker_environment=str(broker_environment or "unknown").lower(),
        strategy_id=str(instance.strategy_id), strategy_version=str(instance.strategy_version),
        symbols=symbol_values, timeframe=timeframe, higher_timeframe="4h",
        execution_monitor_interval=int(monitor_interval_seconds), capital_budget=capital_budget,
        capital_allocation_type=str(getattr(instance, "capital_allocation_type", "fixed_amount") or "fixed_amount"),
        position_allocation_type=allocation_type, position_allocation_value=allocation_value,
        risk_level=str(getattr(instance, "risk_level", "balanced") or "balanced").lower(),
        requested_risk_per_trade=requested_risk, risk_per_trade=effective_risk,
        risk_per_trade_ceiling=limits.max_risk_per_trade_ceiling, max_daily_loss=max_daily_loss,
        max_weekly_drawdown=max_weekly, max_monthly_drawdown=max_monthly,
        requested_max_daily_trades=requested_trades, max_daily_trades=effective_trades,
        requested_max_open_positions=requested_positions, max_open_positions=effective_positions,
        requested_max_leverage=requested_leverage, max_leverage=effective_leverage,
        max_leverage_ceiling=asset_ceiling,
        min_risk_reward=float(getattr(settings, "MIN_RISK_REWARD", 1.8)),
        minimum_notional=max(5.0, float(getattr(settings, "MIN_TRADE_AMOUNT_USDT", 5.0))),
        stop_loss_fraction=stop_fraction, min_stop_loss_fraction=limits.min_stop_loss_pct,
        max_stop_loss_fraction=limits.max_stop_loss_pct,
        consecutive_loss_soft=int(getattr(settings, "MAX_CONSECUTIVE_LOSSES_SOFT", 3)),
        consecutive_loss_hard=min(int(getattr(settings, "MAX_CONSECUTIVE_LOSSES_HARD", 5)), limits.max_consecutive_losses),
        confidence_absolute_floor=absolute_floor, regime_blocked=blocked,
        session_enabled=bool(getattr(settings, "ENSEMBLE_SESSION_FILTER_ENABLED", True)),
        session_windows_utc=str(getattr(settings, "ENSEMBLE_SESSION_WINDOWS_UTC", "")),
        volatility_filter_enabled=bool(risk_params.get("additional_params", {}).get("volatility_filter_enabled", True)),
        ml_mode="disabled" if not bool(getattr(settings, "ML_ENABLED", False)) else ("shadow" if bool(getattr(settings, "ML_SHADOW_MODE", True)) else "blocking"),
        iofs_mode=str(getattr(settings, "IOFS_GATE_MODE", "disabled")) if bool(getattr(settings, "IOFS_GATE_ENABLED", False)) else "disabled",
        event_filter_enabled=bool(getattr(settings, "EVENT_FILTER_ENABLED", False)),
        news_mode="disabled" if not bool(getattr(settings, "NEWS_TRADING_ENABLED", False)) else "advisory",
        external_signal_mode=external_mode, resolved_at=datetime.now(timezone.utc).isoformat(),
        clamp_warnings=tuple(warnings), clamps=tuple(clamps),
    )
    for _name, _value in (
        ("risk_per_trade", effective_risk),
        ("max_daily_trades", effective_trades),
        ("max_open_positions", effective_positions),
        ("max_leverage", effective_leverage),
        ("max_daily_loss", max_daily_loss),
    ):
        if float(_value) <= 0:
            raise EffectivePolicyError(
                "NON_POSITIVE_LIMIT",
                f"Resolved {_name}={_value} is not a tradeable value; correct the risk configuration",
            )

    provisional = EffectiveBotPolicy(**kwargs)
    encoded = json.dumps(provisional.runtime_payload(), sort_keys=True, separators=(",", ":"), default=str).encode()
    policy_hash = hashlib.sha256(encoded).hexdigest()
    policy = EffectiveBotPolicy(**{**kwargs, "policy_hash": policy_hash})
    logger.info(
        "[EFFECTIVE_POLICY] bot=%s hash=%s execution_mode=%s broker_environment=%s capital_budget=%.2f "
        "position_allocation=%s:%.4f risk=%.4f symbols=%s timeframe=%s clamps=%s",
        policy.bot_instance_id, policy.policy_hash, policy.execution_mode, policy.broker_environment,
        policy.capital_budget, policy.position_allocation_type, policy.position_allocation_value,
        policy.risk_per_trade, list(policy.symbols), policy.timeframe, list(policy.clamp_warnings),
    )
    return policy
