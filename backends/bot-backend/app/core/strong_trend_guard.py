"""Safety guard for the paper-only STRONG_TREND experiment."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from app.symbols.universe import parse_symbols


STRONG_TREND = "STRONG_TREND"


def parse_blocked_regimes(value: str) -> set[str]:
    return {
        item.strip().upper()
        for item in str(value or "").split(",")
        if item.strip()
    }


@dataclass(frozen=True)
class StrongTrendGuardStatus:
    allowed_only_in_paper: bool
    configured_unblocked: bool
    effective_unblocked: bool
    forced_blocked: bool
    execution_mode_paper: bool
    ml_disabled: bool
    live_symbols_count: int
    effective_blocked_regimes: tuple[str, ...]
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_strong_trend_guard(
    config: Any,
    *,
    execution_mode: str | None = None,
) -> StrongTrendGuardStatus:
    configured = parse_blocked_regimes(
        getattr(config, "ENSEMBLE_BLOCKED_REGIMES", "")
    )
    live_symbols = parse_symbols(
        getattr(config, "LIVE_SYMBOLS", ""),
        getattr(config, "MAX_SYMBOLS", 100),
    )
    execution_mode_paper = (
        str(
            execution_mode
            if execution_mode is not None
            else getattr(config, "EXECUTION_MODE", "")
        ).strip().lower()
        == "paper"
    )
    ml_disabled = not bool(getattr(config, "ML_ENABLED", False))
    configured_unblocked = STRONG_TREND not in configured
    safety_requirements_met = (
        execution_mode_paper and ml_disabled and not live_symbols
    )

    effective = set(configured)
    forced_blocked = configured_unblocked and not safety_requirements_met
    if forced_blocked:
        effective.add(STRONG_TREND)

    effective_unblocked = STRONG_TREND not in effective
    if effective_unblocked:
        reason = "paper_only_requirements_met"
    elif forced_blocked:
        reason = "forced_blocked_outside_paper_only_requirements"
    else:
        reason = "configured_blocked"

    return StrongTrendGuardStatus(
        allowed_only_in_paper=True,
        configured_unblocked=configured_unblocked,
        effective_unblocked=effective_unblocked,
        forced_blocked=forced_blocked,
        execution_mode_paper=execution_mode_paper,
        ml_disabled=ml_disabled,
        live_symbols_count=len(live_symbols),
        effective_blocked_regimes=tuple(sorted(effective)),
        reason=reason,
    )
