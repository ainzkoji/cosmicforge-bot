"""The single source of truth for the customer risk levels (Step 1.1).

Three immutable, versioned profiles -- Conservative, Balanced, Aggressive --
with the master plan's approved starting values. Every consumer (deployment
preview, deploy validation, engine sizing, the portal's displayed numbers)
reads these records; no percentage is hard-coded anywhere else.

Units
-----
Every ``*_pct`` field is a PERCENTAGE (``Decimal("0.50")`` means 0.50 %, i.e.
one two-hundredth of the budget). ``fraction("per_trade_risk_pct")`` gives the
same value as a fraction (``Decimal("0.005")``). Monetary results are
``Decimal``s quantized to the budget currency's cent; risk amounts are rounded
DOWN so a displayed figure never overstates the risk the engine may take.

Versioning
----------
``RISK_PROFILE_VERSION`` identifies this table. A deployment persists the
level AND the version it was previewed with; a later change to the table is a
new version, never a silent change to deployed bots. Legacy preset names
(``low`` / ``medium`` / ``high``, the bot-backend's own ``get_risk_profile_preset``
values) are a different model: ``normalize_level`` maps the NAMES for new
risk-based deployments, but nothing here rewrites an existing bot's settings.

System ceiling (unresolved policy conflict)
-------------------------------------------
The engine's hard system ceiling is 0.4 % per trade
(``app.risk.system_limits.SystemLimits.max_risk_per_trade_ceiling``). Balanced
(0.50 %) and Aggressive (0.75 %) exceed it. This module carries the approved
profile values and exposes the conflict (``exceeds_system_ceiling``,
``effective_per_trade_risk_pct``); it does NOT widen the ceiling. Until the
project owner approves the wider ceiling explicitly, the engine's effective
per-trade risk for those two levels is the stricter 0.4 %, and the preview
says so (``ceiling_applied``).
"""
from __future__ import annotations

from dataclasses import dataclass, asdict, fields
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN, InvalidOperation
from types import MappingProxyType
from typing import Any, Mapping, Optional

RISK_PROFILE_VERSION = "2026-10-08.v1"
BUDGET_CURRENCY = "USDT"
CENT = Decimal("0.01")
HUNDRED = Decimal("100")

CONSERVATIVE = "conservative"
BALANCED = "balanced"
AGGRESSIVE = "aggressive"
LEVELS = (CONSERVATIVE, BALANCED, AGGRESSIVE)

#: Legacy / onboarding names accepted for NEW risk-based deployments.
LEGACY_LEVEL_NAMES = MappingProxyType({
    "low": CONSERVATIVE, "medium": BALANCED, "high": AGGRESSIVE,
    "moderate": BALANCED, "conservative": CONSERVATIVE, "balanced": BALANCED, "aggressive": AGGRESSIVE,
})

#: The engine's current hard per-trade ceiling, as a percentage (see module docstring).
SYSTEM_PER_TRADE_RISK_CEILING_PCT = Decimal("0.40")


class UnknownRiskLevel(ValueError):
    pass


class InvalidBudget(ValueError):
    pass


@dataclass(frozen=True)
class RiskProfile:
    level: str
    version: str
    per_trade_risk_pct: Decimal        # stop-loss risk per trade, % of budget
    max_open_risk_pct: Decimal         # combined open risk, % of budget
    max_positions: int
    leverage_ceiling: int              # x
    daily_loss_pause_pct: Decimal      # pause the day at this loss, % of budget (magnitude)
    drawdown_reduce_pct: Decimal       # reduce position size from this drawdown, % (magnitude)
    drawdown_stop_pct: Decimal         # flatten and stop from this drawdown, % (magnitude)

    def __post_init__(self):
        if self.level not in LEVELS:
            raise UnknownRiskLevel(self.level)
        for name in ("per_trade_risk_pct", "max_open_risk_pct", "daily_loss_pause_pct", "drawdown_reduce_pct",
                     "drawdown_stop_pct"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive finite Decimal percentage")
        if self.per_trade_risk_pct > self.max_open_risk_pct:
            raise ValueError("per_trade_risk_pct cannot exceed max_open_risk_pct")
        if self.drawdown_reduce_pct >= self.drawdown_stop_pct:
            raise ValueError("drawdown_reduce_pct must be below drawdown_stop_pct")
        if int(self.max_positions) < 1 or int(self.leverage_ceiling) < 1:
            raise ValueError("max_positions and leverage_ceiling must be at least 1")

    def fraction(self, name: str) -> Decimal:
        """A ``*_pct`` field as a fraction of the budget (0.50 % -> 0.005)."""
        return getattr(self, name) / HUNDRED

    def as_dict(self) -> dict:
        """Controlled serialization: every Decimal as a plain string, ints as ints."""
        out = {}
        for f in fields(self):
            value = getattr(self, f.name)
            out[f.name] = str(value) if isinstance(value, Decimal) else value
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RiskProfile":
        values = dict(data)
        for f in fields(cls):
            if f.name.endswith("_pct"):
                values[f.name] = _decimal(values[f.name], f.name)
            elif f.name in ("max_positions", "leverage_ceiling"):
                values[f.name] = int(values[f.name])
        return cls(**values)


def _decimal(value: Any, what: str) -> Decimal:
    if isinstance(value, float):
        value = repr(value)  # floats are accepted only through their shortest repr
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise InvalidBudget(f"{what} is not a decimal number") from exc
    if not result.is_finite():
        raise InvalidBudget(f"{what} must be finite")
    return result


PROFILES: Mapping[str, RiskProfile] = MappingProxyType({
    CONSERVATIVE: RiskProfile(CONSERVATIVE, RISK_PROFILE_VERSION, per_trade_risk_pct=Decimal("0.25"),
                              max_open_risk_pct=Decimal("1"), max_positions=4, leverage_ceiling=1,
                              daily_loss_pause_pct=Decimal("1"), drawdown_reduce_pct=Decimal("5"),
                              drawdown_stop_pct=Decimal("10")),
    BALANCED: RiskProfile(BALANCED, RISK_PROFILE_VERSION, per_trade_risk_pct=Decimal("0.50"),
                          max_open_risk_pct=Decimal("2"), max_positions=6, leverage_ceiling=2,
                          daily_loss_pause_pct=Decimal("2"), drawdown_reduce_pct=Decimal("8"),
                          drawdown_stop_pct=Decimal("15")),
    AGGRESSIVE: RiskProfile(AGGRESSIVE, RISK_PROFILE_VERSION, per_trade_risk_pct=Decimal("0.75"),
                            max_open_risk_pct=Decimal("3"), max_positions=8, leverage_ceiling=3,
                            daily_loss_pause_pct=Decimal("3"), drawdown_reduce_pct=Decimal("12"),
                            drawdown_stop_pct=Decimal("25")),
})

#: Every version this build can read. A deployment made under an older version
#: keeps being interpreted with that version's table.
VERSIONS: Mapping[str, Mapping[str, RiskProfile]] = MappingProxyType({RISK_PROFILE_VERSION: PROFILES})


def normalize_level(level: Any) -> str:
    """The canonical level for an accepted name (legacy names included)."""
    key = str(level or "").strip().lower()
    try:
        return LEGACY_LEVEL_NAMES[key]
    except KeyError:
        raise UnknownRiskLevel(str(level)) from None


def get_profile(level: Any, version: Optional[str] = None) -> RiskProfile:
    table = VERSIONS.get(version or RISK_PROFILE_VERSION)
    if table is None:
        raise UnknownRiskLevel(f"unknown risk profile version {version}")
    return table[normalize_level(level)]


def validate_budget(budget: Any, *, allow_zero: bool = True) -> Decimal:
    """A budget as a non-negative finite Decimal (quantized to the cent, rounded down)."""
    value = _decimal(budget, "budget")
    if value < 0:
        raise InvalidBudget("budget cannot be negative")
    if value == 0 and not allow_zero:
        raise InvalidBudget("budget must be positive")
    return value.quantize(CENT, rounding=ROUND_DOWN)


def exceeds_system_ceiling(profile: RiskProfile, ceiling_pct: Decimal = SYSTEM_PER_TRADE_RISK_CEILING_PCT) -> bool:
    return profile.per_trade_risk_pct > ceiling_pct


def effective_per_trade_risk_pct(profile: RiskProfile, *, ceiling_pct: Decimal = SYSTEM_PER_TRADE_RISK_CEILING_PCT,
                                 ceiling_widening_approved: bool = False) -> Decimal:
    """The per-trade risk the engine may actually use: the profile's value, or
    the system ceiling while the wider value is not explicitly approved."""
    if ceiling_widening_approved or not exceeds_system_ceiling(profile, ceiling_pct):
        return profile.per_trade_risk_pct
    return ceiling_pct


def _down(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_DOWN)


def _even(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_EVEN)


def money_view(level: Any, budget: Any, *, version: Optional[str] = None, currency: str = BUDGET_CURRENCY,
               stop_distance_range_pct: Optional[tuple] = None,
               ceiling_pct: Decimal = SYSTEM_PER_TRADE_RISK_CEILING_PCT,
               ceiling_widening_approved: bool = False) -> dict:
    """What the selected level means in money for ``budget``.

    ``stop_distance_range_pct``: ``(tightest, widest)`` structural stop
    distance in percent that the strategy's setups actually use. Only then is a
    typical position-size range computed (``risk / stop_distance``); without an
    explicit assumption the field is ``None`` with the reason stated, never an
    invented number. Amounts are Decimals (strings in ``as_json``).
    """
    profile = get_profile(level, version)
    amount = validate_budget(budget)
    effective_pct = effective_per_trade_risk_pct(profile, ceiling_pct=ceiling_pct,
                                                 ceiling_widening_approved=ceiling_widening_approved)
    risk_per_trade = _down(amount * effective_pct / HUNDRED)
    approved_risk_per_trade = _down(amount * profile.per_trade_risk_pct / HUNDRED)
    view = {
        "risk_level": profile.level,
        "risk_profile_version": profile.version,
        "budget": {"currency": currency, "amount": amount},
        "per_trade_risk_pct": profile.per_trade_risk_pct,
        "effective_per_trade_risk_pct": effective_pct,
        "ceiling_applied": effective_pct != profile.per_trade_risk_pct,
        "system_per_trade_risk_ceiling_pct": ceiling_pct,
        "risk_per_trade": risk_per_trade,
        "approved_profile_risk_per_trade": approved_risk_per_trade,
        "max_open_risk_pct": profile.max_open_risk_pct,
        "max_open_risk": _down(amount * profile.max_open_risk_pct / HUNDRED),
        "daily_loss_pause_pct": profile.daily_loss_pause_pct,
        "daily_loss_pause": _even(amount * profile.daily_loss_pause_pct / HUNDRED),
        "drawdown_reduce_pct": profile.drawdown_reduce_pct,
        "drawdown_reduce_threshold": _even(amount * profile.drawdown_reduce_pct / HUNDRED),
        "drawdown_stop_pct": profile.drawdown_stop_pct,
        "drawdown_stop_threshold": _even(amount * profile.drawdown_stop_pct / HUNDRED),
        "leverage_ceiling": profile.leverage_ceiling,
        "max_positions": profile.max_positions,
        "typical_position_notional": None,
        "typical_position_assumption": None,
    }
    if stop_distance_range_pct is not None:
        tight, wide = (_decimal(v, "stop distance") for v in stop_distance_range_pct)
        if not 0 < tight <= wide:
            raise ValueError("stop_distance_range_pct must satisfy 0 < tightest <= widest")
        view["typical_position_notional"] = {
            "min": _down(risk_per_trade / (wide / HUNDRED)) if risk_per_trade else Decimal("0.00"),
            "max": _down(risk_per_trade / (tight / HUNDRED)) if risk_per_trade else Decimal("0.00"),
        }
        view["typical_position_assumption"] = {"stop_distance_pct": {"tightest": tight, "widest": wide},
                                               "signal_strength": 1}
    else:
        view["typical_position_assumption"] = "STOP_DISTANCE_ASSUMPTION_REQUIRED"
    return view


def minimum_deployable_budget(level: Any, *, exchange_min_notional: Any, widest_stop_distance_pct: Any,
                              version: Optional[str] = None, ceiling_pct: Decimal = SYSTEM_PER_TRADE_RISK_CEILING_PCT,
                              ceiling_widening_approved: bool = False) -> Decimal:
    """The smallest budget whose full-strength position at the WIDEST stop the
    strategy uses still reaches the exchange minimum notional. Below it a
    correctly sized order is below the exchange minimum and is blocked
    (``RISK_SIZE_BELOW_EXCHANGE_MINIMUM``); the order is never inflated."""
    profile = get_profile(level, version)
    min_notional = _decimal(exchange_min_notional, "exchange_min_notional")
    widest = _decimal(widest_stop_distance_pct, "widest_stop_distance_pct")
    if min_notional <= 0 or widest <= 0:
        raise ValueError("exchange_min_notional and widest_stop_distance_pct must be positive")
    effective = effective_per_trade_risk_pct(profile, ceiling_pct=ceiling_pct,
                                             ceiling_widening_approved=ceiling_widening_approved)
    # notional = budget * effective% * / widest%  >= min_notional
    budget = min_notional * (widest / HUNDRED) / (effective / HUNDRED)
    return budget.quantize(CENT, rounding=ROUND_HALF_EVEN) if budget == budget.quantize(CENT) \
        else (budget.quantize(CENT, rounding=ROUND_DOWN) + CENT)


def as_json(view: Mapping[str, Any]) -> dict:
    """The money view with every Decimal rendered as a string (API payloads)."""
    def render(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, Mapping):
            return {k: render(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [render(v) for v in value]
        return value
    return render(dict(view))


__all__ = ["RISK_PROFILE_VERSION", "BUDGET_CURRENCY", "LEVELS", "CONSERVATIVE", "BALANCED", "AGGRESSIVE",
           "LEGACY_LEVEL_NAMES", "SYSTEM_PER_TRADE_RISK_CEILING_PCT", "RiskProfile", "PROFILES", "VERSIONS",
           "UnknownRiskLevel", "InvalidBudget", "normalize_level", "get_profile", "validate_budget",
           "exceeds_system_ceiling", "effective_per_trade_risk_pct", "money_view", "minimum_deployable_budget",
           "as_json"]
