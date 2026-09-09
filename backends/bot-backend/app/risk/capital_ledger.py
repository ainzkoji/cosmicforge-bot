"""The capital budget invariant, and the sizing chain that enforces it.

    committed margin  +  proposed margin  <=  capital budget

This has to hold whatever the operator configured. `bot_a8117dc719fc` is the
worked example: `capital_budget=120`, `allocation_value=120` fixed, and
`max_open_positions=2`. Nothing rejected that. `resolve_effective_bot_policy`
checks a *single* allocation against the budget but never multiplies by the
slot count, and the executor's pre-trade check is against the **broker's**
available balance, which has nothing to do with the bot's own budget. Two
concurrent positions would have deployed 240 against a 120 budget.

So the budget needs a ledger, and the ledger needs to survive a restart. Both
follow from taking committed margin from **persisted positions** rather than
from anything held in memory:

    committed = SUM(committed_margin) over OPEN positions of this bot

A partial close reduces that row's `committed_margin` in proportion to the
quantity closed; a final close takes the row out of the sum entirely. Capital
is therefore released by the same event stream that releases the position, and
a process that restarts mid-position rebuilds the same number it had before.

THE ORDER OF THE CHAIN MATTERS

    risk-derived safe size          <- authoritative; nothing may exceed it
      -> per-position allocation cap
      -> remaining total capital
      -> leverage / exposure limits
      -> execution minimums and precision

Every stage may only *shrink* the size. That is the whole point: an allocation
cap is a ceiling, not a target, and a fixed allocation must never inflate a
size the risk layer derived. If the result cannot meet the exchange minimum it
is **rejected**, never rounded up — rounding up to reach a minimum is exactly
how a risk-sized trade becomes an un-risk-sized one.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from app.decision.reasons import ExecutionReason, RiskReason

logger = logging.getLogger(__name__)

#: Float comparisons on money. Below this, a residual is zero.
EPSILON = 1e-9


def ensure_position_capital_columns(db: Any) -> bool:
    """Add the columns the ledger reads. Safe to run repeatedly."""
    added = False
    with db.connect() as conn:
        columns = {r[1] for r in conn.execute("PRAGMA table_info(positions)")}
        if "leverage" not in columns:
            conn.execute("ALTER TABLE positions ADD COLUMN leverage REAL")
            added = True
        if "committed_margin" not in columns:
            conn.execute("ALTER TABLE positions ADD COLUMN committed_margin REAL")
            added = True
        if added:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_positions_open_capital "
                "ON positions(bot_instance_id, status)"
            )
    return added


@dataclass(frozen=True)
class CapitalAuthorization:
    """The verdict, and the whole chain that produced it."""

    approved: bool
    reason: str
    detail: str

    capital_budget: float
    committed_margin: float
    available_capital: float

    requested_notional: float
    approved_notional: float
    requested_margin: float
    approved_margin: float
    leverage: float

    stages: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def was_reduced(self) -> bool:
        return self.approved_notional < self.requested_notional - EPSILON

    def observability(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reason": self.reason,
            "detail": self.detail,
            "capital_budget": round(self.capital_budget, 8),
            "committed_margin": round(self.committed_margin, 8),
            "available_capital": round(self.available_capital, 8),
            "requested_notional": round(self.requested_notional, 8),
            "approved_notional": round(self.approved_notional, 8),
            "requested_margin": round(self.requested_margin, 8),
            "approved_margin": round(self.approved_margin, 8),
            "leverage": self.leverage,
            "stages": list(self.stages),
        }


class CapitalLedger:
    """Committed capital for one bot, derived from persisted positions."""

    def __init__(self, db: Any, *, bot_instance_id: str, capital_budget: float) -> None:
        self.db = db
        self.bot_instance_id = bot_instance_id
        self.capital_budget = float(capital_budget or 0.0)

    # ── Persisted state ─────────────────────────────────────────────────────

    def committed_margin(self) -> float:
        """Margin currently tied up in this bot's open positions.

        Read from the database on every call rather than cached: a cached
        number is exactly what a restart, a second process, or an out-of-band
        close would invalidate.
        """
        try:
            with self.db.connect() as conn:
                row = conn.execute(
                    "SELECT COALESCE(SUM(committed_margin), 0.0) FROM positions "
                    "WHERE bot_instance_id=? AND status='OPEN'",
                    (self.bot_instance_id,),
                ).fetchone()
            return max(0.0, float(row[0] or 0.0))
        except Exception as exc:
            # Fail closed: an unreadable ledger must not read as "nothing
            # committed", which would authorise the whole budget again.
            logger.error(
                "[CAPITAL_LEDGER] bot=%s could not read committed margin: %s",
                self.bot_instance_id, exc,
            )
            return self.capital_budget

    def available_capital(self) -> float:
        return max(0.0, self.capital_budget - self.committed_margin())

    def open_positions(self) -> int:
        with self.db.connect() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) FROM positions WHERE bot_instance_id=? AND status='OPEN'",
                (self.bot_instance_id,),
            ).fetchone()[0])

    # ── The chain ───────────────────────────────────────────────────────────

    def authorize(
        self,
        *,
        risk_notional: float,
        leverage: float,
        per_position_cap: float = 0.0,
        min_notional: float = 0.0,
        max_exposure_notional: float = 0.0,
    ) -> CapitalAuthorization:
        """Size an entry through the chain, or reject it with a canonical reason.

        ``risk_notional`` is the size the risk layer derived and is the ceiling
        for everything that follows. ``per_position_cap`` and
        ``max_exposure_notional`` of 0 mean "no cap", not "cap of zero".
        """
        leverage = max(1.0, float(leverage or 1.0))
        requested_notional = float(risk_notional or 0.0)
        requested_margin = requested_notional / leverage
        committed = self.committed_margin()
        available = max(0.0, self.capital_budget - committed)
        stages: list[dict[str, Any]] = []

        def record(name: str, notional: float, note: str) -> None:
            stages.append({
                "stage": name,
                "notional": round(notional, 8),
                "margin": round(notional / leverage, 8),
                "note": note,
            })

        def reject(reason: str, detail: str, notional: float = 0.0) -> CapitalAuthorization:
            return CapitalAuthorization(
                approved=False, reason=reason, detail=detail,
                capital_budget=self.capital_budget, committed_margin=committed,
                available_capital=available, requested_notional=requested_notional,
                approved_notional=notional, requested_margin=requested_margin,
                approved_margin=notional / leverage, leverage=leverage,
                stages=tuple(stages),
            )

        # ── 1. Risk-derived size — the authoritative ceiling ────────────────
        if requested_notional <= EPSILON:
            record("risk_derived", 0.0, "risk layer produced no size")
            return reject(
                RiskReason.STOP_INVALID,
                "the risk layer derived a non-positive size; there is nothing to cap",
            )
        notional = requested_notional
        record("risk_derived", notional, "authoritative ceiling")

        # ── 2. Per-position allocation cap — a ceiling, never a target ──────
        if per_position_cap > EPSILON and notional > per_position_cap:
            notional = per_position_cap
            record("allocation_cap", notional, f"capped to allocation {per_position_cap:g}")
        else:
            record("allocation_cap", notional, "allocation not binding")

        # ── 3. Remaining total capital — the invariant ──────────────────────
        if self.capital_budget <= EPSILON:
            return reject(
                RiskReason.CAPITAL_BUDGET_REQUIRED,
                "no capital budget is configured for this bot",
            )
        if available <= EPSILON:
            return reject(
                RiskReason.INSUFFICIENT_CAPITAL,
                f"capital budget {self.capital_budget:g} is fully committed "
                f"({committed:g} in open positions); nothing left to allocate",
            )
        margin = notional / leverage
        if margin > available + EPSILON:
            notional = available * leverage
            record(
                "capital_remaining", notional,
                f"capped to remaining capital {available:g} "
                f"(budget {self.capital_budget:g} - committed {committed:g})",
            )
        else:
            record("capital_remaining", notional, f"fits in remaining {available:g}")

        # ── 4. Leverage / exposure ceiling ──────────────────────────────────
        if max_exposure_notional > EPSILON and notional > max_exposure_notional:
            notional = max_exposure_notional
            record("exposure_limit", notional, f"capped to exposure {max_exposure_notional:g}")
        else:
            record("exposure_limit", notional, "exposure not binding")

        # ── 5. Execution minimums — reject, never round up ──────────────────
        if min_notional > EPSILON and notional < min_notional - EPSILON:
            record("min_notional", notional, f"below minimum {min_notional:g}")
            return reject(
                ExecutionReason.MIN_NOTIONAL,
                f"the safe size for this trade is {notional:.8g}, under the "
                f"{min_notional:g} exchange minimum. Raising it would mean taking "
                f"more risk than the risk layer allowed, so the entry is rejected.",
                notional=notional,
            )
        record("min_notional", notional, "meets the exchange minimum")

        # The chain may only shrink. If it ever grows, that is a defect, and it
        # is better to refuse than to place a trade nobody sized.
        if notional > requested_notional + EPSILON:
            logger.error(
                "[CAPITAL_LEDGER] bot=%s sizing chain grew %s -> %s",
                self.bot_instance_id, requested_notional, notional,
            )
            return reject(
                RiskReason.INSUFFICIENT_CAPITAL,
                "the sizing chain produced a larger size than the risk layer "
                "derived, which must never happen",
                notional=notional,
            )

        return CapitalAuthorization(
            approved=True, reason=RiskReason.APPROVED,
            detail=f"{notional:.8g} notional / {notional / leverage:.8g} margin "
                   f"against {available:g} available",
            capital_budget=self.capital_budget, committed_margin=committed,
            available_capital=available, requested_notional=requested_notional,
            approved_notional=notional, requested_margin=requested_margin,
            approved_margin=notional / leverage, leverage=leverage,
            stages=tuple(stages),
        )


def margin_for(quantity: float, price: float, leverage: float) -> float:
    """Margin a position ties up. One definition, used everywhere."""
    return abs(float(quantity) * float(price)) / max(1.0, float(leverage or 1.0))
