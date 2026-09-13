"""Per-trade allocation sizing and persisted committed-margin evidence.

For ``allocation_type=fixed_amount``, ``allocation_value`` is a per-trade
margin ceiling. It is not a total bot budget, is not divided by position slots,
and is not reduced by already-open positions. Position count, broker account
affordability, risk sizing, and explicit portfolio limits are separate gates.

The ledger still needs to survive a restart, so committed margin is read from
**persisted positions** rather than from anything held in memory:

    committed = SUM(committed_margin) over OPEN positions of this bot

A partial close reduces that row's `committed_margin` in proportion to the
quantity closed; a final close takes the row out of the sum entirely. Capital
is therefore released by the same event stream that releases the position.

THE ORDER OF THE CHAIN MATTERS

    risk-derived safe size          <- authoritative; nothing may exceed it
      -> per-position allocation cap
      -> configured per-trade margin cap
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


def capital_rejection(
    reason: str,
    detail: str,
    *,
    capital_budget: float = 0.0,
    requested_notional: float = 0.0,
    leverage: float = 1.0,
) -> CapitalAuthorization:
    """A rejection produced before the chain could run.

    Used for infrastructure failures -- an unreadable ledger, a missing budget
    or database -- which must reject the entry, never authorise it.
    """
    lev = max(1.0, float(leverage or 1.0))
    requested = float(requested_notional or 0.0)
    return CapitalAuthorization(
        approved=False, reason=reason, detail=detail,
        capital_budget=float(capital_budget or 0.0), committed_margin=0.0,
        available_capital=0.0, requested_notional=requested,
        approved_notional=0.0, requested_margin=requested / lev,
        approved_margin=0.0, leverage=lev,
        stages=({"stage": "unavailable", "notional": 0.0, "margin": 0.0, "note": detail},),
    )


class CapitalLedger:
    """Committed capital for one bot, derived from persisted positions."""

    def __init__(self, db: Any, *, bot_instance_id: str, capital_budget: float) -> None:
        self.db = db
        self.bot_instance_id = bot_instance_id
        self.capital_budget = float(capital_budget or 0.0)

    # ── Persisted state ─────────────────────────────────────────────────────

    def _read_committed_margin(self) -> float:
        """The committed margin, or an exception. Never a substituted value."""
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(committed_margin), 0.0) FROM positions "
                "WHERE bot_instance_id=? AND status='OPEN'",
                (self.bot_instance_id,),
            ).fetchone()
        return max(0.0, float(row[0] or 0.0))

    def committed_margin(self) -> float:
        """Margin currently tied up in this bot's open positions.

        Read from the database on every call rather than cached: a cached
        number is exactly what a restart, a second process, or an out-of-band
        close would invalidate.
        """
        try:
            return self._read_committed_margin()
        except Exception as exc:
            # Fail closed: an unreadable ledger must not read as "nothing
            # committed", which would authorise the whole budget again.
            logger.error(
                "[CAPITAL_LEDGER] bot=%s could not read committed margin: %s",
                self.bot_instance_id, exc,
            )
            return self.capital_budget

    def available_capital(self) -> float:
        """Configured per-trade margin capacity.

        This intentionally does not subtract open committed margin. For fixed
        allocations, already-open positions are evidence, not a claim against
        the next trade's per-trade allocation.
        """
        return max(0.0, self.capital_budget)

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
        ``capital_budget`` is the configured per-trade margin capacity used as
        a final margin ceiling when no narrower per-position cap binds. It is
        not reduced by persisted open committed margin.
        """
        leverage = max(1.0, float(leverage or 1.0))
        requested_notional = float(risk_notional or 0.0)
        requested_margin = requested_notional / leverage
        try:
            committed = self._read_committed_margin()
        except Exception as exc:
            # Infrastructure failure is never authorisation. Reported as its own
            # reason, not as "budget exhausted", so the cause stays visible.
            logger.error(
                "[CAPITAL_LEDGER] bot=%s ledger unreadable, entry rejected: %s",
                self.bot_instance_id, exc,
            )
            return capital_rejection(
                RiskReason.CAPITAL_LEDGER_UNAVAILABLE,
                f"the capital ledger could not be read: {type(exc).__name__}: {exc}",
                capital_budget=self.capital_budget,
                requested_notional=requested_notional, leverage=leverage,
            )
        available = max(0.0, self.capital_budget)
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

        # ── 3. Configured per-trade margin capacity ─────────────────────────
        if self.capital_budget <= EPSILON:
            return reject(
                RiskReason.CAPITAL_BUDGET_REQUIRED,
                "no capital budget is configured for this bot",
            )
        margin = notional / leverage
        if margin > available + EPSILON:
            notional = available * leverage
            record(
                "per_trade_capital", notional,
                f"capped to configured per-trade margin {available:g}",
            )
        else:
            record(
                "per_trade_capital", notional,
                f"fits configured per-trade margin {available:g}",
            )

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
                   f"against {available:g} per-trade capacity",
            capital_budget=self.capital_budget, committed_margin=committed,
            available_capital=available, requested_notional=requested_notional,
            approved_notional=notional, requested_margin=requested_margin,
            approved_margin=notional / leverage, leverage=leverage,
            stages=tuple(stages),
        )


def margin_for(quantity: float, price: float, leverage: float) -> float:
    """Margin a position ties up. One definition, used everywhere."""
    return abs(float(quantity) * float(price)) / max(1.0, float(leverage or 1.0))
