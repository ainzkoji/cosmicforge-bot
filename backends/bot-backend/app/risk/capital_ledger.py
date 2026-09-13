"""Per-trade allocation, committed-margin evidence, and account reservations.

WHAT ``allocation_value`` MEANS

For ``allocation_type=fixed_amount`` the configured ``allocation_value`` is the
margin ONE approved trade may use. With ``allocation_value=120`` and
``max_open_positions=2`` each trade may use up to 120, so two open positions
may commit ~240 between them. The allocation is never:

    120 in total for the bot
    120 / max_open_positions
    120 - SUM(open committed margin)

``bot_a8117dc719fc`` is the worked example of getting this wrong. The previous
ledger capped every entry at ``capital_budget - committed``, where
``capital_budget`` was ``BotInstance.capital_allocation`` (also 120), so each new
trade received whatever the open ones had left: BCHUSDT was sized at
120 - 59.9635 (ATUSDT, open) = 60.04 margin, and GALAUSDT at 120 - 60.0169
(BCHUSDT, open) = 59.98. ``capital_allocation`` is not an aggregate cap (operator
decision, 2026-09-13). It stays the base for percent allocations and the
daily-loss limit, and policy resolution still requires it to be >= a fixed
allocation.

SEPARATE QUESTIONS, SEPARATE GATES

    per-trade allocation       CapitalLedger.authorize (this module)
    risk-adjusted size         the risk layer; arrives here as ``risk_notional``
    concurrent position count  the policy engine (MAX_POSITIONS_REACHED)
    account affordability      the executor's live preflight, through
                               AccountMarginReservations (this module)
    explicit portfolio limit   ``portfolio_margin_limit``; none is configured
                               today, and one is never derived from the allocation
    fill truth                 broker executedQty / avg price, via reconciliation

COMMITTED MARGIN

    committed = SUM(committed_margin) over OPEN positions of this bot

is read from persisted positions on every call, so a restart rebuilds the same
number. A partial close reduces a row's ``committed_margin`` in proportion to
the quantity closed; a final close takes the row out of the sum. Committed
margin is evidence, and the input to an explicit portfolio limit when one is
configured. It is never subtracted from the next trade's allocation.

THE ORDER OF THE CHAIN MATTERS

    risk-derived safe size          <- authoritative; nothing may exceed it
      -> per-trade allocation       (allocation margin x leverage)
      -> explicit portfolio limit   (only if one is configured)
      -> per-symbol exposure limit
      -> execution minimums and precision

Every stage may only *shrink* the size. That is the whole point: an allocation
is a ceiling, not a target, and a fixed allocation must never inflate a size
the risk layer derived. If the result cannot meet the exchange minimum it is
**rejected**, never rounded up -- rounding up to reach a minimum is exactly how
a risk-sized trade becomes an un-risk-sized one.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from app.decision.reasons import ExecutionReason, RiskReason

logger = logging.getLogger(__name__)

#: Float comparisons on money. Below this, a residual is zero.
EPSILON = 1e-9

#: ``allocation_value`` is scoped to one trade. Recorded on every verdict so the
#: evidence never has to guess which meaning was applied.
ALLOCATION_SCOPE = "PER_TRADE"


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

    #: Configured margin for ONE trade.
    per_trade_allocation: float
    #: Aggregate margin in this bot's open positions. Evidence only.
    committed_margin: float
    open_positions: int
    #: Explicit aggregate margin limit; 0 means none is configured.
    portfolio_margin_limit: float

    requested_notional: float
    approved_notional: float
    requested_margin: float
    approved_margin: float
    leverage: float

    stages: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    allocation_scope: str = ALLOCATION_SCOPE

    @property
    def was_reduced(self) -> bool:
        return self.approved_notional < self.requested_notional - EPSILON

    def observability(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reason": self.reason,
            "detail": self.detail,
            "allocation_scope": self.allocation_scope,
            "configured_trade_allocation": round(self.per_trade_allocation, 8),
            "aggregate_committed_margin": round(self.committed_margin, 8),
            "open_positions": self.open_positions,
            "explicit_portfolio_limit": (
                round(self.portfolio_margin_limit, 8)
                if self.portfolio_margin_limit > EPSILON else None
            ),
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
    per_trade_allocation: float = 0.0,
    requested_notional: float = 0.0,
    leverage: float = 1.0,
) -> CapitalAuthorization:
    """A rejection produced before the chain could run.

    Used for infrastructure failures -- an unreadable ledger, a missing
    allocation or database -- which must reject the entry, never authorise it.
    """
    lev = max(1.0, float(leverage or 1.0))
    requested = float(requested_notional or 0.0)
    return CapitalAuthorization(
        approved=False, reason=reason, detail=detail,
        per_trade_allocation=float(per_trade_allocation or 0.0), committed_margin=0.0,
        open_positions=0, portfolio_margin_limit=0.0,
        requested_notional=requested, approved_notional=0.0,
        requested_margin=requested / lev, approved_margin=0.0, leverage=lev,
        stages=({"stage": "unavailable", "notional": 0.0, "margin": 0.0, "note": detail},),
    )


class CapitalLedger:
    """One bot's per-trade allocation chain over its persisted positions."""

    def __init__(
        self,
        db: Any,
        *,
        bot_instance_id: str,
        per_trade_allocation: float,
        portfolio_margin_limit: float = 0.0,
    ) -> None:
        self.db = db
        self.bot_instance_id = bot_instance_id
        self.per_trade_allocation = float(per_trade_allocation or 0.0)
        self.portfolio_margin_limit = max(0.0, float(portfolio_margin_limit or 0.0))

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

    def _read_open_positions(self) -> int:
        with self.db.connect() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) FROM positions WHERE bot_instance_id=? AND status='OPEN'",
                (self.bot_instance_id,),
            ).fetchone()[0])

    def committed_margin(self) -> float | None:
        """Margin currently tied up in this bot's open positions.

        Read from the database on every call rather than cached: a cached
        number is exactly what a restart, a second process, or an out-of-band
        close would invalidate. ``None`` when unreadable -- unknown is never
        reported as zero.
        """
        try:
            return self._read_committed_margin()
        except Exception as exc:
            logger.error(
                "[CAPITAL_LEDGER] bot=%s could not read committed margin: %s",
                self.bot_instance_id, exc,
            )
            return None

    def open_positions(self) -> int:
        return self._read_open_positions()

    # ── The chain ───────────────────────────────────────────────────────────

    def authorize(
        self,
        *,
        risk_notional: float,
        leverage: float,
        min_notional: float = 0.0,
        max_exposure_notional: float = 0.0,
    ) -> CapitalAuthorization:
        """Size an entry through the chain, or reject it with a canonical reason.

        ``risk_notional`` is the size the risk layer derived and is the ceiling
        for everything that follows. The per-trade allocation is margin, so it
        caps notional at ``per_trade_allocation * leverage``. It is not reduced
        by committed margin. ``max_exposure_notional`` of 0 means "no cap", not
        "cap of zero".
        """
        leverage = max(1.0, float(leverage or 1.0))
        requested_notional = float(risk_notional or 0.0)
        requested_margin = requested_notional / leverage
        try:
            committed = self._read_committed_margin()
            open_count = self._read_open_positions()
        except Exception as exc:
            # Infrastructure failure is never authorisation. Reported as its own
            # reason, so the cause stays visible.
            logger.error(
                "[CAPITAL_LEDGER] bot=%s ledger unreadable, entry rejected: %s",
                self.bot_instance_id, exc,
            )
            return capital_rejection(
                RiskReason.CAPITAL_LEDGER_UNAVAILABLE,
                f"the capital ledger could not be read: {type(exc).__name__}: {exc}",
                per_trade_allocation=self.per_trade_allocation,
                requested_notional=requested_notional, leverage=leverage,
            )
        stages: list[dict[str, Any]] = []

        def record(name: str, notional: float, note: str, reason: str | None = None) -> None:
            stage = {
                "stage": name,
                "notional": round(notional, 8),
                "margin": round(notional / leverage, 8),
                "note": note,
            }
            if reason:
                stage["reason"] = reason
            stages.append(stage)

        def verdict(approved: bool, reason: str, detail: str, notional: float) -> CapitalAuthorization:
            return CapitalAuthorization(
                approved=approved, reason=reason, detail=detail,
                per_trade_allocation=self.per_trade_allocation, committed_margin=committed,
                open_positions=open_count, portfolio_margin_limit=self.portfolio_margin_limit,
                requested_notional=requested_notional, approved_notional=notional,
                requested_margin=requested_margin, approved_margin=notional / leverage,
                leverage=leverage, stages=tuple(stages),
            )

        # ── 1. Risk-derived size — the authoritative ceiling ────────────────
        if requested_notional <= EPSILON:
            record("risk_derived", 0.0, "risk layer produced no size")
            return verdict(
                False, RiskReason.STOP_INVALID,
                "the risk layer derived a non-positive size; there is nothing to cap", 0.0,
            )
        notional = requested_notional
        record("risk_derived", notional, "authoritative ceiling")

        # ── 2. Per-trade allocation — a ceiling for THIS trade ──────────────
        if self.per_trade_allocation <= EPSILON:
            return verdict(
                False, RiskReason.CAPITAL_BUDGET_REQUIRED,
                "no per-trade allocation is configured for this bot", 0.0,
            )
        allocation_notional = self.per_trade_allocation * leverage
        if notional > allocation_notional + EPSILON:
            notional = allocation_notional
            record(
                "per_trade_allocation", notional,
                f"capped to the configured {self.per_trade_allocation:g} margin per trade",
                RiskReason.PER_TRADE_ALLOCATION_LIMIT,
            )
        else:
            record(
                "per_trade_allocation", notional,
                f"fits the configured {self.per_trade_allocation:g} margin per trade",
            )

        # ── 3. Explicit portfolio limit — only when one is configured ───────
        if self.portfolio_margin_limit > EPSILON:
            room = max(0.0, self.portfolio_margin_limit - committed)
            if room <= EPSILON:
                record("portfolio_limit", 0.0, "explicit portfolio limit fully used",
                       RiskReason.PORTFOLIO_EXPOSURE_LIMIT)
                return verdict(
                    False, RiskReason.PORTFOLIO_EXPOSURE_LIMIT,
                    f"explicit portfolio margin limit {self.portfolio_margin_limit:g} "
                    f"is fully used ({committed:g} committed)", 0.0,
                )
            if notional / leverage > room + EPSILON:
                notional = room * leverage
                record(
                    "portfolio_limit", notional,
                    f"capped to the {room:g} margin left under the explicit portfolio "
                    f"limit {self.portfolio_margin_limit:g}",
                    RiskReason.PORTFOLIO_EXPOSURE_LIMIT,
                )
            else:
                record("portfolio_limit", notional, f"fits the explicit portfolio limit ({room:g} left)")
        else:
            record("portfolio_limit", notional, "no explicit portfolio limit is configured")

        # ── 4. Per-symbol exposure ceiling ──────────────────────────────────
        if max_exposure_notional > EPSILON and notional > max_exposure_notional:
            notional = max_exposure_notional
            record("exposure_limit", notional, f"capped to exposure {max_exposure_notional:g}",
                   RiskReason.EXPOSURE_LIMIT)
        else:
            record("exposure_limit", notional, "exposure not binding")

        # ── 5. Execution minimums — reject, never round up ──────────────────
        if min_notional > EPSILON and notional < min_notional - EPSILON:
            record("min_notional", notional, f"below minimum {min_notional:g}")
            return verdict(
                False, ExecutionReason.MIN_NOTIONAL,
                f"the safe size for this trade is {notional:.8g}, under the "
                f"{min_notional:g} exchange minimum. Raising it would mean taking "
                f"more risk than the risk layer allowed, so the entry is rejected.",
                notional,
            )
        record("min_notional", notional, "meets the exchange minimum")

        # The chain may only shrink. If it ever grows, that is a defect, and it
        # is better to refuse than to place a trade nobody sized.
        if notional > requested_notional + EPSILON:
            logger.error(
                "[CAPITAL_LEDGER] bot=%s sizing chain grew %s -> %s",
                self.bot_instance_id, requested_notional, notional,
            )
            return verdict(
                False, RiskReason.INSUFFICIENT_CAPITAL,
                "the sizing chain produced a larger size than the risk layer "
                "derived, which must never happen",
                notional,
            )

        return verdict(
            True, RiskReason.APPROVED,
            f"{notional:.8g} notional / {notional / leverage:.8g} margin against a "
            f"{self.per_trade_allocation:g} per-trade allocation "
            f"({committed:g} committed in {open_count} open positions)",
            notional,
        )


# ══════════════════════════════════════════════════════════════════════════
# Account affordability: in-flight margin reservations
# ══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class AccountReservation:
    """One affordability verdict against a broker account snapshot."""

    token: str | None
    account_key: str
    approved: bool
    reason: str
    requested_margin: float
    reserved_margin: float
    available_balance: float
    #: Margin other in-flight entries on this account reserved but the broker
    #: snapshot does not show yet.
    pending_margin: float
    effective_available: float

    @property
    def reduced(self) -> bool:
        return self.approved and self.reserved_margin < self.requested_margin - EPSILON

    def observability(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reason": self.reason,
            "requested_margin": round(self.requested_margin, 8),
            "reserved_margin": round(self.reserved_margin, 8),
            "broker_available_balance": round(self.available_balance, 8),
            "pending_entry_reservations_margin": round(self.pending_margin, 8),
            "effective_available": round(self.effective_available, 8),
            "reduced": self.reduced,
        }


class AccountMarginReservations:
    """Margin held by in-flight entries, per broker account.

    The broker's availableBalance is authoritative but lags: two entries that
    read it before either order lands would both see the same free margin and
    together overcommit the account. So the affordability check and the
    reservation happen under one lock, and every check subtracts what other
    in-flight entries on the same account have reserved. A reservation ends in
    one of three ways:

      release  the order was rejected, failed or never sent: freed at once
      settle   the order was sent: shrinks to the broker-executed margin (a
               partial fill frees the unfilled part) and lingers
               ``settle_seconds`` so the next snapshot can reflect the fill
      expiry   an unsettled reservation older than ``max_age_seconds`` (a
               thread that died mid-entry) is dropped instead of blocking
               the account forever

    Process-local by design: one runtime owns the account (the runtime lease)
    and its bots run on threads of that process. This is about the account,
    not about ``allocation_value``: each trade still has its own allocation.
    """

    def __init__(
        self,
        *,
        settle_seconds: float = 10.0,
        max_age_seconds: float = 120.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, dict[str, Any]] = {}
        self._settle_seconds = float(settle_seconds)
        self._max_age_seconds = float(max_age_seconds)
        self._clock = clock

    def _prune(self, now: float) -> None:
        for token in [t for t, e in self._entries.items() if e["expires_at"] <= now]:
            del self._entries[token]

    def _pending(self, account_key: str) -> tuple[int, float]:
        entries = [e for e in self._entries.values() if e["account"] == account_key]
        return len(entries), sum(e["margin"] for e in entries)

    def pending_margin(self, account_key: str) -> float:
        with self._lock:
            self._prune(self._clock())
            return self._pending(account_key)[1]

    def snapshot(self, account_key: str) -> dict[str, Any]:
        with self._lock:
            self._prune(self._clock())
            count, margin = self._pending(account_key)
        return {"count": count, "margin": round(margin, 8)}

    def reserve(
        self,
        account_key: str,
        *,
        available_balance: float,
        requested_margin: float,
        safety_buffer: float = 1.0,
        min_available: float = 0.0,
    ) -> AccountReservation:
        """Check affordability and reserve, atomically.

        Blocks with ACCOUNT_AVAILABLE_BALANCE_INSUFFICIENT when the free margin
        net of other reservations is under ``min_available``; otherwise reserves
        ``min(requested, free * safety_buffer)`` and reports a reduction as
        MARGIN_INSUFFICIENT.
        """
        requested = max(0.0, float(requested_margin or 0.0))
        available = float(available_balance or 0.0)
        with self._lock:
            now = self._clock()
            self._prune(now)
            _, pending = self._pending(account_key)
            effective = max(0.0, available - pending)
            if effective < float(min_available or 0.0) or requested <= EPSILON:
                return AccountReservation(
                    token=None, account_key=account_key, approved=False,
                    reason=RiskReason.ACCOUNT_AVAILABLE_BALANCE_INSUFFICIENT,
                    requested_margin=requested, reserved_margin=0.0,
                    available_balance=available, pending_margin=pending,
                    effective_available=effective,
                )
            reserved = min(requested, effective * float(safety_buffer))
            token = uuid.uuid4().hex
            self._entries[token] = {
                "account": account_key,
                "margin": reserved,
                "expires_at": now + self._max_age_seconds,
                "settled": False,
            }
        return AccountReservation(
            token=token, account_key=account_key, approved=True,
            reason=(RiskReason.MARGIN_INSUFFICIENT
                    if reserved < requested - EPSILON else RiskReason.APPROVED),
            requested_margin=requested, reserved_margin=reserved,
            available_balance=available, pending_margin=pending,
            effective_available=effective,
        )

    def settle(self, token: str | None, *, executed_margin: float | None = None) -> None:
        """The order was sent. Hold the broker-executed margin briefly, then expire."""
        if not token:
            return
        with self._lock:
            entry = self._entries.get(token)
            if entry is None:
                return
            margin = entry["margin"] if executed_margin is None else min(
                entry["margin"], max(0.0, float(executed_margin))
            )
            if margin <= EPSILON:
                del self._entries[token]
                return
            entry.update(margin=margin, settled=True,
                         expires_at=self._clock() + self._settle_seconds)

    def release(self, token: str | None) -> None:
        """The order never reached the broker. Free the margin now.

        A settled reservation is left to expire: its order exists at the broker.
        """
        if not token:
            return
        with self._lock:
            entry = self._entries.get(token)
            if entry is not None and not entry["settled"]:
                del self._entries[token]


#: The process's account reservations. Bots of one runtime share it.
ACCOUNT_RESERVATIONS = AccountMarginReservations()


# ══════════════════════════════════════════════════════════════════════════
# Observability
# ══════════════════════════════════════════════════════════════════════════


def allocation_variance(
    *,
    configured_trade_allocation: float,
    pre_trade_target_margin: float,
    actual_committed_margin: float,
) -> dict[str, Any]:
    """How one trade's broker-truth margin compares with its OWN allocation.

    A fill that lands at 120.03 against a 120 allocation is a 0.03 execution
    variance on that trade. It is recorded as it is (broker truth wins) and it
    says nothing about the bot's other positions: the comparison is never made
    against aggregate committed margin.
    """
    configured = float(configured_trade_allocation or 0.0)
    actual = float(actual_committed_margin or 0.0)
    variance = actual - configured
    return {
        "allocation_scope": ALLOCATION_SCOPE,
        "configured_trade_allocation": round(configured, 8),
        "pre_trade_target_margin": round(float(pre_trade_target_margin or 0.0), 8),
        "actual_committed_margin": round(actual, 8),
        "allocation_variance": round(variance, 8),
        "allocation_variance_pct": (
            round(variance / configured * 100.0, 6) if configured > EPSILON else None
        ),
        "exceeds_trade_allocation": variance > EPSILON,
    }


def per_trade_allocation_margin(
    allocation_type: str, allocation_value: float, capital_allocation: float,
) -> float:
    """Margin ONE trade may use. Never divided by slots, never net of open positions."""
    kind = str(allocation_type or "").lower()
    value = float(allocation_value or 0.0)
    if value <= 0:
        return 0.0
    if kind in {"fixed_amount", "fixed_usdt", "fixed"}:
        return value
    if kind in {"percent_balance", "percent_equity", "percent"}:
        capital = float(capital_allocation or 0.0)
        return capital * value / 100.0 if capital > 0 else 0.0
    return 0.0


def capital_diagnostics(
    db: Any,
    *,
    bot_instance_id: str,
    allocation_type: str,
    allocation_value: float,
    capital_allocation: float,
    max_open_positions: int,
    portfolio_margin_limit: float = 0.0,
    account_key: str | None = None,
    broker_available_margin: float | None = None,
    reservations: AccountMarginReservations = ACCOUNT_RESERVATIONS,
) -> dict[str, Any]:
    """The capital picture for one bot, with each concept kept separate."""
    per_trade = per_trade_allocation_margin(allocation_type, allocation_value, capital_allocation)
    ledger = CapitalLedger(
        db, bot_instance_id=bot_instance_id, per_trade_allocation=per_trade,
        portfolio_margin_limit=portfolio_margin_limit,
    )
    committed = ledger.committed_margin()
    try:
        open_count: int | None = ledger.open_positions()
    except Exception:
        open_count = None
    pending = reservations.snapshot(account_key) if account_key else {"count": 0, "margin": 0.0}
    affordable = None
    if broker_available_margin is not None:
        affordable = max(0.0, min(per_trade, float(broker_available_margin) - pending["margin"]))
    return {
        "allocation_type": allocation_type,
        "allocation_value": allocation_value,
        "allocation_scope": ALLOCATION_SCOPE,
        "capital_allocation": capital_allocation,
        "capital_allocation_is_aggregate_limit": False,
        "max_open_positions": max_open_positions,
        "open_positions": open_count,
        "position_slot_available": (
            open_count < max_open_positions if open_count is not None else None
        ),
        "pending_entry_reservations": pending,
        "aggregate_committed_margin": committed,
        "broker_available_margin": broker_available_margin,
        "explicit_portfolio_limit": (
            portfolio_margin_limit if portfolio_margin_limit > EPSILON else None
        ),
        "next_trade_configured_allocation": per_trade,
        # fixed_amount_strict: the policy engine warns on risk but does not cap
        # the allocation. Per-candle reductions (HTF bias, risk compression,
        # event influence) apply at decision time and are in each decision.
        "next_trade_risk_adjusted_allocation": per_trade,
        "next_trade_affordable_allocation": affordable,
    }


def margin_for(quantity: float, price: float, leverage: float) -> float:
    """Margin a position ties up. One definition, used everywhere."""
    return abs(float(quantity) * float(price)) / max(1.0, float(leverage or 1.0))
