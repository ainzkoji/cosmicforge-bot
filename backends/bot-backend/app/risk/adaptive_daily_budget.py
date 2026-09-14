from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from enum import Enum
from statistics import median
from typing import Any, Iterable
from zoneinfo import ZoneInfo


POLICY_VERSION = "adaptive_daily_risk_budget/1.0.0"


class DailyRiskState(str, Enum):
    NORMAL = "NORMAL"
    CAUTION = "CAUTION"
    DEFENSIVE = "DEFENSIVE"
    HARD_STOP = "HARD_STOP"


class DailyRiskReason(str, Enum):
    NORMAL = "DAILY_RISK_NORMAL"
    CAUTION = "DAILY_RISK_CAUTION"
    DEFENSIVE = "DAILY_RISK_DEFENSIVE"
    BUDGET_EXHAUSTED = "DAILY_RISK_BUDGET_EXHAUSTED"
    HARD_CAP_REACHED = "DAILY_HARD_EQUITY_CAP_REACHED"
    DATA_INSUFFICIENT = "DAILY_RISK_DATA_INSUFFICIENT"
    RESERVATION_CONFLICT = "DAILY_RISK_RESERVATION_CONFLICT"


@dataclass(frozen=True)
class AdaptiveDailyRiskPolicy:
    max_daily_loss_pct: float = 0.025
    daily_r_budget: float = 1.5
    minimum_history_trades: int = 30
    risk_lookback_trades: int = 100
    risk_lookback_days: int = 45
    minimum_budget_usdt: float = 6.0
    maximum_budget_usdt: float = 24.0
    caution_consumption_pct: float = 0.50
    defensive_consumption_pct: float = 0.80
    performance_factor_min: float = 0.50
    performance_factor_max: float = 1.00
    volatility_factor_min: float = 0.60
    volatility_factor_max: float = 1.00
    drawdown_factor_min: float = 0.40
    drawdown_factor_max: float = 1.00
    timezone_name: str = "Europe/Rome"
    fee_slippage_buffer_pct: float = 0.001

    def policy_hash(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AdaptiveDailyRiskInputs:
    bot_instance_id: str
    risk_date: date
    day_open_equity: float
    current_equity: float
    realized_pnl_today: float
    fees_today: float = 0.0
    funding_today: float = 0.0
    realized_r_today: float = 0.0
    initial_risk_history_usdt: tuple[float, ...] = ()
    recent_r_history: tuple[float, ...] = ()
    account_drawdown_pct: float = 0.0
    volatility_stress: float = 0.0
    market_regime: str | None = None
    policy_effective: bool = True


@dataclass(frozen=True)
class AdaptiveDailyRiskDecision:
    risk_date: str
    risk_timezone: str
    day_open_equity: float
    current_equity: float
    realized_pnl_today: float
    realized_r_today: float
    fees_today: float
    funding_today: float
    typical_trade_risk_usdt: float
    hard_daily_cap_usdt: float
    base_adaptive_budget_usdt: float
    performance_factor: float
    volatility_factor: float
    drawdown_factor: float
    effective_daily_budget_usdt: float
    effective_daily_budget_r: float
    risk_budget_consumed_usdt: float
    risk_budget_consumed_pct: float
    reserved_risk_usdt: float
    remaining_daily_risk_usdt: float
    daily_risk_state: str
    policy_version: str
    policy_hash: str
    decision_reason: str
    data_sufficient: bool
    policy_effective: bool

    def as_policy_context(self) -> dict[str, Any]:
        return asdict(self)


class AdaptiveDailyRiskBudgetEngine:
    """Authoritative adaptive daily risk budget and reservation component.

    The engine works in net economic loss/risk units, not margin.  It freezes
    the hard cap to day-opening equity supplied by the caller and can tighten,
    but not expand, the budget intraday.
    """

    def __init__(
        self,
        policy: AdaptiveDailyRiskPolicy | None = None,
        *,
        db: Any | None = None,
    ) -> None:
        self.policy = policy or AdaptiveDailyRiskPolicy()
        self._lock = threading.RLock()
        self._reservations: dict[tuple[str, str, str], float] = {}
        self.db = db

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.policy.timezone_name)

    def risk_date_for(self, ts: datetime | None = None) -> date:
        now = ts or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now.astimezone(self.timezone).date()

    def evaluate(self, inputs: AdaptiveDailyRiskInputs) -> AdaptiveDailyRiskDecision:
        p = self.policy
        hard_cap = max(0.0, float(inputs.day_open_equity) * float(p.max_daily_loss_pct))
        history = self._bounded_positive(inputs.initial_risk_history_usdt, p.risk_lookback_trades)
        data_sufficient = len(history) >= p.minimum_history_trades
        if data_sufficient:
            typical = self._robust_typical_risk(history)
            reason = DailyRiskReason.NORMAL
        else:
            typical = p.minimum_budget_usdt / max(p.daily_r_budget, 1e-9)
            reason = DailyRiskReason.DATA_INSUFFICIENT

        base_budget = typical * p.daily_r_budget
        performance_factor = self._performance_factor(inputs.recent_r_history)
        volatility_factor = self._volatility_factor(inputs.volatility_stress, inputs.market_regime)
        drawdown_factor = self._drawdown_factor(inputs.account_drawdown_pct)
        adaptive_budget = base_budget * performance_factor * volatility_factor * drawdown_factor
        effective_budget = min(hard_cap, p.maximum_budget_usdt, max(p.minimum_budget_usdt, adaptive_budget))
        if not data_sufficient:
            effective_budget = min(effective_budget, p.minimum_budget_usdt, hard_cap or p.minimum_budget_usdt)

        consumed = max(0.0, -float(inputs.realized_pnl_today))
        reserved = self.reserved_risk(inputs.bot_instance_id, inputs.risk_date)
        remaining = max(0.0, effective_budget - consumed - reserved)
        consumed_pct = 1.0 if effective_budget <= 0 else min(1.0, (consumed + reserved) / effective_budget)

        state = DailyRiskState.NORMAL
        if consumed >= hard_cap > 0:
            state = DailyRiskState.HARD_STOP
            reason = DailyRiskReason.HARD_CAP_REACHED
        elif remaining <= 1e-9 or consumed_pct >= 1.0:
            state = DailyRiskState.HARD_STOP
            reason = DailyRiskReason.BUDGET_EXHAUSTED
        elif consumed_pct >= p.defensive_consumption_pct:
            state = DailyRiskState.DEFENSIVE
            if reason is DailyRiskReason.NORMAL:
                reason = DailyRiskReason.DEFENSIVE
        elif consumed_pct >= p.caution_consumption_pct:
            state = DailyRiskState.CAUTION
            if reason is DailyRiskReason.NORMAL:
                reason = DailyRiskReason.CAUTION

        if not inputs.policy_effective and state is not DailyRiskState.HARD_STOP:
            reason = DailyRiskReason.DATA_INSUFFICIENT

        decision = AdaptiveDailyRiskDecision(
            risk_date=str(inputs.risk_date),
            risk_timezone=p.timezone_name,
            day_open_equity=float(inputs.day_open_equity),
            current_equity=float(inputs.current_equity),
            realized_pnl_today=float(inputs.realized_pnl_today),
            realized_r_today=float(inputs.realized_r_today),
            fees_today=float(inputs.fees_today),
            funding_today=float(inputs.funding_today),
            typical_trade_risk_usdt=float(typical),
            hard_daily_cap_usdt=float(hard_cap),
            base_adaptive_budget_usdt=float(base_budget),
            performance_factor=float(performance_factor),
            volatility_factor=float(volatility_factor),
            drawdown_factor=float(drawdown_factor),
            effective_daily_budget_usdt=float(effective_budget),
            effective_daily_budget_r=float(effective_budget / typical) if typical > 0 else 0.0,
            risk_budget_consumed_usdt=float(consumed),
            risk_budget_consumed_pct=float(consumed_pct),
            reserved_risk_usdt=float(reserved),
            remaining_daily_risk_usdt=float(remaining),
            daily_risk_state=state.value,
            policy_version=POLICY_VERSION,
            policy_hash=p.policy_hash(),
            decision_reason=reason.value,
            data_sufficient=data_sufficient,
            policy_effective=bool(inputs.policy_effective),
        )
        self.persist_decision(inputs.bot_instance_id, decision)
        return decision

    def reserve(self, bot_instance_id: str, risk_date: date, reservation_id: str, planned_risk_usdt: float, decision: AdaptiveDailyRiskDecision) -> bool:
        planned = float(planned_risk_usdt)
        if planned <= 0:
            return True
        with self._lock:
            available = float(decision.remaining_daily_risk_usdt)
            existing = self._reservations.get((bot_instance_id, str(risk_date), reservation_id), 0.0)
            if planned - existing > available + 1e-9:
                return False
            self._reservations[(bot_instance_id, str(risk_date), reservation_id)] = planned
            return True

    def release(self, bot_instance_id: str, risk_date: date, reservation_id: str) -> None:
        with self._lock:
            self._reservations.pop((bot_instance_id, str(risk_date), reservation_id), None)

    def settle_partial(self, bot_instance_id: str, risk_date: date, reservation_id: str, actual_risk_usdt: float) -> None:
        with self._lock:
            self._reservations[(bot_instance_id, str(risk_date), reservation_id)] = max(0.0, float(actual_risk_usdt))

    def settle_full(self, bot_instance_id: str, risk_date: date, reservation_id: str) -> None:
        self.release(bot_instance_id, risk_date, reservation_id)

    def reserved_risk(self, bot_instance_id: str, risk_date: date) -> float:
        key_prefix = (bot_instance_id, str(risk_date))
        with self._lock:
            return sum(v for (bot, day, _), v in self._reservations.items() if (bot, day) == key_prefix)

    def persist_decision(self, bot_instance_id: str, decision: AdaptiveDailyRiskDecision) -> None:
        if self.db is None:
            return
        try:
            conn_ctx = self.db.connect() if hasattr(self.db, "connect") else self.db
            if hasattr(conn_ctx, "__enter__"):
                with conn_ctx as conn:
                    self._persist(conn, bot_instance_id, decision)
            else:
                self._persist(conn_ctx, bot_instance_id, decision)
        except Exception:
            return

    def _persist(self, conn: sqlite3.Connection, bot_instance_id: str, decision: AdaptiveDailyRiskDecision) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS adaptive_daily_risk_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_instance_id TEXT NOT NULL,
                risk_date TEXT NOT NULL,
                risk_timezone TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                policy_hash TEXT NOT NULL,
                daily_risk_state TEXT NOT NULL,
                decision_reason TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO adaptive_daily_risk_decisions(
                bot_instance_id, risk_date, risk_timezone, policy_version,
                policy_hash, daily_risk_state, decision_reason, payload_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                bot_instance_id,
                decision.risk_date,
                decision.risk_timezone,
                decision.policy_version,
                decision.policy_hash,
                decision.daily_risk_state,
                decision.decision_reason,
                json.dumps(asdict(decision), sort_keys=True),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def _robust_typical_risk(self, values: list[float]) -> float:
        values = sorted(values)
        med = median(values)
        cap = self._percentile(values, 90)
        trimmed = [min(v, cap) for v in values]
        bounded = median(trimmed)
        return max(0.0, float(bounded if bounded > 0 else med))

    def _performance_factor(self, recent_r: Iterable[float]) -> float:
        vals = [float(v) for v in recent_r if v is not None]
        if len(vals) < 10:
            return 1.0
        expectancy = sum(vals[-50:]) / min(len(vals), 50)
        if expectancy >= 0.25:
            return min(self.policy.performance_factor_max, 1.0)
        if expectancy >= 0.0:
            return 0.90
        if expectancy >= -0.25:
            return 0.75
        return self.policy.performance_factor_min

    def _volatility_factor(self, stress: float, regime: str | None) -> float:
        regime_s = str(regime or "").upper()
        stress_f = max(0.0, min(1.0, float(stress or 0.0)))
        factor = 1.0 - 0.4 * stress_f
        if regime_s in {"HIGH_VOLATILITY", "VOLATILITY_SPIKE", "LOW_VOLATILITY_CHOP"}:
            factor = min(factor, 0.75)
        return max(self.policy.volatility_factor_min, min(self.policy.volatility_factor_max, factor))

    def _drawdown_factor(self, drawdown_pct: float) -> float:
        dd = max(0.0, float(drawdown_pct or 0.0))
        if dd >= 8.0:
            return self.policy.drawdown_factor_min
        if dd >= 5.0:
            return 0.60
        if dd >= 3.0:
            return 0.75
        if dd >= 1.5:
            return 0.90
        return 1.0

    @staticmethod
    def _bounded_positive(values: Iterable[float], limit: int) -> list[float]:
        out = [float(v) for v in values if v is not None and float(v) > 0]
        return out[-int(limit):]

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        if len(values) == 1:
            return values[0]
        pos = (len(values) - 1) * percentile / 100.0
        lo = int(pos)
        hi = min(lo + 1, len(values) - 1)
        frac = pos - lo
        return values[lo] * (1.0 - frac) + values[hi] * frac


def planned_initial_risk_usdt(
    *,
    quantity: float,
    entry_price: float,
    stop_price: float,
    side: str = "LONG",
    fee_slippage_buffer_pct: float = 0.001,
    multiplier: float = 1.0,
) -> float:
    qty = abs(float(quantity or 0.0))
    entry = float(entry_price or 0.0)
    stop = float(stop_price or 0.0)
    if qty <= 0 or entry <= 0 or stop <= 0:
        return 0.0
    base = abs(entry - stop) * qty * float(multiplier)
    buffer = abs(entry * qty * float(multiplier)) * max(0.0, float(fee_slippage_buffer_pct))
    return max(0.0, base + buffer)
