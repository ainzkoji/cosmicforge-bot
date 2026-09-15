"""The replay engine (§13.2, §13.8) — the production brain, driven by a clock.

There is deliberately no strategy here, no sizing, no exit logic and no
lifecycle state machine. All of that is the production code, reached through
the production ``PaperRunner``:

    HistoricalMarketDataProvider
      -> MarketSnapshot            the production contract, cut at the clock
      -> Master Ensemble           the production strategy instance
      -> TradingOpportunity
      -> TradingDecisionEngine     the one entry-quality comparison
      -> Risk / CapitalLedger
      -> ExecutionFeasibility
      -> candle claim              replay's idempotency barrier
      -> PaperExecutor
      -> PositionManager           the production lifecycle

What the engine owns is the three things replay legitimately adds: the clock,
an exchange client that can only answer from history, and the identity that
makes the run reproducible.

Two seams were needed in production code to make this work, and both default to
today's behaviour:

* ``TradeExecutor._now_ms()`` — the freshness guards must judge a historical
  candle against the replay timestamp, or every bar is correctly-but-uselessly
  rejected as stale.
* ``PaperBookClient`` (already present from Phase 12) — in paper mode the paper
  book is the position authority, which is exactly what replay needs too.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from app.replay.cost_model import CostModel
from app.replay.fill_models import FillModel, IntrabarPolicy
from app.replay.historical_provider import (
    HistoricalClock,
    HistoricalMarketDataProvider,
    ReplayDataError,
)
from app.replay.identity import ReplayIdentity, dataset_hash
from shared_lib.persistence.evidence_schema import REPLAY

logger = logging.getLogger(__name__)


class ReplayOrderAttempted(AssertionError):
    """A replay tried to reach a broker. It must not be able to."""


class ReplayExchangeClient:
    """Everything the runner asks an exchange, answered from history.

    Market data comes from the provider, cut at the clock. Positions are not
    answered here at all: in paper mode ``PaperBookClient`` wraps this and
    serves them from ``PaperExecutor``, which is the same arrangement the live
    paper runtime uses. Every order-submitting method raises, so a replay
    cannot reach a broker even if a code path tries.
    """

    def __init__(
        self,
        provider: HistoricalMarketDataProvider,
        *,
        symbols: Sequence[str],
        timeframe: str,
        equity: float = 10_000.0,
    ) -> None:
        self._provider = provider
        self.symbols = tuple(s.upper() for s in symbols)
        self.timeframe = timeframe
        self.equity = float(equity)
        self.blocked_calls: list[str] = []

    # ── Market data, always at the clock ────────────────────────────────────

    def klines(self, symbol: str, interval: str | None = None, limit: int = 250, **_: Any):
        rows = self._provider.closed_candles(
            symbol, interval or self.timeframe, limit=limit,
        )
        return [list(r) for r in rows]

    def historical_klines(self, symbol: str, interval: str | None = None, **kw: Any):
        return self.klines(symbol, interval, limit=int(kw.get("limit", 250)))

    def last_price(self, symbol: str) -> float:
        price = self._provider.reference_price(symbol, self.timeframe)
        if price is None:
            raise ReplayDataError(f"{symbol}: no closed candle at the replay clock")
        return price

    def get_prices(self, symbols) -> dict:
        return {s.upper(): self.last_price(s) for s in symbols}

    def mark_price(self, symbol: str) -> dict:
        return {"symbol": symbol.upper(), "markPrice": str(self.last_price(symbol))}

    def book_ticker(self, symbol: str) -> dict:
        price = self.last_price(symbol)
        return {"symbol": symbol.upper(),
                "bidPrice": str(price), "askPrice": str(price),
                "bidQty": "1000", "askQty": "1000"}

    def get_ticker(self, symbol: str) -> dict:
        return {"symbol": symbol.upper(), "lastPrice": str(self.last_price(symbol))}

    def server_time(self) -> int:
        return self._provider.clock.now_ms

    def sync_time(self) -> int:
        return 0

    def ping(self) -> dict:
        return {}

    def test_connection(self) -> dict:
        return {"ok": True, "environment": "replay"}

    # ── Instruments ─────────────────────────────────────────────────────────

    def list_instruments(self):
        from app.models.unified_trading import AssetClass, InstrumentSpec

        return [
            InstrumentSpec(
                symbol_canonical=s, symbol_exchange=s, asset_class=AssetClass.CRYPTO_PERP,
                base_currency=s.replace("USDT", ""), quote_currency="USDT",
                margin_currency="USDT", settlement_currency="USDT",
                contract_size=1, tick_size="0.01", step_size="0.001",
                min_qty="0.001", min_notional="5", price_precision=2,
                qty_precision=3, max_leverage=20,
            )
            for s in self.symbols
        ]

    def get_symbol_filters(self, symbol: str):
        from app.models.unified_trading import SymbolFilters

        return SymbolFilters(min_qty="0.001", step_size="0.001",
                             min_notional="5", tick_size="0.01", contract_size="1")

    def exchange_info(self) -> dict:
        return {"symbols": [{"symbol": s, "status": "TRADING"} for s in self.symbols]}

    def exchange_info_cached(self, ttl_seconds: int = 60) -> dict:
        return self.exchange_info()

    # ── Account ─────────────────────────────────────────────────────────────

    def account(self) -> dict:
        return {
            "totalWalletBalance": str(self.equity),
            "availableBalance": str(self.equity),
            "totalMarginBalance": str(self.equity),
            "totalUnrealizedProfit": "0", "totalMaintMargin": "0",
            "totalInitialMargin": "0", "positions": [],
            "assets": [{"asset": "USDT", "walletBalance": str(self.equity),
                        "availableBalance": str(self.equity)}],
        }

    def account_balance(self) -> dict:
        return self.account()

    def get_account_snapshot(self) -> dict:
        return self.account()

    def position_risk(self, symbol: str | None = None):
        return []

    def position_risk_all(self) -> list:
        return []

    def get_positions(self):
        return []

    def get_position_amt(self, symbol: str) -> float:
        return 0.0

    def get_position_info(self, symbol: str):
        return None

    def open_orders(self, symbol: str | None = None):
        return []

    def get_algo_orders(self, symbol: str, raise_on_error: bool = False) -> list:
        return []

    def user_trades(self, *a, **k) -> list:
        return []

    def income_history(self, *a, **k) -> list:
        return []

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return {"symbol": symbol, "leverage": leverage}

    # ── Anything that would reach a broker ──────────────────────────────────

    def _blocked(self, name: str):
        self.blocked_calls.append(name)
        raise ReplayOrderAttempted(f"a replay must not reach a broker: {name}()")

    def place_order(self, *a, **k): self._blocked("place_order")
    def place_market_order(self, *a, **k): self._blocked("place_market_order")
    def place_protection(self, *a, **k): self._blocked("place_protection")
    def place_stop_market(self, *a, **k): self._blocked("place_stop_market")
    def place_take_profit_market(self, *a, **k): self._blocked("place_take_profit_market")
    def update_protection(self, *a, **k): self._blocked("update_protection")
    def cancel_all_orders(self, *a, **k): self._blocked("cancel_all_orders")
    def close_position_market(self, *a, **k): self._blocked("close_position_market")


# ── Session ─────────────────────────────────────────────────────────────────


@dataclass
class ReplayResult:
    """What one replay produced, and what it can be compared against."""

    replay_id: str
    replay_hash: str
    manifest: dict[str, Any]
    evaluations: int
    decisions: list[dict[str, Any]] = field(default_factory=list)
    fills: list[dict[str, Any]] = field(default_factory=list)
    position_events: list[dict[str, Any]] = field(default_factory=list)
    positions: list[dict[str, Any]] = field(default_factory=list)
    blocked_broker_calls: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def realized_pnl(self) -> float:
        return sum(float(p.get("realized_pnl") or 0.0) for p in self.positions)

    @property
    def total_fees(self) -> float:
        return sum(float(p.get("fees") or 0.0) for p in self.positions)

    def outcome_fingerprint(self) -> str:
        """A hash of everything a determinism check should compare.

        Ids and timestamps are excluded: they are new on every run by design.
        What must be identical is the *decisions, fills, lifecycle and money*.
        """
        import hashlib

        payload = {
            "decisions": [
                {k: d.get(k) for k in (
                    "symbol", "primary_reason", "final_action", "regime",
                    "raw_confidence", "effective_entry_threshold", "buy_score",
                    "sell_score", "quantity", "stop_price", "target_price",
                    "closed_candle_close_time",
                )} for d in self.decisions
            ],
            "fills": [
                {k: f.get(k) for k in (
                    "symbol", "side", "action", "qty", "price", "exit_reason",
                )} for f in self.fills
            ],
            "position_events": [
                {k: e.get(k) for k in (
                    "symbol", "event_type", "quantity", "remaining_qty", "price",
                    "realized_pnl", "stop_price",
                )} for e in self.position_events
            ],
            "positions": [
                {k: p.get(k) for k in (
                    "symbol", "side", "original_qty", "remaining_qty",
                    "realized_qty", "entry_price", "realized_pnl", "fees",
                    "status", "close_reason",
                )} for p in self.positions
            ],
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:32]

    def accounting_residual(self) -> float:
        """open qty - partial closes - final close, summed. Must be 0 (§13.8)."""
        residual = 0.0
        for position in self.positions:
            closed = sum(
                float(e.get("quantity") or 0.0)
                for e in self.position_events
                if e.get("position_id") == position.get("position_id")
                and e.get("event_type") in {
                    "TP1", "PARTIAL_CLOSE", "FINAL_CLOSE", "DAILY_CLOSE",
                    "KILL_SWITCH_CLOSE",
                }
            )
            residual += float(position.get("original_qty") or 0.0) - closed
        return residual


@dataclass(frozen=True)
class RestoredReplayPositionState:
    """Replay restart state reconstructed only from persisted evidence.

    A replay restart may not re-open the original quantity after TP1.  This
    small value object is deliberately evidence-shaped rather than
    PositionManager-shaped so tests can assert the restart contract without
    manufacturing live manager internals.
    """

    position_id: str
    symbol: str
    side: str
    original_qty: float
    remaining_qty: float
    realized_qty: float
    phase: str
    break_even_active: bool
    trailing_active: bool
    stop_price: float | None


def restore_position_state_from_replay_evidence(
    position: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> RestoredReplayPositionState:
    """Reconstruct replay lifecycle state after a simulated process restart.

    The source of truth is the evidence store: the position row and the append
    only lifecycle events.  The function intentionally ignores transient runner
    memory.  If TP1 is the latest economic event, the restored remaining
    quantity is the post-TP1 remainder, not the original size.
    """

    position_id = str(position.get("position_id") or "")
    relevant = [e for e in events if str(e.get("position_id") or "") == position_id]
    if not position_id or not relevant:
        raise ReplayDataError("cannot restore replay position without lifecycle evidence")

    original = float(position.get("original_qty") or 0.0)
    remaining = float(position.get("remaining_qty") or original)
    realized = float(position.get("realized_qty") or 0.0)
    stop_price = position.get("stop_price")
    break_even = False
    trailing = False
    phase = "OPEN"

    for event in relevant:
        event_type = str(event.get("event_type") or "").upper()
        if event.get("remaining_qty") is not None:
            remaining = float(event.get("remaining_qty") or 0.0)
            realized = max(0.0, original - remaining)
        if event.get("stop_price") is not None:
            stop_price = event.get("stop_price")
        if event_type in {"TP1", "PARTIAL_CLOSE"}:
            phase = "TP1_TAKEN"
        elif event_type == "BREAK_EVEN_ACTIVATED":
            break_even = True
            phase = "BREAK_EVEN"
        elif event_type == "TRAILING_ACTIVATED":
            trailing = True
            phase = "RUNNER_TRAILING"
        elif event_type == "STOP_UPDATED":
            if trailing:
                phase = "RUNNER_TRAILING"
        elif event_type in {"FINAL_CLOSE", "DAILY_CLOSE", "KILL_SWITCH_CLOSE"}:
            remaining = 0.0
            realized = original
            phase = "FLAT"

    return RestoredReplayPositionState(
        position_id=position_id,
        symbol=str(position.get("symbol") or ""),
        side=str(position.get("side") or ""),
        original_qty=original,
        remaining_qty=remaining,
        realized_qty=realized,
        phase=phase,
        break_even_active=break_even,
        trailing_active=trailing,
        stop_price=float(stop_price) if stop_price is not None else None,
    )


class ReplaySession:
    """One replay: an identity, a clock, and the production runner."""

    def __init__(
        self,
        db: Any,
        series: Mapping[str, Mapping[str, Sequence[Any]]],
        *,
        bot_instance_id: str,
        symbol: str,
        timeframe: str,
        higher_timeframe: str | None = None,
        capital_budget: float = 10_000.0,
        position_allocation: float = 1_000.0,
        cost_model: CostModel | None = None,
        fill_model: FillModel = FillModel.NEXT_BAR_OPEN,
        intrabar_policy: IntrabarPolicy = IntrabarPolicy.CONSERVATIVE_STOP_FIRST,
        seed: int | None = None,
        dataset_id: str = "inline",
        equity: float = 10_000.0,
        strategy_hook: Any = None,
    ) -> None:
        self.db = db
        self.series = series
        self.bot_instance_id = bot_instance_id
        self.symbol = symbol.upper()
        self.timeframe = timeframe
        self.higher_timeframe = higher_timeframe
        self.capital_budget = capital_budget
        self.position_allocation = position_allocation
        self.cost_model = cost_model or CostModel()
        self.fill_model = fill_model
        self.intrabar_policy = intrabar_policy
        self.seed = seed
        self.dataset_id = dataset_id
        self.equity = equity
        #: Called with the constructed strategy instance. The engine never
        #: replaces the strategy -- §13.2 forbids a replay-only implementation
        #: -- so a caller that needs deterministic market interpretation for a
        #: lifecycle proof adjusts the real ensemble's inputs through this,
        #: exactly as the Phase 12 harness does.
        self.strategy_hook = strategy_hook

        first_close = int(series[self.symbol][timeframe][0][6])
        self.clock = HistoricalClock(now_ms=first_close - 1)
        self.provider = HistoricalMarketDataProvider(series, self.clock)
        self.run_id = uuid.uuid4().hex
        self.runtime_session_id: str | None = None
        self.runner = None
        self.policy = None

    # ── Identity ────────────────────────────────────────────────────────────

    def identity(self) -> ReplayIdentity:
        times = self.provider.evaluation_times(self.symbol, self.timeframe)
        from app.replay.identity import code_revision

        revision, branch, dirty = code_revision()
        return ReplayIdentity(
            dataset_id=self.dataset_id,
            dataset_hash=dataset_hash(self.series),
            symbols=(self.symbol,),
            timeframes=tuple(
                t for t in (self.timeframe, self.higher_timeframe) if t
            ),
            start_ms=times[0], end_ms=times[-1],
            policy_hash=getattr(self.policy, "policy_hash", "unresolved"),
            strategy_id="master_ensemble", strategy_version="1.0.0",
            cost_model_hash=self.cost_model.model_hash,
            fill_model=self.fill_model.value,
            intrabar_policy=self.intrabar_policy.value,
            seed=self.seed, code_revision=revision, branch=branch,
            working_tree_dirty=dirty,
        )

    # ── Construction ────────────────────────────────────────────────────────

    def _ensure_identity_rows(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.db.connect() as conn:
            def insert(table: str, payload: dict) -> None:
                cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
                usable = {k: v for k, v in payload.items() if k in cols}
                conn.execute(
                    f"INSERT OR REPLACE INTO {table} ({','.join(usable)}) "
                    f"VALUES ({','.join('?' * len(usable))})",
                    tuple(usable.values()),
                )

            insert("users", {"id": f"user_{self.bot_instance_id}",
                             "email": f"{self.bot_instance_id}@replay.local",
                             "hashed_password": "!replay-no-login",
                             "is_active": 1, "status": "active",
                             "created_at": now, "updated_at": now})
            insert("broker_accounts", {"id": f"brk_{self.bot_instance_id}",
                                       "user_id": f"user_{self.bot_instance_id}",
                                       "broker_id": "binance", "market_type": "crypto",
                                       "status": "connected", "environment": "demo",
                                       "label": "replay", "active_credential_version": 1,
                                       "created_at": now, "updated_at": now})
            insert("bot_instances", {
                "id": self.bot_instance_id, "user_id": f"user_{self.bot_instance_id}",
                "broker_account_id": f"brk_{self.bot_instance_id}",
                "market_type": "CRYPTO", "strategy_id": "master_ensemble",
                "strategy_version": "1.0.0", "config_id": "__auto_pilot__",
                "risk_profile_id": "__auto_pilot__",
                "symbols_json": json.dumps([self.symbol]),
                "timeframes_json": json.dumps([self.timeframe]),
                "allocation_type": "fixed_amount",
                "allocation_value": self.position_allocation,
                "capital_allocation": self.capital_budget,
                "capital_allocation_type": "fixed_amount",
                "mode": "paper", "status": "replay", "risk_level": "balanced",
                "created_at": now, "updated_at": now, "started_at": now,
            })

    def build(self):
        """Construct the real PaperRunner around the historical provider."""
        from app.core.bot_instance_service import BotInstanceService
        from app.evidence.writers import open_bot_run, open_runtime_session
        from app.exchange.registry import get_instrument_registry
        from app.runner.bot_context import BotRunContext
        from app.runner.effective_policy import resolve_effective_bot_policy
        from app.runner.runner import PaperRunner

        self._ensure_identity_rows()
        self.runtime_session_id = open_runtime_session(
            self.db, database_role="research", database_path=self.db.path,
            process_execution_mode="paper", environment_name="replay",
        )

        service = BotInstanceService(self.db)
        instance = service.get_bot_instance(self.bot_instance_id)
        risk = BotInstanceService.get_risk_profile_preset(instance.risk_level)
        self.policy = resolve_effective_bot_policy(
            instance=instance, broker_environment="demo",
            risk_params=risk, monitor_interval_seconds=10,
        )
        context = BotRunContext.from_effective_policy(self.policy, {
            "api_key": "replay", "api_secret": "replay",
            "broker_type": "binance", "environment": "demo",
            "base_url": "replay://historical",
        })
        context.run_id = self.run_id
        if self.higher_timeframe:
            context.higher_timeframe = self.higher_timeframe

        client = ReplayExchangeClient(
            self.provider, symbols=(self.symbol,), timeframe=self.timeframe,
            equity=self.equity,
        )
        get_instrument_registry().refresh(broker_id="binance", client=client, force=True)

        runner = PaperRunner(client, context=context, effective_policy=self.policy)
        runner.runtime_session_id = self.runtime_session_id
        if self.strategy_hook is not None:
            self.strategy_hook(runner.strategy)
        # The freshness guards must judge a historical candle against the
        # replay timestamp, not against today.
        runner.executor._clock_source = lambda: self.clock.now_ms
        # The broker health monitor compares the exchange's time against local
        # time. A replayed exchange reports the replay clock, so the monitor
        # has to read the same clock or every historical bar looks like drift.
        monitor = getattr(runner.orchestrator, "broker_monitor", None)
        if monitor is not None:
            monitor.clock_source = lambda: self.clock.now_ms
        # Costs are the replay's, not the paper executor's defaults.
        runner.executor.paper_executor.fee_bps = self.cost_model.taker_fee * 10_000
        runner.executor.paper_executor.slippage_bps = self.cost_model.slippage * 10_000

        open_bot_run(
            self.db, run_id=runner.run_id, bot_instance_id=self.bot_instance_id,
            runtime_session_id=self.runtime_session_id,
            user_id=f"user_{self.bot_instance_id}",
            policy_hash=self.policy.policy_hash, provenance=REPLAY,
            execution_mode="paper", broker_environment="demo",
        )
        self.runner = runner
        self.client = client
        return runner

    # ── Running ─────────────────────────────────────────────────────────────

    def run(self, *, start_ms: int | None = None, end_ms: int | None = None) -> ReplayResult:
        if self.runner is None:
            self.build()

        identity = self.identity()
        evaluations = 0
        errors: list[str] = []
        for _close_time in self.provider.step(
            self.symbol, self.timeframe, start_ms=start_ms, end_ms=end_ms
        ):
            try:
                self.runner.run_cycle()
                evaluations += 1
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
                logger.error("[REPLAY] cycle failed at %s: %s", _close_time, exc)

        return self._collect(identity, evaluations, errors)

    def _collect(self, identity: ReplayIdentity, evaluations: int,
                 errors: list[str]) -> ReplayResult:
        def rows(sql: str) -> list[dict[str, Any]]:
            with self.db.connect() as conn:
                return [dict(r) for r in conn.execute(sql, (self.bot_instance_id,))]

        return ReplayResult(
            replay_id=identity.replay_id,
            replay_hash=identity.replay_hash,
            manifest=identity.manifest(),
            evaluations=evaluations,
            decisions=rows(
                "SELECT * FROM trading_decisions WHERE bot_instance_id=? "
                "ORDER BY evaluated_at, symbol"
            ),
            fills=rows(
                "SELECT * FROM trade_fills WHERE bot_instance_id=? ORDER BY id"
            ),
            position_events=rows(
                "SELECT * FROM position_events WHERE bot_instance_id=? "
                "ORDER BY occurred_at, rowid"
            ),
            positions=rows(
                "SELECT * FROM positions WHERE bot_instance_id=? ORDER BY opened_at"
            ),
            blocked_broker_calls=list(self.client.blocked_calls),
            errors=errors,
        )
