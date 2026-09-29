"""CATI runtime order dispatch (Section 25 runtime authority switch, the CATI side).

After the whole-universe cycle has produced immutable TradePlans (``cycle_shadow.trade_plan_stage``), each plan
is ROUTED, never assumed:

    runtime_authority.resolve_order_authority(account scope)
      owner != CATI  -> NOT dispatched; the governing reason is recorded (M0-M5, M6 live, unpromoted M7 live,
                        kill switch, CATI unhealthy, Auto Trading off). The plan stays evidence.
      owner == CATI  -> CATIExecutionBoundary.process_trade_plan, which re-checks governance itself (dual key)
                        and then runs capital readiness, tenancy, instrument revalidation, the EXISTING hard risk
                        and sizing (incl. the 2.5% daily hard-loss cap), adapter support, and the executor.

A dispatch that cannot build its inputs fails closed for that plan. Nothing here ever calls the V2 path: a
failure of CATI halts CATI's new entries and does not hand the entry to V2.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)

NOT_DISPATCHED = "NOT_DISPATCHED"
DISPATCHED = "DISPATCHED"
DISPATCH_INPUT_UNAVAILABLE = "CATI_DISPATCH_INPUT_UNAVAILABLE"


def _authority(runner: Any, *, cati_healthy: bool):
    from app.trading_intelligence.governance.runtime_authority import resolve_order_authority

    ctx = getattr(runner, "context", None)
    # the runner only drives bots whose user started them: a running bot IS the user's Auto Trading switch
    return resolve_order_authority(getattr(runner, "db", None),
                                   broker_account_id=getattr(ctx, "broker_account_id", None),
                                   venue=getattr(ctx, "broker_type", None),
                                   environment=getattr(ctx, "broker_environment", None),
                                   auto_trading_enabled=bool(getattr(runner, "auto_trading_enabled", True)),
                                   cati_healthy=cati_healthy)


def adapter_for(runner: Any, venue: str) -> Any:
    """The account's execution adapter; any venue without a passing contract suite is the fail-closed one."""
    from app.trading_intelligence.execution.adapter import UnvalidatedExecutionAdapter

    broker = str(getattr(getattr(runner, "context", None), "broker_type", "") or "").lower()
    executor = getattr(runner, "executor", None)
    if broker == "binance" and executor is not None:
        from app.trading_intelligence.execution.binance_adapter import BinanceExecutionAdapter

        return BinanceExecutionAdapter(executor, venue=venue, position_manager=getattr(runner, "position_manager", None))
    return UnvalidatedExecutionAdapter(venue)


def boundary_for(runner: Any, venue: str) -> Any:
    from app.exchange.catalog_refresh import VENUE_KEY
    from app.exchange.instruments import InstrumentCatalog
    from app.trading_intelligence.execution.boundary import CATIExecutionBoundary
    from app.trading_intelligence.execution.preflight import SubmissionPreflight

    ctx = runner.context
    broker = str(getattr(ctx, "broker_type", "") or "").lower()
    env = str(getattr(ctx, "broker_environment", "") or "")
    preflight = SubmissionPreflight(catalog=InstrumentCatalog(runner.db), broker=broker,
                                    venue_key=VENUE_KEY.get(broker, broker), catalog_environment=env.upper(),
                                    account_environment=env.lower())
    return CATIExecutionBoundary(orchestrator=runner.orchestrator, adapter=adapter_for(runner, venue), db=runner.db,
                                 account_scope=(getattr(ctx, "user_id", None), getattr(ctx, "broker_account_id", None)),
                                 preflight=preflight, position_manager=getattr(runner, "position_manager", None))


def _inputs(runner: Any, plan: Any, evaluated: Any, capital: Any, now_ms: int) -> Dict[str, Any]:
    from app.execution.tp_sl import calculate_atr
    from app.trading_intelligence.execution.boundary import AccountState
    from app.trading_intelligence.integration.context_adapters import broker_health_from_runner
    from app.trading_intelligence.trade_plan.validation import MarketReference

    symbol = plan.instrument_key.venue_symbol
    interval = getattr(runner, "interval", "15m")
    klines = runner.client.klines(symbol=symbol, interval=interval, limit=250)
    last = klines[-1]
    price, observed = (float(last["close"]), int(last.get("closeTime") or last.get("close_time"))) \
        if isinstance(last, dict) else (float(last[4]), int(last[6]))
    acc = runner.client.account()
    open_positions = len(runner._economic_open_symbols()) if hasattr(runner, "_economic_open_symbols") else 0
    account = AccountState(float(acc.get("totalWalletBalance", 0.0)), float(acc.get("totalMaintMargin", 0.0) or 0.0),
                           float(acc.get("availableBalance", 0.0)), open_positions)
    return dict(market_reference=MarketReference(price, min(observed, now_ms)),
                broker_health=broker_health_from_runner(runner, now_ms),
                venue_capabilities=evaluated.venue_observation.execution_capabilities if evaluated is not None
                and evaluated.venue_observation is not None else None,
                account=account, klines=klines, atr=float(calculate_atr(klines, period=14)) if len(klines) >= 14 else None,
                runtime_session_id=getattr(runner, "runtime_session_id", None), now_ms=now_ms, capital=capital)


def capital_for(plan: Any, capital_view: Any, reservation_status: Optional[str]) -> Any:
    """Section 9.14 readiness of the plan's dry-run capital plan; absent -> None (the boundary fails closed)."""
    from app.trading_intelligence.capital.planner import capital_readiness

    cand = getattr(capital_view, "candidates", {}).get(plan.ranked_opportunity_id) if capital_view is not None else None
    if cand is None or getattr(cand, "capital_plan", None) is None:
        return None
    return capital_readiness(cand.capital_plan, reservation_status=reservation_status)


def dispatch_trade_plans(runner: Any, plan_results: List[Any], *, evaluated: Mapping[str, Any], capital_view: Any = None,
                         reservation_status: Optional[str] = None, cati_healthy: bool = True,
                         now_ms: Optional[int] = None, boundary_factory=None) -> List[Dict[str, Any]]:
    """Route every built plan. Returns one decision per plan; never raises into the cycle."""
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    plans = [r.plan for r in plan_results if getattr(r, "plan", None) is not None]
    if not plans:
        return []
    auth = _authority(runner, cati_healthy=cati_healthy)
    out: List[Dict[str, Any]] = []
    for plan in plans:
        decision = {"trade_plan_id": plan.trade_plan_id, "broker_account_id": plan.broker_account_id,
                    "environment": plan.environment, "authority": auth.to_dict()}
        if not auth.allows("CATI"):
            decision.update(status=NOT_DISPATCHED, reason=auth.reason)
            logger.info("[CATI_AUTHORITY] plan=%s account=%s owner=%s reason=%s: broker submission blocked",
                        plan.trade_plan_id, plan.broker_account_id, auth.owner, auth.reason)
            out.append(decision)
            continue
        try:
            ev = evaluated.get(plan.source_candidate_id)  # keyed by setup_candidate_id (cycle_shadow)
            boundary = (boundary_factory or boundary_for)(runner, plan.venue)
            result = boundary.process_trade_plan(plan, **_inputs(runner, plan, ev,
                                                                 capital_for(plan, capital_view, reservation_status),
                                                                 now))
            decision.update(status=DISPATCHED, boundary_status=result.status, reasons=list(result.reason_codes))
            logger.info("[CATI_AUTHORITY] plan=%s account=%s dispatched: boundary=%s reasons=%s", plan.trade_plan_id,
                        plan.broker_account_id, result.status, ",".join(result.reason_codes) or "-")
        except Exception as exc:  # fail closed for this plan -- never a V2 fallback
            from app.trading_intelligence.integration.errors import record_component_error

            record_component_error("cati_dispatch", exc, cycle_id=plan.cycle_id, bot_instance_id=plan.bot_instance_id,
                                   broker_account_id=plan.broker_account_id)
            decision.update(status=NOT_DISPATCHED, reason=DISPATCH_INPUT_UNAVAILABLE, error=type(exc).__name__)
        out.append(decision)
    return out


__all__ = ["DISPATCHED", "NOT_DISPATCHED", "DISPATCH_INPUT_UNAVAILABLE", "adapter_for", "boundary_for",
           "capital_for", "dispatch_trade_plans"]
