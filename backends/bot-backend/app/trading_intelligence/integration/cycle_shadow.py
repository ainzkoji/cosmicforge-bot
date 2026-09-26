"""Whole-universe CATI cycle shadow (Sections 15.1-15.4, 16), wired into the
live runner. AUTO_ACTIVE_IF_ELIGIBLE (``app.activation.cati``): active by
default; ``CATI_CYCLE_SHADOW_ENABLED=0/false/off`` is an operator override
that switches it OFF (a flag can never switch anything ON past its
prerequisites).

Shape (decision EPOCH = one closed-candle boundary for the bot's timeframe)::

    on_cycle_start(runner)        # opens/continues the epoch; registers every
                                  # flat universe candidate as EXPECTED
    record_symbol(runner, snap)   # per-symbol: evaluate to a terminal state
                                  # (never ranks / reserves / selects)
    on_cycle_end(runner, results) # when EVERY expected symbol is terminal:
                                  # rank the whole batch ONCE, then portfolio-
                                  # select + shadow-reserve + SHADOW TradePlan
                                  # evidence (Section 18); otherwise wait,
                                  # or (epoch rolled over) fail closed

An epoch, not a single runner cycle, is the unit: a broker-universe bot may
defer part of the universe past its per-cycle time budget, and ranking only
the symbols that happened to be evaluated first would reintroduce exactly the
first-come selection bias this layer exists to remove. Nothing here places an
order, reserves a production slot or margin, or alters any V2 decision; every
public function is exception-proof (P9.14).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Optional, Set, Tuple

logger = logging.getLogger(__name__)

ENV_FLAG = "CATI_CYCLE_SHADOW_ENABLED"

_TF_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
          "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "12h": 43_200_000, "1d": 86_400_000}

_lock = threading.RLock()
_coordinator = None
_controller = None
_services: Dict[int, Any] = {}
_epochs: Dict[str, Dict[str, Any]] = {}  # bot_instance_id -> {"epoch": int, "key": tuple, "expected": set}


def is_enabled() -> bool:
    """AUTO_ACTIVE_IF_ELIGIBLE (app.activation.cati.cycle_shadow): active unless an operator set
    ``CATI_CYCLE_SHADOW_ENABLED`` to an explicit off value. Evidence only; never execution."""
    from app.activation.cati import cycle_shadow
    from app.activation.transitions import observe

    return observe(cycle_shadow()).active


def reset_for_tests() -> None:
    global _coordinator, _controller
    with _lock:
        _coordinator = _controller = None
        _services.clear()
        _epochs.clear()


def _get_coordinator():
    global _coordinator
    if _coordinator is None:
        from app.trading_intelligence.ranking.coordinator import CATICycleCoordinator

        _coordinator = CATICycleCoordinator()
    return _coordinator


def _get_controller():
    global _controller
    if _controller is None:
        from app.trading_intelligence.config import load_configured_library
        from app.trading_intelligence.controller.cati_controller import CATIController

        library, _manifest = load_configured_library()
        _controller = CATIController(outcome_library=library)
    return _controller


def _epoch_id(runner: Any, now_ms: int) -> int:
    tf = _TF_MS.get(str(getattr(runner, "interval", "15m")), 900_000)
    return (now_ms // tf) * tf


def _bot_id(runner: Any) -> Optional[str]:
    ctx = getattr(runner, "context", None)
    return getattr(ctx, "bot_instance_id", None) if ctx is not None else None


def _error(component: str, exc: BaseException, runner: Any, *, symbol: Optional[str] = None) -> None:
    """Structured CATI_COMPONENT_ERROR evidence; never raises."""
    try:
        from app.trading_intelligence.integration.errors import record_component_error

        ctx = getattr(runner, "context", None)
        info = _epochs.get(_bot_id(runner) or "")
        cycle = info["key"][1] if info else getattr(runner, "cycle_id", None)
        record_component_error(
            component, exc, cycle_id=cycle,
            bot_instance_id=getattr(ctx, "bot_instance_id", None), broker_account_id=getattr(ctx, "broker_account_id", None),
            symbol=symbol,
        )
    except Exception:
        pass


_ASSET_CLASS = {"CRYPTO": "CRYPTO", "FOREX": "FX", "FX": "FX", "FUTURES": "FUTURES", "EQUITY": "EQUITY"}


def _asset_class(runner: Any, symbol: Optional[str] = None) -> str:
    """Per-INSTRUMENT asset class when the broker universe knows the symbol
    (multi-asset bots: one bot, CRYPTO + FX); otherwise the bot-level class
    exactly as before."""
    ctx = getattr(runner, "context", None)
    if symbol is not None:
        runtime = getattr(runner, "_universe_runtime", None)
        engine = getattr(runtime, "engine", None)
        meta = engine.instrument(symbol) if engine is not None and hasattr(engine, "instrument") else None
        if meta is not None:
            from app.universe.asset_classes import UNDERLYING_TO_CATI

            ut = str(getattr(meta, "underlying_type", None) or "COIN").upper()
            if ut in UNDERLYING_TO_CATI:
                return UNDERLYING_TO_CATI[ut]
    market_type = str(getattr(ctx, "market_type", "CRYPTO") or "CRYPTO").upper()
    if market_type not in _ASSET_CLASS:
        allowed = tuple(getattr(ctx, "allowed_asset_classes", ()) or ())
        return allowed[0] if len(allowed) == 1 else "CRYPTO"
    return _ASSET_CLASS[market_type]


def _session_open(runner: Any, now_ms: int, asset_class: Optional[str] = None) -> bool:
    """Reuses the runtime's canonical session guard (the one PolicyEngine
    uses). A closed session opens no batch -- crypto is always open; FX
    weekends/rollover are not; unknown asset classes are not assumed 24/7
    and simply follow the guard's own default."""
    asset_class = asset_class or _asset_class(runner)
    if asset_class == "CRYPTO":
        return True
    try:
        from app.models.unified_trading import AssetClass
        from app.symbols.market_hours import ForexSessionGuard

        guard_class = {"FX": AssetClass.FOREX_SPOT, "EQUITY": AssetClass.EQUITY_CFD,
                       "FUTURES": AssetClass.COMMODITY_CFD}.get(asset_class, AssetClass.FOREX_SPOT)
        return bool(ForexSessionGuard.is_market_open(guard_class, now_ms))
    except Exception:
        return False  # cannot establish the session: open nothing (fail closed)


def _expected_symbols(runner: Any) -> Set[str]:
    """Flat new-entry candidates: the universe minus held/managed symbols."""
    held = {str(s).upper() for s in getattr(runner, "_universe_open_symbols", set()) or set()}
    out: Set[str] = set()
    for sym in list(getattr(runner, "trade_symbols", []) or []):
        u = str(sym).upper()
        st = getattr(runner, "state", {}).get(sym) if isinstance(getattr(runner, "state", None), dict) else None
        if u in held or (st is not None and getattr(st, "position", None) in ("LONG", "SHORT")):
            continue
        out.add(u)
    return out


def _finalize_epoch(runner: Any, bot: str, note: str) -> None:
    info = _epochs.pop(bot, None)
    if info is None:
        return
    coordinator = _get_coordinator()
    result = coordinator.finalize_bot_cycle(info["key"])
    b = result.batch
    logger.info(
        "[CATI_BATCH] bot=%s epoch=%s note=%s batch_id=%s complete=%s expected=%d completed=%d failed=%s "
        "approved=%d watch=%d rejected=%d ranked=%d reasons=%s",
        bot, info["epoch"], note, b.cycle_batch_id, b.batch_complete, len(b.expected_due_instruments),
        len(b.completed_instruments), ",".join(b.failed_instruments) or "-", len(b.approved_opportunity_ids),
        len(b.watch_ids), len(b.rejected_ids), len(result.ranked), ",".join(b.reason_codes),
    )
    for r in result.ranked:
        logger.info("[CATI_RANK] bot=%s pos=%d instrument=%s side=%s family=%s score=%.4f opp=%s", bot, r.rank_position,
                    r.instrument_key.canonical_symbol, r.side, r.setup_family, r.rank_score, r.economic_opportunity_id)
    if result.ranking_performed and result.ranked:
        _portfolio_stage(runner, result)


def _normalize_rows(rows: Any):
    """Snapshot candles may be dict rows; the portfolio layer wants
    [openTime, o, h, l, c, v, closeTime]. Unparseable => None (=> reload)."""
    out = []
    try:
        for r in rows:
            if isinstance(r, dict):
                g = lambda *k: next(r[x] for x in k if x in r)
                out.append([g("openTime", "open_time"), g("open"), g("high"), g("low"), g("close"), g("volume"),
                            g("closeTime", "close_time")])
            else:
                out.append(list(r))
    except Exception:
        return None
    return out


def _portfolio_stage(runner: Any, result: Any) -> None:
    ctx = getattr(runner, "context", None)
    account = getattr(ctx, "broker_account_id", None)
    if not account:
        logger.info("[CATI_PORTFOLIO] skipped: bot has no broker_account_id")
        return
    from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
    from app.trading_intelligence.portfolio.context import build_portfolio_market_context
    from app.trading_intelligence.portfolio.exposure_builder import build_account_exposure_snapshot
    from app.trading_intelligence.portfolio.service import ShadowAccountPortfolioService

    db = runner.db
    now_ms = int(time.time() * 1000)
    policy = PortfolioPolicy()
    service = _services.get(id(db))
    if service is None:
        service = _services[id(db)] = ShadowAccountPortfolioService(db, policy=policy)

    rows = {ev.instrument: _normalize_rows(ev.candle_rows) for ev in result.evaluations if ev.candle_rows}
    snapshot = build_account_exposure_snapshot(db, account, now_ms, reservation_store=service.store, now_ms=now_ms)
    interval = getattr(runner, "interval", "15m")

    def load(sym: str):
        try:
            return runner.client.klines(symbol=sym, interval=interval, limit=250)
        except Exception:
            return None  # missing history => static-group fallback, recorded

    for rec in snapshot.all_exposures:
        sym = rec.instrument_key.venue_symbol.upper()
        rows.setdefault(sym, None)
    for sym in list(rows):
        if rows[sym] is None:
            rows[sym] = load(sym)
    from app.trading_intelligence.portfolio.factors import FactorModel

    # Only the factors the policy's FactorSet for THIS bot's asset class defines
    # (crypto: configured BTC/ETH series; FX: structural legs need no series).
    classes = {sym: _asset_class(runner, sym) for sym in rows}
    model = FactorModel.from_policy(policy)
    refs = {}
    for asset_class in sorted(set(classes.values()) or {_asset_class(runner)}):
        fs = model.factor_set(asset_class)
        refs.update({d.factor_id: d.canonical_reference for d in (fs.definitions if fs else ()) if d.canonical_reference})
    factors = {fid: (rows.get(ref) or load(ref)) for fid, ref in refs.items()}
    decision_time = max((int(r[6]) for rs in rows.values() if rs for r in rs), default=now_ms)
    market_context = build_portfolio_market_context(rows, decision_time, policy, factor_rows=factors,
                                                    asset_classes=classes)
    evaluated = {o.candidate.setup_candidate_id: o for ev in result.evaluations for o in ev.opportunities}
    _global_market_state_stage(runner, result, decision_time, interval)
    outcome = service.select_and_reserve(
        ranked=result.ranked, broker_account_id=account, bot_instance_id=result.batch.bot_instance_id,
        cycle_id=result.batch.cycle_id, max_open_positions=int(getattr(ctx, "max_open_positions", 0) or 0),
        context=market_context, now_ms=now_ms, evaluated_by_candidate_id=evaluated,
    )
    d = outcome.decision
    logger.info(
        "[CATI_PORTFOLIO] bot=%s account=%s selection=%s selected=%s slots=%d score=%.4f solver=%s reservation=%s/%s reasons=%s",
        d.bot_instance_id, d.broker_account_id, d.portfolio_selection_id, ",".join(d.selected_opportunity_ids) or "-",
        d.available_slots, d.portfolio_score, d.solver, d.reservation_id, d.reservation_status, ",".join(d.reason_codes) or "-",
    )
    trade_plan_stage(db, result, outcome, evaluated, now_ms)
    # Phase 5E/6F: SHADOW capital routing (flag-gated, evidence only, no transfer).
    from app.trading_intelligence.capital.shadow_hook import shadow_capital_routing

    shadow_capital_routing(runner, outcome, evaluated)


def _global_market_state_stage(runner: Any, result: Any, decision_time: int, timeframe: str) -> Any:
    """Section 24: the epoch's GlobalMarketState from the MarketStates it already holds -> append-only
    evidence. Context only (no admission / veto / ranking input). Never raises into the cycle."""
    try:
        from app.activation.cati import global_market_state
        from app.activation.transitions import observe

        if not observe(global_market_state()).active:
            return None
        from app.trading_intelligence.market_state.global_state import build_global_market_state
        from app.trading_intelligence.market_state.global_state_store import GlobalMarketStateStore

        states = [ev.market_state for ev in result.evaluations if getattr(ev, "market_state", None) is not None]
        states += [o.market_state for ev in result.evaluations if getattr(ev, "market_state", None) is None
                   for o in ev.opportunities]
        event_state = event_observed_at = None
        try:
            from app.trading_intelligence.integration.context_adapters import build_event_risk_context

            ctx = build_event_risk_context(runner.db, decision_time)
            event_state = getattr(getattr(ctx, "source_state", None), "value", None) or getattr(ctx, "source_state", None)
            # the calendar read time; sync staleness is already folded into source_state (STALE) upstream
            event_observed_at = getattr(ctx, "as_of", None)
        except Exception:
            event_state = event_observed_at = None
        gms = build_global_market_state(states, decision_time=decision_time, timeframe=timeframe,
                                        event_source_state=event_state, event_observed_at=event_observed_at)
        from app.trading_intelligence.market_state.crypto_context import build_crypto_context
        contexts = {s.instrument_key.canonical_symbol: build_crypto_context(s, as_of_ms=decision_time).to_dict()
                    for s in sorted(states, key=lambda x: (x.instrument_key.venue, x.market_state_id))
                    if s.instrument_key.asset_class == "CRYPTO"}
        b = result.batch
        GlobalMarketStateStore(runner.db).append(gms, cycle_id=b.cycle_id, bot_instance_id=b.bot_instance_id,
                                                 broker_account_id=b.broker_account_id, asset_contexts=contexts)
        rr = gms.component("risk_regime")
        logger.info("[CATI_GLOBAL_STATE] bot=%s cycle=%s state=%s regime=%s inputs=%d classes=%s",
                    b.bot_instance_id, b.cycle_id, gms.global_state_id,
                    rr.label if rr.status == "AVAILABLE" else f"UNAVAILABLE:{rr.reason}",
                    len(gms.input_market_state_ids), ",".join(gms.asset_classes) or "-")
        return gms
    except Exception as exc:
        _error("cycle_shadow.global_market_state", exc, runner)
        return None


def trade_plan_stage(db: Any, result: Any, outcome: Any, evaluated: Dict[str, Any], now_ms: int) -> list:
    """Section 18: one immutable SHADOW TradePlan per portfolio-selected,
    reservation-protected opportunity -- then STOP. No hard-risk call, no
    order, no slot/margin/capital mutation; the reservation stays RESERVED."""
    from app.trading_intelligence.trade_plan.builder import TenantContext, TradePlanBuilder

    d = outcome.decision
    if not d.selected_opportunity_ids:
        return []
    if not d.is_reserved or outcome.reservation is None:
        logger.info("[CATI_TRADEPLAN] bot=%s no plans: reservation=%s", d.bot_instance_id, d.reservation_status)
        return []
    store = None
    try:
        from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore

        store = TradePlanEvidenceStore(db) if db is not None else None
    except Exception as exc:  # schema missing: plans are still logged, never silently dropped
        logger.info("[CATI_TRADEPLAN] evidence store unavailable (%s)", type(exc).__name__)
    b = result.batch
    tenant = TenantContext(broker_account_id=d.broker_account_id, bot_instance_id=d.bot_instance_id,
                           cycle_id=d.cycle_id, user_id=b.user_id, run_id=b.run_id)
    builder = TradePlanBuilder()
    ranked = {r.ranked_opportunity_id: r for r in result.ranked}
    results = []
    for rid in d.selected_opportunity_ids:
        r = ranked.get(rid)
        ev = evaluated.get(r.setup_candidate_id) if r is not None else None
        if ev is None:
            continue
        res = builder.build_from_evaluated(ranked=r, ranking_batch=b, portfolio_decision=d,
                                           reservation=outcome.reservation, evaluated=ev, tenant=tenant, now_ms=now_ms)
        results.append(res)
        plan = res.plan
        logger.info("[CATI_TRADEPLAN] bot=%s opp=%s status=%s plan=%s expiry=%s reasons=%s", d.bot_instance_id,
                    r.economic_opportunity_id, res.status, plan.trade_plan_id if plan else "-",
                    plan.plan_expiry_time if plan else "-", ",".join(res.reason_codes) or "-")
        if plan is not None and store is not None:
            store.append(plan)
            try:  # Section 21: the canonical upstream evidence (OOD / forecast / veto) behind this plan
                from app.trading_intelligence.evidence.stores import DecisionEvidenceStore

                DecisionEvidenceStore(db).append(ev, broker_account_id=plan.broker_account_id, user_id=plan.user_id,
                                                 bot_instance_id=plan.bot_instance_id)
            except Exception as exc:  # never silent: the export then reports UPSTREAM_EVIDENCE_UNAVAILABLE
                from app.trading_intelligence.observability.logging import record_stage_error

                record_stage_error("cycle_shadow.decision_evidence", "EVIDENCE", exc, db=db, cycle_id=plan.cycle_id,
                                   user_id=plan.user_id, broker_account_id=plan.broker_account_id,
                                   bot_instance_id=plan.bot_instance_id)
    return results


# -- public, exception-proof hooks -------------------------------------------------------
def on_cycle_start(runner: Any) -> None:
    if not is_enabled():
        return
    try:
        bot = _bot_id(runner)
        if not bot or getattr(runner, "_universe_runtime", None) is None:
            return  # whole-universe batching only applies to broker-universe bots
        with _lock:
            now_ms = int(time.time() * 1000)
            epoch = _epoch_id(runner, now_ms)
            info = _epochs.get(bot)
            if info is not None and info["epoch"] != epoch:
                # The boundary rolled over before every symbol was evaluated: fail closed.
                _finalize_epoch(runner, bot, "EPOCH_ROLLED_OVER")
                info = None
            coordinator = _get_coordinator()
            # Per-instrument session gating: a multi-asset bot keeps evaluating
            # crypto while FX is closed; a single-class bot behaves as before.
            candidates = _expected_symbols(runner)
            open_cache: Dict[str, bool] = {}

            def _open(sym: str) -> bool:
                ac = _asset_class(runner, sym)
                if ac not in open_cache:
                    open_cache[ac] = _session_open(runner, now_ms, ac)
                return open_cache[ac]

            expected = {s for s in candidates if _open(s)}
            if info is None and not expected and (candidates or not _session_open(runner, now_ms)):
                logger.info("[CATI_BATCH] bot=%s session closed for %s: no batch opened", bot,
                            ",".join(sorted(open_cache)) or _asset_class(runner))
                return
            if info is None:
                ctx = runner.context
                key = coordinator.begin_bot_cycle(
                    bot_instance_id=bot, cycle_id=f"epoch_{epoch}", cycle_time_ms=epoch,
                    user_id=getattr(ctx, "user_id", None), broker_account_id=getattr(ctx, "broker_account_id", None),
                    run_id=str(getattr(runner, "run_id", "") or ""), universe_version="broker_universe",
                    universe_symbols=sorted(expected),
                )
                info = _epochs[bot] = {"epoch": epoch, "key": key, "expected": set()}
            for sym in expected - info["expected"]:
                coordinator.mark_due(info["key"], sym)
            info["expected"] |= expected
    except Exception as exc:
        _error("cycle_shadow.on_cycle_start", exc, runner)


def record_symbol(runner: Any, snapshot: Any, symbol: str, *, venue: str, source: str) -> None:
    """Evaluate one due symbol to a terminal state. Never ranks/reserves."""
    if not is_enabled():
        return
    try:
        bot = _bot_id(runner)
        info = _epochs.get(bot) if bot else None
        if info is None or str(symbol).upper() not in info["expected"]:
            return  # held/managed symbol or not part of the batch
        ctx = runner.context
        from app.trading_intelligence.integration.context_adapters import (
            build_event_risk_context, system_context_from_runner,
        )

        from app.trading_intelligence.integration.venue_context import venue_context_from_runner

        now_ms = int(time.time() * 1000)
        # Canonical runtime broker health (circuit breaker + quarantine flag),
        # scoped to this bot's broker account -- never left UNKNOWN by omission.
        system_context = system_context_from_runner(runner, now_ms=now_ms)
        evaluation = _get_controller().evaluate_symbol(
            snapshot=snapshot, venue=venue, source=source, asset_class=_asset_class(runner, str(symbol)),
            event_context=build_event_risk_context(runner.db, now_ms, venue=venue),
            system_context=system_context,
            user_id=getattr(ctx, "user_id", None), broker_account_id=getattr(ctx, "broker_account_id", None),
            bot_instance_id=bot, run_id=str(getattr(runner, "run_id", "") or ""), cycle_id=getattr(runner, "cycle_id", None),
            # Section 17: venue/account cost evidence, captured only if candidates exist
            venue_context=lambda: venue_context_from_runner(
                runner, str(symbol), broker_health=system_context.broker_health),
            require_venue_economics=True,  # the whole-universe path is the canonical, certifiable one
        )
        _get_coordinator().record_symbol_evaluation(info["key"], evaluation)
    except Exception as exc:
        _error("cycle_shadow.record_symbol", exc, runner, symbol=str(symbol))
        try:
            bot = _bot_id(runner)
            info = _epochs.get(bot) if bot else None
            if info is not None:
                _get_coordinator().record_symbol_failure(info["key"], str(symbol), f"{type(exc).__name__}: {exc}")
        except Exception:
            pass


def on_cycle_end(runner: Any, results: Dict[str, Any], deferred: Tuple[str, ...] = ()) -> None:
    if not is_enabled():
        return
    try:
        bot = _bot_id(runner)
        with _lock:
            info = _epochs.get(bot) if bot else None
            if info is None:
                return
            coordinator = _get_coordinator()
            for sym, res in (results or {}).items():
                if str(sym).upper() in info["expected"] and isinstance(res, dict) and str(res.get("decision")) == "ERROR":
                    coordinator.record_symbol_failure(info["key"], sym, str(res.get("reason_code") or res.get("error") or "SYMBOL_ERROR"))
            cyc = coordinator._open.get(info["key"])
            terminal = set(cyc.records) if cyc is not None else set()
            if info["expected"] and info["expected"] <= terminal:
                _finalize_epoch(runner, bot, "ALL_TERMINAL")
    except Exception as exc:
        _error("cycle_shadow.on_cycle_end", exc, runner)


__all__ = ["ENV_FLAG", "is_enabled", "reset_for_tests", "on_cycle_start", "record_symbol", "on_cycle_end",
           "trade_plan_stage"]
