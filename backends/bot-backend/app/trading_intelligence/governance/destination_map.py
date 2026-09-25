"""Section 24 repository destination map -- SEMANTIC compliance, not a
cosmetic tree match. Each blueprint responsibility maps to the module that
actually owns it; nothing was renamed or duplicated to match a diagram.

Status: PRESENT (implemented) | INTEGRATED (lives in an existing runtime
file) | PARTIAL (only part of the responsibility exists) | MISSING.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Mapping, Tuple

PRESENT, INTEGRATED, PARTIAL, MISSING = "PRESENT", "INTEGRATED", "PARTIAL", "MISSING"
_TI = "app/trading_intelligence"

#: responsibility -> (paths relative to backends/bot-backend, status, note)
RESPONSIBILITY_MAP: Mapping[str, Tuple[Tuple[str, ...], str, str]] = {
    "contracts.instrument": ((f"{_TI}/contracts/instrument.py",), PRESENT, ""),
    "contracts.data_quality": ((f"{_TI}/contracts/data_quality.py",), PRESENT, ""),
    "contracts.market_state": ((f"{_TI}/contracts/market_state.py",), PRESENT, ""),
    "contracts.global_market_state": ((f"{_TI}/contracts/global_market_state.py",
                                       f"{_TI}/market_state/global_state.py",
                                       f"{_TI}/market_state/global_state_store.py"), PRESENT,
                                      "canonical causal, versioned, hashed, tenant/broker-neutral GlobalMarketState "
                                      "built each decision epoch from the epoch's MarketStates "
                                      "(integration/cycle_shadow._global_market_state_stage) with append-only "
                                      "evidence; context only -- consuming it in admission/veto is a governed "
                                      "policy-version change"),
    "contracts.regime": ((f"{_TI}/regime/contracts.py",), PRESENT, ""),
    "contracts.setup": ((f"{_TI}/contracts/setup.py",), PRESENT, ""),
    "contracts.forecast": ((f"{_TI}/contracts/forecast.py",), PRESENT, ""),
    "contracts.economics": ((f"{_TI}/contracts/economics.py", f"{_TI}/contracts/venue_economics.py"), PRESENT, ""),
    "contracts.veto": ((f"{_TI}/contracts/veto.py",), PRESENT, ""),
    "contracts.ranking": ((f"{_TI}/contracts/ranking.py",), PRESENT, ""),
    "contracts.portfolio": ((f"{_TI}/contracts/portfolio.py", f"{_TI}/contracts/portfolio_intel.py",
                             f"{_TI}/contracts/exposure.py"), PRESENT, ""),
    "contracts.trade_plan": ((f"{_TI}/contracts/trade_plan.py",), PRESENT, ""),
    "contracts.position_exit": ((f"{_TI}/contracts/position.py",), PRESENT, ""),
    "market_state.engine": ((f"{_TI}/market_state/engine.py", f"{_TI}/market_state/trend.py",
                             f"{_TI}/market_state/volatility.py", f"{_TI}/market_state/momentum.py",
                             f"{_TI}/market_state/structure.py", f"{_TI}/market_state/liquidity.py",
                             f"{_TI}/market_state/participation.py", f"{_TI}/market_state/derivatives.py",
                             f"{_TI}/market_state/multi_timeframe.py", f"{_TI}/market_state/uncertainty.py"),
                            PRESENT, ""),
    "global_context": ((f"{_TI}/portfolio/factors.py", f"{_TI}/contracts/events.py",
                        f"{_TI}/market_state/global_state.py"), PRESENT,
                       "event-risk context, cross-asset factors and the GlobalMarketState exist; calendar DATA "
                       "freshness is operational (the synced economic_events feed ends 2026-05-29 in the canonical "
                       "DB) and is surfaced explicitly as STALE/UNAVAILABLE, never as 'no event risk'"),
    "regimes": ((f"{_TI}/regime/engine.py", f"{_TI}/regime/policy.py", f"{_TI}/regime/calibration.py"), PRESENT, ""),
    "setups.trend_pullback": ((f"{_TI}/setups/trend_pullback.py",), PRESENT, ""),
    "setups.breakout_expansion": ((f"{_TI}/setups/breakout_expansion.py",), PRESENT, ""),
    "setups.range_reversion": ((f"{_TI}/setups/range_mean_reversion.py",), PRESENT, ""),
    "setups.momentum_continuation": ((f"{_TI}/setups/momentum_continuation.py",), PRESENT, ""),
    "forecasting": ((f"{_TI}/forecast/engine.py", f"{_TI}/forecast/posterior.py", f"{_TI}/forecast/library.py",
                     f"{_TI}/forecast/ood.py"), PRESENT, ""),
    "economics": ((f"{_TI}/economics/canonical.py", f"{_TI}/economics/engine.py", f"{_TI}/venue/cost_model.py"),
                  PRESENT, ""),
    "veto": ((f"{_TI}/veto/engine.py",), PRESENT, ""),
    "ranking": ((f"{_TI}/ranking/engine.py", f"{_TI}/ranking/coordinator.py"), PRESENT, ""),
    "portfolio.exposure": ((f"{_TI}/portfolio/exposure_builder.py",), PRESENT, ""),
    "portfolio.correlations": ((f"{_TI}/portfolio/returns.py", f"{_TI}/portfolio/factors.py"), PRESENT, ""),
    "portfolio.selector": ((f"{_TI}/portfolio/selector.py",), PRESENT, ""),
    "portfolio.reservations": ((f"{_TI}/portfolio/reservation_store.py",), PRESENT, ""),
    "portfolio.policy": ((f"{_TI}/contracts/portfolio_intel.py",), PRESENT, "PortfolioPolicy"),
    "trade_plan": ((f"{_TI}/trade_plan/builder.py", f"{_TI}/trade_plan/validation.py"), PRESENT, ""),
    "position_intelligence": ((f"{_TI}/position/path.py", f"{_TI}/position/forecast.py", f"{_TI}/position/thesis.py",
                               f"{_TI}/position/exit_engine.py", f"{_TI}/position/service.py"), PRESENT, ""),
    "execution_boundary": ((f"{_TI}/execution/boundary.py",), PRESENT, ""),
    "evidence": ((f"{_TI}/evidence/stores.py", f"{_TI}/evidence/lineage.py", f"{_TI}/observability/metrics.py"),
                 PRESENT, ""),
    "integration": ((f"{_TI}/integration/cycle_shadow.py", f"{_TI}/integration/snapshot_adapter.py",
                     f"{_TI}/integration/venue_context.py"), PRESENT, ""),
    "research_certification": ((f"{_TI}/research/certification/pipeline.py", f"{_TI}/research/export.py"), PRESENT, ""),
    "ml_estimators": ((f"{_TI}/ml/contracts.py", f"{_TI}/ml/training.py", f"{_TI}/ml/boundaries.py",
                       f"{_TI}/ml/registry.py", f"{_TI}/ml/promotion.py"), PRESENT, ""),
    "governance_promotion": ((f"{_TI}/governance/phases.py", f"{_TI}/governance/promotion.py"), PRESENT, ""),
    "success_standard": ((f"{_TI}/governance/success_standard.py",), PRESENT, ""),
    # -- existing-file integration points (24.E2) --
    "runner.cycle_integration": (("app/runner/runner.py",), INTEGRATED, "cycle_shadow on_cycle_start/record_symbol/end"),
    "multi_runner.multi_bot": (("app/runner/multi_runner.py",), INTEGRATED,
                               "multi-bot runtime; account-wide portfolio evaluated via cycle_shadow per bot"),
    "orchestrator.hard_risk_entry": (("app/core/trading_orchestrator.py",), INTEGRATED, "process_trade_plan"),
    "market_snapshot": (("app/runner/market_snapshot.py",), INTEGRATED, "immutable MarketSnapshot input"),
    "exchange.interface": (("app/exchange/interface.py",), INTEGRATED, "broker-neutral exchange contract"),
    "replay.engine": (("app/replay/engine.py", "app/replay/historical_provider.py"), INTEGRATED,
                      "causal historical provider reused by the library builder and certification"),
    "research.dataset": (("app/research/dataset.py",), INTEGRATED, "partitions, purge/embargo, holdout guard"),
    "evidence.writers": (("app/evidence/writers.py",), INTEGRATED, "V2 evidence writers; CATI uses evidence/stores"),
    "execution.position_manager": (("app/execution/position_manager.py",), INTEGRATED,
                                   "exit-intent routing target (OFF until promotion)"),
}

#: runtime files that become legacy AFTER promotion -- NOT deleted before M9
LEGACY_AFTER_PROMOTION: Mapping[str, str] = {
    "app/strategy/master_ensemble.py": "V2 alpha; benchmark only from M6, removed at M9",
    "app/strategy/loader.py": "fallback strategy loading -- no V2 fallback from M8",
    "app/strategy/robust_ensemble.py": "V2 expert ensemble",
    "app/strategy/sma_cross.py": "V2 expert", "app/strategy/donchian_breakout.py": "V2 expert",
    "app/strategy/supertrend.py": "V2 expert", "app/strategy/vwap_reversion.py": "V2 expert",
    "app/strategy/squeeze_breakout.py": "V2 expert", "app/strategy/bollinger_reversion.py": "V2 expert",
    "app/strategy/trend_pullback.py": "V2 expert (distinct from the CATI TREND_PULLBACK_V2 specialist)",
    "app/threshold/__init__.py": "confidence threshold -- never CATI alpha admission",
    "app/strategy/strategy_framework.py": "confidence-centric StrategyOutput path",
    "app/ml/scorer.py": "legacy V2 ML entry scorer -- never CATI authority",
}


def verify(root: Path) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    for resp, (paths, status, note) in RESPONSIBILITY_MAP.items():
        missing = [p for p in paths if not (root / p).exists()]
        out[resp] = {"status": MISSING if missing else status, "paths": list(paths), "missing": missing, "note": note}
    return out


def compliance(root: Path) -> Tuple[str, List[str]]:
    res = verify(root)
    gaps = [k for k, v in res.items() if v["status"] in (PARTIAL, MISSING)]
    if any(v["status"] == MISSING for v in res.values()):
        return "FAIL", gaps
    return ("PARTIAL" if gaps else "PASS"), gaps


__all__ = ["RESPONSIBILITY_MAP", "LEGACY_AFTER_PROMOTION", "verify", "compliance", "PRESENT", "INTEGRATED",
           "PARTIAL", "MISSING"]
