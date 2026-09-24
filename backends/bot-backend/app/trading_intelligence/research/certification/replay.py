"""Canonical certification replay (Sections 22.11-22.13).

ONE causal pass over the pre-holdout chronology drives the SAME CATI code the
shadow runtime uses -- ``CATIController.evaluate_symbol`` (MarketSnapshot ->
MarketState -> Regime -> Setups -> Forecast -> canonical venue economics ->
admission -> veto) and ``CATICycleCoordinator`` whole-universe ranking. There
is no research copy of any of it.

Walk-forward: fold ``k`` is evaluated with a forecast library built only from
rows whose label window ends before the fold (purged, embargoed) -- the
library rows come from the canonical library builder (``build_library``),
built once and filtered per fold. The HOLDOUT candles are not even loaded by
the pre-holdout pass.

Outcomes: ``gross_R`` is the MARKET path from the research labeler
(same-bar TP/SL => stop first). Net R subtracts the canonical Section 17
venue cost estimate ONCE. Cost stress re-prices ONLY the cost term (and
re-runs admission/veto under the stressed venue policy); the market outcome
never changes with the multiplier.

Portfolio selection and TradePlan building are ACCOUNT-STATEFUL (slots,
margin, reservations, exposure) and have no historical equivalent; they are
recorded as ``NOT_REPLAYED`` and measured by the forward demo.
"""
from __future__ import annotations

import dataclasses
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import CERTIFICATION_REPLAY_VERSION

from .splits import ChronologyPlan, plan_chronology, purge_for_window

#: veto reasons that depend on RUNTIME context a historical replay cannot
#: supply (no broker, no live calendar); recorded, never fabricated
RUNTIME_ONLY_VETO_REASONS = frozenset({
    "BROKER_HEALTH_NOT_PROVIDED", "BROKER_DEGRADED", "BROKER_UNAVAILABLE", "EVENT_SOURCE_UNAVAILABLE",
    "EVENT_SOURCE_STALE", "MAINTENANCE_UNKNOWN", "MAINTENANCE_STALE", "SYSTEM_HEALTH_NOT_PROVIDED",
})

#: versioned, PREDECLARED parameter-neighbor plan: robustness diagnostics only.
#: A neighbor is NEVER promoted or substituted for the frozen policy.
NEIGHBOR_PLAN_VERSION = "1.0.0"
NEIGHBOR_PLAN: Tuple[Tuple[str, str, float], ...] = (
    ("edge_minus", "minimum_conservative_edge_r", -0.05),
    ("edge_plus", "minimum_conservative_edge_r", +0.05),
    ("ev_minus", "minimum_ev_net_r", -0.02),
    ("ev_plus", "minimum_ev_net_r", +0.02),
)

APPROVE = "APPROVE_FOR_RANKING"
ADMISSIBLE = "ECONOMICALLY_ADMISSIBLE"
NOT_REPLAYED = {"portfolio_selection": "NOT_REPLAYED_ACCOUNT_STATEFUL",
                "trade_plan": "NOT_REPLAYED_ACCOUNT_STATEFUL"}


@dataclass(frozen=True)
class ReplayConfig:
    symbols: Tuple[str, ...]
    timeframe: str
    label_horizon_bars: int = 48
    higher_timeframe: Optional[str] = None
    snapshot_limit: int = 250
    warmup_bars: int = 250
    venue: str = "binance"
    asset_class: str = "CRYPTO"
    folds: int = 6
    holdout_fraction: float = 0.10
    embargo_bars: int = 0
    cost_multipliers: Tuple[float, ...] = (1.0, 1.5, 2.0)
    source_name: str = "historical_candles"

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        d["symbols"] = sorted(self.symbols)
        d["cost_multipliers"] = list(self.cost_multipliers)
        d["neighbor_plan"] = {"version": NEIGHBOR_PLAN_VERSION, "variants": [list(v) for v in NEIGHBOR_PLAN]}
        d["replay_version"] = CERTIFICATION_REPLAY_VERSION
        return d

    @property
    def config_hash(self) -> str:
        return stable_hash(self.to_dict())


@dataclass
class ReplayResult:
    records: List[Dict[str, Any]]
    plan: ChronologyPlan
    integrity: Dict[str, Any]
    counts: Dict[str, Any]
    library: Dict[str, Any]
    replay_hash: str
    config_hash: str
    phase: str
    library_rows: Tuple[Any, ...] = field(default=(), repr=False)
    library_template: Any = field(default=None, repr=False)


# ------------------------------------------------------------------ helpers
def _tf_ms(tf: str) -> int:
    from app.replay.historical_provider import TIMEFRAME_MS

    return TIMEFRAME_MS[tf]


def decision_span(series: Mapping[str, Mapping[str, Sequence[Any]]], cfg: ReplayConfig) -> Tuple[int, int]:
    """First decision after the warmup, last decision whose label horizon is observable."""
    tf = _tf_ms(cfg.timeframe)
    closes = sorted({int(r[6]) for s in series.values() for r in s.get(cfg.timeframe, ())})
    if len(closes) < cfg.warmup_bars + cfg.label_horizon_bars + 2:
        raise ValueError("not enough candles for warmup + label horizon")
    return closes[cfg.warmup_bars], closes[-1] - cfg.label_horizon_bars * tf


def truncate_series(series, end_ms: int):
    """Everything strictly after ``end_ms`` is removed (holdout never loaded)."""
    return {s: {tf: [r for r in rows if int(r[6]) <= end_ms] for tf, rows in tfs.items()} for s, tfs in series.items()}


def _session(ts: int) -> str:
    h = datetime.fromtimestamp(ts / 1000, timezone.utc).hour
    return "ASIA_00_08" if h < 8 else ("EUROPE_08_16" if h < 16 else "AMERICAS_16_24")


def _month(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m")


def volume_proxy_bucket(candles: Sequence[Any], lookback: int = 96) -> str:
    """PAST-ONLY relative-volume bucket: the decision bar's volume against the
    median of the preceding bars INSIDE the causal snapshot. Never reads a
    bar after the decision."""
    if len(candles) < 10:
        return "UNKNOWN"
    vols = [float(c[5]) for c in candles[-(lookback + 1):-1]]
    base = median(vols) if vols else 0.0
    if base <= 0:
        return "UNKNOWN"
    rel = float(candles[-1][5]) / base
    return "LOW" if rel < 0.7 else ("HIGH" if rel > 1.5 else "NORMAL")


def _library_for(rows: Sequence[Any], *, cutoff_ms: int, horizon_ms: int, template: Any) -> Any:
    from app.trading_intelligence.forecast.artifact import default_calibration_status
    from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary

    kept = purge_for_window(rows, decision_time=lambda r: r.label.decision_time, horizon_ms=horizon_ms,
                            cutoff_ms=cutoff_ms)
    lib = HistoricalOutcomeLibrary.build(
        tuple(kept), dataset_source_hash=template.dataset_source_hash,
        candidate_generation_versions=template.candidate_generation_versions,
        label_policy_version=template.label_policy_version, cost_model_version=template.cost_model_version,
        source_kind=template.source_kind)
    # the SAME status a real library artifact loads with (RESEARCH_ONLY for real data)
    return dataclasses.replace(lib, calibration_status=default_calibration_status(template.source_kind))


def _econ(ev: Any, obs: Any, ctx: Any, admission_policy: Any):
    from app.trading_intelligence.economics.canonical import canonical_economics

    return canonical_economics(ev.candidate, ev.market_state, ev.forecast, obs, venue_policy=ctx.cost_policy,
                               reference_notional=ctx.reference_notional, admission_policy=admission_policy)


def _veto(ev: Any, cost: Any, opp: Any, veto_policy: Any):
    from app.trading_intelligence.veto.engine import evaluate_veto

    return evaluate_veto(opportunity=opp, candidate=ev.candidate, market_state=ev.market_state,
                         regime_distribution=ev.regime, forecast=ev.forecast, cost_estimate=cost,
                         policy=veto_policy)


def _neighbor_policies(base: Any) -> Dict[str, Any]:
    return {name: dataclasses.replace(base, **{fld: getattr(base, fld) + step}) for name, fld, step in NEIGHBOR_PLAN}


def chronology_for(series, cfg: ReplayConfig) -> ChronologyPlan:
    """THE chronology for a dataset + config (shared by replay and pipeline)."""
    start, end = decision_span(series, cfg)
    return plan_chronology(decision_start_ms=start, decision_end_ms=end, bar_ms=_tf_ms(cfg.timeframe),
                           label_horizon_bars=cfg.label_horizon_bars, holdout_fraction=cfg.holdout_fraction,
                           embargo_bars=cfg.embargo_bars,
                           htf_ms=_tf_ms(cfg.higher_timeframe) if cfg.higher_timeframe else None, folds=cfg.folds)


# ------------------------------------------------------------------ the pass
def run_replay(series: Mapping[str, Mapping[str, Sequence[Any]]], meta: Mapping[str, Mapping[str, str]],
               cfg: ReplayConfig, *, data_sources: Sequence[str], phase: str = "PRE_HOLDOUT",
               max_decisions: Optional[int] = None, library_rows: Optional[Sequence[Any]] = None,
               library_template: Any = None) -> ReplayResult:
    """``phase`` = PRE_HOLDOUT (walk-forward folds; holdout candles never
    loaded) or HOLDOUT (library from ALL pre-holdout rows, evaluated on the
    reserved window). ``max_decisions`` bounds a determinism re-check."""
    from app.replay.cost_model import BINANCE_FUTURES_STANDARD
    from app.replay.historical_provider import HistoricalClock, HistoricalMarketDataProvider
    from app.research.dataset import future_rows
    from app.trading_intelligence.contracts.forecast import LabelQuality
    from app.trading_intelligence.contracts.ranking import SymbolEvalKind
    from app.trading_intelligence.contracts.veto import VetoPolicy
    from app.trading_intelligence.controller.cati_controller import CATIController
    from app.trading_intelligence.economics.policy import default_admission_policy
    from app.trading_intelligence.forecast.build_library import BuildConfig, run_build
    from app.trading_intelligence.forecast.cohorts import derive_cohort_dimensions
    from app.trading_intelligence.forecast.labels import label_candidate
    from app.trading_intelligence.portfolio.groups import static_group_for
    from app.trading_intelligence.ranking.coordinator import CATICycleCoordinator
    from app.trading_intelligence.versions import RESEARCH_COST_MODEL_VERSION

    from .replay_venue import replay_venue_context

    tf_ms = _tf_ms(cfg.timeframe)
    plan = chronology_for(series, cfg)
    horizon_ms = cfg.label_horizon_bars * tf_ms
    if phase == "PRE_HOLDOUT":
        # the reserved holdout is not loaded at all; labels end before it (purge)
        data = truncate_series(series, plan.holdout.start_ms - 1)
        eval_windows = list(plan.evaluation_folds)
        build_end = plan.folds[-1].end_ms
    elif phase == "HOLDOUT":
        data = series
        eval_windows = [plan.holdout]
        build_end = plan.folds[-1].end_ms
    else:
        raise ValueError(f"unknown replay phase {phase!r}")

    # canonical library rows for the pre-holdout chronology (built once, filtered per fold);
    # a caller reusing them MUST pass the template (identity/versions) they were built with
    if library_rows is None:
        build = run_build(truncate_series(series, plan.holdout.start_ms - 1), meta, BuildConfig(
            symbols=tuple(sorted(cfg.symbols)), timeframe=cfg.timeframe, start_ms=plan.decision_start_ms,
            end_ms=build_end, label_horizon_bars=cfg.label_horizon_bars, higher_timeframe=cfg.higher_timeframe,
            snapshot_limit=cfg.snapshot_limit, venue=cfg.venue, source_name=cfg.source_name),
            source_info={"data_sources": list(data_sources)})
        template, library_rows = build.library, build.library.rows
    elif library_template is None:
        raise ValueError("reused library rows require the library template they were built with")
    else:
        template = library_template

    admission = default_admission_policy()
    veto_policy = VetoPolicy()
    neighbors = _neighbor_policies(admission)
    cost_model = BINANCE_FUTURES_STANDARD
    cost_model_version = f"{RESEARCH_COST_MODEL_VERSION}:{cost_model.model_hash}"
    stress = [m for m in cfg.cost_multipliers if float(m) != 1.0]

    integrity = Counter()
    library_info: Dict[str, Any] = {"rows_total": len(library_rows), "folds": {}}
    records: List[Dict[str, Any]] = []
    counts = Counter()
    first_close = min(int(r[0][6]) for s in data.values() for r in [s[cfg.timeframe]] if s.get(cfg.timeframe))
    clock = HistoricalClock(first_close - 1)
    provider = HistoricalMarketDataProvider({s: dict(v) for s, v in data.items() if v.get(cfg.timeframe)}, clock,
                                            source=cfg.source_name, source_environment="HISTORICAL")
    anchor_closes = sorted({int(r[6]) for s in data.values() for r in s.get(cfg.timeframe, ())})
    decisions_done = 0

    for window in eval_windows:
        cutoff = plan.library_cutoff_for(window) if phase == "PRE_HOLDOUT" else plan.holdout.start_ms - plan.embargo_ms
        library = _library_for(library_rows, cutoff_ms=cutoff, horizon_ms=horizon_ms, template=template)
        max_label_end = max((r.label.decision_time + horizon_ms for r in library.rows), default=None)
        if max_label_end is not None and max_label_end >= cutoff:
            integrity["library_label_crosses_boundary"] += 1
        library_info["folds"][window.name] = {"rows": len(library.rows), "cutoff_ms": cutoff,
                                              "library_id": library.library_id,
                                              "calibration_status": library.calibration_status}
        controller = CATIController(outcome_library=library, admission_policy=admission, veto_policy=veto_policy)
        for t in (c for c in anchor_closes if window.contains(c)):
            if max_decisions is not None and decisions_done >= max_decisions:
                break
            decisions_done += 1
            clock.advance_to(t)
            if phase == "PRE_HOLDOUT" and plan.holdout.contains(t):
                integrity["holdout_read_attempt"] += 1
                continue
            coord = CATICycleCoordinator()
            key = coord.begin_bot_cycle(bot_instance_id="certification_replay", cycle_id=f"t{t}", cycle_time_ms=t,
                                        user_id=None, broker_account_id="certification_replay", run_id=cfg.config_hash[:16],
                                        universe_symbols=sorted(cfg.symbols))
            pending: List[Dict[str, Any]] = []
            for symbol in sorted(cfg.symbols):
                coord.mark_due(key, symbol)
                try:
                    snapshot = provider.build_snapshot(symbol, cfg.timeframe, limit=cfg.snapshot_limit,
                                                       higher_timeframe=cfg.higher_timeframe)
                except Exception:
                    snapshot = None
                if snapshot is None or int(snapshot.candles[-1][6]) != t:
                    coord.record_symbol_failure(key, symbol, "NO_SNAPSHOT_AT_DECISION")
                    counts["no_snapshot"] += 1
                    continue
                object.__setattr__(snapshot, "market_snapshot_id", f"hist_{snapshot.data_hash[:20]}")
                if int(snapshot.candles[-1][6]) > t:
                    integrity["future_candle_in_snapshot"] += 1
                if cfg.higher_timeframe and snapshot.higher_timeframe_candles and \
                        int(snapshot.higher_timeframe_candles[-1][6]) > t:
                    integrity["future_htf_candle"] += 1
                close = float(snapshot.candles[-1][4])
                ctx = replay_venue_context(symbol, close, t, cost_model=cost_model)
                m = meta.get(symbol, {})
                ev = controller.evaluate_symbol(snapshot=snapshot, venue=cfg.venue, source=cfg.source_name,
                                                venue_context=ctx, require_venue_economics=True,
                                                base_asset=m.get("base"), quote_asset=m.get("quote"),
                                                asset_class=cfg.asset_class, run_id=cfg.config_hash[:16],
                                                cycle_id=f"t{t}")
                counts["decision_points"] += 1
                if ev.kind != SymbolEvalKind.EVALUATED.value:
                    coord.record_symbol_evaluation(key, ev) if ev.kind == SymbolEvalKind.NO_CANDIDATES.value \
                        else coord.record_symbol_failure(key, symbol, ev.error or ev.kind)
                    counts[f"symbol_{ev.kind.lower()}"] += 1
                    continue
                coord.record_symbol_evaluation(key, ev)
                primary = data[symbol][cfg.timeframe]
                vol_bucket = volume_proxy_bucket(snapshot.candles)
                for o in ev.opportunities:
                    counts["candidates"] += 1
                    c = o.candidate
                    if o.market_state.decision_time > t or c.decision_time > t:
                        integrity["decision_after_clock"] += 1
                    if len(future_rows(primary, c.decision_time, cfg.label_horizon_bars)) < cfg.label_horizon_bars:
                        counts["censored_future_horizon"] += 1
                        continue
                    label = label_candidate(c, primary, cost_model=cost_model, horizon_bars=cfg.label_horizon_bars,
                                            cost_model_version=cost_model_version)
                    if label.label_quality != LabelQuality.VALID.value:
                        counts[f"label_{label.label_quality.lower()}"] += 1
                        continue
                    dims = derive_cohort_dimensions(setup_family=c.setup_family, side=c.side,
                                                    market_state=o.market_state, regime_distribution=o.regime,
                                                    instrument_group=static_group_for(symbol))
                    base_cost = float(o.cost_estimate.total_cost_R)
                    rec: Dict[str, Any] = {
                        "setup_candidate_id": c.setup_candidate_id, "symbol": symbol,
                        "symbol_group": static_group_for(symbol), "setup_family": c.setup_family, "side": c.side,
                        "decision_time": int(c.decision_time), "fold": window.name, "month": _month(c.decision_time),
                        "session": _session(c.decision_time), "regime": o.regime.dominant_regime,
                        "liquidity_bucket": str(dims.get("liquidity_bucket", "UNKNOWN")),
                        "volume_liquidity_proxy": vol_bucket,
                        "forecast_status": o.forecast.status, "forecast_p": float(o.forecast.p_net_profitable_mean),
                        "forecast_usable": bool(o.forecast.is_usable),
                        "admission_status": o.opportunity.admission_status, "veto_outcome": o.veto.outcome,
                        "veto_reasons": sorted(o.veto.reason_codes),
                        "veto_runtime_only": bool(o.veto.reason_codes) and set(o.veto.reason_codes) <= RUNTIME_ONLY_VETO_REASONS,
                        "venue_source_quality": o.venue_observation.source_quality if o.venue_observation else None,
                        "gross_R": float(label.gross_R), "cost_R": base_cost, "label_model_cost_R": float(label.total_cost_R),
                        "net_R": float(label.gross_R) - base_cost, "terminal_outcome": label.terminal_outcome,
                        "mfe_R": float(label.mfe_R), "mae_R": float(label.mae_R),
                        "same_bar_conservative": "SAME_BAR_CONSERVATIVE_STOP_ASSUMED" in label.reason_codes,
                        "stress": {}, "neighbors": {},
                    }
                    for mult in stress:
                        sctx = replay_venue_context(symbol, close, t, cost_model=cost_model, cost_multiplier=mult)
                        cost_m, opp_m = _econ(o, sctx.observe(c.instrument_key, t), sctx, admission)
                        veto_m = _veto(o, cost_m, opp_m, veto_policy)
                        rec["stress"][str(mult)] = {"cost_R": float(cost_m.total_cost_R),
                                                    "net_R": float(label.gross_R) - float(cost_m.total_cost_R),
                                                    "admission_status": opp_m.admission_status,
                                                    "veto_outcome": veto_m.outcome}
                    base_obs = o.venue_observation
                    for name, pol in neighbors.items():
                        cost_n, opp_n = _econ(o, base_obs, ctx, pol)
                        veto_n = _veto(o, cost_n, opp_n, veto_policy)
                        rec["neighbors"][name] = {"admission_status": opp_n.admission_status,
                                                  "veto_outcome": veto_n.outcome}
                    pending.append(rec)
            result = coord.finalize_bot_cycle(key)
            ranks = {r.setup_candidate_id: i + 1 for i, r in enumerate(result.ranked)}
            for rec in pending:
                rec["rank"] = ranks.get(rec["setup_candidate_id"])
                records.append(rec)
                counts["labeled"] += 1
                counts["admissible"] += rec["admission_status"] == ADMISSIBLE
                counts["approved"] += rec["veto_outcome"] == APPROVE

    records.sort(key=lambda r: (r["decision_time"], r["symbol"], r["setup_candidate_id"]))
    counts_d = dict(sorted(counts.items()))
    counts_d["by_setup_family"] = dict(Counter(r["setup_family"] for r in records))
    counts_d["by_veto_outcome"] = dict(Counter(r["veto_outcome"] for r in records))
    counts_d["by_admission_status"] = dict(Counter(r["admission_status"] for r in records))
    counts_d["veto_reason_counts"] = dict(Counter(x for r in records for x in r["veto_reasons"]))
    integrity_d = {"lookahead_violations": int(sum(integrity.values())), "details": dict(integrity),
                   "decisions_evaluated": decisions_done, **NOT_REPLAYED}
    replay_hash = stable_hash({"config": cfg.config_hash, "phase": phase, "plan": plan.to_dict(),
                               "records": [_identity(r) for r in records]})
    return ReplayResult(records=records, plan=plan, integrity=integrity_d, counts=counts_d, library=library_info,
                        replay_hash=replay_hash, config_hash=cfg.config_hash, phase=phase,
                        library_rows=tuple(library_rows), library_template=template)


def _identity(rec: Mapping[str, Any]) -> dict:
    """The analytical content of a record (floats rounded for cross-platform stability)."""
    def r(x):
        return round(x, 10) if isinstance(x, float) else x

    return {k: (r(v) if not isinstance(v, dict) else {kk: ({k3: r(v3) for k3, v3 in vv.items()} if isinstance(vv, dict)
                                                          else r(vv)) for kk, vv in sorted(v.items())})
            for k, v in sorted(rec.items())}


# ------------------------------------------------------------------ cache
def cache_key(manifest_hash: str, freeze_hash: str, cfg: ReplayConfig, phase: str) -> str:
    return stable_hash({"manifest": manifest_hash, "freeze": freeze_hash, "config": cfg.config_hash, "phase": phase})[:32]


def save_replay(result: ReplayResult, directory: Path, key: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"replay_{key}.json"
    body = {"replay_hash": result.replay_hash, "config_hash": result.config_hash, "phase": result.phase,
            "plan": result.plan.to_dict(), "integrity": result.integrity, "counts": result.counts,
            "library": result.library, "records": result.records}
    path.write_text(json.dumps(body, sort_keys=True, default=str), encoding="utf-8")
    return path


def load_replay(directory: Path, key: str) -> Optional[Dict[str, Any]]:
    path = Path(directory) / f"replay_{key}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


__all__ = ["ReplayConfig", "ReplayResult", "run_replay", "chronology_for", "decision_span", "truncate_series", "volume_proxy_bucket",
           "NEIGHBOR_PLAN", "NEIGHBOR_PLAN_VERSION", "RUNTIME_ONLY_VETO_REASONS", "cache_key", "save_replay",
           "load_replay", "APPROVE", "ADMISSIBLE", "NOT_REPLAYED"]
