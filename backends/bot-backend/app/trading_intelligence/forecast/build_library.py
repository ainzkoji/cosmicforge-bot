"""Deterministic historical CATI outcome-library builder (Section 12.15).

Pipeline, per symbol and per CLOSED-candle decision point::

    HistoricalMarketDataProvider (cut at the clock)   <- reused, no 2nd store
      -> immutable MarketSnapshot (deterministic id)
      -> MarketState -> RegimeDistribution
      -> ALL four setup specialists -> 0..N frozen SetupCandidates
      ------------------------ FREEZE LINE ------------------------
      -> future rows exposed ONLY to the labeler (label_candidate)
      -> cost components -> cohort dimensions -> LibraryRow
    -> HistoricalOutcomeLibrary -> immutable artifact (manifest + rows)

The freeze line is structural: ``_freeze_decision_point`` receives only the
provider (which cannot return a candle past its clock); the full series is
handed to the labeler afterwards. Evidence is generated for EVERY discovered
hypothesis, regardless of whether V2 or CATI would ever have traded it, so
there is no execution-selection bias.

Example (small smoke range)::

    python -m app.trading_intelligence.forecast.build_library \\
        --db data/bot.db --symbols BTCUSDT,ETHUSDT --timeframe 15m \\
        --start 2025-06-01 --end 2025-06-15 --label-horizon-bars 48 \\
        --output data/research/cati_libraries

Example (later certification-scale build -- NOT run automatically)::

    python -m app.trading_intelligence.forecast.build_library \\
        --db data/research/<history>.db --symbols BTCUSDT,ETHUSDT,SOLUSDT,... \\
        --timeframe 15m --higher-timeframe 4h --start 2024-06-01 --end 2026-06-01 \\
        --label-horizon-bars 48 --output data/research/cati_libraries
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.replay.historical_provider import TIMEFRAME_MS, HistoricalClock, HistoricalMarketDataProvider
from app.replay.identity import dataset_hash
from app.research.dataset import assess_quality, future_rows
from app.trading_intelligence.contracts.forecast import ForecastReasonCode, LabelQuality
from app.trading_intelligence.forecast.artifact import write_library_artifact
from app.trading_intelligence.forecast.cohorts import derive_cohort_dimensions
from app.trading_intelligence.forecast.labels import label_candidate
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow
from app.trading_intelligence.forecast.source import (
    SourceClassificationError, classify_data_sources, resolve_source_kind,
)
from app.trading_intelligence.integration.snapshot_adapter import evaluate_market_state
from app.trading_intelligence.portfolio.groups import static_group_for
from app.trading_intelligence.regime.engine import compute_regime_distribution
from app.trading_intelligence.regime.policy import default_policy as default_regime_policy
from app.trading_intelligence.setups.policy import default_policies
from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY, discover_all
from app.trading_intelligence.versions import (
    COHORT_SCHEMA_VERSION,
    LABEL_POLICY_VERSION,
    LIBRARY_BUILDER_VERSION,
    OOD_FEATURE_SCHEMA_VERSION,
    RESEARCH_COST_MODEL_VERSION,
)

CENSORED_FUTURE_HORIZON = "CENSORED_FUTURE_HORIZON"

COST_MODELS: Mapping[str, CostModel] = {"binance_futures_standard": BINANCE_FUTURES_STANDARD}
REGIME_POLICIES = {"default": default_regime_policy}
SETUP_POLICIES = {"default": default_policies}


class BuildError(RuntimeError):
    pass


@dataclass(frozen=True)
class BuildConfig:
    symbols: Tuple[str, ...]
    timeframe: str
    start_ms: int
    end_ms: int
    label_horizon_bars: int = 48
    higher_timeframe: Optional[str] = None
    snapshot_limit: int = 250
    venue: str = "binance"
    source_name: str = "historical_candles"
    cost_model_name: str = "binance_futures_standard"
    allow_gross_only: bool = False
    setup_policy_name: str = "default"
    regime_policy_name: str = "default"
    #: HistoricalLibrarySourceKind. In-memory builds (tests) must state it;
    #: the CLI derives it from ``historical_candles.data_source``.
    source_kind: str = "UNKNOWN"
    source_provider: str = "unknown"
    source_dataset_id: str = "in_memory"
    market_type: str = "crypto"


@dataclass
class BuildResult:
    library: HistoricalOutcomeLibrary
    provenance: Dict[str, Any]
    skipped: Dict[str, int] = field(default_factory=dict)


# -- historical source (reuses the existing historical_candles table) -------------
def load_series_from_db(
    db_path: str, symbols: Sequence[str], timeframe: str, start_ms: int, end_ms: int, *,
    label_horizon_bars: int, warmup_bars: int, higher_timeframe: Optional[str] = None,
    market_type: str = "crypto", data_source: Optional[str] = None,
) -> Tuple[Dict[str, Dict[str, List[list]]], Dict[str, Dict[str, str]], Dict[str, Any]]:
    """Read-only load from the existing ``historical_candles`` table. No new
    candle store is created. Returns (series, per-symbol base/quote meta,
    source info)."""
    if timeframe not in TIMEFRAME_MS:
        raise BuildError(f"unsupported timeframe: {timeframe}")
    tf_ms = TIMEFRAME_MS[timeframe]
    lo = start_ms - warmup_bars * tf_ms
    hi = end_ms + (label_horizon_bars + 1) * tf_ms
    frames = [(timeframe, lo, hi)]
    if higher_timeframe:
        htf_ms = TIMEFRAME_MS[higher_timeframe]
        frames.append((higher_timeframe, start_ms - warmup_bars * htf_ms, end_ms))
    conn = sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True)
    series: Dict[str, Dict[str, List[list]]] = {}
    meta: Dict[str, Dict[str, str]] = {}
    sources: set = set()
    versions: set = set()
    try:
        for symbol in symbols:
            sym = symbol.upper()
            series[sym] = {}
            for tf, frame_lo, frame_hi in frames:
                span = TIMEFRAME_MS[tf]
                sql = ("SELECT open_time, open, high, low, close, volume, quote_volume, trades, base_currency, "
                       "quote_currency, data_source, data_version FROM historical_candles "
                       "WHERE symbol=? AND interval=? AND market_type=? AND open_time BETWEEN ? AND ?")
                params: List[Any] = [sym, tf, market_type, frame_lo, frame_hi]
                if data_source:
                    sql += " AND data_source=?"
                    params.append(data_source)
                sql += " ORDER BY open_time ASC"
                rows, seen = [], set()
                for (ot, o, h, l, c, v, qv, tr, base, quote, ds, dv) in conn.execute(sql, params):
                    if ot in seen:
                        raise BuildError(f"{sym} {tf}: duplicate open_time {ot} (multiple data_sources? pass --data-source)")
                    seen.add(ot)
                    rows.append([int(ot), float(o), float(h), float(l), float(c), float(v), int(ot) + span - 1,
                                 float(qv or 0.0), int(tr or 0)])
                    sources.add(ds)
                    versions.add(dv)
                    if base and quote:
                        meta.setdefault(sym, {"base": base, "quote": quote})
                if rows:
                    series[sym][tf] = rows
    finally:
        conn.close()
    series = {s: v for s, v in series.items() if timeframe in v}
    # A NULL data_source is kept as "" so it classifies UNKNOWN (never silently dropped).
    return series, meta, {"data_sources": sorted({str(x or "") for x in sources}), "data_versions": sorted(x for x in versions if x)}


# -- the decision point: feature path only, NO future rows ---------------------------
def _freeze_decision_point(
    provider: HistoricalMarketDataProvider, symbol: str, timeframe: str, cfg: BuildConfig,
    meta: Mapping[str, str], regime_policy: Any, setup_policies: Mapping[str, Any],
):
    snapshot = provider.build_snapshot(
        symbol, timeframe, limit=cfg.snapshot_limit, higher_timeframe=cfg.higher_timeframe,
    )
    if snapshot is None:
        return None
    # The provider mints a random correlation id; analytical identity must be
    # a function of the data, so pin it deterministically.
    object.__setattr__(snapshot, "market_snapshot_id", f"hist_{snapshot.data_hash[:20]}")
    market_state = evaluate_market_state(
        snapshot, venue=cfg.venue, source=cfg.source_name,
        base_asset=meta.get("base"), quote_asset=meta.get("quote"), use_cache=False,
    )
    regime = compute_regime_distribution(market_state, regime_policy)
    candidates = discover_all(
        snapshot=snapshot, market_state=market_state, regime_distribution=regime, policies=setup_policies,
    ) if market_state.is_usable else ()
    return snapshot, market_state, regime, tuple(candidates)


def _resolve_workers(workers: Optional[int], jobs: int) -> int:
    if workers is None:
        import os

        workers = int(os.environ.get("CATI_BUILD_WORKERS", "1") or 1)
    return max(1, min(int(workers), jobs))


def _build_symbol(job) -> Dict[str, Any]:
    """The per-symbol body of ``run_build`` (top level so a spawned worker can run it)."""
    symbol, sym_series, sym_meta, cfg, cost_model, cost_model_version = job
    regime_policy = REGIME_POLICIES[cfg.regime_policy_name]()
    setup_policies = SETUP_POLICIES[cfg.setup_policy_name]()
    out: Dict[str, Any] = {"symbol": symbol, "missing_timeframe": False, "rows": [], "counts": Counter(),
                           "by_family": Counter(), "by_side": Counter(), "by_regime": Counter(),
                           "quality": Counter(), "skipped": Counter(), "first_decision": None, "last_decision": None}
    if cfg.timeframe not in sym_series:
        out["missing_timeframe"] = True
        return out
    counts, skipped, rows_out = out["counts"], out["skipped"], out["rows"]
    primary_rows = sym_series[cfg.timeframe]
    q = assess_quality(primary_rows, symbol=symbol, timeframe=cfg.timeframe)
    out["quality_summary"] = {
        "rows": q.rows, "missing_bars": q.missing_bars, "completeness": q.completeness,
        "missing_windows": [list(w) for w in q.missing_windows[:20]], "missing_window_count": len(q.missing_windows),
    }
    clock = HistoricalClock(int(primary_rows[0][6]) - 1)
    provider = HistoricalMarketDataProvider({symbol: dict(sym_series)}, clock, source=cfg.source_name,
                                            source_environment="HISTORICAL")
    group = static_group_for(symbol)
    for t in provider.step(symbol, cfg.timeframe, start_ms=cfg.start_ms, end_ms=cfg.end_ms):
        counts["decision_points"] += 1
        out["first_decision"] = t if out["first_decision"] is None else out["first_decision"]
        out["last_decision"] = t
        frozen = _freeze_decision_point(provider, symbol, cfg.timeframe, cfg, sym_meta, regime_policy, setup_policies)
        if frozen is None:
            skipped["no_snapshot"] += 1
            continue
        _snapshot, market_state, regime, candidates = frozen
        if not market_state.is_usable:
            skipped["market_state_unusable"] += 1
            continue
        # ---------------- FREEZE LINE: candidates are immutable from here ----------------
        for candidate in candidates:
            counts["candidates"] += 1
            if len(future_rows(primary_rows, candidate.decision_time, cfg.label_horizon_bars)) < cfg.label_horizon_bars:
                skipped[CENSORED_FUTURE_HORIZON] += 1
                continue
            label = label_candidate(
                candidate, primary_rows, cost_model=cost_model, horizon_bars=cfg.label_horizon_bars,
                cost_model_version=cost_model_version,
            )
            if label.label_quality != LabelQuality.VALID.value:
                skipped[f"label_{label.label_quality.lower()}"] += 1
                continue
            dims = derive_cohort_dimensions(
                setup_family=candidate.setup_family, side=candidate.side, market_state=market_state,
                regime_distribution=regime, instrument_group=group,
            )
            rows_out.append(LibraryRow(
                label=label, cohort_dimensions=dims,
                continuous_features={
                    "room_to_target_R": float(candidate.room_to_target_R or 0.0),
                    "initial_risk_fraction": float(candidate.initial_structural_risk / candidate.trigger_reference),
                },
            ))
            counts["labeled"] += 1
            out["by_family"][candidate.setup_family] += 1
            out["by_side"][candidate.side] += 1
            out["by_regime"][regime.dominant_regime] += 1
            out["quality"][label.label_quality] += 1
    return out


def run_build(series: Mapping[str, Mapping[str, Sequence[Any]]], meta: Mapping[str, Mapping[str, str]],
              cfg: BuildConfig, *, source_info: Optional[Mapping[str, Any]] = None,
              workers: Optional[int] = None) -> BuildResult:
    """``workers``: per-symbol process parallelism (default ``CATI_BUILD_WORKERS`` or 1). Execution detail only:
    it is not part of ``BuildConfig`` and never changes the rows, counts or library identity."""
    cost_model = COST_MODELS.get(cfg.cost_model_name)
    if cost_model is None:
        if cfg.cost_model_name == "zero_gross_only" and cfg.allow_gross_only:
            cost_model = CostModel.zero()
        else:
            raise BuildError(f"unknown cost model {cfg.cost_model_name!r} (zero cost requires --allow-gross-only)")
    cost_source_quality = "GROSS_ONLY" if cost_model == CostModel.zero() else "MODELED_RESEARCH_ASSUMPTION"
    # Source kind: derived from the data's recorded provenance; a declared kind
    # may only DOWNGRADE it (never claim REAL_MARKET for unproven data).
    derived_kind = classify_data_sources((source_info or {}).get("data_sources", ()))
    try:
        source_kind = resolve_source_kind(None if cfg.source_kind == "UNKNOWN" else cfg.source_kind, derived_kind)
    except SourceClassificationError as exc:
        raise BuildError(str(exc)) from exc
    regime_policy = REGIME_POLICIES[cfg.regime_policy_name]()
    setup_policies = SETUP_POLICIES[cfg.setup_policy_name]()
    cost_model_version = f"{RESEARCH_COST_MODEL_VERSION}:{cost_model.model_hash}"
    tf_ms = TIMEFRAME_MS[cfg.timeframe]

    rows_out: List[LibraryRow] = []
    counts = Counter()
    by_family, by_side, by_regime, quality = Counter(), Counter(), Counter(), Counter()
    skipped: Counter = Counter()
    per_symbol: Dict[str, Any] = {}
    first_decision = last_decision = None

    # One independent pass per symbol (own provider/clock); results are merged in sorted-symbol order, so any
    # worker count reproduces the sequential rows, counts and identity exactly.
    jobs = [(symbol, dict(series[symbol]), dict(meta.get(symbol, {})), cfg, cost_model, cost_model_version)
            for symbol in sorted(series)]
    n = _resolve_workers(workers, len(jobs))
    from app.trading_intelligence.forecast.library import RowInterner

    # each symbol's rows are interned as they arrive (shared immutable values: identical rows, ~40% less resident
    # memory) and the raw part is released, so a certification-scale build never holds two copies
    interner = RowInterner()

    def merge(part) -> None:
        nonlocal first_decision, last_decision
        if part["missing_timeframe"]:
            skipped["symbol_missing_timeframe"] += 1
            return
        per_symbol[part["symbol"]] = part["quality_summary"]
        rows_out.extend(interner.row(r) for r in part["rows"])
        part["rows"] = None
        for acc, key in ((counts, "counts"), (by_family, "by_family"), (by_side, "by_side"),
                         (by_regime, "by_regime"), (quality, "quality"), (skipped, "skipped")):
            acc.update(part[key])
        if part["first_decision"] is not None:
            first_decision = part["first_decision"] if first_decision is None else first_decision
            last_decision = part["last_decision"]

    if n <= 1:
        for job in jobs:
            merge(_build_symbol(job))
    else:
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=n, mp_context=mp.get_context("spawn")) as pool:
            for part in pool.map(_build_symbol, jobs):  # yielded in job (sorted-symbol) order
                merge(part)

    versions = {fam: spec.setup_version for fam, spec in SPECIALIST_REGISTRY.items()}
    library = HistoricalOutcomeLibrary.build(
        tuple(rows_out), dataset_source_hash=dataset_hash(series), candidate_generation_versions=versions,
        label_policy_version=LABEL_POLICY_VERSION, cost_model_version=cost_model_version,
        source_kind=source_kind,
    )
    provenance = {
        "source_provider": cfg.source_provider, "source_dataset_id": cfg.source_dataset_id,
        "venue": cfg.venue, "market_type": cfg.market_type,
        "start_time": cfg.start_ms, "end_time": cfg.end_ms,
        "source_range": {"requested_start_ms": cfg.start_ms, "requested_end_ms": cfg.end_ms,
                         "first_decision_ms": first_decision, "last_decision_ms": last_decision},
        "regime_policy_hash": regime_policy.policy_hash,
        "setup_policy_hashes": {fam: pol.policy_hash for fam, pol in setup_policies.items()},
        "ood_feature_schema_version": OOD_FEATURE_SCHEMA_VERSION,
        "created_from_start": cfg.start_ms, "created_from_end": cfg.end_ms,
        "symbols": sorted(series), "instruments": {s: dict(meta.get(s, {})) for s in sorted(series)},
        "timeframe": cfg.timeframe, "higher_timeframe": cfg.higher_timeframe,
        "requested_interval": cfg.timeframe, "actual_interval": cfg.timeframe,
        "label_horizon_bars": cfg.label_horizon_bars,
        "candidate_count": counts["candidates"], "labeled_count": counts["labeled"],
        "skipped_count": counts["candidates"] - counts["labeled"],
        "skipped_reasons": dict(sorted(skipped.items())),
        "decision_point_count": counts["decision_points"],
        "counts_by_setup_family": dict(sorted(by_family.items())),
        "counts_by_side": dict(sorted(by_side.items())),
        "counts_by_regime": dict(sorted(by_regime.items())),
        "label_quality_counts": dict(sorted(quality.items())),
        "cost_model_name": cfg.cost_model_name, "cost_model_hash": cost_model.model_hash,
        "cost_source_quality": cost_source_quality,
        "source": {"name": cfg.source_name, **dict(source_info or {}), "per_symbol_quality": per_symbol},
        "builder_version": LIBRARY_BUILDER_VERSION, "cohort_schema_version_built": COHORT_SCHEMA_VERSION,
    }
    return BuildResult(library=library, provenance=provenance, skipped=dict(skipped))


def build_and_write(series, meta, cfg: BuildConfig, output_dir: str, *, source_info=None,
                    governance: Optional[Mapping[str, Any]] = None, workers: Optional[int] = None
                    ) -> Tuple[Path, BuildResult]:
    """``governance``: the frozen dataset / universe / holdout / code identity this build is pinned to. It is
    recorded in the (hashed) manifest, so a library built from other data or code is a different artifact."""
    result = run_build(series, meta, cfg, source_info=source_info, workers=workers)
    provenance = dict(result.provenance)
    if governance:
        provenance["governance"] = dict(governance)
    path = write_library_artifact(result.library, output_dir, provenance=provenance)
    return path, result


def governance_pins(*, universe_manifest: Optional[str], dataset_manifest: Optional[str],
                    holdout_start_ms: Optional[int], cfg: BuildConfig) -> Dict[str, Any]:
    """Verify the frozen inputs a governed (runtime) library is pinned to. Raises ``BuildError``."""
    pins: Dict[str, Any] = {}
    if universe_manifest:
        from app.market_data.universe import load_frozen_universe

        u = load_frozen_universe(universe_manifest)
        if tuple(sorted(cfg.symbols)) != tuple(sorted(u["selected_symbols"])):
            raise BuildError("symbols differ from the frozen universe membership")
        if not (u["window_start_ms"] <= cfg.start_ms and cfg.end_ms < u["window_end_ms"]):
            raise BuildError("build range lies outside the frozen universe window")
        pins.update(universe_id=u["universe_id"], universe_hash=u["universe_hash"])
    if dataset_manifest:
        from app.market_data.universe import verify_dataset_payload

        d = json.loads(Path(dataset_manifest).read_text(encoding="utf-8"))
        try:
            pins["dataset_manifest_hash"] = verify_dataset_payload(d)
        except Exception as exc:
            raise BuildError(f"dataset manifest refused: {exc}") from exc
        if pins.get("universe_hash") and d.get("universe_hash") != pins["universe_hash"]:
            raise BuildError("dataset manifest belongs to another universe")
    if holdout_start_ms is not None:
        # the labeler reads up to (label_horizon + 1) bars past ``end``: all of it must precede the holdout
        last_read = cfg.end_ms + (cfg.label_horizon_bars + 1) * TIMEFRAME_MS[cfg.timeframe]
        if last_read >= holdout_start_ms:
            raise BuildError(f"HOLDOUT_OVERLAP: label reads reach {last_read} >= holdout start {holdout_start_ms}")
        pins.update(holdout_start_ms=int(holdout_start_ms), last_candle_read_bound_ms=last_read)
    if not pins:
        return pins  # an ungoverned (research / smoke) build carries no governance block
    from app.replay.identity import code_revision

    commit, _branch, dirty = code_revision()
    pins.update(code_commit=commit, source_tree_dirty=dirty)
    return pins


def _parse_time(text: str) -> int:
    if text.isdigit():
        return int(text)
    return int(datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Build an immutable CATI HistoricalOutcomeLibrary artifact")
    ap.add_argument("--db", required=True, help="sqlite DB holding the historical_candles table (read-only)")
    ap.add_argument("--historical-source", default="db", choices=["db"])
    ap.add_argument("--symbols", default=None, help="comma separated venue symbols (or --universe-manifest)")
    ap.add_argument("--timeframe", default="15m")
    ap.add_argument("--higher-timeframe", default=None)
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (UTC) or epoch ms")
    ap.add_argument("--end", required=True)
    ap.add_argument("--label-horizon-bars", type=int, default=48)
    ap.add_argument("--output", required=True)
    ap.add_argument("--cost-model", default="binance_futures_standard")
    ap.add_argument("--allow-gross-only", action="store_true")
    ap.add_argument("--setup-policy", default="default")
    ap.add_argument("--regime-policy", default="default")
    ap.add_argument("--venue", default="binance")
    ap.add_argument("--market-type", default="crypto")
    ap.add_argument("--data-source", default=None)
    ap.add_argument("--snapshot-limit", type=int, default=250)
    ap.add_argument("--universe-manifest", default=None, help="frozen universe manifest: its members are the symbols")
    ap.add_argument("--dataset-manifest", default=None, help="frozen (lineage v2) dataset manifest the build is pinned to")
    ap.add_argument("--holdout-start-ms", type=int, default=None,
                    help="refuse unless every candle the build reads (labels included) precedes this holdout start")
    ap.add_argument("--workers", type=int, default=None, help="per-symbol processes (identity-neutral)")
    ap.add_argument("--declare-source-kind", default=None,
                    help="optional DOWNGRADE of the source kind derived from data_source (never an upgrade)")
    args = ap.parse_args(argv)
    if args.universe_manifest:
        from app.market_data.universe import load_frozen_universe

        symbols = tuple(load_frozen_universe(args.universe_manifest)["selected_symbols"])
    elif args.symbols:
        symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    else:
        ap.error("--symbols or --universe-manifest is required")

    cfg = BuildConfig(
        symbols=symbols, timeframe=args.timeframe,
        start_ms=_parse_time(args.start), end_ms=_parse_time(args.end), label_horizon_bars=args.label_horizon_bars,
        higher_timeframe=args.higher_timeframe, snapshot_limit=args.snapshot_limit, venue=args.venue,
        cost_model_name=args.cost_model, allow_gross_only=args.allow_gross_only,
        setup_policy_name=args.setup_policy, regime_policy_name=args.regime_policy,
    )
    try:
        governance = governance_pins(universe_manifest=args.universe_manifest, dataset_manifest=args.dataset_manifest,
                                     holdout_start_ms=args.holdout_start_ms, cfg=cfg)
        series, meta, source_info = load_series_from_db(
            args.db, cfg.symbols, cfg.timeframe, cfg.start_ms, cfg.end_ms, label_horizon_bars=cfg.label_horizon_bars,
            warmup_bars=cfg.snapshot_limit, higher_timeframe=cfg.higher_timeframe, market_type=args.market_type,
            data_source=args.data_source,
        )
        if not series:
            raise BuildError("no historical candles found for the requested symbols/range")
        if args.universe_manifest and len(series) != len(cfg.symbols):
            raise BuildError(f"FROZEN_UNIVERSE_INCOMPLETE_{len(series)}_OF_{len(cfg.symbols)}")
        if args.holdout_start_ms is not None and any(
                int(r[0]) >= args.holdout_start_ms for tfs in series.values() for rows in tfs.values() for r in rows):
            raise BuildError("HOLDOUT_OVERLAP: a loaded candle opens at or after the holdout start")
        derived = classify_data_sources(source_info.get("data_sources", ()))
        try:
            kind = resolve_source_kind(args.declare_source_kind, derived)
        except SourceClassificationError as exc:
            raise BuildError(str(exc)) from exc
        import dataclasses as _dc

        cfg = _dc.replace(
            cfg, source_kind=kind, source_provider=",".join(source_info.get("data_sources", ())) or "unknown",
            source_dataset_id=f"sqlite:{Path(args.db).name}:historical_candles:{args.market_type}", market_type=args.market_type,
        )
        path, result = build_and_write(series, meta, cfg, args.output, source_info=source_info,
                                       governance=governance or None, workers=args.workers)
    except BuildError as exc:
        print(f"BUILD FAILED: {exc}", file=sys.stderr)
        return 2
    p = result.provenance
    print(json.dumps({"artifact": str(path), "library_hash": result.library.library_hash,
                      "source_kind": result.library.source_kind,
                      "candidates": p["candidate_count"], "labeled": p["labeled_count"],
                      "skipped": p["skipped_reasons"]}, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
