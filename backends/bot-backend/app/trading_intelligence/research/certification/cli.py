"""Operator interface (Section 22.32)::

    python -m app.trading_intelligence.research.certification plan \\
        --db ../shared/shared_lib/persistence/cosmicforge.db --symbols BTCUSDT,ETHUSDT --timeframe 15m

    python -m app.trading_intelligence.research.certification run --stage ALL \\
        --db ../shared/shared_lib/persistence/cosmicforge.db --symbols BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT \\
        --timeframe 15m --research-db data/research/certification.db --artifacts data/research/certification

    python -m app.trading_intelligence.research.certification report --artifacts data/research/certification
    python -m app.trading_intelligence.research.certification status --research-db data/research/certification.db

    python -m app.trading_intelligence.research.certification plan \\
        --db ../shared/shared_lib/persistence/cosmicforge.db \\
        --universe-manifest ../../docs/research/cati_crypto_universe_binance_v1.json

The candle database is opened READ-ONLY. ``run`` never opens the holdout
unless ``--open-holdout`` is passed (and the pipeline's own preconditions
hold). Nothing here enables CATI execution.

``--universe-manifest`` pins the run to a FROZEN universe manifest
(``app.market_data.universe.load_frozen_universe`` refuses an edited file):
its members replace ``--symbols``, its window is the default ``--start`` /
``--end``, and its ``universe_hash`` becomes part of the dataset identity, so
a different universe can never masquerade as the frozen one.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

BACKFILL = ("python scripts/ml/backfill_historical_candles.py --db-path {db} --symbols {symbols} "
            "--intervals {tf} --days {days}")


def _ms(text: Optional[str], default: int) -> int:
    if not text:
        return default
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _coverage(db: str, symbols: Sequence[str], tf: str) -> dict:
    out = {}
    conn = sqlite3.connect(f"file:{Path(db).as_posix()}?mode=ro", uri=True)
    try:
        for s in symbols:
            row = conn.execute("SELECT MIN(open_time), MAX(open_time), COUNT(*), GROUP_CONCAT(DISTINCT data_source) "
                               "FROM historical_candles WHERE symbol=? AND interval=? AND market_type='crypto'",
                               (s, tf)).fetchone()
            out[s] = {"start_ms": row[0], "end_ms": row[1], "rows": row[2], "data_sources": row[3]}
    finally:
        conn.close()
    return out


def _universe(args):
    """(manifest | None). Applies a frozen universe manifest to the args in place."""
    path = getattr(args, "universe_manifest", None)
    if not path:
        return None
    from app.market_data.universe import load_frozen_universe

    m = load_frozen_universe(path)
    args.symbols = ",".join(m["selected_symbols"])
    if m.get("timeframes") and args.timeframe not in m["timeframes"]:
        raise SystemExit(f"timeframe {args.timeframe} is not in the frozen universe {m['timeframes']}")
    fmt = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    args.start = args.start or fmt(m["window_start_ms"])
    args.end = args.end or fmt(m["window_end_ms"] - 1)
    return m


def _universe_summary(m) -> Optional[dict]:
    if not m:
        return None
    return {"universe_id": m["universe_id"], "universe_hash": m["universe_hash"], "role": m["role"],
            "members": len(m["selected_symbols"]), "window_start_ms": m["window_start_ms"],
            "window_end_ms": m["window_end_ms"], "rule_version": m["rule_version"],
            "survivorship_bias": m.get("survivorship_bias")}


def _load(args):
    from app.trading_intelligence.forecast.build_library import load_series_from_db

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    cov = _coverage(args.db, symbols, args.timeframe)
    starts = [c["start_ms"] for c in cov.values() if c["start_ms"]]
    ends = [c["end_ms"] for c in cov.values() if c["end_ms"]]
    if not starts:
        return symbols, cov, None, None, None
    start = _ms(args.start, max(starts))
    end = _ms(args.end, min(ends))
    series, meta, source = load_series_from_db(args.db, symbols, args.timeframe, start, end, label_horizon_bars=0,
                                               warmup_bars=0, higher_timeframe=args.higher_timeframe,
                                               data_source=args.data_source)
    return symbols, cov, series, meta, source


def _blocked(args, symbols, reason: str) -> int:
    from .pipeline import blocked_report

    cmd = BACKFILL.format(db=args.db or "<cosmicforge.db>", symbols=",".join(symbols), tf=args.timeframe, days=730)
    rep = blocked_report(reason=reason, symbols=symbols, timeframe=args.timeframe,
                         backfill_commands=(f"BACKFILL: {cmd}",))
    jpath, _ = rep.write(Path(args.artifacts))
    print(json.dumps({"stage": getattr(args, "stage", "PLAN"), "status": "BLOCKED_DATA", "reason": reason,
                      "overall": rep.overall_status, "policy_hash": rep.certification_policy["policy_hash"],
                      "artifact": str(jpath), "backfill": cmd}, indent=2))
    return 2


def cmd_plan(args) -> int:
    from .policy import policy_summary
    from .replay import ReplayConfig, chronology_for

    universe = _universe(args)
    if not args.db or not Path(args.db).exists():
        return _blocked(args, [s.upper() for s in args.symbols.split(",")], "CANDLE_DATABASE_NOT_FOUND")
    symbols, cov, series, meta, source = _load(args)
    if not series:
        return _blocked(args, symbols, "NO_CANDLES_FOR_SCOPE")
    cfg = ReplayConfig(symbols=tuple(sorted(series)), timeframe=args.timeframe,
                       label_horizon_bars=args.label_horizon_bars, higher_timeframe=args.higher_timeframe)
    plan = chronology_for(series, cfg)
    days = (max(c["end_ms"] for c in cov.values() if c["end_ms"]) - min(c["start_ms"] for c in cov.values() if c["start_ms"])) / 86_400_000
    missing = sorted(s for s in symbols if s not in series)
    print(json.dumps({"universe": _universe_summary(universe), "symbols_requested": len(symbols),
                      "symbols_with_data": len(series), "symbols_missing_data": missing,
                      "coverage": cov, "coverage_days": round(days, 2), "data_sources": source,
                      "chronology": plan.to_dict(), "policy": policy_summary(), "config_hash": cfg.config_hash,
                      "feasible": {"FAST": plan.evaluation_window.days >= 30 if plan.evaluation_window else False,
                                   "MEDIUM": plan.evaluation_window.days >= 180 if plan.evaluation_window else False,
                                   "FULL": days >= 730},
                      "backfill_for_full": BACKFILL.format(db=args.db, symbols=",".join(symbols), tf=args.timeframe,
                                                           days=730)}, indent=2, default=str))
    return 0


def cmd_run(args) -> int:
    from .pipeline import certify
    from .registry import SqliteResearchStore
    from .replay import ReplayConfig

    universe = _universe(args)
    symbols = [s.upper() for s in args.symbols.split(",")]
    if not args.db or not Path(args.db).exists():
        return _blocked(args, symbols, "CANDLE_DATABASE_NOT_FOUND")
    symbols, _cov, series, meta, source = _load(args)
    if universe is not None and len(series) != len(symbols):
        # a frozen universe is certified whole or not at all -- never a silent subset
        return _blocked(args, symbols, f"FROZEN_UNIVERSE_INCOMPLETE_{len(series)}_OF_{len(symbols)}")
    if not series:
        return _blocked(args, symbols, "NO_CANDLES_FOR_SCOPE")
    cfg = ReplayConfig(symbols=tuple(sorted(series)), timeframe=args.timeframe,
                       label_horizon_bars=args.label_horizon_bars, higher_timeframe=args.higher_timeframe)
    Path(args.research_db).parent.mkdir(parents=True, exist_ok=True)
    runtime = SqliteResearchStore(args.runtime_db) if args.runtime_db else None
    report = certify(series, meta, cfg=cfg, data_sources=source["data_sources"], source_provider="binance",
                     dataset_identity=(f"historical_candles:{Path(args.db).name}"
                                       + (f":universe:{universe['universe_hash']}" if universe else "")),
                     research_db=SqliteResearchStore(args.research_db), artifact_dir=Path(args.artifacts),
                     runtime_db=runtime, open_holdout=args.open_holdout,
                     library_output=Path(args.library_output) if args.library_output else None,
                     library_governance=_library_governance(args, universe))
    jpath, mpath = report.write(Path(args.artifacts))
    wanted = [s for s in report.stages if args.stage in ("ALL", s)]
    print(json.dumps({
        "dataset_hash": report.dataset_manifest.get("dataset_hash"),
        "policy_hash": report.certification_policy["policy_hash"],
        "policy_freeze_hash": report.policy_freeze["freeze_hash"],
        "stages": {s: {"status": report.stages[s].status, "reasons": list(report.stages[s].reason_codes)} for s in wanted},
        "overall": report.overall_status, "blocking": list(report.blocking_gates),
        "artifact": str(jpath), "markdown": str(mpath), "report_hash": report.report_hash}, indent=2))
    return 0


def _library_governance(args, universe) -> dict:
    out = {"universe_id": universe["universe_id"], "universe_hash": universe["universe_hash"]} if universe else {}
    if getattr(args, "dataset_manifest", None):
        from app.market_data.universe import verify_dataset_payload

        d = json.loads(Path(args.dataset_manifest).read_text(encoding="utf-8"))
        out["dataset_manifest_hash"] = verify_dataset_payload(d)  # refuses a tampered / unfrozen manifest
        if universe and d.get("universe_hash") != universe["universe_hash"]:
            raise SystemExit("dataset manifest belongs to another universe")
    return out


def cmd_report(args) -> int:
    files = sorted(Path(args.artifacts).glob("certification_*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        print(json.dumps({"status": "NO_REPORT", "artifacts": args.artifacts}))
        return 1
    data = json.loads(files[-1].read_text(encoding="utf-8"))
    print(json.dumps({"artifact": str(files[-1]), "report_hash": data["report_hash"], "overall": data["overall_status"],
                      "blocking": data["blocking_gates"],
                      "stages": {k: v["status"] for k, v in data["stages"].items()},
                      "gates": {g["gate"]: g["status"] for g in data["gates"]}}, indent=2))
    return 0


def cmd_status(args) -> int:
    from .registry import CertificationRunStore, ExperimentRegistry, SqliteResearchStore

    db = SqliteResearchStore(args.research_db)
    runs = CertificationRunStore(db).runs()
    exps = ExperimentRegistry(db).all()
    with db.connect() as conn:
        holds = [dict(r) for r in conn.execute("SELECT holdout_id, event, policy_freeze_hash, recorded_at "
                                               "FROM cati_holdout_registry ORDER BY recorded_at")]
    print(json.dumps({"certification_runs": [{k: r[k] for k in ("certification_run_id", "stage", "status",
                                                                  "dataset_hash", "policy_freeze_hash")} for r in runs],
                      "experiments": [{k: e[k] for k in ("experiment_id", "status", "dataset_hash")} for e in exps],
                      "holdout_events": holds}, indent=2))
    return 0


def cmd_readiness(args) -> int:
    """Section 22.3 pre-holdout readiness from RECORDED evidence only: the frozen dataset manifest, the report's
    policy freeze and the FULL/MEDIUM stage run persisted in the research DB. Reads no candles, opens nothing and
    authorizes nothing (``holdout_guard.authorize_holdout`` stays a separate, deliberate operator action)."""
    from .freeze import PolicyFreezeManifest
    from .holdout_guard import pre_holdout_readiness
    from .registry import CertificationRunStore, SqliteResearchStore

    manifest = json.loads(Path(args.dataset_manifest).read_text(encoding="utf-8")) if args.dataset_manifest else None
    report = json.loads(Path(args.report).read_text(encoding="utf-8")) if args.report else None
    freeze = run = None
    if report:
        pf = report["policy_freeze"]
        freeze = PolicyFreezeManifest(policy_hashes=pf["policy_hashes"], source_commit=pf["source_commit"],
                                      source_tree_dirty=pf["source_tree_dirty"], schema_version=pf["schema_version"])
        dataset_hash = report["dataset_manifest"].get("dataset_hash")
        runs = [r for r in CertificationRunStore(SqliteResearchStore(args.research_db)).runs()
                if r["dataset_hash"] == dataset_hash and r["policy_freeze_hash"] == freeze.freeze_hash]
        for stage in ("FULL", "MEDIUM"):
            hit = [r for r in runs if r["stage"] == stage]
            if hit:
                run = {"stage": stage, "status": hit[-1]["status"], "artifact_hash": hit[-1]["artifact_hash"]}
                break
    out = pre_holdout_readiness(dataset_manifest=manifest, acquisition_state=args.acquisition_state, freeze=freeze,
                                pre_holdout_run=run)
    print(json.dumps({**out, "pre_holdout_run": run, "report": args.report,
                      "dataset_manifest": args.dataset_manifest}, indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="certification", description="CATI Section 22 research & certification")
    sub = p.add_subparsers(dest="command", required=True)

    def data_args(sp):
        sp.add_argument("--db", help="candle database (opened read-only)")
        sp.add_argument("--symbols", default="BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT")
        sp.add_argument("--timeframe", default="15m")
        sp.add_argument("--higher-timeframe", default=None)
        sp.add_argument("--start")
        sp.add_argument("--end")
        sp.add_argument("--data-source", default=None)
        sp.add_argument("--label-horizon-bars", type=int, default=48)
        sp.add_argument("--artifacts", default="data/research/certification")
        sp.add_argument("--universe-manifest", default=None,
                        help="frozen universe manifest JSON (members + window + hash pin the run)")

    plan = sub.add_parser("plan")
    data_args(plan)
    plan.set_defaults(func=cmd_plan)
    run = sub.add_parser("run")
    data_args(run)
    run.add_argument("--stage", default="ALL", choices=["ALL", "FAST", "MEDIUM", "STRESS", "FULL", "HOLDOUT",
                                                         "FORWARD_DEMO"])
    run.add_argument("--research-db", default="data/research/certification.db")
    run.add_argument("--runtime-db", default=None, help="runtime evidence DB for forward demo / operational gates")
    run.add_argument("--open-holdout", action="store_true")
    run.add_argument("--library-output", default=None,
                     help="persist the canonical pre-holdout outcome library artifact here (runtime library)")
    run.add_argument("--dataset-manifest", default=None, help="frozen dataset manifest the run is pinned to")
    run.set_defaults(func=cmd_run)
    rep = sub.add_parser("report")
    rep.add_argument("--artifacts", default="data/research/certification")
    rep.set_defaults(func=cmd_report)
    st = sub.add_parser("status")
    st.add_argument("--research-db", default="data/research/certification.db")
    st.set_defaults(func=cmd_status)
    rd = sub.add_parser("readiness")
    rd.add_argument("--dataset-manifest", default=None, help="frozen dataset (lineage v2) manifest JSON")
    rd.add_argument("--acquisition-state", default=None, help="COMPLETE once the dataset's acquisition finished")
    rd.add_argument("--report", default=None, help="certification report JSON of the pre-holdout run")
    rd.add_argument("--research-db", default="data/research/certification.db")
    rd.set_defaults(func=cmd_readiness)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
