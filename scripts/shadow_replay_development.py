"""Replay the development period through the CATI platform cycle driver and reconcile it with Step 2.

Section H, Steps 3.6 and 3.15. The frozen dataset's development rows (never a held-back row: the dataset handle
refuses them) are fed day by day to the real ``PlatformCycleDriver`` in a scratch database. Each model account's
recorded decisions are then held against

* the committed reference of the development run (``development_primary_daily_targets.csv`` and the files
  beside it), for the primary account (mandate sizing, Balanced), and
* the evaluator recomputed on the same bars, for every account.

Nothing is certified by this script and no order can be placed by it: the pipeline runs in shadow on model
accounts that have no exchange. Usage, from ``backends/bot-backend``::

    python ../../scripts/shadow_replay_development.py --out ../../docs/implementation/evidence/step3
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run-id", default="run_0b88d389f8da50dc48de5e51")
    ap.add_argument("--out", required=True, help="directory for the reconciliation report")
    ap.add_argument("--last-day", default=None, help="stop early (a development day), for a quick check")
    args = ap.parse_args()

    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    from app.trading_intelligence.families.daily_trend import spec
    from app.trading_intelligence.families.daily_trend import targets as T
    from app.trading_intelligence.pipeline import daily_trend as DT
    from app.trading_intelligence.pipeline import shadow_compare as SC
    from app.trading_intelligence.pipeline.accounts import ModelAccounts, model_account_id
    from app.trading_intelligence.pipeline.driver import COMPLETED, PlatformCycleDriver
    from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
    from app.trading_intelligence.pipeline.market_data import FrozenPanelBars
    from app.trading_intelligence.research.evaluator import official as O
    from app.trading_intelligence.research.evaluator import simulator as S
    from app.trading_intelligence.research.governance.register import ResearchRegister, default_register_path, repository_root

    root = repository_root()
    register = ResearchRegister(default_register_path())
    run = next(r for r in register.runs(mandate_id=spec.MANDATE_ID) if r["run_id"] == args.run_id)
    hypothesis = register.hypothesis(spec.HYPOTHESIS_ID)                # the registered periods, not retyped here
    periods = O.Periods(hypothesis["development_period"]["start"], hypothesis["development_period"]["end"],
                        hypothesis["holdout_period"]["start"], hypothesis["holdout_period"]["end"])
    last_day = args.last_day or periods.development_end
    dataset = O.DatasetHandle(root / "data/research/binance_usdm_daily_v1", root / "docs/research")
    panel = dataset.panel(periods=periods, last_day=last_day)          # refuses any day after the development period
    initial_equity = spec.INTERPRETATION["initial_equity_usdt"]
    first, last = DT.day_of(periods.development_start), DT.day_of(last_day)

    scratch = Path(tempfile.mkdtemp(prefix="cati_shadow_replay_"))
    db = DB(str(scratch / "replay.db"))
    migrate(db)
    registry = FamilyRegistry(db)
    registry.register_all(actor_ref="system:shadow_replay_development")
    driver = PlatformCycleDriver(db, accounts=ModelAccounts(initial_equity=initial_equity),
                                 data={"1d": FrozenPanelBars(panel, dataset.metadata)}, registry=registry,
                                 owner="shadow_replay_development")
    started = time.time()
    stage_ms = {}
    # the evaluator's account starts on the first development day, so its first decision is at the close before
    # it; the panel has no bar there, so the pipeline's account is opened at the first close and both are
    # compared from the first day they share
    for day in range(first, last + 1):
        result = driver.run_cycle(spec.FAMILY_ID, DT.day_text(day), now_ms=DT.close_ms(day) + 60_000)
        if result.status != COMPLETED:
            print(f"cycle {DT.day_text(day)}: {result.status} {result.reason} {result.account_results}", file=sys.stderr)
            return 2
        for stage, ms in result.timings_ms.items():
            stage_ms[stage] = stage_ms.get(stage, 0.0) + ms
        if (day - first) % 200 == 0:
            print(f"{DT.day_text(day)} cycles={day - first + 1} elapsed={time.time() - started:.0f}s", flush=True)
    replay_seconds = time.time() - started

    features = S.prepare(panel, dataset.metadata)
    accounts = {}
    for policy in (T.MANDATE, T.EXECUTABLE):
        for level in ("conservative", "balanced", "aggressive"):
            account = model_account_id(policy, level)
            out = SC.compare_account_forward(db, family_id=spec.FAMILY_ID, account_id=account, features=features,
                                             risk_level=level, policy=policy, first_day=first + 1, last_day=last,
                                             initial_equity=initial_equity)
            accounts[account] = {"against": "EVALUATOR_RECOMPUTED_ON_THE_SAME_BARS", **out["summary"],
                                 "first_alerts": out["alerts"][:5]}
    primary = model_account_id(T.MANDATE, "balanced")
    run_dir = root / "docs/research/mandate_004/runs" / args.run_id
    reference = SC.reference_from_csv(run_dir / "development_primary_daily_targets.csv",
                                      daily_csv=run_dir / "development_primary_daily.csv",
                                      rejections_csv=run_dir / "development_primary_rejections.csv")
    shadow = SC.shadow_from_db(db, primary, spec.FAMILY_ID)
    # The committed run opened its account on the first development day with nothing to decide before it; the
    # pipeline's account is one day younger. Until the first target appears both hold cash only and are equal.
    days = [d for d in sorted(shadow) if d in reference]
    committed = SC.compare(shadow, reference, days=days)
    alerts = SC.record(db, family_id=spec.FAMILY_ID, account_id=primary,
                       reference_label=f"COMMITTED_DEVELOPMENT_RUN:{args.run_id}", comparisons=committed)
    targeted = sum(1 for d in days if reference[d]["targets"])
    report = {
        "what": "Section H Step 3.6 / 3.15: the development period replayed through the CATI platform cycle driver "
                "in shadow, reconciled with the Step 2 evaluator",
        "mandate_id": spec.MANDATE_ID, "hypothesis_id": spec.HYPOTHESIS_ID,
        "rule_hash": registry.entry(spec.FAMILY_ID).definition.rule_hash,
        "reference_run_id": args.run_id, "reference_result_hash": run["end"]["result_hash"],
        "dataset_hash": dataset.dataset_hash, "manifest_hash": dataset.manifest_hash,
        "period": {"first_cycle": periods.development_start, "last_cycle": last_day,
                   "held_back_rows_read": 0, "note": "the dataset handle refuses rows after the development period"},
        "cycles": last - first + 1, "mode": "SHADOW", "orders_sent": 0,
        "tolerance": {"relative": SC.TOLERANCE, "precision_band": SC.PRECISION_BAND},
        "primary_account_against_the_committed_reference": {
            "account": primary, "files": ["development_primary_daily_targets.csv", "development_primary_daily.csv",
                                          "development_primary_rejections.csv"],
            "days_compared": len(days), "days_with_targets": targeted, **SC.summary(committed),
            "first_alerts": alerts[:5]},
        "accounts_against_the_recomputed_evaluator": accounts,
        "timing": {"replay_seconds": round(replay_seconds, 1), "seconds_per_cycle": round(replay_seconds / (last - first + 1), 3),
                   "stage_seconds": {k: round(v / 1000.0, 1) for k, v in sorted(stage_ms.items())},
                   "host": "Windows workstation (not a certification of Linux behaviour)"},
        "source_commit": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=root).stdout.strip(),
        "working_tree_dirty": bool(subprocess.run(["git", "status", "--porcelain", "--", "backends/bot-backend/app",
                                                   "backends/shared"], capture_output=True, text=True, cwd=root).stdout.strip()),
    }
    ok = report["primary_account_against_the_committed_reference"]["by_status"] == {SC.MATCH: len(days)} and all(
        set(a["by_status"]) == {SC.MATCH} for a in accounts.values())
    report["verdict"] = "REPRODUCED" if ok else "DIVERGED"
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    name = "development_shadow_reconciliation.json" if args.last_day is None else "development_shadow_reconciliation.partial.json"
    (out / name).write_bytes((json.dumps(report, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    print(json.dumps({k: report[k] for k in ("verdict", "cycles", "timing")}, indent=1))
    print("primary:", report["primary_account_against_the_committed_reference"]["by_status"],
          "largest", report["primary_account_against_the_committed_reference"]["largest_relative_difference"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
