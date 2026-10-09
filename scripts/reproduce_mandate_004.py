"""Recompute a registered Mandate 004 run from scratch and compare it with the stored result (deliverable D12).

    backends/venv/Scripts/python.exe scripts/reproduce_mandate_004.py --run-id run_...

The official command is idempotent: asked for a run that exists, it returns the stored result without
computing anything. A reproduction has to compute. So this script copies the research register AS IT WAS just
before that run started into a scratch file, runs the same official entry point against the copy and a scratch
artifact folder, and compares:

* the run id (same pinned inputs and same evaluation source give the same id);
* the result hash (exact on the same platform and library versions);
* every number of every scenario, within ``--tolerance`` (for a different platform).

The committed register and the committed artifacts are not written to. A held-back run is reproduced only
when the committed register already shows its holdout as evaluated: reproducing never opens anything.
No credential is needed: the inputs are the public dataset and the repository.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "backends" / "bot-backend"), str(ROOT / "backends" / "shared")]


def _differences(a, b, tol, path=""):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k in ("provenance",):
                continue
            if k not in a or k not in b:
                yield f"{path}/{k}: present on one side only"
            else:
                yield from _differences(a[k], b[k], tol, f"{path}/{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            yield f"{path}: length {len(a)} vs {len(b)}"
        else:
            for i, (x, y) in enumerate(zip(a, b)):
                yield from _differences(x, y, tol, f"{path}[{i}]")
    elif isinstance(a, float) or isinstance(b, float):
        if a is None or b is None or abs(float(a) - float(b)) > tol * max(1.0, abs(float(a)), abs(float(b))):
            yield f"{path}: {a} vs {b}"
    elif a != b:
        yield f"{path}: {a!r} vs {b!r}"


def main() -> int:
    from app.trading_intelligence.research.evaluator import official as O
    from app.trading_intelligence.research.governance.history import HISTORICAL_ANCHOR
    from app.trading_intelligence.research.governance.register import ResearchRegister

    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--tolerance", type=float, default=O.NUMERIC_TOLERANCE)
    ap.add_argument("--store", default=str(ROOT / "data" / "research" / "binance_usdm_daily_v1"))
    args = ap.parse_args()

    committed = ResearchRegister()
    records = committed.records(anchor=tuple(HISTORICAL_ANCHOR))
    run = next((r for r in committed.runs() if r["run_id"] == args.run_id), None)
    if run is None or run["state"] != "RUN_COMPLETED":
        print(json.dumps({"status": "REFUSED", "reason": "not a completed run in the committed register"}))
        return 2
    if run["run_type"] == O.HOLDOUT and committed.holdout(run["holdout_id"])["status"] != "HOLDOUT_BURNED":
        print(json.dumps({"status": "REFUSED", "reason": "the holdout of this run is not recorded as evaluated"}))
        return 2
    stored = json.loads((ROOT / run["end"]["artifact_dir"] / "result.json").read_text(encoding="utf-8"))
    lines = committed.path.read_bytes().splitlines(keepends=True)
    cut = run["started_seq"] - 1                                  # the register as it was before RUN_STARTED
    if run["run_type"] == O.HOLDOUT:                              # keep the authorization, drop the opening and the result
        cut = min(r["seq"] for r in records if r["type"] == "HOLDOUT_OPENED" and r["body"]["holdout_id"] == run["holdout_id"]) - 1
        cut = min(cut, run["started_seq"] - 1)
    with tempfile.TemporaryDirectory() as tmp:
        scratch = ResearchRegister(Path(tmp) / "register.jsonl")
        scratch.path.write_bytes(b"".join(lines[:cut]))
        dataset = O.DatasetHandle(Path(args.store), ROOT / "docs" / "research")
        again = O.run_evaluation(run["run_type"], register=scratch, dataset=dataset, artifacts_root=Path(tmp) / "runs",
                                 researcher="independent reproduction", parent_run_id=run.get("parent_run_id"),
                                 reason_for_rerun=run.get("reason_for_rerun"), anchor=tuple(HISTORICAL_ANCHOR),
                                 require_committed_source=False)
    diffs = list(_differences(stored, again, args.tolerance))
    primary = stored["primary_scenario"]
    block = "holdout" if "holdout" in stored["blocks"] else "development"
    s, t = stored["blocks"][block]["scenarios"][primary], again["blocks"][block]["scenarios"][primary]
    out = {"run_id": args.run_id, "run_id_reproduced": again["run_id"] == args.run_id,
           "result_hash_identical": again["result_hash"] == stored["result_hash"], "tolerance": args.tolerance,
           "differences_beyond_tolerance": diffs[:20], "source_fingerprint_matches": again["source_fingerprint"] == stored["source_fingerprint"],
           "headline": {k: {"stored": s[k], "reproduced": t[k]} for k in ("net_return", "gross_return", "sharpe", "max_drawdown")},
           "trades": {"stored": s["trades"]["trades_closed"], "reproduced": t["trades"]["trades_closed"]},
           "verdict": {"stored": stored["verdict"], "reproduced": again["verdict"]},
           "status": "REPRODUCED" if not diffs else "DIFFERS"}
    print(json.dumps(out, indent=1))
    return 0 if not diffs else 1


if __name__ == "__main__":
    sys.exit(main())
