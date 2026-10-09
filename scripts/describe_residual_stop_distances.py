"""Describe the stop distances of the residual momentum family's committed historical trades.

    backends/venv/Scripts/python.exe scripts/describe_residual_stop_distances.py

Section H, Step 2.4 (6.10). Input: ``docs/research/cati_edge003/selected_portfolio_labels.jsonl.gz`` -- the
selected trades of Mandate 003's single frozen run, which FAILED its economic gate (hypothesis 15).

This is a DESCRIPTION of a failed family, not a test and not a search: it splits the trades that were already
evaluated by one fixed, pre-existing threshold (the engine's 15% maximum stop distance) and reports both
halves. It changes no verdict and is not a new hypothesis. Output:
``docs/research/mandate_004/residual_stop_distance.json``.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "research" / "cati_edge003" / "selected_portfolio_labels.jsonl.gz"
OUT = ROOT / "docs" / "research" / "mandate_004" / "residual_stop_distance.json"
FAMILY, LIMIT = "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", 0.15


def main() -> int:
    rows = [json.loads(line) for line in gzip.open(SOURCE, "rt", encoding="utf-8")]
    rows = [r for r in rows if r["family"] == FAMILY]
    dist = [r["risk"] / r["entry_prices"][0] for r in rows]                 # stop distance as a fraction of the entry price
    net = [r["net_R"] for r in rows]
    inside = [n for n, d in zip(net, dist) if d <= LIMIT]
    beyond = [n for n, d in zip(net, dist) if d > LIMIT]
    ordered = sorted(dist)
    mean = lambda v: sum(v) / len(v) if v else None                         # noqa: E731
    out = {
        "schema": "residual-stop-distance-description-v1", "family": FAMILY, "hypothesis_id": "H-015",
        "nature": "DESCRIPTION_OF_A_FAILED_FAMILY_NOT_A_TEST", "engine_limit": LIMIT,
        "source": SOURCE.relative_to(ROOT).as_posix(), "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        "stop_distance": "risk / entry price (the same fraction the engine checks)",
        "trades": len(rows), "within_limit": len(inside), "beyond_limit": len(beyond),
        "share_beyond_limit": len(beyond) / len(rows), "median_stop_distance": ordered[len(ordered) // 2],
        "max_stop_distance": ordered[-1], "mean_net_R_all": mean(net), "mean_net_R_within_limit": mean(inside),
        "mean_net_R_beyond_limit": mean(beyond),
        "prospective_records": {
            "recorded_on": "2026-10-08", "decisions_recorded": 74, "outside_engine_stop_limit": 55,
            "source": "docs/implementation/STEP1_PORTAL_TO_ENGINE_STATUS.md (deployment preview on the runtime database)",
            "kind": "FORWARD_OBSERVATION_RECORDS: mode OBSERVE, entry authority BLOCKED, reference = decision close, "
                    "portfolio-selected and counterfactual candidates together; not executable plans",
            "counted_by": "backends/bot-backend/app/core/deployment_service.py stop_distance_assumptions"},
        "stop_geometry": "risk = max(distance to the prior-24h swing + 0.25 x ATR14, 2 x ATR14, 0.3% of close) on hourly bars",
        "engine_behaviour": "a plan whose stop is beyond 15% of the live price is rejected "
                            "(CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX); the stop is never moved or clipped"}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_bytes((json.dumps(out, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    print(json.dumps({k: out[k] for k in ("trades", "within_limit", "beyond_limit", "share_beyond_limit", "mean_net_R_all",
                                          "mean_net_R_within_limit", "mean_net_R_beyond_limit")}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
