"""Read-only status of forward-only residual evidence; no prices/holdout queries."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[1]


def status(path):
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_tracker'").fetchone():
            return {"status": "NOT_ENROLLED", "observed_at": datetime.now(timezone.utc).isoformat()}
        trackers = [dict(x) for x in conn.execute("SELECT * FROM cati_residual_tracker")]
        for tracker in trackers:
            tracker["detail"] = json.loads(tracker.pop("detail_json"))
        counts = [dict(x) for x in conn.execute("""SELECT portfolio_selected,lifecycle,outcome,COUNT(*) AS records,
            AVG(gross_R) AS mean_gross_R,AVG(cost_R) AS mean_cost_R,AVG(net_R) AS mean_net_R
            FROM cati_residual_decisions GROUP BY portfolio_selected,lifecycle,outcome""")]
        decisions = [dict(x) for x in conn.execute("""SELECT decision_time,recorded_at,selected_symbol,side,
            score,entry_reference,entry_price,stop,target,risk,portfolio_selected,lifecycle,outcome,
            outcome_time,gross_R,cost_R,net_R,risk_state_json,eligible_universe_json
            FROM cati_residual_decisions ORDER BY decision_time DESC LIMIT 5""")]
        for decision in decisions:
            decision["risk_state"] = json.loads(decision.pop("risk_state_json"))
            decision["eligible_universe"] = json.loads(decision.pop("eligible_universe_json"))
        sources = [dict(x) for x in conn.execute("SELECT source,COUNT(*) AS closed_input_bars,COUNT(DISTINCT symbol) AS symbols,MAX(received_at) AS latest_received_at FROM cati_residual_inputs GROUP BY source")]
    return {"observed_at": datetime.now(timezone.utc).isoformat(), "mode": "OBSERVE",
            "execution_authority": "NONE", "tracker": trackers, "populations": counts,
            "latest_decisions": decisions, "input_sources": sources,
            "counterfactuals_are_not_portfolio_trades": True, "holdout_queries": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=ROOT / "backends/shared/shared_lib/persistence/cosmicforge.db")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = status(args.db)
    text = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text)
