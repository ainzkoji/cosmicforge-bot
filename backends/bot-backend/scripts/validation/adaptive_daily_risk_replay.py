#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
BOT_ROOT = ROOT / "backends" / "bot-backend"
if str(BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(BOT_ROOT))

from app.risk.adaptive_daily_replay import (  # noqa: E402
    DEFAULT_STARTING_EQUITY,
    build_report_payload,
    load_replay_dataset,
    replay_policies,
    write_replay_artifacts,
)


def _default_db() -> Path:
    return ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"


def _code_revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "UNKNOWN"


def _connect_read_only(path: Path) -> sqlite3.Connection:
    uri = path.resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay-calibrate the adaptive daily risk policy against closed canonical broker outcomes."
    )
    parser.add_argument("--db-path", default=str(_default_db()))
    parser.add_argument("--starting-equity", type=float, default=DEFAULT_STARTING_EQUITY)
    parser.add_argument(
        "--report-path",
        default=str(ROOT / "docs" / "adaptive_daily_risk_replay_report.md"),
    )
    parser.add_argument(
        "--json-path",
        default=str(ROOT / "docs" / "adaptive_daily_risk_replay_results.json"),
    )
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    with _connect_read_only(db_path) as conn:
        dataset = load_replay_dataset(conn)

    results = replay_policies(dataset, starting_equity=float(args.starting_equity))
    payload = build_report_payload(
        dataset=dataset,
        results=results,
        code_revision=_code_revision(),
        canonical_db_path=str(db_path),
    )
    write_replay_artifacts(
        payload,
        report_path=Path(args.report_path),
        json_path=Path(args.json_path),
    )
    print(f"dataset_hash={dataset.dataset_hash}")
    print(f"opportunities={dataset.source_summary['opportunity_count']}")
    print(f"risk_observations={dataset.source_summary['risk_observation_count']}")
    print(f"report={Path(args.report_path).resolve()}")
    print(f"json={Path(args.json_path).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
