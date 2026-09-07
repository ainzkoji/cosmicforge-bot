#!/usr/bin/env python3
"""Track when Section 5A may be retried and whether Section 5B remains blocked."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import pandas as pd

from scripts.ml.check_readiness import check_readiness


logger = logging.getLogger(__name__)

DEFAULT_ORGANIC_DATASET = _BOT_ROOT / "models" / "datasets" / "training_v2_organic.parquet"
DEFAULT_IOFS_DATASET = _BOT_ROOT / "models" / "datasets" / "training_v2_iofs_organic.parquet"
DEFAULT_PAPER_STATUS = _BOT_ROOT / "models" / "reports" / "iofs_paper_validation_status.md"
DEFAULT_PAPER_REVIEWS = _BOT_ROOT / "models" / "reports" / "iofs_paper_trade_reviews.md"
DEFAULT_SECTION5B_STATUS = _BOT_ROOT / "models" / "reports" / "section5b_blocker_status.md"
DEFAULT_ARTIFACTS_DIR = _BOT_ROOT / "models" / "artifacts"
DEFAULT_OUTPUT_JSON = _BOT_ROOT / "models" / "reports" / "ml_training_readiness_status.json"
DEFAULT_OUTPUT_MD = _BOT_ROOT / "models" / "reports" / "ml_training_readiness_status.md"

DEFAULT_MIN_ORGANIC_ROWS = 500
DEFAULT_MIN_IOFS_ORGANIC_ROWS = 300
DEFAULT_MIN_CLOSED_IOFS_TRADES = 20


def evaluate_training_readiness(
    *,
    organic_dataset: str | Path = DEFAULT_ORGANIC_DATASET,
    iofs_organic_dataset: str | Path = DEFAULT_IOFS_DATASET,
    paper_status_path: str | Path = DEFAULT_PAPER_STATUS,
    paper_reviews_path: str | Path = DEFAULT_PAPER_REVIEWS,
    section5b_status_path: str | Path = DEFAULT_SECTION5B_STATUS,
    artifacts_dir: str | Path = DEFAULT_ARTIFACTS_DIR,
    active_env_path: str | Path | None = None,
    min_organic_rows: int = DEFAULT_MIN_ORGANIC_ROWS,
    min_iofs_organic_rows: int = DEFAULT_MIN_IOFS_ORGANIC_ROWS,
    min_closed_iofs_trades: int = DEFAULT_MIN_CLOSED_IOFS_TRADES,
) -> dict[str, Any]:
    organic_path = Path(organic_dataset)
    iofs_path = Path(iofs_organic_dataset)
    paper_status = Path(paper_status_path)
    paper_reviews = Path(paper_reviews_path)
    section5b_status = Path(section5b_status_path)
    artifacts = Path(artifacts_dir)
    env_path = Path(active_env_path) if active_env_path else _BOT_ROOT / ".env"

    organic_rows = _dataset_rows(organic_path)
    iofs_organic_rows = _iofs_organic_rows(iofs_path)
    closed_trades = _closed_iofs_paper_trades(paper_status)
    review_entries = _review_entry_count(paper_reviews)

    minimum_organic_rows_met = organic_rows >= 300
    recommended_organic_rows_met = organic_rows >= min_organic_rows
    iofs_data_available = iofs_organic_rows > 0
    iofs_retry_rows_met = iofs_organic_rows >= min_iofs_organic_rows
    closed_trade_retry_threshold_met = closed_trades >= min_closed_iofs_trades
    ready_to_retry_5a = (
        recommended_organic_rows_met
        or iofs_retry_rows_met
        or closed_trade_retry_threshold_met
    )

    organic_readiness = check_readiness(
        organic_path,
        section4_status_path=paper_status,
        artifacts_dir=artifacts,
    )
    accepted_artifact = organic_readiness["accepted_model_artifact"]
    promotion_guard_allows_5b = (
        accepted_artifact["accepted_model_artifact_exists"]
        and organic_readiness["models_production_unchanged"]
    )
    ready_for_5b = bool(
        organic_readiness["ready_for_5b_shadow_deployment"]
        and promotion_guard_allows_5b
    )

    blocking_reasons: list[str] = []
    if not ready_to_retry_5a:
        blocking_reasons.extend(
            [
                f"Organic rows {organic_rows} are below retry threshold {min_organic_rows}.",
                f"IOFS organic/paper rows {iofs_organic_rows} are below retry threshold "
                f"{min_iofs_organic_rows}.",
                f"Closed IOFS paper trades {closed_trades} are below retry threshold "
                f"{min_closed_iofs_trades}.",
            ]
        )
    if not accepted_artifact["accepted_model_artifact_exists"]:
        blocking_reasons.append("No accepted validated runtime-compatible v2 .pkl artifact exists.")
    if not promotion_guard_allows_5b:
        blocking_reasons.append("Section 5B promotion guard does not allow deployment.")

    active_env = _read_env(env_path)
    last_candidate_reasons = [
        _plain_text(reason)
        for reason in organic_readiness.get("latest_candidate_rejection_reasons") or []
    ]
    last_candidate_result = (
        "REJECTED: " + " | ".join(last_candidate_reasons)
        if last_candidate_reasons
        else "No rejected candidate result found."
    )
    next_action = (
        "section_5b_ready"
        if ready_for_5b
        else "retry_section_5a_manually"
        if ready_to_retry_5a
        else "continue_paper_validation"
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ready_to_retry_5a": ready_to_retry_5a,
        "ready_for_5b": ready_for_5b,
        "organic_rows": organic_rows,
        "iofs_organic_rows": iofs_organic_rows,
        "closed_iofs_paper_trades": closed_trades,
        "paper_trade_review_entries": review_entries,
        "minimum_organic_rows_met": minimum_organic_rows_met,
        "recommended_organic_rows_met": recommended_organic_rows_met,
        "iofs_data_available": iofs_data_available,
        "iofs_retry_rows_met": iofs_retry_rows_met,
        "closed_trade_retry_threshold_met": closed_trade_retry_threshold_met,
        "thresholds": {
            "minimum_organic_rows": 300,
            "recommended_organic_rows": min_organic_rows,
            "minimum_iofs_organic_rows": min_iofs_organic_rows,
            "minimum_closed_iofs_trades": min_closed_iofs_trades,
        },
        "accepted_model_artifact": accepted_artifact,
        "promotion_guard_allows_5b": promotion_guard_allows_5b,
        "last_candidate_result": last_candidate_result,
        "section5b_status_report_exists": section5b_status.exists(),
        "blocking_reasons": blocking_reasons,
        "next_action": next_action,
        "auto_training_enabled": False,
        "safety": {
            "execution_mode": active_env.get("EXECUTION_MODE"),
            "ml_enabled": active_env.get("ML_ENABLED"),
            "iofs_gate_mode": active_env.get("IOFS_GATE_MODE"),
            "models_production_unchanged": organic_readiness["models_production_unchanged"],
        },
    }


def render_markdown(result: dict[str, Any]) -> str:
    blockers = result["blocking_reasons"] or ["None"]
    lines = [
        "# ML Training Readiness Status",
        "",
        f"- Last updated timestamp: {result['generated_at']}",
        f"- Section 5A retry status: {'Ready for manual retry' if result['ready_to_retry_5a'] else 'Not ready'}",
        f"- Section 5B status: {'Ready' if result['ready_for_5b'] else 'Blocked'}",
        f"- Organic row count: {result['organic_rows']}",
        f"- IOFS organic/paper row count: {result['iofs_organic_rows']}",
        f"- Closed IOFS paper trades: {result['closed_iofs_paper_trades']}",
        f"- Paper trade review entries: {result['paper_trade_review_entries']}",
        f"- Last candidate result: {result['last_candidate_result']}",
        f"- Next action: {result['next_action']}",
        "",
        "## Current Blockers",
        "",
        *[f"- {reason}" for reason in blockers],
        "",
        "## Next Retry Condition",
        "",
        f"- Organic rows reach {result['thresholds']['recommended_organic_rows']}; or",
        f"- IOFS organic/paper rows reach {result['thresholds']['minimum_iofs_organic_rows']}; or",
        f"- Closed IOFS paper trades reach {result['thresholds']['minimum_closed_iofs_trades']}.",
        "",
        "## ML Safety Status",
        "",
        f"- ML_ENABLED: {result['safety']['ml_enabled']}",
        f"- EXECUTION_MODE: {result['safety']['execution_mode']}",
        f"- IOFS_GATE_MODE: {result['safety']['iofs_gate_mode']}",
        f"- models/production unchanged: {str(result['safety']['models_production_unchanged']).lower()}",
        "- Auto-training enabled: false",
        "- Auto-promotion enabled: false",
        "",
    ]
    return "\n".join(lines)


def write_readiness_reports(
    result: dict[str, Any],
    *,
    output_json: str | Path = DEFAULT_OUTPUT_JSON,
    output_md: str | Path = DEFAULT_OUTPUT_MD,
) -> None:
    json_path = Path(output_json)
    md_path = Path(output_md)
    previous = _read_previous_status(json_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_markdown(result), encoding="utf-8")
    _log_readiness_state(result, previous)


def run_watcher(
    *,
    output_json: str | Path = DEFAULT_OUTPUT_JSON,
    output_md: str | Path = DEFAULT_OUTPUT_MD,
    **kwargs: Any,
) -> dict[str, Any]:
    result = evaluate_training_readiness(**kwargs)
    write_readiness_reports(result, output_json=output_json, output_md=output_md)
    return result


def _dataset_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(len(pd.read_parquet(path)))
    except Exception:
        meta = path.with_name(f"{path.stem}_meta.json")
        try:
            return int(json.loads(meta.read_text(encoding="utf-8")).get("row_count") or 0)
        except Exception:
            return 0


def _iofs_organic_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        frame = pd.read_parquet(path)
    except Exception:
        return 0
    if "data_source" not in frame.columns:
        return int(len(frame))
    return int(frame["data_source"].isin(["organic", "paper"]).sum())


def _closed_iofs_paper_trades(path: Path) -> int:
    if not path.exists():
        return 0
    match = re.search(
        r"number_of_closed_paper_trades:\s*(\d+)",
        path.read_text(encoding="utf-8"),
        flags=re.IGNORECASE,
    )
    return int(match.group(1)) if match else 0


def _review_entry_count(path: Path) -> int:
    if not path.exists():
        return 0
    text = path.read_text(encoding="utf-8")
    return len(
        re.findall(
            r"^##\s+(?:Trade\b|Review\s+(?!Queue\b))",
            text,
            flags=re.IGNORECASE | re.MULTILINE,
        )
    )


def _read_env(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def _plain_text(value: Any) -> str:
    return str(value).replace("\u2014", "-").replace("\u2013", "-")


def _read_previous_status(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _log_readiness_state(result: dict[str, Any], previous: dict[str, Any]) -> None:
    if result["ready_for_5b"]:
        event = "SECTION_5B_READY"
    elif result["ready_to_retry_5a"]:
        event = "ML_RETRY_READY"
    else:
        event = "SECTION_5B_STILL_BLOCKED"
    changed = (
        previous.get("ready_to_retry_5a") != result["ready_to_retry_5a"]
        or previous.get("ready_for_5b") != result["ready_for_5b"]
    )
    log = logger.warning if changed else logger.info
    log(
        "[ML_RETRY_WATCHER] %s changed=%s organic_rows=%s iofs_organic_rows=%s "
        "closed_iofs_paper_trades=%s next_action=%s",
        event,
        str(changed).lower(),
        result["organic_rows"],
        result["iofs_organic_rows"],
        result["closed_iofs_paper_trades"],
        result["next_action"],
    )


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--organic-dataset", default=str(DEFAULT_ORGANIC_DATASET))
    parser.add_argument("--iofs-organic-dataset", default=str(DEFAULT_IOFS_DATASET))
    parser.add_argument("--paper-status-path", default=str(DEFAULT_PAPER_STATUS))
    parser.add_argument("--paper-reviews-path", default=str(DEFAULT_PAPER_REVIEWS))
    parser.add_argument("--section5b-status-path", default=str(DEFAULT_SECTION5B_STATUS))
    parser.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACTS_DIR))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument("--output-md", default=str(DEFAULT_OUTPUT_MD))
    parser.add_argument(
        "--min-organic-rows",
        type=int,
        default=_env_int("ML_RETRY_MIN_ORGANIC_ROWS", DEFAULT_MIN_ORGANIC_ROWS),
    )
    parser.add_argument(
        "--min-iofs-organic-rows",
        type=int,
        default=_env_int("ML_RETRY_MIN_IOFS_ORGANIC_ROWS", DEFAULT_MIN_IOFS_ORGANIC_ROWS),
    )
    parser.add_argument(
        "--min-closed-iofs-trades",
        type=int,
        default=_env_int("ML_RETRY_MIN_CLOSED_IOFS_TRADES", DEFAULT_MIN_CLOSED_IOFS_TRADES),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_watcher(
        organic_dataset=args.organic_dataset,
        iofs_organic_dataset=args.iofs_organic_dataset,
        paper_status_path=args.paper_status_path,
        paper_reviews_path=args.paper_reviews_path,
        section5b_status_path=args.section5b_status_path,
        artifacts_dir=args.artifacts_dir,
        output_json=args.output_json,
        output_md=args.output_md,
        min_organic_rows=args.min_organic_rows,
        min_iofs_organic_rows=args.min_iofs_organic_rows,
        min_closed_iofs_trades=args.min_closed_iofs_trades,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
