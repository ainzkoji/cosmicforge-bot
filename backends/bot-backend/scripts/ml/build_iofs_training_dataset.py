#!/usr/bin/env python3
"""Build source-separated IOFS organic and replay research datasets."""
from __future__ import annotations

import argparse
import json
import sqlite3
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

from shared_lib.ml.contract import (
    LABEL_COLUMNS,
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
)


IOFS_FEATURE_COLUMNS = [
    "iofs_score",
    "iofs_passed",
    "iofs_reason",
    "iofs_direction",
    "iofs_risk_profile",
    "trend_direction",
    "trend_adx",
    "trend_ema_sep_pct",
    "structure_level",
    "structure_retest_active",
    "structure_retest_distance_atr",
    "structure_candles_since_break",
    "trigger_confirmed",
    "trigger_pattern",
    "trigger_wick_ratio",
    "session_window",
    "symbol",
    "hour_utc",
    "regime",
]
REPLAY_LABEL_COLUMNS = ["label_win", "label_r_multiple", "label_exit_reason"]
LEAKAGE_FIELDS = {
    "outcome",
    "r_multiple",
    "exit_time",
    "candles_held",
    "ambiguous_candle",
    "tp1_hit",
    "label_win",
    "label_r_multiple",
    "label_exit_reason",
    *LABEL_COLUMNS,
}


def resolve_db_path(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    return (_SHARED_ROOT / "shared_lib" / "persistence" / "cosmicforge.db").resolve()


def load_iofs_events(db_path: Path) -> pd.DataFrame:
    query = """
        SELECT trace_id, timestamp_utc, symbol, details_json
        FROM events
        WHERE event_type = 'IOFS_GATE'
          AND trace_id IS NOT NULL
        ORDER BY id DESC
    """
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        rows = connection.execute(query).fetchall()
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for trace_id, timestamp_utc, symbol, details_json in rows:
        if trace_id in seen:
            continue
        seen.add(trace_id)
        details = json.loads(details_json or "{}")
        records.append(
            {
                "trace_id": str(trace_id),
                "iofs_timestamp_utc": timestamp_utc,
                "iofs_score": details.get("score"),
                "iofs_passed": details.get("passed"),
                "iofs_reason": details.get("reason"),
                "iofs_direction": details.get("direction"),
                "iofs_risk_profile": details.get("risk_profile"),
                "trend_direction": details.get("trend_direction"),
                "trend_adx": details.get("trend_adx"),
                "trend_ema_sep_pct": details.get("trend_ema_sep_pct"),
                "structure_level": details.get("structure_level"),
                "structure_retest_active": details.get("structure_retest_active"),
                "structure_retest_distance_atr": details.get("structure_retest_distance_atr"),
                "structure_candles_since_break": details.get("structure_candles_since_break"),
                "trigger_confirmed": details.get("trigger_confirmed"),
                "trigger_pattern": details.get("trigger_pattern"),
                "trigger_wick_ratio": details.get("trigger_wick_ratio"),
                "session_window": _session_window(timestamp_utc),
                "symbol": details.get("symbol") or symbol,
                "hour_utc": _hour_utc(timestamp_utc),
                "regime": None,
            }
        )
    return pd.DataFrame(records)


def build_organic_iofs_dataset(
    organic: pd.DataFrame,
    iofs_events: pd.DataFrame,
) -> pd.DataFrame:
    """Keep only organic/paper trades with trace-linked pre-entry IOFS metadata."""
    base = organic.copy()
    if "trace_id" not in base.columns or iofs_events.empty:
        for column in IOFS_FEATURE_COLUMNS:
            if column not in base.columns:
                base[column] = pd.Series(dtype="object")
        base["data_source"] = pd.Series(dtype="object")
        return base.iloc[0:0].copy()

    merged = base.merge(
        iofs_events,
        on="trace_id",
        how="inner",
        validate="one_to_one",
        suffixes=("", "_iofs"),
    )
    for column in IOFS_FEATURE_COLUMNS:
        iofs_column = f"{column}_iofs"
        if iofs_column in merged.columns:
            merged[column] = merged[iofs_column].combine_first(merged.get(column))
            merged = merged.drop(columns=[iofs_column])
    merged["data_source"] = "paper"
    return merged


def build_replay_dataset(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in records:
        timestamp = record.get("signal_time") or record.get("timestamp_utc")
        r_multiple = _number(record.get("r_multiple"))
        row = {
            "data_source": "replay",
            "iofs_score": record.get("score"),
            "iofs_passed": record.get("passed"),
            "iofs_reason": record.get("reason"),
            "iofs_direction": record.get("direction"),
            "iofs_risk_profile": record.get("risk_profile"),
            "trend_direction": record.get("trend_direction"),
            "trend_adx": record.get("trend_adx"),
            "trend_ema_sep_pct": record.get("trend_ema_sep_pct"),
            "structure_level": record.get("structure_level"),
            "structure_retest_active": record.get("structure_retest_active"),
            "structure_retest_distance_atr": record.get("structure_retest_distance_atr"),
            "structure_candles_since_break": record.get("structure_candles_since_break"),
            "trigger_confirmed": record.get("trigger_confirmed"),
            "trigger_pattern": record.get("trigger_pattern"),
            "trigger_wick_ratio": record.get("trigger_wick_ratio"),
            "session_window": record.get("session_window"),
            "symbol": record.get("symbol"),
            "hour_utc": _hour_utc(timestamp),
            "regime": None,
            "signal_time": timestamp,
            "label_win": int(r_multiple > 0) if r_multiple is not None else None,
            "label_r_multiple": r_multiple,
            "label_exit_reason": record.get("outcome"),
        }
        rows.append(row)
    return pd.DataFrame(rows, columns=[
        "data_source", *IOFS_FEATURE_COLUMNS, "signal_time", *REPLAY_LABEL_COLUMNS
    ])


def leakage_fields_in_features(feature_columns: list[str]) -> list[str]:
    return sorted(set(feature_columns) & LEAKAGE_FIELDS)


def write_dataset(
    frame: pd.DataFrame,
    output_path: Path,
    *,
    data_source: str,
    feature_columns: list[str],
    status: str,
    source_details: dict[str, Any],
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output_path, index=False)
    meta_path = output_path.with_name(f"{output_path.stem}_meta.json")
    source_counts = (
        {str(key): int(value) for key, value in frame["data_source"].value_counts().items()}
        if "data_source" in frame.columns else {}
    )
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "contract_version": ML_CONTRACT_VERSION,
        "schema_hash": ML_FEATURE_SCHEMA_HASH,
        "runtime_feature_columns": list(ML_FEATURE_COLUMNS),
        "runtime_feature_count": len(ML_FEATURE_COLUMNS),
        "feature_columns": feature_columns,
        "feature_count": len(feature_columns),
        "label_columns": REPLAY_LABEL_COLUMNS,
        "row_count": int(len(frame)),
        "data_source": data_source,
        "source_counts": source_counts,
        "contains_replay_rows": bool(source_counts.get("replay", 0)),
        "production_candidate_allowed": data_source in {"organic", "paper"} and len(frame) >= 300,
        "research_only": data_source == "replay",
        "leakage_fields_in_features": leakage_fields_in_features(feature_columns),
        "leakage_check_passed": not leakage_fields_in_features(feature_columns),
        **source_details,
    }
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return meta_path


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--organic-dataset", required=True)
    parser.add_argument("--replay-setups", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--replay-output")
    parser.add_argument("--db-path")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    organic_path = Path(args.organic_dataset)
    output_path = Path(args.output)
    replay_output = (
        Path(args.replay_output)
        if args.replay_output
        else output_path.with_name("training_v2_iofs_replay.parquet")
    )
    db_path = resolve_db_path(args.db_path)
    organic = pd.read_parquet(organic_path)
    iofs_events = load_iofs_events(db_path)
    organic_iofs = build_organic_iofs_dataset(organic, iofs_events)
    replay_records = read_jsonl(Path(args.replay_setups))
    replay = build_replay_dataset(replay_records)

    organic_meta = write_dataset(
        organic_iofs,
        output_path,
        data_source="paper",
        feature_columns=list(ML_FEATURE_COLUMNS) + IOFS_FEATURE_COLUMNS,
        status="READY" if len(organic_iofs) >= 300 else "IOFS_ORGANIC_DATA_INSUFFICIENT",
        source_details={
            "input_organic_rows": int(len(organic)),
            "available_iofs_event_rows": int(len(iofs_events)),
            "trace_linked_iofs_organic_rows": int(len(organic_iofs)),
            "input_organic_dataset": str(organic_path.resolve()),
        },
    )
    replay_meta = write_dataset(
        replay,
        replay_output,
        data_source="replay",
        feature_columns=IOFS_FEATURE_COLUMNS,
        status="RESEARCH_ONLY",
        source_details={
            "input_replay_rows": int(len(replay_records)),
            "input_replay_setups": str(Path(args.replay_setups).resolve()),
        },
    )
    print(f"IOFS_ORGANIC_STATUS={'READY' if len(organic_iofs) >= 300 else 'IOFS_ORGANIC_DATA_INSUFFICIENT'}")
    print(f"iofs_organic_rows={len(organic_iofs)}")
    print(f"iofs_replay_rows={len(replay)}")
    print(f"organic_output={output_path}")
    print(f"organic_meta={organic_meta}")
    print(f"replay_output={replay_output}")
    print(f"replay_meta={replay_meta}")
    return 0


def _hour_utc(value: Any) -> int | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc).hour
    except ValueError:
        return None


def _session_window(value: Any) -> str | None:
    hour = _hour_utc(value)
    if hour is None:
        return None
    if 7 <= hour < 10:
        return "07:00-10:00"
    if 13 <= hour < 16:
        return "13:00-16:00"
    return "OUTSIDE_SESSION"


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())
