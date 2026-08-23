from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any


ML_CONTRACT_VERSION = "entry_quality_v2"

ML_FEATURE_COLUMNS: tuple[str, ...] = (
    "regime_enc",
    "adx_normalized",
    "atr_pct",
    "is_weekend",
    "session_asia",
    "session_london",
    "session_ny",
    "compression_ratio",
    "breakout_pressure",
    "sl_distance_pct",
    "tp_distance_pct",
    "planned_rr",
    "sl_atr_ratio",
    "confidence_normed",
    "breakout_vs_atr",
    "threshold_gap",
    "consensus_gap",
    "active_count_normed",
    "side_enc",
    "htf_opposed",
    "open_positions_count",
)

EVENT_TIMING_FEATURE_COLUMNS: tuple[str, ...] = (
    "minutes_to_next_event",
    "minutes_since_last_event",
    "is_blackout_active",
)

MARKET_REACTION_FEATURE_COLUMNS: tuple[str, ...] = (
    "volatility_post_event",
    "volume_spike_post_event",
    "reaction_type",
    "max_adverse_move",
)

EVENT_FEATURE_COLUMNS: tuple[str, ...] = (
    EVENT_TIMING_FEATURE_COLUMNS + MARKET_REACTION_FEATURE_COLUMNS
)

LABEL_COLUMNS: tuple[str, ...] = (
    "label",
    "label_win",
    "label_r_multiple",
    "label_exit_reason",
    "label_mfe_pct",
    "label_mae_pct",
    "label_slippage_pct",
    "label_exit_regime",
    "label_duration_seconds",
    "label_realized_pnl",
)

METADATA_COLUMNS: tuple[str, ...] = (
    "trace_id",
    "position_id",
    "open_timestamp",
    "close_timestamp",
    "account_id",
    "bot_instance_id",
    "dataset_build_timestamp",
)

CRITICAL_SOURCE_FIELDS: tuple[str, ...] = (
    "timeframe",
    "regime_state",
    "regime_confidence",
    "confidence",
    "chosen_strategy",
    "adx",
    "atr_pct",
    "ma_slope",
    "compression_ratio",
    "breakout_pressure",
    "buy_score",
    "sell_score",
    "threshold",
    "sl_plan",
    "tp_plan",
    "stop_loss_price",
)

_REGIME_MAP = {
    "STRONG_TREND": 4,
    "WEAK_TREND": 3,
    "HIGH_VOLATILITY": 2,
    "RANGE": 1,
    "LOW_VOLATILITY_CHOP": 0,
    "TRENDING": 4,
    "STANDARD": 3,
    "RANGING": 1,
    "CHOPPY": 0,
}


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


ML_FEATURE_SCHEMA_HASH = _stable_hash(
    {
        "contract_version": ML_CONTRACT_VERSION,
        "feature_columns": list(ML_FEATURE_COLUMNS),
        "label_columns": list(LABEL_COLUMNS),
    }
)


def build_contract_metadata(*, training_dataset_path: str | None = None) -> dict[str, Any]:
    return {
        "contract_version": ML_CONTRACT_VERSION,
        "schema_hash": ML_FEATURE_SCHEMA_HASH,
        "feature_columns": list(ML_FEATURE_COLUMNS),
        "event_feature_columns": list(EVENT_FEATURE_COLUMNS),
        "event_timing_feature_columns": list(EVENT_TIMING_FEATURE_COLUMNS),
        "market_reaction_feature_columns": list(MARKET_REACTION_FEATURE_COLUMNS),
        "dataset_feature_columns": list(ML_FEATURE_COLUMNS + EVENT_FEATURE_COLUMNS),
        "label_columns": list(LABEL_COLUMNS),
        "training_dataset_path": training_dataset_path,
    }


def get_contract_status(
    *,
    feature_columns: list[str] | tuple[str, ...] | None,
    schema_hash: str | None,
) -> dict[str, Any]:
    columns = list(feature_columns or [])
    expected_columns = list(ML_FEATURE_COLUMNS)
    columns_match = columns == expected_columns
    schema_match = bool(schema_hash) and schema_hash == ML_FEATURE_SCHEMA_HASH
    return {
        "contract_version": ML_CONTRACT_VERSION,
        "schema_hash": ML_FEATURE_SCHEMA_HASH,
        "feature_columns": expected_columns,
        "compatible": bool(columns_match and schema_match),
        "schema_hash_matches": schema_match,
        "feature_columns_match": columns_match,
    }


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _to_int(value: Any) -> int | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _to_bool_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return 1
    if text in {"0", "false", "no"}:
        return 0
    return None


def _parse_timestamp(row: Mapping[str, Any]) -> datetime | None:
    raw = row.get("trace_ts") or row.get("ts")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def compute_feature_value(row: Mapping[str, Any], feature_name: str) -> Any:
    if feature_name == "regime_enc":
        regime_raw = str(row.get("regime_state") or "UNKNOWN").upper()
        return _REGIME_MAP.get(regime_raw, -1)

    if feature_name == "adx_normalized":
        adx = _to_float(row.get("adx"))
        if adx is None:
            return None
        return max(0.0, min(adx / 50.0, 1.0))

    if feature_name == "atr_pct":
        return _to_float(row.get("atr_pct"))

    ts = _parse_timestamp(row)
    if feature_name == "is_weekend":
        return 1 if ts and ts.weekday() >= 5 else 0 if ts else None
    if feature_name == "session_asia":
        return 1 if ts and 0 <= ts.hour < 8 else 0 if ts else None
    if feature_name == "session_london":
        return 1 if ts and 8 <= ts.hour < 16 else 0 if ts else None
    if feature_name == "session_ny":
        return 1 if ts and 16 <= ts.hour < 24 else 0 if ts else None

    if feature_name == "compression_ratio":
        return _to_float(row.get("compression_ratio"))
    if feature_name == "breakout_pressure":
        return _to_float(row.get("breakout_pressure"))

    entry_price = _to_float(row.get("open_price") or row.get("last_price") or row.get("mark_price"))
    stop_loss_price = _to_float(row.get("stop_loss_price") or row.get("sl_plan"))
    tp_plan = _to_float(row.get("tp_plan"))
    atr_pct = _to_float(row.get("atr_pct"))
    confidence = _to_float(row.get("confidence") or row.get("final_confidence"))
    threshold = _to_float(row.get("threshold"))
    buy_score = _to_float(row.get("buy_score"))
    sell_score = _to_float(row.get("sell_score"))
    active_count = _to_float(row.get("active_strategy_count"))

    if feature_name == "sl_distance_pct":
        if entry_price and stop_loss_price is not None:
            return abs(entry_price - stop_loss_price) / entry_price
        return None
    if feature_name == "tp_distance_pct":
        if entry_price and tp_plan is not None:
            return abs(tp_plan - entry_price) / entry_price
        return None
    if feature_name == "planned_rr":
        sl_distance = compute_feature_value(row, "sl_distance_pct")
        tp_distance = compute_feature_value(row, "tp_distance_pct")
        if sl_distance is None or tp_distance is None or sl_distance == 0:
            return None
        return tp_distance / sl_distance
    if feature_name == "sl_atr_ratio":
        sl_distance = compute_feature_value(row, "sl_distance_pct")
        if sl_distance is None or atr_pct in (None, 0.0):
            return None
        return sl_distance / atr_pct
    if feature_name == "confidence_normed":
        if confidence is None or threshold in (None, 0.0):
            return None
        return confidence / threshold
    if feature_name == "breakout_vs_atr":
        breakout_pressure = _to_float(row.get("breakout_pressure"))
        if breakout_pressure is None or atr_pct in (None, 0.0):
            return None
        return abs(breakout_pressure) / atr_pct
    if feature_name == "threshold_gap":
        if confidence is None or threshold is None:
            return None
        return confidence - threshold
    if feature_name == "consensus_gap":
        if buy_score is None or sell_score is None:
            return None
        return buy_score - sell_score
    if feature_name == "active_count_normed":
        if active_count is None:
            return None
        return max(0.0, min(active_count / 4.0, 1.0))
    if feature_name == "side_enc":
        side = str(row.get("side") or row.get("signal") or "").upper()
        return 1 if side in {"BUY", "LONG"} else 0 if side in {"SELL", "SHORT"} else None
    if feature_name == "htf_opposed":
        return _to_bool_int(row.get("htf_opposed"))
    if feature_name == "open_positions_count":
        return _to_int(row.get("open_positions_count"))

    raise KeyError(f"Unknown ML feature: {feature_name}")
