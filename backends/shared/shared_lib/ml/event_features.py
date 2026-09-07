from __future__ import annotations

import json
from typing import Any

import pandas as pd

from shared_lib.ml.contract import EVENT_FEATURE_COLUMNS


def _coerce_utc(series: pd.Series) -> pd.Series:
    # Standardize to datetime64[us, UTC] — pandas 3.x raises MergeError on
    # mixed precision (e.g. datetime64[s] vs datetime64[us]) in merge_asof.
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    if parsed.dt.tz is not None:
        try:
            return parsed.astype("datetime64[us, UTC]")
        except (TypeError, ValueError):
            pass
    return parsed


def _normalize_side(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.upper()


def _compute_adverse_move(
    side: str,
    max_move_pct: float | None,
    min_move_pct: float | None,
) -> float | None:
    if side in {"BUY", "LONG"}:
        if min_move_pct is None:
            return None
        return abs(min(0.0, float(min_move_pct)))
    if side in {"SELL", "SHORT"}:
        if max_move_pct is None:
            return None
        return max(0.0, float(max_move_pct))
    candidates = [abs(float(v)) for v in (max_move_pct, min_move_pct) if v is not None]
    return max(candidates) if candidates else None


def _expand_events_for_symbols(
    events_df: pd.DataFrame,
    symbols: list[str],
    resolver: Any,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    symbol_set = {sym.upper() for sym in symbols if sym}
    for ev in events_df.to_dict("records"):
        affected = resolver(
            str(ev.get("event_type") or ""),
            str(ev.get("country_currency") or ""),
            list(symbol_set),
        )
        if affected is None:
            targets = sorted(symbol_set)
        else:
            targets = [sym.upper() for sym in affected if sym and sym.upper() in symbol_set]
        for symbol in targets:
            rows.append(
                {
                    "symbol": symbol,
                    "event_id": ev.get("event_id"),
                    "scheduled_utc": ev.get("scheduled_utc"),
                }
            )
    return pd.DataFrame(rows)


def _expand_windows_for_symbols(
    windows_df: pd.DataFrame,
    symbols: list[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    symbol_set = {sym.upper() for sym in symbols if sym}
    for win in windows_df.to_dict("records"):
        if bool(win.get("is_global")):
            targets = sorted(symbol_set)
        else:
            affected_raw = win.get("affected_symbols")
            try:
                affected = json.loads(affected_raw) if affected_raw else []
            except (TypeError, ValueError):
                affected = []
            targets = [sym.upper() for sym in affected if sym and sym.upper() in symbol_set]
        for symbol in targets:
            rows.append(
                {
                    "symbol": symbol,
                    "start_utc": win.get("start_utc"),
                    "end_utc": win.get("end_utc"),
                }
            )
    return pd.DataFrame(rows)


def build_event_feature_frame(
    raw: pd.DataFrame,
    *,
    economic_events: pd.DataFrame,
    blackout_windows: pd.DataFrame,
    reactions: pd.DataFrame,
    symbol_resolver: Any,
) -> pd.DataFrame:
    """
    Build leak-safe event features for dataset generation only.

    Leakage rules:
      - Event timing features use scheduled_utc only.
      - Reaction features use only reactions whose post_window_end_utc <= trace_ts.
      - No actual/revised macro values are used.
    """
    out = pd.DataFrame(index=raw.index)
    if raw.empty:
        for col in EVENT_FEATURE_COLUMNS:
            out[col] = pd.Series(dtype="float64" if col != "reaction_type" else "object")
        return out[list(EVENT_FEATURE_COLUMNS)]

    base = pd.DataFrame(index=raw.index)
    base["symbol"] = raw["symbol"].fillna("").astype(str).str.upper()
    base["feature_ts"] = _coerce_utc(raw["trace_ts"])
    base["side"] = _normalize_side(raw.get("side", pd.Series(index=raw.index, dtype="object")))
    if "signal" in raw.columns:
        missing_side = base["side"].eq("")
        base.loc[missing_side, "side"] = _normalize_side(raw.loc[missing_side, "signal"])

    symbols = sorted(base["symbol"].dropna().unique().tolist())

    events_expanded = _expand_events_for_symbols(economic_events, symbols, symbol_resolver)
    if not events_expanded.empty:
        events_expanded["scheduled_utc"] = _coerce_utc(events_expanded["scheduled_utc"])
        left_raw = base.reset_index().rename(columns={"index": "_row_id"})
        # merge_asof requires the on-key to be globally sorted (pandas 3.x).
        # Null rows are dropped because NaT values cannot be sorted.
        left = left_raw[left_raw["feature_ts"].notna()].sort_values(
            ["feature_ts", "symbol"], kind="mergesort"
        )
        right = events_expanded[events_expanded["scheduled_utc"].notna()].sort_values(
            ["scheduled_utc", "symbol"], kind="mergesort"
        )

        past = pd.merge_asof(
            left,
            right,
            by="symbol",
            left_on="feature_ts",
            right_on="scheduled_utc",
            direction="backward",
        )
        future = pd.merge_asof(
            left,
            right,
            by="symbol",
            left_on="feature_ts",
            right_on="scheduled_utc",
            direction="forward",
        )
        past_minutes = pd.Series(
            ((past["feature_ts"] - past["scheduled_utc"]).dt.total_seconds() / 60.0).to_numpy(),
            index=past["_row_id"].to_numpy(),
            name="minutes_since_last_event",
        ).reindex(raw.index)
        future_minutes = pd.Series(
            ((future["scheduled_utc"] - future["feature_ts"]).dt.total_seconds() / 60.0).to_numpy(),
            index=future["_row_id"].to_numpy(),
            name="minutes_to_next_event",
        ).reindex(raw.index)
        out = out.join(past_minutes)
        out = out.join(future_minutes)
    else:
        out["minutes_since_last_event"] = pd.Series([pd.NA] * len(base), index=raw.index, dtype="Float64")
        out["minutes_to_next_event"] = pd.Series([pd.NA] * len(base), index=raw.index, dtype="Float64")

    windows_expanded = _expand_windows_for_symbols(blackout_windows, symbols)
    base["_is_blackout_active"] = 0
    if not windows_expanded.empty:
        windows_expanded["start_utc"] = _coerce_utc(windows_expanded["start_utc"])
        windows_expanded["end_utc"] = _coerce_utc(windows_expanded["end_utc"])
        for symbol in symbols:
            row_idx = base.index[base["symbol"] == symbol]
            if len(row_idx) == 0:
                continue
            ts_values = base.loc[row_idx, "feature_ts"]
            blocked = pd.Series(False, index=row_idx)
            symbol_windows = windows_expanded[windows_expanded["symbol"] == symbol]
            for _, win in symbol_windows.iterrows():
                blocked |= (ts_values >= win["start_utc"]) & (ts_values <= win["end_utc"])
            base.loc[row_idx, "_is_blackout_active"] = blocked.astype("int8")
    out["is_blackout_active"] = base["_is_blackout_active"].astype("int8")

    reaction_df = reactions.copy()
    if not reaction_df.empty:
        reaction_df["symbol"] = reaction_df["symbol"].fillna("").astype(str).str.upper()
        reaction_df["post_window_end_utc"] = _coerce_utc(
            reaction_df.get("post_window_end_utc", pd.Series(dtype="object"))
        )
        reaction_df["reaction_available_at"] = reaction_df["post_window_end_utc"].fillna(
            _coerce_utc(reaction_df.get("event_time_utc", pd.Series(dtype="object")))
        )
        reaction_df = reaction_df[reaction_df["reaction_available_at"].notna()]
        # pandas merge_asof requires the merge key to be globally sorted even
        # when using `by=...`; sorting by symbol first can fail on mixed-symbol
        # reaction histories.
        reaction_df = reaction_df.sort_values(
            ["reaction_available_at", "symbol"], kind="mergesort"
        )

        left_raw = base.reset_index().rename(columns={"index": "_row_id"})
        # merge_asof requires global sort on left_on key (pandas 3.x).
        left = left_raw[left_raw["feature_ts"].notna()].sort_values(
            ["feature_ts", "symbol"], kind="mergesort"
        )
        merged = pd.merge_asof(
            left,
            reaction_df,
            by="symbol",
            left_on="feature_ts",
            right_on="reaction_available_at",
            direction="backward",
        )
        merged.index = merged["_row_id"].to_numpy()

        out["volatility_post_event"] = pd.to_numeric(
            merged["volatility_expansion_ratio"], errors="coerce"
        ).reindex(raw.index)
        out["volume_spike_post_event"] = pd.to_numeric(
            merged["volume_spike_ratio"], errors="coerce"
        ).reindex(raw.index)
        out["reaction_type"] = (
            merged["reaction_type"]
            .fillna("NO_PRIOR_EVENT")
            .astype(str)
            .reindex(raw.index)
        )
        out["max_adverse_move"] = [
            _compute_adverse_move(
                str(base.loc[idx, "side"]),
                merged.loc[idx, "max_move_pct"] if idx in merged.index else None,
                merged.loc[idx, "min_move_pct"] if idx in merged.index else None,
            )
            for idx in raw.index
        ]
    else:
        out["volatility_post_event"] = pd.Series([pd.NA] * len(base), index=raw.index, dtype="Float64")
        out["volume_spike_post_event"] = pd.Series([pd.NA] * len(base), index=raw.index, dtype="Float64")
        out["reaction_type"] = pd.Series(["NO_PRIOR_EVENT"] * len(base), index=raw.index, dtype="object")
        out["max_adverse_move"] = pd.Series([pd.NA] * len(base), index=raw.index, dtype="Float64")

    return out[list(EVENT_FEATURE_COLUMNS)]
