"""Post-hoc DIAGNOSTIC_ONLY decomposition; never generates or promotes candidates."""
import argparse
import gzip
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "backends/bot-backend"), str(ROOT / "backends/shared"), str(ROOT / "scripts")]
from evaluate_cati_alpha import load_closed
from app.trading_intelligence.setups.alpha_v2 import closed_features
from app.trading_intelligence.portfolio.groups import static_group_for


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if np.isfinite(value) else None
    return value


def deciles(series):
    # Equal values stay together; never invent rank distinctions for ties.
    if series.nunique(dropna=True) < 2:
        return pd.Series("CONSTANT", index=series.index)
    return pd.qcut(series, 10, labels=False, duplicates="drop").astype("string").fillna("UNAVAILABLE")


def distribution(series):
    x = pd.to_numeric(series, errors="coerce").dropna()
    return {"n": len(x), "missing": len(series)-len(x), "mean": x.mean(), "std": x.std(),
            **{str(p): x.quantile(p) for p in (0, .01, .1, .25, .5, .75, .9, .99, 1)}}


def metrics(frame):
    return {"classification": "DIAGNOSTIC_ONLY", "n": len(frame), "days": frame.day.nunique(),
            "gross_R": frame.gross_R.mean(), "cost_R": frame.cost_R.mean(), "net_R": frame.net_R.mean(),
            "net_2x_cost_R": (frame.gross_R-2*frame.cost_R).mean(),
            "TARGET_rate": frame.terminal.eq("TARGET").mean(), "STOP_rate": frame.terminal.eq("STOP").mean(),
            "TIMEOUT_rate": frame.terminal.eq("TIMEOUT").mean(), "profitable_rate": frame.net_R.gt(0).mean()}


def dependence(frame):
    n = len(frame)
    clusters = frame.groupby("decision_time").size()
    blocks = frame.groupby(frame.decision_time // 43200000).size()
    group_clusters = frame.loc[frame.instrument_group.ne("UNKNOWN")].groupby(["decision_time", "instrument_group"]).size()
    day_sums = frame.groupby("day").net_R.agg(["sum", "count"])
    centered = day_sums["sum"]-frame.net_R.mean()*day_sums["count"]
    centered.index = pd.to_datetime(centered.index)
    centered = centered.reindex(pd.date_range(centered.index.min(), centered.index.max(), freq="D"), fill_value=0)
    scores = centered.to_numpy()
    lag = min(7, len(scores)-1)
    meat = np.dot(scores, scores)
    for k in range(1, lag+1):
        meat += 2*(1-k/(lag+1))*np.dot(scores[k:], scores[:-k])
    variance_mean = max(0, meat)/n**2
    variance_ess = min(n, frame.net_R.var()/variance_mean) if variance_mean > 0 else None
    crowding = frame.groupby("decision_time").agg(signals=("net_R", "size"),
            gross_R=("gross_R", "mean"), net_R=("net_R", "mean"), sign_mean=("sign", "mean"))
    crowding["decile"] = deciles(crowding.signals)
    adjacent = frame.sort_values("decision_time").groupby("symbol").decision_time.diff().dropna()
    ordered = np.sort(frame.decision_time.to_numpy())
    live_horizons = np.searchsorted(ordered, ordered, side="left")-np.searchsorted(ordered, ordered-43200000, side="right")
    monthly = frame.groupby("month").agg(signals=("gross_R", "size"), gross_R=("gross_R", "mean"))
    return {"raw_samples": n,
            "same_instrument_adjacent_horizon_overlap_share": adjacent.lt(43200000).mean(),
            "prior_labels_with_12h_horizon_still_open": distribution(pd.Series(live_horizons)),
            "concurrent_count_vs_gross_spearman": crowding.signals.corr(crowding.gross_R, method="spearman"),
            "monthly_count_vs_gross_spearman": monthly.signals.corr(monthly.gross_R, method="spearman"), "unique_timestamps": len(clusters), "same_timestamp_cluster_size": distribution(clusters),
            "share_in_multi_signal_timestamps": frame.decision_time.map(clusters).gt(1).mean(),
            "same_group_cluster_size": distribution(group_clusters), "unknown_group_share": frame.instrument_group.eq("UNKNOWN").mean(),
            "overlapping_12h_horizon": "Consecutive observations across instruments share many future candles; raw n is not independent.",
            "nonoverlapping_12h_calendar_blocks": len(blocks), "kish_weighted_12h_block_units": n*n/float((blocks**2).sum()),
            "effective_information_count_7day_HAC_variance_proxy": variance_ess,
            "HAC_mean_standard_error_R": np.sqrt(variance_mean), "HAC_lag_days": lag,
            "not_an_admission_sample_count": True,
            "crowding_timestamp_summary": [dict(decile=str(k), **metrics(frame[frame.decision_time.isin(g.index)]))
                                            for k, g in crowding.groupby("decile")],
            "btc_direction_alignment": frame.btc_direction_alignment.mean(),
            "eth_direction_alignment": frame.eth_direction_alignment.mean()}


def run(output):
    output.mkdir(parents=True, exist_ok=True)
    source = ROOT / "docs/research/artifacts/cati_alpha_v2_fixed"
    original = json.loads((source / "report.json").read_text())
    registry = json.loads((source / "registry.json").read_text())
    raw = gzip.decompress((source / "labels.jsonl.gz").read_bytes())
    if hashlib.sha256(raw).hexdigest() != original["labels_sha256"]:
        raise ValueError("immutable label hash mismatch")
    records = []
    for line in raw.splitlines():
        row = json.loads(line)
        candidate = row["candidate"]
        trigger = candidate["trigger_reference"]
        risk = abs(trigger-candidate["structural_invalidation"])
        target = abs(candidate["target_reference"]-trigger)
        sign = 1 if row["side"] == "LONG" else -1
        row.update(sign=sign, structural_risk_price=risk, structural_risk_pct=100*risk/trigger,
            stop_distance_pct=100*abs(row["entry_next_open"]-candidate["structural_invalidation"])/row["entry_next_open"],
            target_distance_pct=100*abs(candidate["target_reference"]-row["entry_next_open"])/row["entry_next_open"],
            target_room_R=target/risk, modeled_round_trip_cost_pct=100*row["cost_R"]*risk/row["entry_next_open"],
            entry_excursion_R=sign*(row["entry_next_open"]-trigger)/risk,
            exit_excursion_R=sign*(row["exit_price"]-(candidate["structural_invalidation"] if row["terminal"] == "STOP" else candidate["target_reference"]))/risk if row["terminal"] != "TIMEOUT" else None,
            exit_shortfall_to_target_R=sign*(candidate["target_reference"]-row["exit_price"])/risk,
            holding_minutes_lower=row["event_bar"]*15, holding_minutes_upper=(row["event_bar"]+1)*15,
            turnover_price_fraction=(row["entry_next_open"]+abs(row["exit_price"]))/trigger,
            instrument_group=static_group_for(row["symbol"]))
        records.append({k: v for k, v in row.items() if k not in ("candidate", "causal_features")})
    frame = pd.DataFrame(records)
    if len(frame) != 40492 or frame.label_id.nunique() != len(frame):
        raise ValueError("label count or uniqueness defect")
    dates = pd.to_datetime(frame.decision_time, unit="ms", utc=True)
    frame["day"], frame["year"], frame["month"] = dates.dt.strftime("%Y-%m-%d"), dates.dt.year, dates.dt.strftime("%Y-%m")
    frame["weekday"] = dates.dt.day_name()
    frame["UTC_session"] = pd.cut(dates.dt.hour, [-1, 7, 15, 23], labels=["00-08", "08-16", "16-24"]).astype(str)
    step = 900000
    start = registry["development_start_ms"]-1280*step
    stop = registry["development_stop_ms"]-1
    times = pd.Index(sorted(frame.decision_time.unique()))
    matrix, one_bar_matrix, bench, fingerprints = {}, {}, {}, {}
    conn = sqlite3.connect((ROOT / "backends/shared/shared_lib/persistence/cosmicforge.db").as_uri()+"?mode=ro", uri=True)
    feature_rows = []
    for symbol in registry["symbols"]:
        rows, digest = load_closed(conn, symbol, "15m", start, stop)
        if digest != original["source_queries"][symbol]["sha256"]:
            raise ValueError("changed development source: " + symbol)
        f = closed_features(rows, "15m", stop).set_index("closed_at")
        f["return_1"] = f.close.pct_change(fill_method=None).where(f.open_time.diff().eq(step))
        f["realized_volatility_24h"] = f.return_1.rolling(96).std()*np.sqrt(96)
        f["atr_fraction"] = f.atr/f.close
        f["volatility_ratio"] = f.atr/f.atr_baseline
        f["trend_slope"] = f.slope
        matrix[symbol] = f.return_96.where(f.continuous_97).reindex(times)
        one_bar_matrix[symbol] = f.return_1.reindex(times)
        if symbol in ("BTCUSDT", "ETHUSDT"):
            bench[symbol] = f[["return_96", "trend_slope"]].reindex(times)
        selected = f.reindex(frame.loc[frame.symbol.eq(symbol), "decision_time"])
        for name in ("realized_volatility_24h", "atr_fraction", "volatility_ratio", "trend_slope", "volume_ratio"):
            frame.loc[frame.symbol.eq(symbol), name] = selected[name].to_numpy()
        fingerprints[symbol] = digest
        print("causal diagnostic join", symbol, flush=True)
    conn.close()
    return_matrix = pd.DataFrame(matrix)
    dispersion = return_matrix.std(axis=1)
    frame["cross_sectional_dispersion"] = frame.decision_time.map(dispersion)
    for name, benchmark in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")):
        returns = frame.decision_time.map(bench[benchmark].return_96)
        frame[name+"_market_regime"] = np.select([returns.isna(), returns.gt(.02), returns.lt(-.02)], ["UNAVAILABLE", "UP_24H", "DOWN_24H"], default="RANGE_24H")
        frame[name.lower()+"_direction_alignment"] = (frame.sign*returns).gt(0).where(returns.notna())
    frame["volatility_regime"] = pd.cut(frame.volatility_ratio, [-np.inf, .75, 1.25, np.inf], labels=["CONTRACTED", "NORMAL", "EXPANDED"]).astype(str)
    frame["trend_regime"] = pd.cut(frame.trend_slope, [-np.inf, -.25, .25, np.inf], labels=["DOWN", "FLAT", "UP"]).astype(str)
    frame["volume_regime"] = pd.cut(frame.volume_ratio, [-np.inf, 1, 2, np.inf], labels=["BELOW_PRIOR_MEAN", "NORMAL_TO_2X", "ABOVE_2X"]).astype(str)
    frame["executed_target_stop_ratio"] = frame.target_distance_pct/frame.stop_distance_pct
    frame["structural_risk_ATR"] = frame.structural_risk_pct/(100*frame.atr_fraction)
    dimensions = ["side", "symbol", "instrument_group", "year", "month", "UTC_session", "weekday", "volatility_regime", "trend_regime", "BTC_market_regime", "ETH_market_regime", "volume_regime"]
    numbers = ["cost_R", "gross_R", "net_R", "structural_risk_pct", "stop_distance_pct", "target_distance_pct", "target_room_R", "holding_minutes_lower", "holding_minutes_upper", "mfe_bar_bound_R", "mae_bar_bound_R", "entry_excursion_R", "exit_excursion_R", "exit_shortfall_to_target_R", "modeled_round_trip_cost_pct", "turnover_price_fraction", "realized_volatility_24h", "cross_sectional_dispersion", "volume_ratio", "atr_fraction", "executed_target_stop_ratio", "structural_risk_ATR"]
    report = {"classification": "DIAGNOSTIC_ONLY", "promotion_allowed": False, "new_research_evaluated": False,
        "holdout_opened": False, "holdout_queries": 0, "source_labels_sha256": original["labels_sha256"],
        "causal_source_hashes": fingerprints, "causal_feature_close_upper_bound": stop, "populations": {},
        "quality": {"rows": len(frame), "unique_label_ids": frame.label_id.nunique(), "nulls": frame[numbers].isna().sum().to_dict()},
        "measurement_limits": ["MFE/MAE are terminal-bar censored bounds, not exact path ordering.", "Holding time is a within-bar interval; modeled funding reserves the full 12h horizon.", "Turnover is hypothetical candidate turnover, not executable portfolio turnover.", "Regimes/deciles and all subgroup outcomes are post-hoc diagnostic only.", "HAC variance-equivalent ESS and Kish block units are approximations, not certified independent evidence counts.", "15m noise cannot be causally identified without a separately frozen timeframe experiment."]}
    actual_corr = pd.DataFrame(one_bar_matrix).corr(min_periods=300)
    actual_upper = actual_corr.to_numpy()[np.triu_indices(len(actual_corr), 1)]
    report["cross_asset_closed_15m_return_correlation"] = distribution(pd.Series(actual_upper))
    report["cross_asset_correlation_scope"] = "Closed 15m returns at diagnostic decision timestamps; pairwise >=300 observations, missing histories excluded."
    corr = return_matrix.diff().corr(min_periods=300)
    # Correlation of differences in rolling 24h returns is a common-shock proxy;
    # do not present it as non-overlapping 15m realized return correlation.
    upper = corr.to_numpy()[np.triu_indices(len(corr), 1)]
    report["cross_asset_common_shock_correlation_proxy"] = distribution(pd.Series(upper))
    for family, sub in [("COMBINED_DIAGNOSTIC", frame)] + list(frame.groupby("family")):
        sub = sub.copy()
        for name in ("cost_R", "target_room_R", "cross_sectional_dispersion", "realized_volatility_24h", "structural_risk_pct"):
            sub[name+"_decile"] = deciles(sub[name])
        frequency = sub.groupby("decision_time").size()
        sub["concurrent_signal_count"] = sub.decision_time.map(frequency)
        sub["signal_frequency_decile"] = deciles(sub.concurrent_signal_count)
        facets = dimensions + [name+"_decile" for name in ("cost_R", "target_room_R", "cross_sectional_dispersion", "realized_volatility_24h", "structural_risk_pct")] + ["signal_frequency_decile"]
        report["populations"][family] = {"summary": metrics(sub), "distributions": {name: distribution(sub[name]) for name in numbers},
            "dependence": dependence(sub),
            "geometry": {"MFE_bound_below_target_share": sub.mfe_bar_bound_R.lt(sub.target_room_R).mean(),
                         "timeout_gross_R": sub.loc[sub.terminal.eq("TIMEOUT"), "gross_R"].mean()}, "subgroups": {name: [{"group": str(key), **metrics(group)} for key, group in sub.groupby(name, dropna=False)] for name in facets}}
    c = frame[frame.family.eq("MULTI_TIMEFRAME_TREND_PULLBACK")]
    report["C_diagnostic"] = {"median_round_trip_cost_pct": c.modeled_round_trip_cost_pct.median(),
        "median_structural_risk_pct": c.structural_risk_pct.median(), "median_cost_R": c.cost_R.median(),
        "gross_expectancy_R": c.gross_R.mean(), "break_even_mean_cost_R": c.gross_R.mean(),
        "break_even_cost_fraction_of_current_mean_cost": c.gross_R.mean()/c.cost_R.mean(),
        "required_gross_improvement_R_normal_costs": c.cost_R.mean()-c.gross_R.mean(),
        "required_gross_improvement_R_2x_costs": 2*c.cost_R.mean()-c.gross_R.mean(),
        "cost_policy_unchanged": True, "interpretation": "Break-even is arithmetic diagnosis, not authorization to reduce costs or retune C."}
    (output / "diagnostics.json").write_text(json.dumps(clean(report), indent=2, allow_nan=False), encoding="utf-8")
    frame.to_csv(output / "diagnostic_rows.csv.gz", index=False, compression={"method": "gzip", "mtime": 0})
    return clean(report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.output)
    print(json.dumps(result["C_diagnostic"]))
