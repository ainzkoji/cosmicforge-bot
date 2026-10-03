"""Inventory stored development data without querying reserved holdout prices."""
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOUNDARY = 1783876499999
STEPS = {"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000}


def inventory():
    result = {"holdout_opened": False, "holdout_queries": 0, "price_close_exclusive_upper_bound": BOUNDARY,
              "databases": [], "manifests": []}
    files = sorted((ROOT / "data").rglob("*.db"))
    files += [ROOT / "backends/shared/shared_lib/persistence/cosmicforge.db"]
    for path in files:
        record = {"path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size, "tables": {}}
        conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in ("historical_candles", "market_candles", "market_feature_observations", "fx_reference_quotes"):
            if table not in tables:
                continue
            cols = [row[1] for row in conn.execute("PRAGMA table_info(" + table + ")")]
            profile = {"columns": cols, "coverage": []}
            if table in ("historical_candles", "market_candles"):
                time = "interval" if table == "historical_candles" else "timeframe"
                symbol = "symbol" if table == "historical_candles" else "venue_symbol"
                source = "data_source" if table == "historical_candles" else "source"
                for timeframe, step in STEPS.items():
                    sql = f"SELECT {symbol} symbol,{source} source,COUNT(*) rows,COUNT(DISTINCT open_time) distinct_opens,MIN(open_time) first_open,MAX(open_time)+?-1 last_close,SUM(CASE WHEN open<=0 OR high<=0 OR low<=0 OR close<=0 OR volume<0 OR high<MAX(open,close,low) OR low>MIN(open,close,high) THEN 1 ELSE 0 END) invalid_ohlcv FROM {table} WHERE {time}=? AND open_time+?-1<? GROUP BY {symbol},{source}"
                    profile["coverage"] += [dict(row) | {"timeframe": timeframe,
                        "availability": "HISTORICAL_AVAILABLE", "scope": "development prefix only",
                        "causal_usability": "closed-bar usable subject to gaps, source identity and final quality checks"}
                        for row in conn.execute(sql, (step, timeframe, step, BOUNDARY))]
            elif table == "market_feature_observations":
                profile["coverage"] = [dict(row) for row in conn.execute(
                    "SELECT feature,source,status,COUNT(*) rows,COUNT(DISTINCT venue_symbol) symbols,"
                    "MIN(observed_at) first_observation,MAX(observed_at) last_observation "
                    "FROM market_feature_observations WHERE observed_at<? GROUP BY feature,source,status", (BOUNDARY,))]
            else:
                profile["coverage"] = [dict(row) for row in conn.execute(
                    "SELECT provider,pair,timeframe,COUNT(*) rows,MIN(open_time) first_open,MAX(open_time) last_open "
                    "FROM fx_reference_quotes WHERE open_time<? GROUP BY provider,pair,timeframe", (BOUNDARY,))]
            record["tables"][table] = profile
        result["databases"].append(record)
        conn.close()
        print(record["path"], "inventoried", flush=True)
    for path in sorted((ROOT / "data/research").rglob("*manifest*.json")):
        raw = path.read_bytes()
        try:
            data = json.loads(raw.decode("utf-8-sig"))
        except (ValueError, UnicodeError):
            continue
        result["manifests"].append({"path": str(path.relative_to(ROOT)), "sha256": hashlib.sha256(raw).hexdigest(),
            "declarative_metadata_only": True, "dataset_id": data.get("dataset_id"),
            "symbols": data.get("symbols"), "timeframes": data.get("derived_timeframes", data.get("timeframes")),
            "base_timeframe": data.get("base_timeframe"), "start_ms": data.get("start_ms"), "end_ms": data.get("end_ms"),
            "quality": data.get("quality"), "rows": data.get("rows"), "checksums": data.get("checksums"),
            "usable_prefix_must_end_before": BOUNDARY})
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = inventory()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2), encoding="utf-8")
