import sqlite3, json, datetime
from pathlib import Path

db_path = Path(r"backends/shared/shared_lib/persistence/cosmicforge.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

def table_exists(name: str) -> bool:
    return cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None

tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]

out = {"db": str(db_path), "tables": tables}

provider_table = None
for cand in ("news_provider_health", "news_source_health", "news_provider_status"):
    if table_exists(cand):
        provider_table = cand
        break
out["provider_table"] = provider_table

window = None
if provider_table:
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({provider_table})").fetchall()]
    time_cols = [c for c in ("created_at", "ts_utc", "timestamp_utc", "updated_at") if c in cols]
    out["provider_cols"] = cols
    out["provider_time_cols"] = time_cols
    if time_cols:
        tcol = time_cols[0]
        latest = cur.execute(f"SELECT MAX({tcol}) FROM {provider_table}").fetchone()[0]
        out["provider_latest_ts"] = latest
        if latest is not None:
            try:
                if isinstance(latest, (int, float)):
                    end_dt = datetime.datetime.fromtimestamp(latest, tz=datetime.timezone.utc)
                else:
                    end_dt = datetime.datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
                start_dt = end_dt - datetime.timedelta(minutes=10)
                window = (start_dt.isoformat(), end_dt.isoformat())
            except Exception as e:
                out["provider_latest_ts_parse_error"] = str(e)

def count_in_window(table: str, start_iso: str, end_iso: str):
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    for tcol in ("created_at", "ts_utc", "timestamp_utc", "updated_at"):
        if tcol in cols:
            c = cur.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {tcol} >= ? AND {tcol} <= ?",
                (start_iso, end_iso),
            ).fetchone()[0]
            return c, tcol
    return None, None

main_tables = {
    "raw_news_items": None,
    "news_clusters": None,
    "news_intelligence_signals": None,
    "news_cluster_narratives": None,
}
for logical, cands in {
    "raw_news_items": ("raw_news_items",),
    "news_clusters": ("news_clusters",),
    "news_intelligence_signals": ("news_intelligence_signals",),
    "news_cluster_narratives": ("news_cluster_narratives", "cluster_narratives"),
}.items():
    for cand in cands:
        if table_exists(cand):
            main_tables[logical] = cand
            break
out["main_tables"] = main_tables

if window:
    start_iso, end_iso = window
    out["window_utc"] = {"start": start_iso, "end": end_iso}
    for logical, table in main_tables.items():
        if table:
            c, tcol = count_in_window(table, start_iso, end_iso)
            out[f"{logical}_count_window"] = c
            out[f"{logical}_time_col"] = tcol

if provider_table:
    cols = out.get("provider_cols", [])
    src_col = "news_source_id" if "news_source_id" in cols else ("source_id" if "source_id" in cols else None)
    tcol = out.get("provider_time_cols", [None])[0]
    if src_col and tcol:
        allow = ["coindesk.com", "cointelegraph.com", "decrypt.co", "bitcoinmagazine.com"]
        placeholders = ",".join(["?"] * len(allow))
        q = f"""
        WITH latest AS (
          SELECT {src_col} AS src, MAX({tcol}) AS mx
          FROM {provider_table}
          WHERE {src_col} IN ({placeholders})
          GROUP BY {src_col}
        )
        SELECT p.*
        FROM {provider_table} p
        JOIN latest l ON l.src = p.{src_col} AND l.mx = p.{tcol}
        ORDER BY p.{src_col}
        """
        out["provider_latest_rows"] = [dict(r) for r in cur.execute(q, allow).fetchall()]

sig_table = main_tables["news_intelligence_signals"]
if sig_table:
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({sig_table})").fetchall()]
    shadow_col = "shadow_only" if "shadow_only" in cols else None
    trade_col = "should_affect_trading" if "should_affect_trading" in cols else None
    tcol = next((c for c in ("created_at", "ts_utc", "timestamp_utc") if c in cols), None)
    if shadow_col and trade_col:
        if window and tcol:
            start_iso, end_iso = window
            out["unsafe_signals_count_window"] = cur.execute(
                f"SELECT COUNT(*) FROM {sig_table} WHERE ({shadow_col}!=1 OR {trade_col}!=0) AND {tcol}>=? AND {tcol}<=?",
                (start_iso, end_iso),
            ).fetchone()[0]
        out["unsafe_signals_count_all_time"] = cur.execute(
            f"SELECT COUNT(*) FROM {sig_table} WHERE ({shadow_col}!=1 OR {trade_col}!=0)"
        ).fetchone()[0]

narr_table = main_tables["news_cluster_narratives"]
if narr_table:
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({narr_table})").fetchall()]
    if "cluster_id" in cols and "narrative_type" in cols:
        if window:
            start_iso, end_iso = window
            tcol = "created_at" if "created_at" in cols else None
            where = "" if not tcol else f"WHERE {tcol}>=? AND {tcol}<=?"
            params = () if not tcol else (start_iso, end_iso)
            out["duplicate_narratives_rows_window"] = cur.execute(
                f"SELECT COUNT(*) FROM (SELECT cluster_id,narrative_type,COUNT(*) n FROM {narr_table} {where} GROUP BY cluster_id,narrative_type HAVING n>1)",
                params,
            ).fetchone()[0]
        out["duplicate_narratives_rows_all_time"] = cur.execute(
            f"SELECT COUNT(*) FROM (SELECT cluster_id,narrative_type,COUNT(*) n FROM {narr_table} GROUP BY cluster_id,narrative_type HAVING n>1)"
        ).fetchone()[0]

print(json.dumps(out, indent=2, default=str))
