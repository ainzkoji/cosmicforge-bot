from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_DB = Path("backends/shared/shared_lib/persistence/cosmicforge.db")
DEFAULT_JSON = Path("reports/profitability/profitability_report.json")
DEFAULT_CSV = Path("reports/profitability/profitability_report.csv")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        val = float(value)
        if math.isnan(val):
            return None
        return val
    except (TypeError, ValueError):
        return None


def _sum(values: list[float | None]) -> float:
    return float(sum(v for v in values if v is not None))


def _avg(values: list[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def _pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator * 100.0 / denominator)


def _profit_factor(pnls: list[float | None]) -> float | None:
    wins = sum(v for v in pnls if v is not None and v > 0)
    losses = abs(sum(v for v in pnls if v is not None and v < 0))
    if losses == 0:
        return None if wins == 0 else float("inf")
    return float(wins / losses)


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return value
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _exit_bucket(row: dict[str, Any]) -> str:
    reason = str(row.get("exit_reason") or row.get("trigger_source") or "").upper()
    if "TIME_EXIT" in reason or "TIME" in reason:
        return "TIME_EXIT"
    if "TP" in reason or "TAKE_PROFIT" in reason:
        return "TP"
    if "SL" in reason or "STOP" in reason:
        return "SL"
    return "OTHER"


def _trade_view(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "id": row.get("id"),
        "timestamp_utc": row.get("timestamp_utc"),
        "symbol": row.get("symbol"),
        "side": row.get("side"),
        "position_id": row.get("position_id"),
        "realized_pnl": row.get("realized_pnl"),
        "r_multiple": row.get("r_multiple"),
        "exit_reason": row.get("exit_reason"),
        "trigger_source": row.get("trigger_source"),
    }


def load_fills(db_path: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM trade_fills
            WHERE COALESCE(account_id, '') != 'backfill'
              AND COALESCE(initiator_type, '') != 'SHADOW'
            ORDER BY timestamp_utc ASC, id ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def build_report(fills: list[dict[str, Any]], now: datetime | None = None) -> dict[str, Any]:
    now = now or _utc_now()
    open_fills = [r for r in fills if str(r.get("action")).upper() == "OPEN"]
    close_fills = [r for r in fills if str(r.get("action")).upper() == "CLOSE"]
    closed_with_pnl = [r for r in close_fills if _safe_float(r.get("realized_pnl")) is not None]
    pnls = [_safe_float(r.get("realized_pnl")) for r in close_fills]
    r_values = [_safe_float(r.get("r_multiple")) for r in close_fills]
    wins = [p for p in pnls if p is not None and p > 0]
    losses = [p for p in pnls if p is not None and p < 0]

    closed_position_ids = {r.get("position_id") for r in close_fills if r.get("position_id")}
    open_trades = [
        r
        for r in open_fills
        if not r.get("position_id") or r.get("position_id") not in closed_position_ids
    ]

    best = max(closed_with_pnl, key=lambda r: _safe_float(r.get("realized_pnl")) or 0, default=None)
    worst = min(closed_with_pnl, key=lambda r: _safe_float(r.get("realized_pnl")) or 0, default=None)

    position_linked_closed_trades = len(
        {
            r.get("position_id")
            for r in open_fills
            if r.get("position_id") and r.get("position_id") in closed_position_ids
        }
    )
    closed_trade_count = len(close_fills)
    open_trade_count = len(open_trades)

    overall = {
        "total_fills": len(fills),
        "total_trades": closed_trade_count + open_trade_count,
        "closed_trades": closed_trade_count,
        "open_trades": open_trade_count,
        "raw_open_fills": len(open_fills),
        "raw_close_fills": len(close_fills),
        "position_linked_closed_trades": position_linked_closed_trades,
        "total_realized_pnl": _sum(pnls),
        "win_rate_pct": _pct(len(wins), len(closed_with_pnl)),
        "average_win": _avg(wins),
        "average_loss": _avg(losses),
        "profit_factor": _profit_factor(pnls),
        "average_r_multiple": _avg(r_values),
        "best_trade": _trade_view(best),
        "worst_trade": _trade_view(worst),
    }

    per_symbol: list[dict[str, Any]] = []
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in close_fills:
        by_symbol[str(row.get("symbol") or "UNKNOWN")].append(row)

    for symbol, rows in sorted(by_symbol.items()):
        symbol_pnls = [_safe_float(r.get("realized_pnl")) for r in rows]
        symbol_r = [_safe_float(r.get("r_multiple")) for r in rows]
        symbol_wins = [p for p in symbol_pnls if p is not None and p > 0]
        exit_counts = Counter(_exit_bucket(r) for r in rows)
        per_symbol.append(
            {
                "symbol": symbol,
                "trades": len(rows),
                "win_rate_pct": _pct(len(symbol_wins), len([p for p in symbol_pnls if p is not None])),
                "total_pnl": _sum(symbol_pnls),
                "average_pnl": _avg(symbol_pnls),
                "average_r_multiple": _avg(symbol_r),
                "sl_count": exit_counts.get("SL", 0),
                "tp_count": exit_counts.get("TP", 0),
                "time_exit_count": exit_counts.get("TIME_EXIT", 0),
                "other_count": exit_counts.get("OTHER", 0),
            }
        )
    per_symbol.sort(key=lambda row: row["total_pnl"], reverse=True)

    recent = {}
    for label, delta in {
        "last_24h": timedelta(hours=24),
        "last_48h": timedelta(hours=48),
        "last_7d": timedelta(days=7),
    }.items():
        cutoff = now - delta
        rows = [r for r in close_fills if (_parse_ts(r.get("timestamp_utc")) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff]
        recent_pnls = [_safe_float(r.get("realized_pnl")) for r in rows]
        recent_wins = [p for p in recent_pnls if p is not None and p > 0]
        recent[label] = {
            "closed_trades": len(rows),
            "total_realized_pnl": _sum(recent_pnls),
            "win_rate_pct": _pct(len(recent_wins), len([p for p in recent_pnls if p is not None])),
            "average_pnl": _avg(recent_pnls),
            "profit_factor": _profit_factor(recent_pnls),
        }

    order_dupes = Counter(
        (r.get("order_id"), r.get("action"), r.get("symbol"))
        for r in fills
        if r.get("order_id")
    )
    exact_dupes = Counter(
        (
            r.get("position_id"),
            r.get("action"),
            r.get("symbol"),
            r.get("side"),
            r.get("timestamp_utc"),
            r.get("qty"),
            r.get("price"),
        )
        for r in fills
    )
    slippages = [_safe_float(r.get("slippage_pct")) for r in fills]
    slippages_non_null = [s for s in slippages if s is not None]
    biggest_slip_row = max(
        (r for r in fills if _safe_float(r.get("slippage_pct")) is not None),
        key=lambda r: abs(_safe_float(r.get("slippage_pct")) or 0),
        default=None,
    )

    risk_quality = {
        "duplicate_order_id_action_symbol_groups": sum(1 for count in order_dupes.values() if count > 1),
        "duplicate_exact_fill_groups": sum(1 for count in exact_dupes.values() if count > 1),
        "missing_run_id_count": sum(1 for r in fills if not r.get("run_id")),
        "missing_position_id_count": sum(1 for r in fills if not r.get("position_id")),
        "closed_fills_null_exit_reason": sum(1 for r in close_fills if r.get("exit_reason") is None),
        "closed_fills_null_r_multiple": sum(1 for r in close_fills if r.get("r_multiple") is None),
        "average_slippage_pct": _avg(slippages_non_null),
        "biggest_abs_slippage_pct": max((abs(s) for s in slippages_non_null), default=None),
        "biggest_slippage_fill": _trade_view(biggest_slip_row),
    }

    return {
        "generated_at": now.isoformat(),
        "database": str(DEFAULT_DB),
        "scope": "actual trade_fills only; excludes account_id='backfill' and initiator_type='SHADOW'",
        "overall": overall,
        "per_symbol": per_symbol,
        "recent": recent,
        "risk_execution_quality": risk_quality,
    }


def write_json(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(report), indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")


def _csv_value(value: Any) -> Any:
    if isinstance(value, float):
        if math.isinf(value):
            return "inf"
        return round(value, 8)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def write_csv(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["section", "name", "metric", "value", "symbol"],
        )
        writer.writeheader()
        for metric, value in report["overall"].items():
            writer.writerow({"section": "overall", "name": "overall", "metric": metric, "value": _csv_value(value), "symbol": ""})
        for label, metrics in report["recent"].items():
            for metric, value in metrics.items():
                writer.writerow({"section": "recent", "name": label, "metric": metric, "value": _csv_value(value), "symbol": ""})
        for row in report["per_symbol"]:
            symbol = row["symbol"]
            for metric, value in row.items():
                if metric == "symbol":
                    continue
                writer.writerow({"section": "per_symbol", "name": symbol, "metric": metric, "value": _csv_value(value), "symbol": symbol})
        for metric, value in report["risk_execution_quality"].items():
            writer.writerow({"section": "risk_execution_quality", "name": "risk_execution_quality", "metric": metric, "value": _csv_value(value), "symbol": ""})


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if math.isinf(value):
            return "inf"
        return f"{value:,.4f}"
    return str(value)


def print_console(report: dict[str, Any], json_path: Path, csv_path: Path) -> None:
    overall = report["overall"]
    risk = report["risk_execution_quality"]
    print("CosmicForge Profitability Report")
    print("--------------------------------")
    print(f"Generated: {report['generated_at']}")
    print(f"JSON: {json_path}")
    print(f"CSV:  {csv_path}")
    print("")
    print("Overall")
    print(f"  Total trades:        {overall['total_trades']}")
    print(f"  Closed trades:       {overall['closed_trades']}")
    print(f"  Open trades:         {overall['open_trades']}")
    print(f"  Total realized PnL:  {_fmt(overall['total_realized_pnl'])}")
    print(f"  Win rate:            {_fmt(overall['win_rate_pct'])}%")
    print(f"  Average win:         {_fmt(overall['average_win'])}")
    print(f"  Average loss:        {_fmt(overall['average_loss'])}")
    print(f"  Profit factor:       {_fmt(overall['profit_factor'])}")
    print(f"  Average R multiple:  {_fmt(overall['average_r_multiple'])}")
    print(f"  Best trade:          {_csv_value(overall['best_trade'])}")
    print(f"  Worst trade:         {_csv_value(overall['worst_trade'])}")
    print("")
    print("Recent")
    for label, metrics in report["recent"].items():
        print(
            f"  {label}: closed={metrics['closed_trades']} "
            f"pnl={_fmt(metrics['total_realized_pnl'])} "
            f"win_rate={_fmt(metrics['win_rate_pct'])}%"
        )
    print("")
    print("Top/Bottom Symbols By PnL")
    for row in report["per_symbol"][:5]:
        print(f"  TOP {row['symbol']}: trades={row['trades']} pnl={_fmt(row['total_pnl'])} win_rate={_fmt(row['win_rate_pct'])}%")
    for row in report["per_symbol"][-5:]:
        print(f"  BOT {row['symbol']}: trades={row['trades']} pnl={_fmt(row['total_pnl'])} win_rate={_fmt(row['win_rate_pct'])}%")
    print("")
    print("Risk / Execution Quality")
    print(f"  Duplicate order groups:      {risk['duplicate_order_id_action_symbol_groups']}")
    print(f"  Duplicate exact fill groups: {risk['duplicate_exact_fill_groups']}")
    print(f"  Missing run_id count:        {risk['missing_run_id_count']}")
    print(f"  Missing position_id count:   {risk['missing_position_id_count']}")
    print(f"  CLOSE null exit_reason:      {risk['closed_fills_null_exit_reason']}")
    print(f"  CLOSE null r_multiple:       {risk['closed_fills_null_r_multiple']}")
    print(f"  Average slippage pct:        {_fmt(risk['average_slippage_pct'])}")
    print(f"  Biggest abs slippage pct:    {_fmt(risk['biggest_abs_slippage_pct'])}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a read-only profitability report from trade_fills.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--csv-output", type=Path, default=DEFAULT_CSV)
    args = parser.parse_args()

    fills = load_fills(args.db)
    report = build_report(fills)
    report["database"] = str(args.db)
    write_json(report, args.json_output)
    write_csv(report, args.csv_output)
    print_console(report, args.json_output, args.csv_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
