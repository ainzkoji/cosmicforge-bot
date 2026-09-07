from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_DB = Path("backends/shared/shared_lib/persistence/cosmicforge.db")
DEFAULT_JSON = Path("reports/symbol_quality/symbol_quality_report.json")
DEFAULT_CSV = Path("reports/symbol_quality/symbol_quality_report.csv")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
        if math.isnan(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _avg(values: list[float | None]) -> float | None:
    vals = [value for value in values if value is not None]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def _sum(values: list[float | None]) -> float:
    return float(sum(value for value in values if value is not None))


def _pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator * 100.0 / denominator)


def _profit_factor(pnls: list[float | None]) -> float | str | None:
    wins = sum(value for value in pnls if value is not None and value > 0)
    losses = abs(sum(value for value in pnls if value is not None and value < 0))
    if losses == 0:
        return None if wins == 0 else "inf"
    return float(wins / losses)


def _parse_ts(value: Any) -> datetime:
    raw = str(value or "").strip()
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _parse_symbols(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def load_live_symbols(env_path: Path) -> list[str]:
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("LIVE_SYMBOLS="):
                return _parse_symbols(line.split("=", 1)[1])
        for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("TRADE_SYMBOLS="):
                return _parse_symbols(line.split("=", 1)[1])
    return _parse_symbols(os.environ.get("LIVE_SYMBOLS") or os.environ.get("TRADE_SYMBOLS"))


def _exit_bucket(row: dict[str, Any]) -> str:
    reason = str(row.get("exit_reason") or row.get("trigger_source") or "").upper()
    if "TIME_EXIT" in reason or "TIME" in reason:
        return "TIME_EXIT"
    if "TP" in reason or "TAKE_PROFIT" in reason:
        return "TP"
    if "SL" in reason or "STOP" in reason:
        return "SL"
    return "OTHER"


def _loss_streak_and_drawdown(rows: list[dict[str, Any]]) -> tuple[int, float]:
    ordered = sorted(rows, key=lambda row: (_parse_ts(row.get("timestamp_utc")), row.get("id") or 0))
    max_loss_streak = 0
    current_loss_streak = 0
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for row in ordered:
        pnl = _safe_float(row.get("realized_pnl")) or 0.0
        if pnl < 0:
            current_loss_streak += 1
            max_loss_streak = max(max_loss_streak, current_loss_streak)
        elif pnl > 0:
            current_loss_streak = 0
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return max_loss_streak, float(max_drawdown)


def load_rows(db_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        fills = conn.execute(
            """
            SELECT *
            FROM trade_fills
            WHERE COALESCE(account_id, '') != 'backfill'
              AND COALESCE(initiator_type, '') != 'SHADOW'
            ORDER BY timestamp_utc ASC, id ASC
            """
        ).fetchall()
        shadow = conn.execute(
            """
            SELECT *
            FROM dynamic_universe_shadow_diagnostics
            ORDER BY created_at ASC, id ASC
            """
        ).fetchall()
    return [dict(row) for row in fills], [dict(row) for row in shadow]


def live_symbol_performance(fills: list[dict[str, Any]], live_symbols: list[str]) -> list[dict[str, Any]]:
    close_fills = [row for row in fills if str(row.get("action")).upper() == "CLOSE"]
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in close_fills:
        by_symbol[str(row.get("symbol") or "UNKNOWN").upper()].append(row)

    rows: list[dict[str, Any]] = []
    for symbol in sorted(set(live_symbols) | set(by_symbol.keys())):
        symbol_rows = by_symbol.get(symbol, [])
        pnls = [_safe_float(row.get("realized_pnl")) for row in symbol_rows]
        r_values = [_safe_float(row.get("r_multiple")) for row in symbol_rows]
        wins = [pnl for pnl in pnls if pnl is not None and pnl > 0]
        losses = [pnl for pnl in pnls if pnl is not None and pnl < 0]
        exits = Counter(_exit_bucket(row) for row in symbol_rows)
        max_loss_streak, max_drawdown = _loss_streak_and_drawdown(symbol_rows)
        rows.append(
            {
                "symbol": symbol,
                "in_current_live_config": symbol in live_symbols,
                "closed_trades": len(symbol_rows),
                "total_pnl": _sum(pnls),
                "win_rate_pct": _pct(len(wins), len([pnl for pnl in pnls if pnl is not None])),
                "profit_factor": _profit_factor(pnls),
                "average_r_multiple": _avg(r_values),
                "average_pnl": _avg(pnls),
                "average_win": _avg(wins),
                "average_loss": _avg(losses),
                "sl_count": exits.get("SL", 0),
                "tp_count": exits.get("TP", 0),
                "time_exit_count": exits.get("TIME_EXIT", 0),
                "other_count": exits.get("OTHER", 0),
                "max_loss_streak": max_loss_streak,
                "max_drawdown_pnl": max_drawdown,
            }
        )

    def score(row: dict[str, Any]) -> float:
        pf = row["profit_factor"]
        pf_num = 3.0 if pf == "inf" else (pf or 0.0)
        return (
            row["total_pnl"] * 1.0
            + (row["win_rate_pct"] or 0.0) * 0.25
            + pf_num * 10.0
            + (row["average_r_multiple"] or 0.0) * 20.0
            + min(row["closed_trades"], 20) * 0.5
            + row["max_drawdown_pnl"] * 0.1
        )

    for row in rows:
        row["quality_score"] = round(score(row), 4)
    rows.sort(key=lambda row: row["quality_score"], reverse=True)
    return rows


def shadow_opportunity_ranking(shadow_rows: list[dict[str, Any]], live_symbols: list[str]) -> list[dict[str, Any]]:
    live_set = set(live_symbols)
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in shadow_rows:
        symbol = str(row.get("symbol") or "").upper()
        if symbol and symbol not in live_set and not bool(row.get("in_live_config")):
            by_symbol[symbol].append(row)

    results: list[dict[str, Any]] = []
    for symbol, rows in sorted(by_symbol.items()):
        evaluated = [row for row in rows if row.get("was_evaluated")]
        passes = [row for row in rows if row.get("would_pass_strategy")]
        confidences = [_safe_float(row.get("confidence")) for row in evaluated]
        pass_confidences = [_safe_float(row.get("confidence")) for row in passes]
        signals = Counter(str(row.get("signal") or "UNKNOWN").upper() for row in passes)
        reasons = Counter(str(row.get("reason") or "UNKNOWN") for row in rows)
        ranks = [_safe_float(row.get("rank")) for row in rows]
        volumes = [_safe_float(row.get("quote_volume_24h")) for row in rows]
        spreads = [_safe_float(row.get("spread_bps")) for row in rows]
        repeated_signal_consistency = _pct(max(signals.values(), default=0), len(passes))
        avg_spread = _avg(spreads)
        avg_volume = _avg(volumes)
        score = (
            len(passes) * 12.0
            + (_avg(pass_confidences) or _avg(confidences) or 0.0) * 25.0
            + (_safe_float(max(pass_confidences, default=None)) or 0.0) * 10.0
            + (repeated_signal_consistency or 0.0) * 0.08
            + math.log10(max(avg_volume or 1.0, 1.0)) * 2.0
            - (avg_spread or 0.0) * 0.8
            - (_avg(ranks) or 100.0) * 0.08
        )
        results.append(
            {
                "symbol": symbol,
                "shadow_rows": len(rows),
                "evaluated_count": len(evaluated),
                "would_pass_count": len(passes),
                "would_pass_rate_pct": _pct(len(passes), len(evaluated)),
                "average_confidence": _avg(confidences),
                "average_pass_confidence": _avg(pass_confidences),
                "max_confidence": max([value for value in confidences if value is not None], default=None),
                "average_rank": _avg(ranks),
                "average_quote_volume_24h": avg_volume,
                "average_spread_bps": avg_spread,
                "repeated_signal_consistency_pct": repeated_signal_consistency,
                "top_pass_signal": signals.most_common(1)[0][0] if signals else None,
                "dominant_reason": reasons.most_common(1)[0][0] if reasons else None,
                "opportunity_score": round(score, 4),
            }
        )
    results.sort(key=lambda row: row["opportunity_score"], reverse=True)
    return results


def classify_live_symbols(rows: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    keep: list[str] = []
    review: list[dict[str, Any]] = []
    insufficient_sample: list[str] = []
    for row in rows:
        if not row["in_current_live_config"]:
            continue
        reasons: list[str] = []
        if row["closed_trades"] == 0:
            insufficient_sample.append(row["symbol"])
            keep.append(row["symbol"])
            continue
        if row["closed_trades"] >= 3 and row["total_pnl"] < 0:
            reasons.append("negative total PnL")
        if row["closed_trades"] >= 5 and (row["win_rate_pct"] or 0) < 30:
            reasons.append("poor win rate")
        if row["closed_trades"] >= 5 and (row["profit_factor"] not in ("inf", None)) and row["profit_factor"] < 0.8:
            reasons.append("profit factor below 0.8")
        if row["closed_trades"] >= 5 and row["max_loss_streak"] >= 4:
            reasons.append("loss streak >= 4")
        if row["closed_trades"] >= 5 and row["sl_count"] >= max(3, row["tp_count"] * 2):
            reasons.append("repeated SL hits versus TP exits")
        if reasons:
            review.append({"symbol": row["symbol"], "reasons": reasons, "metrics": row})
        else:
            keep.append(row["symbol"])
    return keep, review, insufficient_sample


def build_report(db_path: Path, live_symbols: list[str]) -> dict[str, Any]:
    fills, shadow_rows = load_rows(db_path)
    live_ranking = live_symbol_performance(fills, live_symbols)
    shadow_ranking = shadow_opportunity_ranking(shadow_rows, live_symbols)
    keep, review, insufficient_sample = classify_live_symbols(live_ranking)
    watchlist = [
        row["symbol"]
        for row in shadow_ranking
        if row["would_pass_count"] >= 2
        and (row["average_pass_confidence"] or 0.0) >= 0.45
        and (row["average_spread_bps"] is None or row["average_spread_bps"] <= 5.0)
        and (row["average_quote_volume_24h"] or 0.0) >= 25_000_000
    ][:15]
    review_symbols = {item["symbol"] for item in review}
    severe_review_symbols = {
        item["symbol"]
        for item in review
        if "negative total PnL" in item["reasons"]
        and (
            "poor win rate" in item["reasons"]
            or "profit factor below 0.8" in item["reasons"]
            or "repeated SL hits versus TP exits" in item["reasons"]
        )
    }
    provisional_live = [
        symbol
        for symbol in live_symbols
        if symbol not in severe_review_symbols
    ]
    possible_next = provisional_live + [symbol for symbol in watchlist if symbol not in provisional_live]
    possible_next = possible_next[: max(20, min(30, len(possible_next)))]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": str(db_path),
        "scope": "actual executed trade_fills plus read-only dynamic_universe_shadow_diagnostics; excludes backfill and shadow trade_fills",
        "current_live_symbols": live_symbols,
        "live_symbol_performance_ranking": live_ranking,
        "weak_live_symbols": review,
        "shadow_opportunity_ranking": shadow_ranking,
        "candidate_additions": [row for row in shadow_ranking if row["symbol"] in watchlist],
        "recommendation": {
            "keep_list": keep,
            "remove_review_list": [item["symbol"] for item in review],
            "insufficient_sample_list": insufficient_sample,
            "severe_review_list": sorted(severe_review_symbols),
            "add_to_watchlist": watchlist,
            "possible_next_curated_list": possible_next,
        },
    }


def _csv_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 8)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def write_outputs(report: dict[str, Any], json_path: Path, csv_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["section", "symbol", "metric", "value"])
        writer.writeheader()
        for row in report["live_symbol_performance_ranking"]:
            for metric, value in row.items():
                if metric != "symbol":
                    writer.writerow({"section": "live_symbol_performance", "symbol": row["symbol"], "metric": metric, "value": _csv_value(value)})
        for item in report["weak_live_symbols"]:
            writer.writerow({"section": "weak_live_symbols", "symbol": item["symbol"], "metric": "reasons", "value": _csv_value(item["reasons"])})
        for row in report["shadow_opportunity_ranking"]:
            for metric, value in row.items():
                if metric != "symbol":
                    writer.writerow({"section": "shadow_opportunity", "symbol": row["symbol"], "metric": metric, "value": _csv_value(value)})
        for metric, value in report["recommendation"].items():
            writer.writerow({"section": "recommendation", "symbol": "", "metric": metric, "value": _csv_value(value)})


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return str(value)


def print_console(report: dict[str, Any], json_path: Path, csv_path: Path) -> None:
    print("CosmicForge Symbol Quality Report")
    print("---------------------------------")
    print(f"Generated: {report['generated_at']}")
    print(f"JSON: {json_path}")
    print(f"CSV:  {csv_path}")
    print("")
    print("Live symbol performance ranking")
    for row in report["live_symbol_performance_ranking"][:20]:
        marker = "LIVE" if row["in_current_live_config"] else "OLD"
        print(
            f"  {marker} {row['symbol']}: pnl={_fmt(row['total_pnl'])} "
            f"trades={row['closed_trades']} win={_fmt(row['win_rate_pct'])}% "
            f"pf={_fmt(row['profit_factor'])} avgR={_fmt(row['average_r_multiple'])} "
            f"loss_streak={row['max_loss_streak']}"
        )
    print("")
    print("Weak live symbols to review")
    for item in report["weak_live_symbols"]:
        print(f"  {item['symbol']}: {', '.join(item['reasons'])}")
    if not report["weak_live_symbols"]:
        print("  none")
    print("")
    print("Top shadow non-live opportunities")
    for row in report["shadow_opportunity_ranking"][:15]:
        print(
            f"  {row['symbol']}: passes={row['would_pass_count']} "
            f"avg_conf={_fmt(row['average_confidence'])} max_conf={_fmt(row['max_confidence'])} "
            f"vol={_fmt(row['average_quote_volume_24h'], 0)} spread={_fmt(row['average_spread_bps'])}bps "
            f"signal={row['top_pass_signal'] or 'n/a'}"
        )
    print("")
    rec = report["recommendation"]
    print("Final recommendation")
    print(f"  Keep: {', '.join(rec['keep_list']) or 'none'}")
    print(f"  Review/remove: {', '.join(rec['remove_review_list']) or 'none'}")
    print(f"  Insufficient sample: {', '.join(rec['insufficient_sample_list']) or 'none'}")
    print(f"  Severe review: {', '.join(rec['severe_review_list']) or 'none'}")
    print(f"  Add to watchlist: {', '.join(rec['add_to_watchlist']) or 'none'}")
    print(f"  Possible next curated list: {', '.join(rec['possible_next_curated_list']) or 'none'}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a read-only live-vs-shadow symbol quality report.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--env", type=Path, default=Path("backends/bot-backend/.env"))
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--csv-output", type=Path, default=DEFAULT_CSV)
    args = parser.parse_args()

    live_symbols = load_live_symbols(args.env)
    report = build_report(args.db, live_symbols)
    write_outputs(report, args.json_output, args.csv_output)
    print_console(report, args.json_output, args.csv_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
