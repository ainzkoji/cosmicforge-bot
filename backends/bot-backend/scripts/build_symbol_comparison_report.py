from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_DB = Path("backends/shared/shared_lib/persistence/cosmicforge.db")
DEFAULT_OUTPUT = Path("reports/symbol_quality/live_vs_dynamic_top20_comparison.md")


def _symbols_from_json(raw: Any) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(str(raw))
    except Exception:
        parsed = [item.strip() for item in str(raw).split(",")]
    if isinstance(parsed, dict):
        parsed = parsed.get("symbols") or parsed.get("trade_symbols") or []
    return [str(item).strip().upper() for item in parsed if str(item).strip()]


def build_report(db_path: Path, top_n: int = 20) -> str:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    latest_run = conn.execute(
        """
        SELECT ranking_run_id, bot_instance_id, COUNT(*) AS row_count, MAX(created_at) AS latest_at
        FROM symbol_universe_rankings
        WHERE ranking_run_id IS NOT NULL
        GROUP BY ranking_run_id, bot_instance_id
        ORDER BY MAX(id) DESC
        LIMIT 1
        """
    ).fetchone()
    if not latest_run:
        raise RuntimeError("No symbol_universe_rankings rows found")

    bot = conn.execute(
        """
        SELECT id, status, strategy_id, symbols_json, updated_at, created_at
        FROM bot_instances
        WHERE id = ?
        """,
        (latest_run["bot_instance_id"],),
    ).fetchone()
    if not bot:
        bot = conn.execute(
            """
            SELECT id, status, strategy_id, symbols_json, updated_at, created_at
            FROM bot_instances
            WHERE symbols_json IS NOT NULL AND symbols_json != '[]'
            ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END, updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    if not bot:
        raise RuntimeError("No bot_instances row with symbols_json found")

    curated = _symbols_from_json(bot["symbols_json"])
    curated_set = set(curated)

    rows = conn.execute(
        """
        SELECT symbol, rank, score, recommended_action, inclusion_reason, exclusion_reason, diagnostics_json
        FROM symbol_universe_rankings
        WHERE ranking_run_id = ?
        ORDER BY rank ASC
        """,
        (latest_run["ranking_run_id"],),
    ).fetchall()
    ranking = {str(row["symbol"]).upper(): dict(row) for row in rows}
    top_symbols = [str(row["symbol"]).upper() for row in rows[:top_n]]
    top_set = set(top_symbols)
    top_floor = min(float(row["score"] or 0) for row in rows[:top_n]) if rows[:top_n] else 0.0

    ordered: list[str] = []
    for symbol in top_symbols + curated:
        if symbol not in ordered:
            ordered.append(symbol)

    common = [symbol for symbol in curated if symbol in top_set]
    weak = [symbol for symbol in curated if symbol not in top_set]
    opportunities = [symbol for symbol in top_symbols if symbol not in curated_set]

    lines: list[str] = []
    lines.append("# Live vs Dynamic Top 20 Symbol Comparison")
    lines.append("")
    lines.append(f"- Bot source: `{bot['id']}` (`{bot['status']}`, strategy `{bot['strategy_id']}`)")
    lines.append(f"- Ranking run: `{latest_run['ranking_run_id']}`")
    lines.append(f"- Ranking rows: `{latest_run['row_count']}`")
    lines.append(f"- Ranking timestamp: `{latest_run['latest_at']}`")
    lines.append(f"- Curated symbols: `{len(curated)}`")
    lines.append(f"- Top-N: `{top_n}`")
    lines.append(f"- Top-{top_n} floor score: `{top_floor:.4f}`")
    lines.append("")
    lines.append("## Symbols Currently Traded")
    lines.append(", ".join(f"`{symbol}`" for symbol in curated))
    lines.append("")
    lines.append(f"## Top {top_n} Ranked Symbols")
    lines.append(", ".join(f"`{symbol}`" for symbol in top_symbols))
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Intersection: {', '.join(f'`{symbol}`' for symbol in common) if common else 'none'}")
    lines.append(f"- WEAK curated-not-top{top_n}: {', '.join(f'`{symbol}`' for symbol in weak) if weak else 'none'}")
    lines.append(f"- OPPORTUNITY top{top_n}-not-curated: {', '.join(f'`{symbol}`' for symbol in opportunities) if opportunities else 'none'}")
    lines.append("")
    lines.append("| Symbol | In Curated | In Top-N | Score | Score Diff | Action | Verdict | Reason |")
    lines.append("|---|---:|---:|---:|---:|---|---|---|")

    for symbol in ordered:
        info = ranking.get(symbol, {})
        in_curated = symbol in curated_set
        in_top = symbol in top_set
        score = info.get("score")
        score_f = float(score) if score is not None else 0.0
        score_diff = score_f - top_floor if score is not None else None
        action = info.get("recommended_action") or "UNRANKED"
        reason = info.get("exclusion_reason") or info.get("inclusion_reason") or ""

        if in_curated and in_top:
            verdict = "KEEP"
            reason = reason or "curated symbol is inside dynamic top N"
        elif in_curated and not in_top:
            verdict = "WEAK"
            reason = reason or "curated symbol ranked below current top N"
        elif in_top and not in_curated:
            verdict = "OPPORTUNITY"
            reason = reason or "dynamic top N but not in curated live list"
        else:
            verdict = "IGNORE"

        diff_text = "n/a" if score_diff is None else f"{score_diff:.4f}"
        lines.append(
            f"| {symbol} | {'Yes' if in_curated else 'No'} | {'Yes' if in_top else 'No'} | "
            f"{score_f:.4f} | {diff_text} | {action} | {verdict} | {reason} |"
        )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare curated bot symbols to latest dynamic symbol rankings.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    report = build_report(args.db, args.top_n)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(report)
    print(f"Saved: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
