from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.symbols.symbol_scoring import SymbolScoreInput, score_symbol
from shared_lib.persistence.db import DB


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _avg(values: list[float | None]) -> float | None:
    vals = [value for value in values if value is not None]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def parse_symbol_csv(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {item.strip().upper() for item in raw.split(",") if item.strip()}


class SymbolUniverseRankingRecorder:
    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_universe_rankings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ranking_run_id TEXT,
                    created_at TEXT NOT NULL,
                    bot_instance_id TEXT,
                    mode TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    rank INTEGER,
                    score REAL,
                    recommended_action TEXT NOT NULL,
                    selected_for_trading INTEGER NOT NULL DEFAULT 0,
                    preserved_for_management INTEGER NOT NULL DEFAULT 0,
                    quote_volume_24h REAL,
                    spread_bps REAL,
                    volatility_quality REAL,
                    candle_sufficiency INTEGER,
                    funding_stability REAL,
                    open_interest REAL,
                    signal_frequency REAL,
                    average_confidence REAL,
                    would_pass_count INTEGER,
                    recent_performance_score REAL,
                    inclusion_reason TEXT,
                    exclusion_reason TEXT,
                    diagnostics_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            existing_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(symbol_universe_rankings)").fetchall()
            }
            if "ranking_run_id" not in existing_cols:
                conn.execute("ALTER TABLE symbol_universe_rankings ADD COLUMN ranking_run_id TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_created "
                "ON symbol_universe_rankings(created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_run "
                "ON symbol_universe_rankings(ranking_run_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_symbol "
                "ON symbol_universe_rankings(symbol)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_action "
                "ON symbol_universe_rankings(recommended_action)"
            )

    def record_many(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.db.connect() as conn:
            print(f"[DYNAMIC_SHADOW_DEBUG] ranking DB insert start rows={len(rows)}")
            conn.executemany(
                """
                INSERT INTO symbol_universe_rankings (
                    ranking_run_id, created_at, bot_instance_id, mode, symbol, rank, score,
                    recommended_action, selected_for_trading, preserved_for_management,
                    quote_volume_24h, spread_bps, volatility_quality, candle_sufficiency,
                    funding_stability, open_interest, signal_frequency, average_confidence,
                    would_pass_count, recent_performance_score, inclusion_reason,
                    exclusion_reason, diagnostics_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.get("ranking_run_id"),
                        row.get("created_at") or _utc_now_iso(),
                        row.get("bot_instance_id"),
                        row.get("mode") or "dynamic_shadow",
                        row["symbol"],
                        row.get("rank"),
                        row.get("score"),
                        row["recommended_action"],
                        1 if row.get("selected_for_trading") else 0,
                        1 if row.get("preserved_for_management") else 0,
                        row.get("quote_volume_24h"),
                        row.get("spread_bps"),
                        row.get("volatility_quality"),
                        row.get("candle_sufficiency"),
                        row.get("funding_stability"),
                        row.get("open_interest"),
                        row.get("signal_frequency"),
                        row.get("average_confidence"),
                        row.get("would_pass_count"),
                        row.get("recent_performance_score"),
                        row.get("inclusion_reason"),
                        row.get("exclusion_reason"),
                        json.dumps(row.get("diagnostics") or {}),
                    )
                    for row in rows
                ],
            )
            print(f"[DYNAMIC_SHADOW_DEBUG] ranking DB insert committed rows={len(rows)}")


class DynamicSymbolSelector:
    """Shadow-only scorer/ranker for future automatic symbol selection."""

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()
        self.recorder = SymbolUniverseRankingRecorder(self.db)

    def _shadow_stats(self) -> dict[str, dict[str, Any]]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, was_evaluated, would_pass_strategy, confidence, diagnostics_json
                FROM dynamic_universe_shadow_diagnostics
                WHERE in_live_config = 0
                """
            ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row["symbol"]).upper()].append(dict(row))

        stats: dict[str, dict[str, Any]] = {}
        for symbol, items in grouped.items():
            evaluated = [item for item in items if item.get("was_evaluated")]
            passed = [item for item in items if item.get("would_pass_strategy")]
            confidences = [_safe_float(item.get("confidence")) for item in evaluated]
            pass_confidences = [_safe_float(item.get("confidence")) for item in passed]
            atr_values: list[float | None] = []
            for item in items:
                try:
                    meta = (json.loads(item.get("diagnostics_json") or "{}").get("meta") or {})
                    atr_values.append(_safe_float(meta.get("atr_pct")))
                except Exception:
                    pass
            atr_avg = _avg(atr_values)
            # Balanced volatility scores best: enough movement but not extreme.
            volatility_quality = None
            if atr_avg is not None:
                volatility_quality = max(0.0, min(1.0, 1.0 - abs(atr_avg - 2.5) / 5.0))
            stats[symbol] = {
                "shadow_rows": len(items),
                "evaluated_count": len(evaluated),
                "would_pass_count": len(passed),
                "confidence_sample_count": len([value for value in confidences if value is not None]),
                "average_confidence": _avg(confidences),
                "average_pass_confidence": _avg(pass_confidences),
                "max_confidence": max([value for value in confidences if value is not None], default=None),
                "volatility_quality": volatility_quality,
                "signal_frequency": (len(passed) / len(evaluated)) if evaluated else 0.0,
            }
        return stats

    def _live_performance(self) -> dict[str, dict[str, Any]]:
        with self.db.connect() as conn:
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(trade_fills)").fetchall()}
            if not {"symbol", "realized_pnl", "action"}.issubset(cols):
                return {}
            where = ["action = 'CLOSE'"]
            if "account_id" in cols:
                where.append("COALESCE(account_id, '') != 'backfill'")
            if "initiator_type" in cols:
                where.append("COALESCE(initiator_type, '') != 'SHADOW'")
            r_column = "r_multiple" if "r_multiple" in cols else "NULL AS r_multiple"
            rows = conn.execute(
                f"""
                SELECT symbol, realized_pnl, {r_column}
                FROM trade_fills
                WHERE {' AND '.join(where)}
                """
            ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row["symbol"]).upper()].append(dict(row))

        perf: dict[str, dict[str, Any]] = {}
        for symbol, items in grouped.items():
            pnls = [_safe_float(item.get("realized_pnl")) for item in items]
            valid_pnls = [p for p in pnls if p is not None]
            r_values = [_safe_float(item.get("r_multiple")) for item in items]
            wins = [pnl for pnl in pnls if pnl is not None and pnl > 0]
            losses = [pnl for pnl in pnls if pnl is not None and pnl < 0]
            loss_abs = abs(sum(losses))
            profit_factor: float | str | None
            if loss_abs == 0:
                profit_factor = "inf" if wins else None
            else:
                profit_factor = float(sum(wins) / loss_abs)
            perf[symbol] = {
                "recent_total_pnl": float(sum(pnl for pnl in pnls if pnl is not None)),
                "recent_win_rate_pct": (len(wins) * 100.0 / len(valid_pnls)) if valid_pnls else None,
                "recent_profit_factor": profit_factor,
                "average_r_multiple": _avg(r_values),
            }
        return perf

    def rank_shadow_universe(
        self,
        universe: dict[str, Any],
        *,
        live_symbols: set[str],
        bot_instance_id: str | None,
        persist: bool = True,
    ) -> list[dict[str, Any]]:
        mode = str(getattr(settings, "SYMBOL_UNIVERSE_MODE", "static") or "static")
        top_n = int(getattr(settings, "AUTO_SYMBOL_TOP_N", 20) or 20)
        denylist = parse_symbol_csv(getattr(settings, "AUTO_SYMBOL_DENYLIST", ""))
        allow_manual = bool(getattr(settings, "AUTO_SYMBOL_ALLOW_MANUAL_REVIEW", False))
        ranking_run_id = f"rank_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}_{uuid.uuid4().hex[:8]}"
        print(
            "[DYNAMIC_SHADOW_DEBUG] selector start "
            f"mode={mode} auto_enabled={bool(getattr(settings, 'AUTO_SYMBOL_SELECTION_ENABLED', False))} "
            f"run_id={ranking_run_id}"
        )
        shadow_stats = self._shadow_stats()
        live_perf = self._live_performance()

        candidates = universe.get("structural_candidates") or universe.get("ranked_candidates") or []
        print(f"[DYNAMIC_SHADOW_DEBUG] selector candidates={len(candidates)}")
        rows: list[dict[str, Any]] = []
        for candidate in candidates:
            symbol = str(candidate.get("symbol") or "").upper()
            stats = shadow_stats.get(symbol, {})
            perf = live_perf.get(symbol, {})
            score = score_symbol(
                SymbolScoreInput(
                    symbol=symbol,
                    rank=candidate.get("rank"),
                    quote_volume_24h=candidate.get("quote_volume_24h"),
                    spread_bps=candidate.get("spread_bps"),
                    exclusion_reasons=candidate.get("exclusion_reasons") or [],
                    shadow_rows=int(stats.get("shadow_rows") or 0),
                    evaluated_count=int(stats.get("evaluated_count") or 0),
                    would_pass_count=int(stats.get("would_pass_count") or 0),
                    confidence_sample_count=int(stats.get("confidence_sample_count") or 0),
                    average_confidence=stats.get("average_confidence"),
                    max_confidence=stats.get("max_confidence"),
                    average_pass_confidence=stats.get("average_pass_confidence"),
                    recent_total_pnl=perf.get("recent_total_pnl"),
                    recent_win_rate_pct=perf.get("recent_win_rate_pct"),
                    recent_profit_factor=perf.get("recent_profit_factor"),
                    average_r_multiple=perf.get("average_r_multiple"),
                    volatility_quality=stats.get("volatility_quality"),
                    candle_sufficiency=None,
                    funding_stability=None,
                    open_interest=None,
                    denylisted=symbol in denylist,
                    allow_manual_review=allow_manual,
                )
            )
            rows.append(
                {
                    "created_at": _utc_now_iso(),
                    "ranking_run_id": ranking_run_id,
                    "bot_instance_id": bot_instance_id,
                    "mode": mode,
                    "symbol": symbol,
                    "rank": candidate.get("rank"),
                    "score": score["score"],
                    "recommended_action": score["recommended_action"],
                    "selected_for_trading": False,
                    "preserved_for_management": False,
                    "quote_volume_24h": candidate.get("quote_volume_24h"),
                    "spread_bps": candidate.get("spread_bps"),
                    "volatility_quality": stats.get("volatility_quality"),
                    "candle_sufficiency": None,
                    "funding_stability": None,
                    "open_interest": None,
                    "signal_frequency": stats.get("signal_frequency"),
                    "average_confidence": stats.get("average_confidence"),
                    "would_pass_count": stats.get("would_pass_count") or 0,
                    "recent_performance_score": perf.get("recent_total_pnl"),
                    "inclusion_reason": score["inclusion_reason"],
                    "exclusion_reason": score["exclusion_reason"],
                    "diagnostics": {
                        "shadow_only": True,
                        "auto_symbol_selection_enabled": bool(getattr(settings, "AUTO_SYMBOL_SELECTION_ENABLED", False)),
                        "in_live_config": symbol in live_symbols,
                        "denylisted": symbol in denylist,
                        "manual_review": score["manual_review"],
                        "components": score["components"],
                        "shadow_stats": {
                            "shadow_rows": int(stats.get("shadow_rows") or 0),
                            "evaluated_count": int(stats.get("evaluated_count") or 0),
                            "confidence_sample_count": int(stats.get("confidence_sample_count") or 0),
                            "average_pass_confidence": stats.get("average_pass_confidence"),
                            "max_confidence": stats.get("max_confidence"),
                        },
                    },
                }
            )

        rows.sort(key=lambda item: (item["score"], -(item.get("quote_volume_24h") or 0.0)), reverse=True)
        trade_count = 0
        for idx, row in enumerate(rows, start=1):
            row["rank"] = idx
            if row["recommended_action"] == "TRADE" and trade_count < top_n:
                row["selected_for_trading"] = False  # dynamic_shadow only; recommendation, not executor allowlist.
                row["diagnostics"]["recommended_top_n_slot"] = trade_count + 1
                trade_count += 1
            elif row["recommended_action"] == "TRADE":
                row["recommended_action"] = "WATCH"
                row["inclusion_reason"] = "eligible_but_outside_top_n_recommendation"

        if persist:
            self.recorder.record_many(rows)
        actions: dict[str, int] = {}
        for row in rows:
            action = str(row.get("recommended_action") or "UNKNOWN")
            actions[action] = actions.get(action, 0) + 1
        print(
            "[DYNAMIC_SHADOW_DEBUG] selector scored "
            f"run_id={ranking_run_id} rows={len(rows)} actions={actions}"
        )
        return rows
