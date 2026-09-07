from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.symbols import symbol_scoring
from shared_lib.persistence.db import DB


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        raw = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _json_loads(raw: Any, default: Any) -> Any:
    try:
        if not raw:
            return default
        return json.loads(str(raw))
    except Exception:
        return default


def _table_exists(conn: Any, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_cols(conn: Any, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _scoring_model_changed_at() -> datetime:
    configured = _parse_dt(getattr(settings, "AUTO_SYMBOL_SCORING_MODEL_CHANGED_AT", ""))
    if configured is not None:
        return configured
    path = Path(getattr(symbol_scoring, "__file__", "") or "")
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    except Exception:
        return _utc_now()


@dataclass(frozen=True)
class PromotionEvaluation:
    decision_type: str
    status: str
    selected_symbols: list[str]
    evidence_summary: dict[str, Any]
    ranking_run_ids: list[str]
    failure_reasons: list[str]

    @property
    def ready(self) -> bool:
        return self.decision_type == "PROMOTION_RECOMMENDED" and self.status == "PASS"


class SymbolPromotionDecisionLedger:
    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_universe_promotion_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    bot_instance_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    from_mode TEXT NOT NULL,
                    to_mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    selected_symbols_json TEXT NOT NULL DEFAULT '[]',
                    evidence_summary_json TEXT NOT NULL DEFAULT '{}',
                    ranking_run_ids_json TEXT NOT NULL DEFAULT '[]',
                    failure_reasons_json TEXT NOT NULL DEFAULT '[]',
                    executed INTEGER NOT NULL DEFAULT 0,
                    executed_at TEXT,
                    audit_event_type TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_promotion_decisions_created "
                "ON symbol_universe_promotion_decisions(created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_promotion_decisions_bot "
                "ON symbol_universe_promotion_decisions(bot_instance_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_promotion_decisions_type "
                "ON symbol_universe_promotion_decisions(decision_type)"
            )

    def record(
        self,
        evaluation: PromotionEvaluation,
        *,
        bot_instance_id: str,
        from_mode: str,
        to_mode: str = "auto_top_n",
        executed: bool = False,
    ) -> dict[str, Any]:
        created_at = _utc_now_iso()
        audit_event_type = (
            "SYMBOL_UNIVERSE_PROMOTION_RECOMMENDED"
            if evaluation.decision_type == "PROMOTION_RECOMMENDED"
            else "SYMBOL_UNIVERSE_PROMOTION_EVALUATED"
        )
        with self.db.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO symbol_universe_promotion_decisions (
                    created_at, bot_instance_id, decision_type, from_mode, to_mode,
                    status, selected_symbols_json, evidence_summary_json,
                    ranking_run_ids_json, failure_reasons_json, executed,
                    executed_at, audit_event_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    bot_instance_id,
                    evaluation.decision_type,
                    from_mode,
                    to_mode,
                    evaluation.status,
                    json.dumps(evaluation.selected_symbols),
                    json.dumps(evaluation.evidence_summary),
                    json.dumps(evaluation.ranking_run_ids),
                    json.dumps(evaluation.failure_reasons),
                    1 if executed else 0,
                    created_at if executed else None,
                    audit_event_type,
                ),
            )
            row_id = cur.lastrowid
        return {
            "id": row_id,
            "created_at": created_at,
            "decision_type": evaluation.decision_type,
            "status": evaluation.status,
            "selected_symbols": evaluation.selected_symbols,
            "failure_reasons": evaluation.failure_reasons,
            "executed": executed,
        }


class SymbolPromotionEvaluator:
    """Step 1 auto-promotion evaluator.

    This class only evaluates and persists evidence. It deliberately does not
    mutate SYMBOL_UNIVERSE_MODE, runner symbols, executor allowlists, sizing,
    leverage, allocation, risk, or entry protection.
    """

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()
        self.ledger = SymbolPromotionDecisionLedger(self.db)

    def evaluate(self, *, bot_instance_id: str | None = None) -> PromotionEvaluation:
        now = _utc_now()
        from_mode = str(getattr(settings, "SYMBOL_UNIVERSE_MODE", "static") or "static")
        top_n = int(getattr(settings, "AUTO_SYMBOL_TOP_N", 20) or 20)
        min_hours = int(getattr(settings, "AUTO_SYMBOL_PROMOTION_MIN_HOURS", 72) or 72)
        min_runs = int(getattr(settings, "AUTO_SYMBOL_PROMOTION_MIN_RANKING_RUNS", 100) or 100)
        min_trade_symbols = int(getattr(settings, "AUTO_SYMBOL_PROMOTION_MIN_TRADE_SYMBOLS", 10) or 10)
        trade_ratio_threshold = float(getattr(settings, "AUTO_SYMBOL_PROMOTION_TRADE_RUN_RATIO", 0.60) or 0.60)
        stability_threshold = float(getattr(settings, "AUTO_SYMBOL_PROMOTION_TOPN_STABILITY", 0.70) or 0.70)
        stability_hours = int(getattr(settings, "AUTO_SYMBOL_PROMOTION_STABILITY_HOURS", 24) or 24)
        min_would_pass = int(getattr(settings, "AUTO_SYMBOL_PROMOTION_MIN_WOULD_PASS", 3) or 3)
        min_confidence_samples = int(getattr(settings, "AUTO_SYMBOL_PROMOTION_MIN_CONFIDENCE_SAMPLES", 20) or 20)
        min_avg_conf = float(getattr(settings, "AUTO_SYMBOL_PROMOTION_MIN_AVG_CONFIDENCE", 0.25) or 0.25)
        min_volume = float(getattr(settings, "AUTO_SYMBOL_PROMOTION_MIN_QUOTE_VOLUME", 50_000_000.0) or 0.0)
        max_spread = float(getattr(settings, "AUTO_SYMBOL_PROMOTION_MAX_SPREAD_BPS", 6.0) or 6.0)
        require_candles = bool(getattr(settings, "AUTO_SYMBOL_PROMOTION_REQUIRE_CANDLE_SUFFICIENCY", True))
        require_funding = bool(getattr(settings, "AUTO_SYMBOL_PROMOTION_REQUIRE_FUNDING_STABILITY", False))

        scoring_changed_at = _scoring_model_changed_at()
        observation_hours = max(0.0, (now - scoring_changed_at).total_seconds() / 3600.0)
        failures: list[str] = []

        with self.db.connect() as conn:
            run_rows = conn.execute(
                """
                SELECT ranking_run_id, MAX(created_at) AS max_created, COUNT(*) AS row_count
                FROM symbol_universe_rankings
                WHERE ranking_run_id IS NOT NULL
                  AND created_at >= ?
                  AND (? IS NULL OR bot_instance_id = ?)
                GROUP BY ranking_run_id
                ORDER BY MAX(created_at) DESC
                """,
                (scoring_changed_at.isoformat(), bot_instance_id, bot_instance_id),
            ).fetchall()
            ranking_run_ids = [str(row["ranking_run_id"]) for row in run_rows]
            latest_run_id = ranking_run_ids[0] if ranking_run_ids else None

            latest_rows = []
            if latest_run_id:
                latest_rows = conn.execute(
                    """
                    SELECT *
                    FROM symbol_universe_rankings
                    WHERE ranking_run_id = ?
                      AND (? IS NULL OR bot_instance_id = ?)
                    ORDER BY rank ASC
                    """,
                    (latest_run_id, bot_instance_id, bot_instance_id),
                ).fetchall()

            stability = self._topn_stability(
                conn,
                bot_instance_id=bot_instance_id,
                since=now - timedelta(hours=stability_hours),
                latest_run_id=latest_run_id,
                top_n=top_n,
            )
            trade_ratios = self._trade_ratios(conn, bot_instance_id=bot_instance_id, run_ids=ranking_run_ids)
            shadow_stats = self._shadow_stats(conn)
            safety = self._live_safety_snapshot(conn)

        if from_mode != "dynamic_shadow":
            failures.append("mode_is_not_dynamic_shadow")
        if observation_hours < min_hours:
            failures.append("observation_window_too_short")
        if len(ranking_run_ids) < min_runs:
            failures.append("insufficient_ranking_runs")
        if stability["topn_stability"] < stability_threshold:
            failures.append("topn_stability_below_threshold")

        stable_candidates: list[dict[str, Any]] = []
        for row in latest_rows:
            if row["recommended_action"] != "TRADE":
                continue
            symbol = str(row["symbol"]).upper()
            ratio = trade_ratios.get(symbol, 0.0)
            if ratio < trade_ratio_threshold:
                continue
            candidate_failures = self._candidate_failures(
                row,
                shadow_stats.get(symbol, {}),
                min_would_pass=min_would_pass,
                min_confidence_samples=min_confidence_samples,
                min_avg_conf=min_avg_conf,
                min_volume=min_volume,
                max_spread=max_spread,
                require_candles=require_candles,
                require_funding=require_funding,
            )
            if candidate_failures:
                continue
            stable_candidates.append(
                {
                    "symbol": symbol,
                    "rank": row["rank"],
                    "score": row["score"],
                    "trade_run_ratio": ratio,
                }
            )

        if len(stable_candidates) < min_trade_symbols:
            failures.append("insufficient_stable_trade_candidates")

        safety_failures = [item["reason"] for item in safety["failures"]]
        failures.extend(safety_failures)

        stable_candidates.sort(key=lambda item: (item["rank"] if item["rank"] is not None else 9999))
        selected = [item["symbol"] for item in stable_candidates[:top_n]]
        evidence = {
            "from_mode": from_mode,
            "target_mode": "auto_top_n",
            "auto_symbol_promotion_enabled": bool(getattr(settings, "AUTO_SYMBOL_PROMOTION_ENABLED", False)),
            "scoring_model_changed_at": scoring_changed_at.isoformat(),
            "observation_hours": round(observation_hours, 4),
            "required_observation_hours": min_hours,
            "ranking_run_count": len(ranking_run_ids),
            "required_ranking_runs": min_runs,
            "latest_ranking_run_id": latest_run_id,
            "latest_trade_count": sum(1 for row in latest_rows if row["recommended_action"] == "TRADE"),
            "stable_trade_candidate_count": len(stable_candidates),
            "required_stable_trade_candidates": min_trade_symbols,
            "topn_stability": stability["topn_stability"],
            "required_topn_stability": stability_threshold,
            "stability_run_count": stability["run_count"],
            "selected_candidates": stable_candidates[:top_n],
            "live_safety": safety,
        }
        decision_type = "PROMOTION_RECOMMENDED" if not failures else "PROMOTION_EVALUATED"
        status = "PASS" if not failures else "FAIL"
        return PromotionEvaluation(
            decision_type=decision_type,
            status=status,
            selected_symbols=selected if not failures else [],
            evidence_summary=evidence,
            ranking_run_ids=ranking_run_ids,
            failure_reasons=sorted(set(failures)),
        )

    def evaluate_and_record(self, *, bot_instance_id: str | None = None) -> dict[str, Any]:
        evaluation = self.evaluate(bot_instance_id=bot_instance_id)
        return self.ledger.record(
            evaluation,
            bot_instance_id=bot_instance_id or "default",
            from_mode=str(getattr(settings, "SYMBOL_UNIVERSE_MODE", "static") or "static"),
            executed=False,
        )

    def _trade_ratios(self, conn: Any, *, bot_instance_id: str | None, run_ids: list[str]) -> dict[str, float]:
        if not run_ids:
            return {}
        placeholders = ",".join("?" for _ in run_ids)
        params: list[Any] = list(run_ids)
        bot_filter = ""
        if bot_instance_id is not None:
            bot_filter = " AND bot_instance_id = ?"
            params.append(bot_instance_id)
        rows = conn.execute(
            f"""
            SELECT symbol,
                   SUM(CASE WHEN recommended_action = 'TRADE' THEN 1 ELSE 0 END) AS trade_runs,
                   COUNT(*) AS seen_runs
            FROM symbol_universe_rankings
            WHERE ranking_run_id IN ({placeholders}) {bot_filter}
            GROUP BY symbol
            """,
            params,
        ).fetchall()
        total_runs = max(len(run_ids), 1)
        return {str(row["symbol"]).upper(): float(row["trade_runs"] or 0) / total_runs for row in rows}

    def _topn_stability(
        self,
        conn: Any,
        *,
        bot_instance_id: str | None,
        since: datetime,
        latest_run_id: str | None,
        top_n: int,
    ) -> dict[str, Any]:
        if not latest_run_id:
            return {"topn_stability": 0.0, "run_count": 0}
        params: list[Any] = [since.isoformat()]
        bot_filter = ""
        if bot_instance_id is not None:
            bot_filter = " AND bot_instance_id = ?"
            params.append(bot_instance_id)
        run_ids = [
            str(row["ranking_run_id"])
            for row in conn.execute(
                f"""
                SELECT ranking_run_id
                FROM symbol_universe_rankings
                WHERE ranking_run_id IS NOT NULL
                  AND created_at >= ?
                  {bot_filter}
                GROUP BY ranking_run_id
                ORDER BY MAX(created_at) DESC
                """,
                params,
            ).fetchall()
        ]
        if not run_ids:
            return {"topn_stability": 0.0, "run_count": 0}
        placeholders = ",".join("?" for _ in run_ids)
        rows = conn.execute(
            f"""
            SELECT ranking_run_id, symbol, rank
            FROM symbol_universe_rankings
            WHERE ranking_run_id IN ({placeholders})
              AND rank <= ?
            """,
            [*run_ids, top_n],
        ).fetchall()
        by_run: dict[str, set[str]] = {}
        for row in rows:
            by_run.setdefault(str(row["ranking_run_id"]), set()).add(str(row["symbol"]).upper())
        latest = by_run.get(latest_run_id, set())
        if not latest:
            return {"topn_stability": 0.0, "run_count": len(run_ids)}
        overlaps = [len(latest & symbols) / max(top_n, 1) for symbols in by_run.values() if symbols]
        return {
            "topn_stability": float(statistics.mean(overlaps)) if overlaps else 0.0,
            "run_count": len(run_ids),
        }

    def _shadow_stats(self, conn: Any) -> dict[str, dict[str, Any]]:
        if not _table_exists(conn, "dynamic_universe_shadow_diagnostics"):
            return {}
        rows = conn.execute(
            """
            SELECT symbol,
                   SUM(CASE WHEN was_evaluated = 1 THEN 1 ELSE 0 END) AS evaluated_count,
                   SUM(CASE WHEN confidence IS NOT NULL THEN 1 ELSE 0 END) AS confidence_sample_count,
                   AVG(confidence) AS average_confidence
            FROM dynamic_universe_shadow_diagnostics
            GROUP BY symbol
            """
        ).fetchall()
        return {
            str(row["symbol"]).upper(): {
                "evaluated_count": int(row["evaluated_count"] or 0),
                "confidence_sample_count": int(row["confidence_sample_count"] or 0),
                "average_confidence": row["average_confidence"],
            }
            for row in rows
        }

    def _candidate_failures(
        self,
        row: Any,
        shadow_stats: dict[str, Any],
        *,
        min_would_pass: int,
        min_confidence_samples: int,
        min_avg_conf: float,
        min_volume: float,
        max_spread: float,
        require_candles: bool,
        require_funding: bool,
    ) -> list[str]:
        symbol = str(row["symbol"]).upper()
        failures: list[str] = []
        diagnostics = _json_loads(row["diagnostics_json"], {})
        components = diagnostics.get("components") or {}
        if diagnostics.get("manual_review") or components.get("manual_penalty", 0) > 0:
            failures.append(f"{symbol}:manual_review")
        if diagnostics.get("denylisted"):
            failures.append(f"{symbol}:denylisted")
        if int(row["would_pass_count"] or 0) < min_would_pass:
            failures.append(f"{symbol}:would_pass_below_threshold")
        if int(shadow_stats.get("confidence_sample_count") or 0) < min_confidence_samples:
            failures.append(f"{symbol}:confidence_samples_below_threshold")
        avg_conf = shadow_stats.get("average_confidence")
        if avg_conf is None or float(avg_conf) < min_avg_conf:
            failures.append(f"{symbol}:average_confidence_below_threshold")
        if float(row["quote_volume_24h"] or 0.0) < min_volume:
            failures.append(f"{symbol}:quote_volume_below_threshold")
        if row["spread_bps"] is None or float(row["spread_bps"]) > max_spread:
            failures.append(f"{symbol}:spread_above_threshold")
        if require_candles and int(row["candle_sufficiency"] or 0) != 1:
            failures.append(f"{symbol}:candle_sufficiency_missing")
        if require_funding and row["funding_stability"] is None:
            failures.append(f"{symbol}:funding_stability_missing")
        return failures

    def _live_safety_snapshot(self, conn: Any) -> dict[str, Any]:
        failures: list[dict[str, Any]] = []
        checks: dict[str, Any] = {}

        if _table_exists(conn, "pending_entries"):
            cols = _table_cols(conn, "pending_entries")
            state_col = "state" if "state" in cols else "status" if "status" in cols else None
            symbol_col = "symbol" if "symbol" in cols else None
            side_col = "side" if "side" in cols else "direction" if "direction" in cols else None
            if state_col and symbol_col:
                side_expr = side_col if side_col else "''"
                rows = conn.execute(
                    f"""
                    SELECT {symbol_col} AS symbol, {side_expr} AS side, COUNT(*) AS cnt
                    FROM pending_entries
                    WHERE UPPER(COALESCE({state_col}, '')) NOT IN
                        ('CLOSED', 'CANCELLED', 'CANCELED', 'EXPIRED', 'FAILED', 'RELEASED', 'FLAT')
                    GROUP BY {symbol_col}, {side_expr}
                    HAVING COUNT(*) > 1
                    """
                ).fetchall()
                checks["duplicate_unresolved_pending_entries"] = len(rows)
                if rows:
                    failures.append({"reason": "duplicate_unresolved_pending_entries", "count": len(rows)})
                if "updated_at" in cols or "created_at" in cols:
                    ts_col = "updated_at" if "updated_at" in cols else "created_at"
                    ttl = int(getattr(settings, "AUTO_SYMBOL_PROMOTION_SUBMIT_UNKNOWN_TTL_SECONDS", 900) or 900)
                    cutoff = (_utc_now() - timedelta(seconds=ttl)).isoformat()
                    stale = conn.execute(
                        f"""
                        SELECT COUNT(*) AS cnt
                        FROM pending_entries
                        WHERE UPPER(COALESCE({state_col}, '')) = 'SUBMIT_UNKNOWN'
                          AND COALESCE({ts_col}, '') < ?
                        """,
                        (cutoff,),
                    ).fetchone()
                    checks["stale_submit_unknown"] = int(stale["cnt"] or 0)
                    if int(stale["cnt"] or 0) > 0:
                        failures.append({"reason": "stale_submit_unknown", "count": int(stale["cnt"] or 0)})

        if _table_exists(conn, "position_lifecycle_state"):
            cols = _table_cols(conn, "position_lifecycle_state")
            if {"sl_order_id", "tp_order_id"}.issubset(cols):
                phase_col = "phase" if "phase" in cols else "state" if "state" in cols else None
                where = ""
                if phase_col:
                    where = f"WHERE UPPER(COALESCE({phase_col}, '')) NOT IN ('CLOSED', 'FLAT', 'DONE', 'CANCELLED', 'CANCELED')"
                naked = conn.execute(
                    f"""
                    SELECT COUNT(*) AS cnt
                    FROM position_lifecycle_state
                    {where}
                    {'AND' if where else 'WHERE'} sl_order_id IS NULL
                      AND tp_order_id IS NULL
                    """
                ).fetchone()
                checks["open_naked_lifecycle_positions"] = int(naked["cnt"] or 0)
                if int(naked["cnt"] or 0) > 0:
                    failures.append({"reason": "open_naked_positions", "count": int(naked["cnt"] or 0)})

        if _table_exists(conn, "entry_protection_events"):
            cols = _table_cols(conn, "entry_protection_events")
            if "event_type" in cols or "reason" in cols:
                text_col = "event_type" if "event_type" in cols else "reason"
                recent = conn.execute(
                    f"""
                    SELECT COUNT(*) AS cnt
                    FROM entry_protection_events
                    WHERE UPPER(COALESCE({text_col}, '')) LIKE '%VIOLATION%'
                       OR UPPER(COALESCE({text_col}, '')) LIKE '%INVARIANT%'
                    """
                ).fetchone()
                checks["entry_protection_violations"] = int(recent["cnt"] or 0)
                if int(recent["cnt"] or 0) > 0:
                    failures.append({"reason": "entry_protection_violations", "count": int(recent["cnt"] or 0)})

        return {"passed": not failures, "checks": checks, "failures": failures}
