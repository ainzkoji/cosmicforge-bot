from __future__ import annotations

import json
import math
import subprocess
import sys
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from shared_lib.ml.contract import (
    CRITICAL_SOURCE_FIELDS,
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
    build_contract_metadata,
    compute_feature_value,
    get_contract_status,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.ml_runtime_status import latest_ml_runtime_status

from app.admin_ml.queries import MLAdminQueries
from app.core.config import settings


CRITICAL_FEATURE_FIELDS = tuple(CRITICAL_SOURCE_FIELDS)
V2_FEATURE_FIELDS = tuple(ML_FEATURE_COLUMNS)

REQUIRED_TRADES = 200
REQUIRED_WINS = 50
MIN_FEATURE_COVERAGE_PCT = 90.0
MIN_LINKAGE_HEALTH_PCT = 95.0
DEFAULT_ACTIVITY_DAYS = 30
DEFAULT_ACTIVITY_PAGE_SIZE = 50
DEFAULT_FEATURE_RECENT_LIMIT = 500
DEFAULT_SHADOW_DAYS = 90
DEFAULT_DRIFT_WINDOW_DAYS = 30
ALERT_ACTIVITY_DAYS = 7
MAX_ACTION_LOG_LINES = 40
ACTION_CONFIRM_PHRASES = {
    "rebuild_dataset": "REBUILD DATASET",
    "run_training": "RUN TRAINING",
    "run_validation": "RUN VALIDATION",
    "deploy_shadow": "DEPLOY TO SHADOW",
    "promote_live": "PROMOTE TO LIVE",
    "rollback_shadow": "ROLL BACK TO SHADOW",
    "disable_ml": "DISABLE ML",
}
SUPPORTED_ACTIONS = {"rebuild_dataset", "run_training", "run_validation"}


@dataclass
class LinkedTradeRow:
    data: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.data.get(item)

    @property
    def has_trace(self) -> bool:
        return bool(self["trace_id"])

    @property
    def effective_realized_pnl(self) -> float | None:
        if self["realized_pnl_count"]:
            return float(self["realized_pnl_sum"] or 0.0)

        open_price = self["open_price"]
        close_price = self["avg_close_price"]
        open_qty = self["open_qty"]
        close_qty = self["close_qty"]
        side = (self["side"] or "").upper()
        if open_price is None or close_price is None or open_qty is None or close_qty is None:
            return None

        matched_qty = min(float(open_qty), float(close_qty))
        if side == "LONG":
            return (float(close_price) - float(open_price)) * matched_qty
        if side == "SHORT":
            return (float(open_price) - float(close_price)) * matched_qty
        return None

    @property
    def has_full_feature_coverage(self) -> bool:
        for field in CRITICAL_FEATURE_FIELDS:
            value = self[field]
            if value is None:
                return False
            if isinstance(value, str) and not value.strip():
                return False
        return True

    @property
    def event_timestamp(self) -> str | None:
        return self["trace_ts"] or self["open_timestamp_utc"] or self["close_timestamp_utc"]


class MLAdminService:
    """Service layer for admin ML overview, monitoring, and readiness."""

    def __init__(
        self,
        db: DB | None = None,
        *,
        artifact_dir: Path | None = None,
        dataset_dir: Path | None = None,
        log_dir: Path | None = None,
        command_runner: Callable[..., Any] | None = None,
    ):
        self.db = db or DB()
        repo_root = Path(__file__).resolve().parents[4]
        self.repo_root = repo_root
        self.bot_backend_root = repo_root / "backends" / "bot-backend"
        default_model_root = repo_root / "backends" / "bot-backend" / "models"
        self.artifact_dir = artifact_dir or (default_model_root / "artifacts")
        self.dataset_dir = dataset_dir or (default_model_root / "datasets")
        self.log_dir = log_dir or (default_model_root / "logs")
        self.command_runner = command_runner or subprocess.run
        self._ensure_admin_ml_tables()

    def _ensure_admin_ml_tables(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ml_validation_history (
                    model_version TEXT PRIMARY KEY,
                    training_date TEXT,
                    dataset_used TEXT,
                    train_rows INTEGER,
                    test_rows INTEGER,
                    train_auc REAL,
                    test_auc REAL,
                    validation_method TEXT,
                    notes TEXT,
                    verdict TEXT NOT NULL,
                    deployed_mode TEXT NOT NULL DEFAULT 'not_deployed',
                    metadata_path TEXT,
                    validation_path TEXT,
                    source_payload_json TEXT,
                    synced_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ml_action_runs (
                    id TEXT PRIMARY KEY,
                    action_key TEXT NOT NULL,
                    requested_by_admin_id TEXT,
                    requested_by_email TEXT,
                    confirmation_phrase TEXT,
                    note TEXT,
                    status TEXT NOT NULL,
                    reason TEXT,
                    supported INTEGER NOT NULL DEFAULT 0,
                    readiness_snapshot_json TEXT,
                    request_payload_json TEXT,
                    dataset_path TEXT,
                    target_model_version TEXT,
                    log_path TEXT,
                    command_json TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    result_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ml_runtime_status (
                    scope_key TEXT PRIMARY KEY,
                    bot_instance_id TEXT,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    shadow_mode INTEGER NOT NULL DEFAULT 0,
                    loaded INTEGER NOT NULL DEFAULT 0,
                    model_version TEXT,
                    model_path TEXT,
                    metadata_path TEXT,
                    encoders_path TEXT,
                    threshold REAL,
                    hard_block_floor REAL,
                    contract_version TEXT,
                    schema_hash TEXT,
                    last_score_timestamp TEXT,
                    last_update_timestamp TEXT NOT NULL,
                    load_error TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ml_action_runs_action_created "
                "ON ml_action_runs(action_key, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ml_action_runs_status_created "
                "ON ml_action_runs(status, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ml_validation_history_training_date "
                "ON ml_validation_history(training_date DESC)"
            )

    def get_overview(self) -> dict[str, Any]:
        gate = self.get_training_gate()
        runtime_status = self._latest_runtime_status()
        latest_dataset_meta = self._latest_dataset_metadata()

        with self.db.connect() as conn:
            row = conn.execute(MLAdminQueries.OVERVIEW_SQL).fetchone()

        configured_enabled = bool(getattr(settings, "ML_ENABLED", False))
        configured_shadow_mode = bool(getattr(settings, "ML_SHADOW_MODE", False))
        runtime_enabled = bool(runtime_status["enabled"]) if runtime_status else configured_enabled
        runtime_shadow_mode = bool(runtime_status["shadow_mode"]) if runtime_status else configured_shadow_mode
        ml_enabled = runtime_enabled
        ml_shadow_mode = runtime_shadow_mode
        ml_mode = self._ml_mode(ml_enabled=ml_enabled, shadow_mode=ml_shadow_mode)
        model_path = (
            (runtime_status.get("model_path") if runtime_status else None)
            or getattr(settings, "ML_MODEL_PATH", "")
            or None
        )
        current_model_version = (
            (runtime_status.get("model_version") if runtime_status else None)
            or (row["current_model_version"] if row else None)
            or self._path_stem(model_path)
        )
        if runtime_status and runtime_status.get("threshold") is not None:
            current_threshold = runtime_status.get("threshold")
        elif row and row["current_threshold"] is not None:
            current_threshold = row["current_threshold"]
        else:
            current_threshold = getattr(settings, "ML_SCORE_THRESHOLD", None)
        contract_status = get_contract_status(
            feature_columns=(latest_dataset_meta or {}).get("meta", {}).get("feature_columns"),
            schema_hash=(latest_dataset_meta or {}).get("meta", {}).get("schema_hash"),
        )
        if not latest_dataset_meta:
            contract_status = {
                **build_contract_metadata(),
                "compatible": False,
                "schema_hash_matches": False,
                "feature_columns_match": False,
            }

        return {
            "ml_enabled": ml_enabled,
            "ml_mode": ml_mode,
            "current_model_version": current_model_version,
            "current_threshold": current_threshold,
            "current_hard_block_floor": (
                runtime_status.get("hard_block_floor")
                if runtime_status and runtime_status.get("hard_block_floor") is not None
                else getattr(settings, "ML_HARD_BLOCK_FLOOR", None)
            ),
            "model_artifact_path": model_path,
            "encoder_path": (
                (runtime_status.get("encoders_path") if runtime_status else None)
                or getattr(settings, "ML_ENCODERS_PATH", "")
                or None
            ),
            "metadata_path": (
                (runtime_status.get("metadata_path") if runtime_status else None)
                or getattr(settings, "ML_METADATA_PATH", "")
                or None
            ),
            "last_model_load_time": runtime_status.get("last_update_timestamp") if runtime_status else None,
            "last_bot_restart_time": row["last_bot_restart_time"] if row else None,
            "current_ml_status": self._current_ml_status(
                gate=gate,
                ml_mode=ml_mode,
                current_model_version=current_model_version,
            ),
            "runtime_loaded": bool(runtime_status.get("loaded")) if runtime_status else False,
            "runtime_load_error": runtime_status.get("load_error") if runtime_status else None,
            "last_successful_score_timestamp": runtime_status.get("last_score_timestamp") if runtime_status else None,
            "contract_version": contract_status["contract_version"],
            "schema_hash": contract_status["schema_hash"],
            "schema_compatible": contract_status["compatible"],
            "configured_defaults": {
                "ml_enabled": configured_enabled,
                "ml_shadow_mode": configured_shadow_mode,
                "model_path": getattr(settings, "ML_MODEL_PATH", "") or None,
                "encoders_path": getattr(settings, "ML_ENCODERS_PATH", "") or None,
                "metadata_path": getattr(settings, "ML_METADATA_PATH", "") or None,
            },
        }

    def get_training_gate(self) -> dict[str, Any]:
        with self.db.connect() as conn:
            total_opens = conn.execute(MLAdminQueries.TOTAL_OPENS_SQL).fetchone()["total_opens"]
            linked_rows = self._load_linked_trade_rows(conn)

        linked_trades = [row for row in linked_rows if row.has_trace]
        total_completed_positions = len(linked_rows)
        excluded_open_positions = max(int(total_opens or 0) - total_completed_positions, 0)
        linkage = self.get_linkage_health()
        summary = self._summarize_training_gate(linked_trades, linkage_healthy=bool(linkage["linkage_healthy"]))
        latest_dataset_meta = self._latest_dataset_metadata()
        if latest_dataset_meta:
            dataset_truth = self._training_gate_from_dataset_meta(
                latest_dataset_meta["meta"],
                fallback_summary=summary,
                linkage_healthy=bool(linkage["linkage_healthy"]),
            )
            summary = {
                **summary,
                **dataset_truth,
            }

        return {
            **summary,
            "excluded_open_positions": excluded_open_positions,
        }

    def get_dashboard_summary(self) -> dict[str, Any]:
        overview = self.get_overview()
        gate = self.get_training_gate()
        return {
            "ml_mode": overview["ml_mode"],
            "current_model_version": overview["current_model_version"],
            "total_linked_completed_trades": gate["total_linked_completed_trades"],
            "wins": gate["wins"],
            "feature_coverage_pct": gate["feature_coverage_pct"],
            "linkage_healthy": gate["linkage_healthy"],
            "training_ready": gate["training_ready"],
            "status": gate["status"],
            "contract_version": overview.get("contract_version"),
            "schema_hash": overview.get("schema_hash"),
            "schema_compatible": overview.get("schema_compatible"),
        }

    def get_feature_completeness(self, recent_limit: int = DEFAULT_FEATURE_RECENT_LIMIT) -> dict[str, Any]:
        with self.db.connect() as conn:
            linked_rows = [row for row in self._load_linked_trade_rows(conn) if row.has_trace]

        sorted_rows = sorted(
            linked_rows,
            key=lambda row: row.event_timestamp or "",
            reverse=True,
        )
        recent_rows = sorted_rows[:recent_limit]

        feature_rows: list[dict[str, Any]] = []
        for feature_name in V2_FEATURE_FIELDS:
            recent_values = [self._compute_v2_feature_value(row, feature_name) for row in recent_rows]
            lifetime_values = [self._compute_v2_feature_value(row, feature_name) for row in linked_rows]
            null_count_recent = sum(1 for value in recent_values if value is None)
            null_count_lifetime = sum(1 for value in lifetime_values if value is None)

            last_seen_populated_at = None
            for row in sorted_rows:
                if self._compute_v2_feature_value(row, feature_name) is not None:
                    last_seen_populated_at = row.event_timestamp
                    break

            if linked_rows and null_count_lifetime == len(linked_rows):
                status = "broken"
            elif recent_rows and null_count_recent == len(recent_rows):
                status = "broken"
            elif null_count_recent > 0 or null_count_lifetime > 0:
                status = "partially_missing"
            else:
                status = "healthy"

            feature_rows.append(
                {
                    "feature_name": feature_name,
                    "null_count_recent": null_count_recent,
                    "null_pct_recent": self._pct(null_count_recent, len(recent_rows)),
                    "null_count_lifetime": null_count_lifetime,
                    "null_pct_lifetime": self._pct(null_count_lifetime, len(linked_rows)),
                    "last_seen_populated_at": last_seen_populated_at,
                    "status": status,
                }
            )

        recent_non_null = sum(
            1
            for row in recent_rows
            for feature_name in V2_FEATURE_FIELDS
            if self._compute_v2_feature_value(row, feature_name) is not None
        )
        lifetime_non_null = sum(
            1
            for row in linked_rows
            for feature_name in V2_FEATURE_FIELDS
            if self._compute_v2_feature_value(row, feature_name) is not None
        )

        recent_total = len(recent_rows) * len(V2_FEATURE_FIELDS)
        lifetime_total = len(linked_rows) * len(V2_FEATURE_FIELDS)
        broken_count = sum(1 for row in feature_rows if row["status"] == "broken")
        partial_count = sum(1 for row in feature_rows if row["status"] == "partially_missing")

        return {
            "recent_window_size": len(recent_rows),
            "recent_window_basis": f"last_{recent_limit}_linked_completed_trades",
            "recent_completeness_pct": self._pct(recent_non_null, recent_total),
            "lifetime_completeness_pct": self._pct(lifetime_non_null, lifetime_total),
            "features": feature_rows,
            "broken_feature_count": broken_count,
            "partially_missing_feature_count": partial_count,
        }

    def get_linkage_health(self) -> dict[str, Any]:
        with self.db.connect() as conn:
            live_scope_filter = "WHERE COALESCE(account_id, '') != 'backfill'"
            post_fix_start = conn.execute(
                f"""
                SELECT MIN(timestamp_utc) AS post_fix_start
                FROM trade_fills
                {live_scope_filter}
                """
            ).fetchone()["post_fix_start"]
            where_clause = live_scope_filter
            params: tuple[Any, ...] = ()

            total_post_fix_fills = conn.execute(
                f"SELECT COUNT(*) AS c FROM trade_fills {where_clause}",
                params,
            ).fetchone()["c"]
            fills_with_non_null_run_id = conn.execute(
                f"SELECT COUNT(*) AS c FROM trade_fills {where_clause + (' AND' if where_clause else ' WHERE') + ' run_id IS NOT NULL'}",
                params,
            ).fetchone()["c"]
            fills_with_non_null_cycle_id = conn.execute(
                f"SELECT COUNT(*) AS c FROM trade_fills {where_clause + (' AND' if where_clause else ' WHERE') + ' cycle_id IS NOT NULL'}",
                params,
            ).fetchone()["c"]
            fills_with_non_null_position_id = conn.execute(
                f"SELECT COUNT(*) AS c FROM trade_fills {where_clause + (' AND' if where_clause else ' WHERE') + ' position_id IS NOT NULL'}",
                params,
            ).fetchone()["c"]

            open_where = f"{where_clause + (' AND' if where_clause else ' WHERE')} action = 'OPEN'"
            close_where = f"{where_clause + (' AND' if where_clause else ' WHERE')} action = 'CLOSE'"

            open_fill_count = conn.execute(
                f"SELECT COUNT(*) AS c FROM trade_fills {open_where}",
                params,
            ).fetchone()["c"]
            close_fill_count = conn.execute(
                f"SELECT COUNT(*) AS c FROM trade_fills {close_where}",
                params,
            ).fetchone()["c"]

            linked_counts = conn.execute(
                f"""
                WITH open_fills AS (
                    SELECT id, run_id, cycle_id, symbol, position_id
                    FROM trade_fills
                    {open_where}
                ),
                close_fills AS (
                    SELECT DISTINCT position_id
                    FROM trade_fills
                    {close_where}
                      AND position_id IS NOT NULL
                )
                SELECT
                    SUM(
                        CASE WHEN EXISTS (
                            SELECT 1
                            FROM decision_traces dt
                            WHERE dt.run_id = of.run_id
                              AND dt.cycle_id = of.cycle_id
                              AND dt.symbol = of.symbol
                        ) THEN 1 ELSE 0 END
                    ) AS opens_with_trace_link,
                    SUM(
                        CASE WHEN EXISTS (
                            SELECT 1
                            FROM close_fills cf
                            WHERE cf.position_id = of.position_id
                        ) THEN 1 ELSE 0 END
                    ) AS opens_with_close_link,
                    SUM(
                        CASE WHEN EXISTS (
                            SELECT 1
                            FROM decision_traces dt
                            WHERE dt.run_id = of.run_id
                              AND dt.cycle_id = of.cycle_id
                              AND dt.symbol = of.symbol
                        ) AND EXISTS (
                            SELECT 1
                            FROM close_fills cf
                            WHERE cf.position_id = of.position_id
                        ) THEN 1 ELSE 0 END
                    ) AS fully_linked_completed_trades
                FROM open_fills of
                """,
                params + params,
            ).fetchone()

            unmatched_close_fills = conn.execute(
                f"""
                SELECT COUNT(*) AS c
                FROM trade_fills tf_close
                {close_where}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM trade_fills tf_open
                      WHERE tf_open.action = 'OPEN'
                        AND tf_open.position_id = tf_close.position_id
                  )
                """,
                params,
            ).fetchone()["c"]

        orphan_open_fills = max(int(open_fill_count or 0) - int(linked_counts["opens_with_close_link"] or 0), 0)
        fully_linked_completed_trades = int(linked_counts["fully_linked_completed_trades"] or 0)
        fully_linked_completed_trades_pct = self._pct(fully_linked_completed_trades, int(open_fill_count or 0))

        run_cov = self._pct(fills_with_non_null_run_id, total_post_fix_fills)
        cycle_cov = self._pct(fills_with_non_null_cycle_id, total_post_fix_fills)
        position_cov = self._pct(fills_with_non_null_position_id, total_post_fix_fills)

        linkage_healthy = (
            total_post_fix_fills > 0
            and run_cov >= MIN_LINKAGE_HEALTH_PCT
            and cycle_cov >= MIN_LINKAGE_HEALTH_PCT
            and position_cov >= MIN_LINKAGE_HEALTH_PCT
            and fully_linked_completed_trades_pct >= MIN_LINKAGE_HEALTH_PCT
            and unmatched_close_fills == 0
        )

        return {
            "post_fix_start": post_fix_start,
            "total_post_fix_fills": int(total_post_fix_fills or 0),
            "fills_with_non_null_run_id": int(fills_with_non_null_run_id or 0),
            "fills_with_non_null_cycle_id": int(fills_with_non_null_cycle_id or 0),
            "fills_with_non_null_position_id": int(fills_with_non_null_position_id or 0),
            "run_id_coverage_pct": run_cov,
            "cycle_id_coverage_pct": cycle_cov,
            "position_id_coverage_pct": position_cov,
            "fully_linked_completed_trades": fully_linked_completed_trades,
            "fully_linked_completed_trades_pct": fully_linked_completed_trades_pct,
            "orphan_open_fills": orphan_open_fills,
            "unmatched_close_fills": int(unmatched_close_fills or 0),
            "linkage_healthy": linkage_healthy,
            "scope": "live_only",
        }

    def get_activity(
        self,
        *,
        days: int = DEFAULT_ACTIVITY_DAYS,
        page: int = 1,
        page_size: int = DEFAULT_ACTIVITY_PAGE_SIZE,
    ) -> dict[str, Any]:
        page = max(page, 1)
        page_size = max(1, min(page_size, 200))
        offset = (page - 1) * page_size
        window_start = f"-{days} days"

        with self.db.connect() as conn:
            summary_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT
                        ts,
                        symbol,
                        regime_state,
                        signal,
                        ml_score,
                        ml_action,
                        ml_model_version,
                        COALESCE(ml_threshold, threshold) AS threshold
                    FROM decision_traces
                    WHERE ml_action IS NOT NULL
                      AND ts >= datetime('now', ?)
                    ORDER BY ts DESC
                    """,
                    (window_start,),
                ).fetchall()
            ]
            total_recent_rows = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM decision_traces
                WHERE ml_action IS NOT NULL
                  AND ts >= datetime('now', ?)
                """,
                (window_start,),
            ).fetchone()["c"]
            recent_rows = [
                dict(row)
                for row in conn.execute(
                    MLAdminQueries.ACTIVITY_RECENT_ROWS_SQL,
                    {
                        "window_start": window_start,
                        "limit": page_size,
                        "offset": offset,
                    },
                ).fetchall()
            ]

        allow_count = 0
        shadow_count = 0
        block_count = 0
        skip_count = 0
        total_ml_scored_entries = 0
        score_sum = 0.0
        score_distribution = {bucket: 0 for bucket in self._score_buckets()}
        per_symbol: dict[str, dict[str, int]] = {}
        per_regime: dict[str, dict[str, int]] = {}
        per_session: dict[str, dict[str, int]] = {}

        for row in summary_rows:
            action = (row["ml_action"] or "SKIP").upper()
            score = row["ml_score"]
            symbol = row["symbol"] or "UNKNOWN"
            regime = row["regime_state"] or "UNKNOWN"
            session = self._session_for_timestamp(row["ts"])

            if action == "ALLOW":
                allow_count += 1
            elif action == "SHADOW":
                shadow_count += 1
            elif action == "BLOCK":
                block_count += 1
            else:
                skip_count += 1

            if score is not None:
                total_ml_scored_entries += 1
                score_sum += float(score)
                score_distribution[self._bucket_for_score(float(score))] += 1

            self._bump_action_counter(per_symbol, symbol, action)
            self._bump_action_counter(per_regime, regime, action)
            self._bump_action_counter(per_session, session, action)

        overview = self.get_overview()
        average_ml_score = round(score_sum / total_ml_scored_entries, 4) if total_ml_scored_entries else None

        return {
            "window_days": days,
            "page": page,
            "page_size": page_size,
            "total_recent_rows": int(total_recent_rows or 0),
            "total_ml_scored_entries": total_ml_scored_entries,
            "allow_count": allow_count,
            "shadow_count": shadow_count,
            "block_count": block_count,
            "skip_count": skip_count,
            "average_ml_score": average_ml_score,
            "current_threshold": overview["current_threshold"],
            "current_hard_floor": overview["current_hard_block_floor"],
            "score_distribution": [
                {"bucket": bucket, "count": count}
                for bucket, count in score_distribution.items()
            ],
            "per_symbol_actions": self._flatten_action_counters(per_symbol),
            "per_regime_actions": self._flatten_action_counters(per_regime),
            "per_session_actions": self._flatten_action_counters(per_session),
            "recent_activity_rows": recent_rows,
        }

    def get_shadow_performance(self, *, days: int = DEFAULT_SHADOW_DAYS) -> dict[str, Any]:
        with self.db.connect() as conn:
            linked_rows = [row for row in self._load_linked_trade_rows(conn) if row.has_trace]

        groups = {
            "ALLOW": self._empty_group_stats(),
            "SHADOW": self._empty_group_stats(),
            "BLOCK": self._empty_group_stats(),
        }
        total = 0
        good_allows = 0
        bad_allows = 0
        good_blocks = 0
        bad_blocks = 0

        cutoff = None
        if days > 0:
            cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)

        for row in linked_rows:
            action = (row["ml_action"] or "").upper()
            if action not in groups:
                continue
            if cutoff is not None and row.event_timestamp:
                ts_value = self._to_epoch(row.event_timestamp)
                if ts_value is not None and ts_value < cutoff:
                    continue

            total += 1
            pnl = row.effective_realized_pnl
            stats = groups[action]
            stats["count"] += 1
            if pnl is None or abs(float(pnl)) < 1e-9:
                stats["breakevens"] += 1
            elif pnl > 0:
                stats["wins"] += 1
            else:
                stats["losses"] += 1
            stats["total_pnl"] += float(pnl or 0.0)

            if action == "ALLOW":
                if pnl is not None and pnl > 0:
                    good_allows += 1
                elif pnl is not None and pnl < 0:
                    bad_allows += 1
            else:
                if pnl is not None and pnl < 0:
                    good_blocks += 1
                elif pnl is not None and pnl > 0:
                    bad_blocks += 1

        for stats in groups.values():
            stats["average_pnl"] = round(stats["total_pnl"] / stats["count"], 4) if stats["count"] else 0.0
            stats["total_pnl"] = round(stats["total_pnl"], 4)

        return {
            "window_days": days,
            "total_linked_completed_trades_with_ml_attribution": total,
            "decision_groups": groups,
            "good_allows": good_allows,
            "bad_allows": bad_allows,
            "good_blocks": good_blocks,
            "bad_blocks": bad_blocks,
            "classification_logic": (
                "good_allows = ALLOW rows with positive realized pnl; "
                "bad_allows = ALLOW rows with negative realized pnl; "
                "good_blocks = SHADOW or BLOCK rows with negative realized pnl; "
                "bad_blocks = SHADOW or BLOCK rows with positive realized pnl."
            ),
        }

    def get_validation_history(self, *, limit: int | None = None) -> dict[str, Any]:
        self._sync_validation_history()
        with self.db.connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT
                        model_version,
                        training_date,
                        dataset_used,
                        train_rows,
                        test_rows,
                        train_auc,
                        test_auc,
                        validation_method,
                        notes,
                        verdict,
                        deployed_mode
                    FROM ml_validation_history
                    ORDER BY training_date DESC, model_version DESC
                    """
                ).fetchall()
            ]
        items = rows
        if limit is not None:
            items = items[:limit]

        return {
            "items": items,
            "source_note": (
                "Validation history is synchronized from bot-backend artifact metadata/validation files "
                "into ml_validation_history for retained admin visibility."
            ),
        }

    def get_dataset_builder_status(self) -> dict[str, Any]:
        feature_completeness = self.get_feature_completeness()
        gate = self.get_training_gate()
        latest_dataset_meta = self._latest_dataset_metadata()
        latest_dataset_path = None
        last_dataset_build_time = None
        dataset_source_date_range = None
        linked_trade_count = gate["total_linked_completed_trades"]
        fully_usable_rows = self._compute_fully_usable_rows()
        dropped_rows = max(int(linked_trade_count or 0) - fully_usable_rows, 0)
        dropped_row_reasons = [
            {"reason": feature["feature_name"], "count": feature["null_count_lifetime"]}
            for feature in feature_completeness["features"]
            if feature["null_count_lifetime"] > 0
        ]
        feature_null_counts: dict[str, Any] | None = None
        label_null_counts: dict[str, Any] | None = None
        class_balance: dict[str, Any] | None = None
        schema_compatibility = {
            **build_contract_metadata(),
            "compatible": False,
            "schema_hash_matches": False,
            "feature_columns_match": False,
        }
        source_note = (
            "Dataset status is derived from linked-trade heuristics because no persisted builder ledger was found yet."
        )

        if latest_dataset_meta:
            meta = latest_dataset_meta["meta"]
            dataset_source_date_range = meta.get("date_range")
            last_dataset_build_time = meta.get("dataset_build_timestamp")
            latest_dataset_path = latest_dataset_meta["dataset_path"]
            linked_trade_count = int(meta.get("row_count") or linked_trade_count or 0)
            fully_usable_rows = int(meta.get("usable_row_count") or linked_trade_count or 0)
            dropped_rows = int(meta.get("dropped_row_count") or 0)
            dropped_row_reasons = [
                {"reason": key, "count": int(value or 0)}
                for key, value in (meta.get("drop_reasons") or {}).items()
            ]
            dropped_row_reasons.sort(key=lambda item: item["count"], reverse=True)
            feature_null_counts = meta.get("feature_null_counts")
            label_null_counts = meta.get("label_null_counts")
            class_balance = meta.get("class_balance")
            schema_compatibility = get_contract_status(
                feature_columns=meta.get("feature_columns"),
                schema_hash=meta.get("schema_hash"),
            )
            source_note = "Dataset status is sourced from persisted training dataset metadata."

        if feature_completeness["broken_feature_count"] > 0:
            feature_status = "broken"
        elif feature_completeness["partially_missing_feature_count"] > 0:
            feature_status = "partially_missing"
        else:
            feature_status = "healthy"

        return {
            "dataset_source_date_range": dataset_source_date_range,
            "linked_trade_count": int(linked_trade_count or 0),
            "fully_usable_rows": fully_usable_rows,
            "dropped_rows": dropped_rows,
            "dropped_row_reasons": dropped_row_reasons,
            "feature_completeness_status": feature_status,
            "label_distribution": {
                "wins": int(self._class_balance_stats(class_balance or {}, "label_win")["positive"] if class_balance else gate["wins"]),
                "losses": int(self._class_balance_stats(class_balance or {}, "label_win")["negative"] if class_balance else gate["losses"]),
                "breakevens": gate["breakeven_trades"],
                "single_class": bool(self._class_balance_stats(class_balance or {}, "label_win")["single_class"] if class_balance else gate["label_distribution_single_class"]),
            },
            "last_dataset_build_time": last_dataset_build_time,
            "last_dataset_path": str(latest_dataset_path) if latest_dataset_path else None,
            "rebuild_dataset_allowed": bool(int(linked_trade_count or 0) > 0 and not self._has_inflight_action()),
            "source_note": source_note,
            "contract_version": schema_compatibility["contract_version"],
            "schema_hash": schema_compatibility["schema_hash"],
            "schema_compatible": schema_compatibility["compatible"],
            "feature_null_counts": feature_null_counts,
            "label_null_counts": label_null_counts,
            "class_balance": class_balance,
        }

    def get_alerts(self) -> dict[str, Any]:
        overview = self.get_overview()
        gate = self.get_training_gate()
        feature = self.get_feature_completeness(recent_limit=200)
        linkage = self.get_linkage_health()
        activity = self.get_activity(days=ALERT_ACTIVITY_DAYS, page=1, page_size=20)
        validation = self.get_validation_history(limit=3)
        drift = self.get_drift_monitoring(days=DEFAULT_DRIFT_WINDOW_DAYS)
        dataset_status = self.get_dataset_builder_status()

        alerts: list[dict[str, Any]] = []

        if feature["broken_feature_count"] > 0:
            alerts.append(
                {
                    "code": "feature_broken",
                    "level": "danger",
                    "title": "Critical ML features are broken",
                    "body": (
                        f"{feature['broken_feature_count']} tracked v2 features are entirely missing in the recent window. "
                        "Training and promotion actions must stay blocked until feature capture recovers."
                    ),
                }
            )
        elif feature["recent_completeness_pct"] + 5 < feature["lifetime_completeness_pct"]:
            alerts.append(
                {
                    "code": "feature_null_rate_spike",
                    "level": "warning",
                    "title": "Recent feature completeness has degraded",
                    "body": (
                        f"Recent completeness is {feature['recent_completeness_pct']}% versus "
                        f"{feature['lifetime_completeness_pct']}% lifetime. This suggests a fresh null-rate spike."
                    ),
                }
            )

        if not linkage["linkage_healthy"]:
            run_cov = self._pct(linkage["fills_with_non_null_run_id"], linkage["total_post_fix_fills"])
            cycle_cov = self._pct(linkage["fills_with_non_null_cycle_id"], linkage["total_post_fix_fills"])
            position_cov = self._pct(linkage["fills_with_non_null_position_id"], linkage["total_post_fix_fills"])
            alerts.append(
                {
                    "code": "linkage_unhealthy",
                    "level": "danger",
                    "title": "Trade linkage health is below the training threshold",
                    "body": (
                        f"run_id coverage={run_cov}%, cycle_id coverage={cycle_cov}%, "
                        f"position_id coverage={position_cov}%, linked completed trades={linkage['fully_linked_completed_trades_pct']}%."
                    ),
                }
            )

        if gate["training_ready"]:
            alerts.append(
                {
                    "code": "training_ready",
                    "level": "success",
                    "title": "Training gate is satisfied",
                    "body": "The current linked dataset satisfies the minimum readiness requirements for a guarded training run.",
                }
            )
        elif gate["status"] == "blocked":
            alerts.append(
                {
                    "code": "training_blocked",
                    "level": "warning",
                    "title": "Training is blocked",
                    "body": "At least one gating condition is failing. Review feature coverage, linkage, and label health before retraining.",
                }
            )

        if gate["wins"] >= REQUIRED_WINS:
            alerts.append(
                {
                    "code": "wins_threshold_reached",
                    "level": "info",
                    "title": "Win threshold has been reached",
                    "body": f"{gate['wins']} linked winning trades are available, which meets the {REQUIRED_WINS} win requirement.",
                }
            )

        if gate["label_distribution_single_class"]:
            alerts.append(
                {
                    "code": "single_class_labels",
                    "level": "danger",
                    "title": "Label distribution is single-class",
                    "body": "The completed linked trades do not currently include both wins and losses, so training would overfit immediately.",
                }
            )

        if not gate.get("dataset_schema_compatible", dataset_status.get("schema_compatible", False)):
            alerts.append(
                {
                    "code": "dataset_contract_mismatch",
                    "level": "danger",
                    "title": "Latest built dataset is not contract-compatible",
                    "body": "The latest dataset metadata does not match the canonical feature contract used by trainer and scorer.",
                }
            )

        if activity["total_ml_scored_entries"] == 0:
            alerts.append(
                {
                    "code": "ml_scoring_stopped",
                    "level": "warning",
                    "title": "No recent ML scoring activity was found",
                    "body": f"No ML-scored decision traces were recorded in the last {ALERT_ACTIVITY_DAYS} days.",
                }
            )

        missing_artifacts = [
            label
            for label, value in (
                ("model artifact", overview["model_artifact_path"]),
                ("encoder", overview["encoder_path"]),
                ("metadata", overview["metadata_path"]),
            )
            if not value
        ]
        if missing_artifacts:
            alerts.append(
                {
                    "code": "missing_model_artifacts",
                    "level": "warning",
                    "title": "Model artifact references are incomplete",
                    "body": f"Missing references: {', '.join(missing_artifacts)}.",
                }
            )
        elif overview.get("runtime_load_error"):
            alerts.append(
                {
                    "code": "runtime_model_load_error",
                    "level": "danger",
                    "title": "Runtime scorer could not load the configured artifact",
                    "body": str(overview["runtime_load_error"]),
                }
            )

        latest_validation = validation["items"][0] if validation["items"] else None
        if latest_validation and latest_validation["verdict"] in {"overfit", "rejected"}:
            alerts.append(
                {
                    "code": "validation_overfit",
                    "level": "danger",
                    "title": "Latest validation verdict is not promotion-safe",
                    "body": (
                        f"{latest_validation['model_version']} is marked {latest_validation['verdict']}. "
                        "Keep deployment actions blocked until a healthier candidate exists."
                    ),
                }
            )

        if drift["live_win_rate"] is not None and drift["historical_win_rate"] is not None and drift["win_rate_delta"] <= -5:
            alerts.append(
                {
                    "code": "live_underperformance",
                    "level": "warning",
                    "title": "Recent live win rate is under the historical baseline",
                    "body": (
                        f"Recent live win rate is {drift['live_win_rate']}% versus "
                        f"{drift['historical_win_rate']}% historical, a delta of {drift['win_rate_delta']}%."
                    ),
                }
            )

        alerts.sort(
            key=lambda item: {"danger": 0, "warning": 1, "info": 2, "success": 3}.get(item["level"], 99)
        )
        return {
            "generated_at": self._utc_now_iso(),
            "items": alerts,
        }

    def get_control_panel(self) -> dict[str, Any]:
        overview = self.get_overview()
        gate = self.get_training_gate()
        dataset_status = self.get_dataset_builder_status()
        validation = self.get_validation_history(limit=5)
        self._refresh_stale_action_runs()

        latest_training = self._latest_action_run("run_training")
        latest_dataset = self._latest_action_run("rebuild_dataset")
        latest_validation = self._latest_action_run("run_validation")
        actions = self._build_action_definitions(
            overview=overview,
            gate=gate,
            dataset_status=dataset_status,
            validation=validation,
        )
        training_action = next((action for action in actions if action["action_key"] == "run_training"), None)

        return {
            "readiness_status": gate["status"],
            "training_allowed_right_now": bool(training_action and training_action["allowed"]),
            "current_dataset_path": dataset_status["last_dataset_path"],
            "target_output_model_version": self._next_model_version(overview["current_model_version"]),
            "last_training_run_status": latest_training["status"] if latest_training else None,
            "last_training_run_logs": self._read_log_tail(latest_training["log_path"]) if latest_training else [],
            "last_dataset_rebuild_status": latest_dataset["status"] if latest_dataset else None,
            "last_validation_run_status": latest_validation["status"] if latest_validation else None,
            "actions": actions,
            "recent_action_runs": self._list_action_runs(limit=8),
        }

    def get_drift_monitoring(self, *, days: int = DEFAULT_DRIFT_WINDOW_DAYS) -> dict[str, Any]:
        with self.db.connect() as conn:
            linked_rows = [row for row in self._load_linked_trade_rows(conn) if row.has_trace]
            score_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT ts, symbol, regime_state, ml_score
                    FROM decision_traces
                    WHERE ml_score IS NOT NULL
                    ORDER BY ts DESC
                    """
                ).fetchall()
            ]

        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
        recent_rows: list[LinkedTradeRow] = []
        for row in linked_rows:
            ts_value = self._to_epoch(row.event_timestamp)
            if ts_value is None or ts_value >= cutoff:
                recent_rows.append(row)

        recent_non_flat = [row for row in recent_rows if row.effective_realized_pnl is not None and abs(float(row.effective_realized_pnl or 0.0)) > 1e-9]
        historical_non_flat = [
            row for row in linked_rows if row.effective_realized_pnl is not None and abs(float(row.effective_realized_pnl or 0.0)) > 1e-9
        ]

        recent_wins = sum(1 for row in recent_non_flat if float(row.effective_realized_pnl or 0.0) > 0)
        historical_wins = sum(1 for row in historical_non_flat if float(row.effective_realized_pnl or 0.0) > 0)
        live_win_rate = self._pct(recent_wins, len(recent_non_flat)) if recent_non_flat else None
        historical_win_rate = self._pct(historical_wins, len(historical_non_flat)) if historical_non_flat else None

        recent_score_rows = [row for row in score_rows if self._to_epoch(row.get("ts")) is None or self._to_epoch(row.get("ts")) >= cutoff]
        training_score_distribution = self._historical_score_distribution(score_rows)

        symbol_stats = self._distribution_with_pnl(recent_rows, "symbol")
        regime_stats = self._distribution_with_pnl(recent_rows, "regime_state")
        session_stats = self._session_distribution(recent_rows)
        score_band_stats = self._score_band_pnl(recent_rows)

        return {
            "window_days": days,
            "live_win_rate": live_win_rate,
            "historical_win_rate": historical_win_rate,
            "win_rate_delta": (
                round((live_win_rate or 0.0) - (historical_win_rate or 0.0), 2)
                if live_win_rate is not None and historical_win_rate is not None
                else None
            ),
            "live_score_distribution": self._historical_score_distribution(recent_score_rows),
            "training_score_distribution": training_score_distribution,
            "symbol_distribution": symbol_stats,
            "regime_distribution": regime_stats,
            "session_distribution": session_stats,
            "average_pnl_by_regime": [
                {"key": item["key"], "average_pnl": item["average_pnl"], "count": item["count"]}
                for item in regime_stats
            ],
            "average_pnl_by_symbol": [
                {"key": item["key"], "average_pnl": item["average_pnl"], "count": item["count"]}
                for item in symbol_stats
            ],
            "average_pnl_by_score_band": score_band_stats,
            "source_note": (
                "Training score distribution is approximated from retained historical ML scoring activity, "
                "because per-sample training scores are not persisted in model artifacts today."
            ),
        }

    def trigger_action(
        self,
        *,
        action_key: str,
        admin: dict[str, Any],
        confirmation_phrase: str,
        note: str | None = None,
        scheduler: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        if action_key not in ACTION_CONFIRM_PHRASES:
            raise ValueError(f"Unsupported ML admin action: {action_key}")
        if not self._can_manage_ml(admin):
            raise PermissionError("Admin does not have permission to run ML actions.")
        if confirmation_phrase.strip().upper() != ACTION_CONFIRM_PHRASES[action_key]:
            raise ValueError("Confirmation phrase did not match the required safety phrase.")

        overview = self.get_overview()
        gate = self.get_training_gate()
        dataset_status = self.get_dataset_builder_status()
        validation = self.get_validation_history(limit=5)
        action_def = next(
            action for action in self._build_action_definitions(
                overview=overview,
                gate=gate,
                dataset_status=dataset_status,
                validation=validation,
            )
            if action["action_key"] == action_key
        )

        now = self._utc_now_iso()
        action_id = str(uuid.uuid4())
        status = "queued" if action_def["allowed"] and action_def["supported"] else ("unsupported" if not action_def["supported"] else "blocked")
        row = {
            "id": action_id,
            "action_key": action_key,
            "requested_by_admin_id": admin.get("id"),
            "requested_by_email": admin.get("email"),
            "confirmation_phrase": confirmation_phrase.strip().upper(),
            "note": note,
            "status": status,
            "reason": action_def.get("blocked_reason"),
            "supported": 1 if action_def["supported"] else 0,
            "readiness_snapshot_json": json.dumps(
                {
                    "overview": overview,
                    "training_gate": gate,
                    "dataset_builder_status": dataset_status,
                }
            ),
            "request_payload_json": json.dumps({"note": note}),
            "dataset_path": action_def.get("dataset_path"),
            "target_model_version": action_def.get("target_model_version"),
            "log_path": action_def.get("log_path"),
            "command_json": json.dumps(action_def.get("command") or []),
            "started_at": None,
            "finished_at": now if status in {"blocked", "unsupported"} else None,
            "created_at": now,
            "updated_at": now,
            "result_json": json.dumps({"supported": action_def["supported"], "allowed": action_def["allowed"]}),
        }

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO ml_action_runs (
                    id, action_key, requested_by_admin_id, requested_by_email, confirmation_phrase,
                    note, status, reason, supported, readiness_snapshot_json, request_payload_json,
                    dataset_path, target_model_version, log_path, command_json,
                    started_at, finished_at, created_at, updated_at, result_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["action_key"],
                    row["requested_by_admin_id"],
                    row["requested_by_email"],
                    row["confirmation_phrase"],
                    row["note"],
                    row["status"],
                    row["reason"],
                    row["supported"],
                    row["readiness_snapshot_json"],
                    row["request_payload_json"],
                    row["dataset_path"],
                    row["target_model_version"],
                    row["log_path"],
                    row["command_json"],
                    row["started_at"],
                    row["finished_at"],
                    row["created_at"],
                    row["updated_at"],
                    row["result_json"],
                ),
            )

        self._audit_action(admin=admin, action_key=action_key, status=status, note=note, reason=row["reason"])
        if status == "queued":
            if scheduler is not None:
                scheduler(self._execute_action_run, action_id)
            else:
                self._execute_action_run(action_id)

        return self._serialize_action_run(row)

    def _execute_action_run(self, action_id: str) -> None:
        row = self._get_action_run(action_id)
        if not row:
            return

        command = json.loads(row["command_json"] or "[]")
        log_path = Path(row["log_path"]) if row.get("log_path") else None
        started_at = self._utc_now_iso()
        self._update_action_run(
            action_id,
            status="running",
            started_at=started_at,
            updated_at=started_at,
            reason=None,
        )

        if log_path:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(
                f"[{started_at}] starting {row['action_key']}\n",
                encoding="utf-8",
            )

        try:
            completed = self.command_runner(
                command,
                cwd=str(self.bot_backend_root),
                capture_output=True,
                text=True,
                check=False,
            )
            output = self._combine_process_output(completed.stdout, completed.stderr)
            if log_path:
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(output)
                    if output and not output.endswith("\n"):
                        handle.write("\n")
                    handle.write(f"[{self._utc_now_iso()}] exit_code={completed.returncode}\n")

            finished_at = self._utc_now_iso()
            status = "succeeded" if int(completed.returncode) == 0 else "failed"
            reason = None if status == "succeeded" else f"Process exited with code {completed.returncode}"
            result = {
                "returncode": int(completed.returncode),
                "stdout_tail": self._tail_text(completed.stdout or ""),
                "stderr_tail": self._tail_text(completed.stderr or ""),
            }
            self._update_action_run(
                action_id,
                status=status,
                finished_at=finished_at,
                updated_at=finished_at,
                reason=reason,
                result_json=json.dumps(result),
            )
            if status == "succeeded" and row["action_key"] == "run_training":
                self._sync_validation_history()
            if status == "succeeded" and row["action_key"] in {"rebuild_dataset", "run_validation"}:
                self._sync_validation_history()
        except Exception as exc:
            finished_at = self._utc_now_iso()
            if log_path:
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(traceback.format_exc())
            self._update_action_run(
                action_id,
                status="failed",
                finished_at=finished_at,
                updated_at=finished_at,
                reason=str(exc),
                result_json=json.dumps({"exception": traceback.format_exc()}),
            )

    def get_dashboard(self) -> dict[str, Any]:
        overview = self.get_overview()
        training_gate = self.get_training_gate()
        feature = self.get_feature_completeness(recent_limit=200)
        linkage = self.get_linkage_health()
        activity = self.get_activity(days=DEFAULT_ACTIVITY_DAYS, page=1, page_size=10)
        shadow = self.get_shadow_performance(days=DEFAULT_SHADOW_DAYS)
        validation = self.get_validation_history(limit=3)
        dataset_status = self.get_dataset_builder_status()
        alerts = self.get_alerts()
        control_panel = self.get_control_panel()
        drift = self.get_drift_monitoring(days=DEFAULT_DRIFT_WINDOW_DAYS)

        return {
            "overview": overview,
            "training_gate": training_gate,
            "feature_completeness": {
                "recent_completeness_pct": feature["recent_completeness_pct"],
                "lifetime_completeness_pct": feature["lifetime_completeness_pct"],
                "broken_feature_count": feature["broken_feature_count"],
                "partially_missing_feature_count": feature["partially_missing_feature_count"],
            },
            "linkage_health": linkage,
            "activity_summary": {
                "window_days": activity["window_days"],
                "total_ml_scored_entries": activity["total_ml_scored_entries"],
                "allow_count": activity["allow_count"],
                "shadow_count": activity["shadow_count"],
                "block_count": activity["block_count"],
                "skip_count": activity["skip_count"],
                "average_ml_score": activity["average_ml_score"],
                "current_threshold": activity["current_threshold"],
                "current_hard_floor": activity["current_hard_floor"],
                "recent_activity_rows": activity["recent_activity_rows"],
            },
            "shadow_performance": {
                "window_days": shadow["window_days"],
                "total_linked_completed_trades_with_ml_attribution": shadow["total_linked_completed_trades_with_ml_attribution"],
                "decision_groups": shadow["decision_groups"],
                "good_allows": shadow["good_allows"],
                "bad_allows": shadow["bad_allows"],
                "good_blocks": shadow["good_blocks"],
                "bad_blocks": shadow["bad_blocks"],
            },
            "validation_history": {
                "total_models": len(validation["items"]),
                "latest_model": validation["items"][0] if validation["items"] else None,
                "source_note": validation["source_note"],
            },
            "dataset_builder_status": dataset_status,
            "alerts": alerts,
            "control_panel": control_panel,
            "drift_monitoring": {
                "window_days": drift["window_days"],
                "live_win_rate": drift["live_win_rate"],
                "historical_win_rate": drift["historical_win_rate"],
                "win_rate_delta": drift["win_rate_delta"],
                "symbol_distribution": drift["symbol_distribution"],
                "regime_distribution": drift["regime_distribution"],
                "session_distribution": drift["session_distribution"],
                "average_pnl_by_score_band": drift["average_pnl_by_score_band"],
                "source_note": drift["source_note"],
            },
        }

    def _load_linked_trade_rows(self, conn) -> list[LinkedTradeRow]:
        return [LinkedTradeRow(dict(row)) for row in conn.execute(MLAdminQueries.LINKED_COMPLETED_ROWS_SQL).fetchall()]

    def _summarize_training_gate(self, linked_rows: list[LinkedTradeRow], *, linkage_healthy: bool) -> dict[str, Any]:
        total_linked_completed_trades = len(linked_rows)
        wins = 0
        losses = 0
        breakeven_trades = 0
        full_feature_coverage = 0
        missing_critical_features = 0
        critical_feature_non_null_counts = {field: 0 for field in CRITICAL_FEATURE_FIELDS}

        for row in linked_rows:
            pnl = row.effective_realized_pnl
            if pnl is None or abs(float(pnl)) < 1e-9:
                breakeven_trades += 1
            elif pnl > 0:
                wins += 1
            else:
                losses += 1

            if row.has_full_feature_coverage:
                full_feature_coverage += 1
            else:
                missing_critical_features += 1

            for field in CRITICAL_FEATURE_FIELDS:
                value = row[field]
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                critical_feature_non_null_counts[field] += 1

        feature_coverage_pct = self._pct(full_feature_coverage, total_linked_completed_trades)
        current_win_rate = self._pct(wins, wins + losses)
        label_distribution_single_class = wins == 0 or losses == 0
        critical_feature_broken = any(
            total_linked_completed_trades > 0 and count == 0
            for count in critical_feature_non_null_counts.values()
        )
        training_ready = (
            total_linked_completed_trades >= REQUIRED_TRADES
            and wins >= REQUIRED_WINS
            and feature_coverage_pct >= MIN_FEATURE_COVERAGE_PCT
            and linkage_healthy
            and not critical_feature_broken
            and not label_distribution_single_class
        )

        status = self._gate_status(
            total_linked_completed_trades=total_linked_completed_trades,
            wins=wins,
            linkage_healthy=linkage_healthy,
            critical_feature_broken=critical_feature_broken,
            label_distribution_single_class=label_distribution_single_class,
            feature_coverage_pct=feature_coverage_pct,
            training_ready=training_ready,
        )

        return {
            "total_linked_completed_trades": total_linked_completed_trades,
            "required_trades": REQUIRED_TRADES,
            "wins": wins,
            "required_wins": REQUIRED_WINS,
            "losses": losses,
            "breakeven_trades": breakeven_trades,
            "trades_with_full_feature_coverage": full_feature_coverage,
            "trades_missing_critical_features": missing_critical_features,
            "current_win_rate": current_win_rate,
            "feature_coverage_pct": feature_coverage_pct,
            "linkage_healthy": linkage_healthy,
            "label_distribution_single_class": label_distribution_single_class,
            "training_ready": training_ready,
            "status": status,
        }

    def _training_gate_from_dataset_meta(
        self,
        metadata: dict[str, Any],
        *,
        fallback_summary: dict[str, Any],
        linkage_healthy: bool,
    ) -> dict[str, Any]:
        row_count = int(metadata.get("row_count") or fallback_summary["total_linked_completed_trades"] or 0)
        usable_row_count = int(metadata.get("usable_row_count") or row_count or 0)
        class_balance = metadata.get("class_balance") or {}
        win_label_balance = self._class_balance_stats(class_balance, "label_win")
        wins = int(win_label_balance["positive"])
        losses = int(win_label_balance["negative"])
        breakevens = int(
            metadata.get("label_null_counts", {}).get("label_realized_pnl", fallback_summary["breakeven_trades"])
            or 0
        )
        feature_coverage_pct = self._pct(usable_row_count, row_count) if row_count else 0.0
        label_distribution_single_class = bool(win_label_balance["single_class"])
        schema_compatibility = get_contract_status(
            feature_columns=metadata.get("feature_columns"),
            schema_hash=metadata.get("schema_hash"),
        )
        critical_feature_broken = bool(
            metadata.get("feature_null_counts")
            and any(
                int((metadata.get("feature_null_counts") or {}).get(feature, 0)) >= row_count > 0
                for feature in V2_FEATURE_FIELDS
            )
        )
        training_ready = (
            row_count >= REQUIRED_TRADES
            and wins >= REQUIRED_WINS
            and feature_coverage_pct >= MIN_FEATURE_COVERAGE_PCT
            and linkage_healthy
            and not critical_feature_broken
            and not label_distribution_single_class
            and schema_compatibility["compatible"]
        )
        status = self._gate_status(
            total_linked_completed_trades=row_count,
            wins=wins,
            linkage_healthy=linkage_healthy,
            critical_feature_broken=critical_feature_broken or (not schema_compatibility["compatible"]),
            label_distribution_single_class=label_distribution_single_class,
            feature_coverage_pct=feature_coverage_pct,
            training_ready=training_ready,
        )
        return {
            "total_linked_completed_trades": row_count,
            "wins": wins,
            "losses": losses,
            "breakeven_trades": breakevens,
            "trades_with_full_feature_coverage": usable_row_count,
            "trades_missing_critical_features": max(row_count - usable_row_count, 0),
            "current_win_rate": self._pct(wins, wins + losses),
            "feature_coverage_pct": feature_coverage_pct,
            "label_distribution_single_class": label_distribution_single_class,
            "training_ready": training_ready,
            "status": status,
            "dataset_schema_compatible": schema_compatibility["compatible"],
            "dataset_contract_version": metadata.get("contract_version"),
            "dataset_schema_hash": metadata.get("schema_hash"),
            "dataset_path": metadata.get("training_dataset_path"),
            "blocking_reasons": self._dataset_blocking_reasons(
                row_count=row_count,
                wins=wins,
                feature_coverage_pct=feature_coverage_pct,
                linkage_healthy=linkage_healthy,
                critical_feature_broken=critical_feature_broken,
                label_distribution_single_class=label_distribution_single_class,
                schema_compatible=schema_compatibility["compatible"],
            ),
        }

    @staticmethod
    def _dataset_blocking_reasons(
        *,
        row_count: int,
        wins: int,
        feature_coverage_pct: float,
        linkage_healthy: bool,
        critical_feature_broken: bool,
        label_distribution_single_class: bool,
        schema_compatible: bool,
    ) -> list[str]:
        reasons: list[str] = []
        if row_count < REQUIRED_TRADES:
            reasons.append("insufficient_built_dataset_rows")
        if wins < REQUIRED_WINS:
            reasons.append("insufficient_built_dataset_wins")
        if feature_coverage_pct < MIN_FEATURE_COVERAGE_PCT:
            reasons.append("built_dataset_feature_coverage_below_threshold")
        if not linkage_healthy:
            reasons.append("live_linkage_health_below_threshold")
        if critical_feature_broken:
            reasons.append("critical_features_broken_in_built_dataset")
        if label_distribution_single_class:
            reasons.append("built_dataset_single_class_labels")
        if not schema_compatible:
            reasons.append("dataset_contract_mismatch")
        return reasons

    def _compute_v2_feature_value(self, row: LinkedTradeRow, feature_name: str) -> Any:
        try:
            return compute_feature_value(row.data, feature_name)
        except Exception:
            return None

    def _compute_fully_usable_rows(self) -> int:
        with self.db.connect() as conn:
            linked_rows = [row for row in self._load_linked_trade_rows(conn) if row.has_trace]
        usable = 0
        for row in linked_rows:
            if all(self._compute_v2_feature_value(row, feature) is not None for feature in V2_FEATURE_FIELDS):
                usable += 1
        return usable

    def _sync_validation_history(self) -> None:
        overview = self.get_overview()
        current_model_version = overview["current_model_version"]
        now = self._utc_now_iso()

        with self.db.connect() as conn:
            for meta_file in sorted(self.artifact_dir.glob("*_meta.json")):
                metadata = self._read_json(meta_file)
                if not metadata:
                    continue

                model_version = meta_file.stem.removesuffix("_meta")
                validation_file = meta_file.with_name(meta_file.name.replace("_meta.json", "_validation.json"))
                validation_data = self._read_json(validation_file)
                train_rows, test_rows = self._train_test_rows(metadata)
                test_auc = (
                    self._safe_nested(metadata, "walk_forward_metrics", "auc", "mean")
                    or self._safe_nested(validation_data or {}, "lgbm_binary_aggregate", "auc", "mean")
                )
                train_auc = (
                    metadata.get("train_auc")
                    or self._safe_nested(validation_data or {}, "lgbm_binary_folds", 0, "train_metrics", "auc")
                )
                verdict = self._validation_verdict(metadata=metadata, validation_data=validation_data, model_version=model_version, current_model_version=current_model_version, ml_mode=overview["ml_mode"])
                deployed_mode = self._validation_deployed_mode(
                    metadata=metadata,
                    model_version=model_version,
                    current_model_version=current_model_version,
                    ml_mode=overview["ml_mode"],
                )
                notes = metadata.get("champion_reason") or self._validation_notes(validation_data)

                conn.execute(
                    """
                    INSERT INTO ml_validation_history (
                        model_version,
                        training_date,
                        dataset_used,
                        train_rows,
                        test_rows,
                        train_auc,
                        test_auc,
                        validation_method,
                        notes,
                        verdict,
                        deployed_mode,
                        metadata_path,
                        validation_path,
                        source_payload_json,
                        synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(model_version) DO UPDATE SET
                        training_date=excluded.training_date,
                        dataset_used=excluded.dataset_used,
                        train_rows=excluded.train_rows,
                        test_rows=excluded.test_rows,
                        train_auc=excluded.train_auc,
                        test_auc=excluded.test_auc,
                        validation_method=excluded.validation_method,
                        notes=excluded.notes,
                        verdict=excluded.verdict,
                        deployed_mode=excluded.deployed_mode,
                        metadata_path=excluded.metadata_path,
                        validation_path=excluded.validation_path,
                        source_payload_json=excluded.source_payload_json,
                        synced_at=excluded.synced_at
                    """,
                    (
                        model_version,
                        metadata.get("training_date"),
                        metadata.get("dataset_path"),
                        train_rows,
                        test_rows,
                        train_auc,
                        test_auc,
                        self._validation_method(metadata),
                        notes,
                        verdict,
                        deployed_mode,
                        str(meta_file),
                        str(validation_file) if validation_file.exists() else None,
                        json.dumps({"metadata": metadata, "validation": validation_data}, default=str),
                        now,
                    ),
                )

    def _build_action_definitions(
        self,
        *,
        overview: dict[str, Any],
        gate: dict[str, Any],
        dataset_status: dict[str, Any],
        validation: dict[str, Any],
    ) -> list[dict[str, Any]]:
        inflight = self._has_inflight_action()
        current_model = overview["current_model_version"]
        latest_validation = validation["items"][0] if validation["items"] else None
        dataset_path = dataset_status["last_dataset_path"]
        next_model_version = self._next_model_version(current_model)

        definitions: list[dict[str, Any]] = []

        def add_action(
            *,
            action_key: str,
            label: str,
            supported: bool,
            allowed: bool,
            blocked_reason: str | None,
            dangerous: bool,
            dataset_path_value: str | None = None,
            target_model_version: str | None = None,
            command: list[str] | None = None,
        ) -> None:
            definitions.append(
                {
                    "action_key": action_key,
                    "label": label,
                    "supported": supported,
                    "allowed": allowed,
                    "blocked_reason": blocked_reason,
                    "dangerous": dangerous,
                    "requires_confirmation": True,
                    "confirmation_phrase": ACTION_CONFIRM_PHRASES[action_key],
                    "dataset_path": dataset_path_value,
                    "target_model_version": target_model_version,
                    "log_path": str(self._action_log_path(action_key)),
                    "command": command,
                }
            )

        rebuild_allowed = bool(dataset_status["linked_trade_count"] > 0 and not inflight)
        rebuild_reason = None
        if dataset_status["linked_trade_count"] <= 0:
            rebuild_reason = "No linked completed trades are available for dataset building."
        elif inflight:
            rebuild_reason = "Another ML admin action is already running."
        add_action(
            action_key="rebuild_dataset",
            label="Rebuild Dataset",
            supported=True,
            allowed=rebuild_allowed,
            blocked_reason=rebuild_reason,
            dangerous=False,
            dataset_path_value=dataset_path,
            command=self._command_for_action(
                "rebuild_dataset",
                dataset_path=dataset_path,
                target_model_version=next_model_version,
                linked_trade_count=dataset_status["linked_trade_count"],
                threshold=overview["current_threshold"],
            ),
        )

        training_allowed = bool(
            gate["training_ready"]
            and dataset_path
            and dataset_status.get("schema_compatible", False)
            and not inflight
        )
        if not gate["training_ready"]:
            training_reason = "Training readiness gate is not satisfied."
        elif not dataset_path:
            training_reason = "No rebuilt dataset artifact is available."
        elif not dataset_status.get("schema_compatible", False):
            training_reason = "Latest built dataset does not match the canonical ML contract."
        elif inflight:
            training_reason = "Another ML admin action is already running."
        else:
            training_reason = None
        add_action(
            action_key="run_training",
            label="Run Training",
            supported=True,
            allowed=training_allowed,
            blocked_reason=training_reason,
            dangerous=True,
            dataset_path_value=dataset_path,
            target_model_version=next_model_version,
            command=self._command_for_action(
                "run_training",
                dataset_path=dataset_path,
                target_model_version=next_model_version,
                linked_trade_count=dataset_status["linked_trade_count"],
                threshold=overview["current_threshold"],
            ),
        )

        validation_allowed = bool(current_model and not inflight)
        validation_reason = None if validation_allowed else ("No current model version is configured." if not current_model else "Another ML admin action is already running.")
        add_action(
            action_key="run_validation",
            label="Run Validation",
            supported=True,
            allowed=validation_allowed,
            blocked_reason=validation_reason,
            dangerous=False,
            dataset_path_value=dataset_path,
            target_model_version=current_model,
            command=self._command_for_action(
                "run_validation",
                dataset_path=dataset_path,
                target_model_version=current_model,
                linked_trade_count=dataset_status["linked_trade_count"],
                threshold=overview["current_threshold"],
            ),
        )

        unsupported_reason = "This action is intentionally blocked because safe runtime deployment orchestration is not implemented in the admin backend yet."
        for action_key, label in (
            ("deploy_shadow", "Deploy to Shadow"),
            ("promote_live", "Promote to Live"),
            ("rollback_shadow", "Roll Back to Shadow"),
            ("disable_ml", "Disable ML"),
        ):
            reason = unsupported_reason
            if action_key == "promote_live" and latest_validation and latest_validation["verdict"] not in {"accepted", "shadow_only"}:
                reason = f"Latest validation verdict is {latest_validation['verdict']}; promotion must remain blocked."
            add_action(
                action_key=action_key,
                label=label,
                supported=False,
                allowed=False,
                blocked_reason=reason,
                dangerous=True,
                dataset_path_value=dataset_path,
                target_model_version=current_model,
                command=None,
            )

        return definitions

    def _command_for_action(
        self,
        action_key: str,
        *,
        dataset_path: str | None,
        target_model_version: str | None,
        linked_trade_count: int,
        threshold: float | None,
    ) -> list[str] | None:
        python_exe = sys.executable or "python"
        if action_key == "rebuild_dataset":
            min_trades = max(1, int(linked_trade_count or 1))
            return [
                python_exe,
                str(self.bot_backend_root / "scripts" / "ml" / "build_dataset.py"),
                "--db-path",
                str(self.db.path),
                "--output-dir",
                str(self.dataset_dir),
                "--min-trades",
                str(min_trades),
                "--verbose",
            ]
        if action_key == "run_training" and dataset_path and target_model_version:
            return [
                python_exe,
                str(self.bot_backend_root / "scripts" / "ml" / "train_entry_model.py"),
                "--dataset-path",
                str(dataset_path),
                "--output-dir",
                str(self.artifact_dir),
                "--model-version",
                target_model_version,
                "--scale-pos-weight",
                "auto",
                "--calibrate",
                "--verbose",
            ]
        if action_key == "run_validation":
            output_csv = self.log_dir / f"shadow_validation_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            return [
                python_exe,
                str(self.bot_backend_root / "scripts" / "ml" / "analyze_shadow.py"),
                "--log-dir",
                str(self.log_dir),
                "--db-path",
                str(self.db.path),
                "--threshold",
                str(threshold or getattr(settings, "ML_SCORE_THRESHOLD", 0.4) or 0.4),
                "--min-days",
                "14",
                "--output-csv",
                str(output_csv),
                "--verbose",
            ]
        return None

    def _distribution_with_pnl(self, rows: list[LinkedTradeRow], key: str) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, float]] = {}
        for row in rows:
            group_key = (row[key] or "UNKNOWN") if key != "symbol" else (row["symbol"] or "UNKNOWN")
            stats = grouped.setdefault(group_key, {"count": 0, "pnl_sum": 0.0})
            stats["count"] += 1
            stats["pnl_sum"] += float(row.effective_realized_pnl or 0.0)
        total = sum(int(item["count"]) for item in grouped.values())
        result = []
        for group_key, stats in grouped.items():
            count = int(stats["count"])
            result.append(
                {
                    "key": str(group_key),
                    "count": count,
                    "pct": self._pct(count, total),
                    "average_pnl": round(float(stats["pnl_sum"]) / count, 4) if count else 0.0,
                }
            )
        result.sort(key=lambda item: item["count"], reverse=True)
        return result

    def _session_distribution(self, rows: list[LinkedTradeRow]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, float]] = {}
        for row in rows:
            session = self._session_for_timestamp(row.event_timestamp)
            stats = grouped.setdefault(session, {"count": 0, "pnl_sum": 0.0})
            stats["count"] += 1
            stats["pnl_sum"] += float(row.effective_realized_pnl or 0.0)
        total = sum(int(item["count"]) for item in grouped.values())
        result = []
        for session, stats in grouped.items():
            count = int(stats["count"])
            result.append(
                {
                    "key": session,
                    "count": count,
                    "pct": self._pct(count, total),
                    "average_pnl": round(float(stats["pnl_sum"]) / count, 4) if count else 0.0,
                }
            )
        result.sort(key=lambda item: item["count"], reverse=True)
        return result

    def _score_band_pnl(self, rows: list[LinkedTradeRow]) -> list[dict[str, Any]]:
        grouped = {bucket: {"count": 0, "pnl_sum": 0.0} for bucket in self._score_buckets()}
        for row in rows:
            score = self._as_float(row["ml_score"])
            if score is None:
                continue
            bucket = self._bucket_for_score(score)
            grouped[bucket]["count"] += 1
            grouped[bucket]["pnl_sum"] += float(row.effective_realized_pnl or 0.0)
        return [
            {
                "bucket": bucket,
                "count": int(stats["count"]),
                "average_pnl": round(float(stats["pnl_sum"]) / stats["count"], 4) if stats["count"] else 0.0,
            }
            for bucket, stats in grouped.items()
        ]

    def _historical_score_distribution(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        buckets = {bucket: 0 for bucket in self._score_buckets()}
        for row in rows:
            score = self._as_float(row.get("ml_score"))
            if score is None:
                continue
            buckets[self._bucket_for_score(score)] += 1
        return [{"bucket": bucket, "count": count} for bucket, count in buckets.items()]

    def _has_inflight_action(self) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM ml_action_runs
                WHERE status IN ('queued', 'running')
                """
            ).fetchone()
        return bool(row and int(row["c"] or 0) > 0)

    def _latest_action_run(self, action_key: str) -> dict[str, Any] | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM ml_action_runs
                WHERE action_key = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (action_key,),
            ).fetchone()
        return self._serialize_action_run(dict(row)) if row else None

    def _list_action_runs(self, *, limit: int = 8) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT *
                    FROM ml_action_runs
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]
        return [self._serialize_action_run(row) for row in rows]

    def _get_action_run(self, action_id: str) -> dict[str, Any] | None:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM ml_action_runs WHERE id = ?", (action_id,)).fetchone()
        return dict(row) if row else None

    def _update_action_run(self, action_id: str, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values()) + [action_id]
        with self.db.connect() as conn:
            conn.execute(
                f"UPDATE ml_action_runs SET {assignments} WHERE id = ?",
                values,
            )

    def _refresh_stale_action_runs(self) -> None:
        now = datetime.now(timezone.utc).timestamp()
        with self.db.connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT id, status, created_at, updated_at, reason
                    FROM ml_action_runs
                    WHERE status IN ('queued', 'running')
                    """
                ).fetchall()
            ]
        for row in rows:
            updated_at = self._to_epoch(row.get("updated_at") or row.get("created_at"))
            if updated_at is None or (now - updated_at) < 4 * 3600:
                continue
            self._update_action_run(
                row["id"],
                status="failed",
                updated_at=self._utc_now_iso(),
                finished_at=self._utc_now_iso(),
                reason="Marked stale after backend restart or hung execution.",
            )

    def _serialize_action_run(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row.get("id"),
            "action_key": row.get("action_key"),
            "requested_by_admin_id": row.get("requested_by_admin_id"),
            "requested_by_email": row.get("requested_by_email"),
            "note": row.get("note"),
            "status": row.get("status"),
            "reason": row.get("reason"),
            "supported": bool(row.get("supported")),
            "dataset_path": row.get("dataset_path"),
            "target_model_version": row.get("target_model_version"),
            "log_path": row.get("log_path"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "result": self._loads_maybe(row.get("result_json")),
            "log_tail": self._read_log_tail(row.get("log_path")),
        }

    def _audit_action(
        self,
        *,
        admin: dict[str, Any],
        action_key: str,
        status: str,
        note: str | None,
        reason: str | None,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO auth_audit_log (id, event_type, user_id, email, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    f"ml_admin_{action_key}",
                    admin.get("id"),
                    admin.get("email"),
                    json.dumps({"status": status, "note": note, "reason": reason}),
                    self._utc_now_iso(),
                ),
            )

    def _can_manage_ml(self, admin: dict[str, Any]) -> bool:
        role = (admin.get("role") or "admin").lower()
        return bool(admin.get("is_superuser")) or role in {"admin", "ml_admin", "ops"}

    def _action_log_path(self, action_key: str) -> Path:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return self.log_dir / f"admin_{action_key}_{stamp}.log"

    def _read_log_tail(self, log_path: str | None, *, max_lines: int = MAX_ACTION_LOG_LINES) -> list[str]:
        if not log_path:
            return []
        path = Path(log_path)
        if not path.exists():
            return []
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            return lines[-max_lines:]
        except Exception:
            return []

    def _combine_process_output(self, stdout: str | None, stderr: str | None) -> str:
        parts = []
        if stdout:
            parts.append(stdout)
        if stderr:
            if parts:
                parts.append("\n[stderr]\n")
            parts.append(stderr)
        return "".join(parts)

    def _tail_text(self, value: str, *, max_chars: int = 1200) -> str:
        if len(value) <= max_chars:
            return value
        return value[-max_chars:]

    def _train_test_rows(self, metadata: dict[str, Any]) -> tuple[int | None, int | None]:
        row_count = metadata.get("row_count")
        val_frac = metadata.get("val_frac")
        test_frac = metadata.get("test_frac")
        if not isinstance(row_count, int) or not isinstance(val_frac, (int, float)) or not isinstance(test_frac, (int, float)):
            return None, None
        test_rows = int(round(row_count * float(test_frac)))
        val_rows = int(round(row_count * float(val_frac)))
        train_rows = max(int(row_count) - test_rows - val_rows, 0)
        return train_rows, test_rows

    def _validation_verdict(
        self,
        *,
        metadata: dict[str, Any],
        validation_data: dict[str, Any] | None,
        model_version: str,
        current_model_version: str | None,
        ml_mode: str,
    ) -> str:
        accepted = bool(metadata.get("accepted", False))
        warnings = [str(item).lower() for item in (validation_data or {}).get("warnings", [])]
        train_auc = self._as_float(metadata.get("train_auc"))
        test_auc = self._as_float(self._safe_nested(metadata, "walk_forward_metrics", "auc", "mean"))
        overfit = any("overfit" in warning for warning in warnings)
        if train_auc is not None and test_auc is not None and train_auc - test_auc >= 0.12:
            overfit = True
        if overfit:
            return "overfit"
        if not accepted:
            return "rejected"
        if model_version == current_model_version and ml_mode == "shadow":
            return "shadow_only"
        if model_version != current_model_version and current_model_version:
            return "rolled_back"
        return "accepted"

    def _validation_deployed_mode(
        self,
        *,
        metadata: dict[str, Any],
        model_version: str,
        current_model_version: str | None,
        ml_mode: str,
    ) -> str:
        accepted = bool(metadata.get("accepted", False))
        if not accepted:
            return "rejected"
        if model_version == current_model_version:
            if ml_mode == "shadow":
                return "shadow"
            if ml_mode == "active":
                return "live"
        if current_model_version and model_version != current_model_version:
            return "rolled_back"
        return "not_deployed"

    def _validation_notes(self, validation_data: dict[str, Any] | None) -> str | None:
        if not validation_data:
            return None
        warnings = validation_data.get("warnings") or []
        if warnings:
            return "; ".join(str(item) for item in warnings)
        return None

    def _next_model_version(self, current_model_version: str | None) -> str:
        date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
        if not current_model_version:
            return f"entry_quality_v1.0_{date_part}"
        try:
            prefix, version_token, _old_date = current_model_version.rsplit("_", 2)
            if version_token.startswith("v"):
                major_text, minor_text = version_token[1:].split(".", 1)
                return f"{prefix}_v{int(major_text)}.{int(minor_text) + 1}_{date_part}"
        except Exception:
            pass
        return f"{current_model_version}_{date_part}"

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _latest_dataset_metadata(self) -> dict[str, Any] | None:
        dataset_files = sorted(self.dataset_dir.glob("training_*_meta.json"), key=lambda path: path.stat().st_mtime, reverse=True)
        for meta_file in dataset_files:
            metadata = self._read_json(meta_file)
            if metadata:
                dataset_path = meta_file.with_name(meta_file.name.replace("_meta.json", ".parquet"))
                if not dataset_path.exists():
                    dataset_path = Path(str(metadata.get("training_dataset_path") or "")).expanduser() if metadata.get("training_dataset_path") else dataset_path
                metadata.setdefault("training_dataset_path", str(dataset_path) if dataset_path else None)
                metadata.setdefault("contract_version", ML_CONTRACT_VERSION)
                metadata.setdefault("schema_hash", ML_FEATURE_SCHEMA_HASH)
                metadata.setdefault("feature_columns", list(ML_FEATURE_COLUMNS))
                metadata.setdefault("label_columns", list(build_contract_metadata()["label_columns"]))
                return {"meta_file": meta_file, "meta": metadata, "dataset_path": dataset_path}
        return None

    def _latest_runtime_status(self) -> dict[str, Any] | None:
        return latest_ml_runtime_status(self.db)

    @staticmethod
    def _ml_mode(*, ml_enabled: bool, shadow_mode: bool) -> str:
        if not ml_enabled:
            return "disabled"
        return "shadow" if shadow_mode else "active"

    @staticmethod
    def _path_stem(path_value: str | None) -> str | None:
        if not path_value:
            return None
        try:
            return Path(path_value).stem or None
        except Exception:
            return None

    @staticmethod
    def _class_balance_stats(class_balance: dict[str, Any], key: str) -> dict[str, Any]:
        nested = class_balance.get(key)
        if isinstance(nested, dict):
            positive = int(nested.get("positive") or 0)
            negative = int(nested.get("negative") or 0)
            single_class = bool(nested.get("single_class", positive == 0 or negative == 0))
            return {
                "positive": positive,
                "negative": negative,
                "single_class": single_class,
            }
        positive = int(class_balance.get(f"{key}_positive_count") or 0)
        negative = int(class_balance.get(f"{key}_negative_count") or 0)
        return {
            "positive": positive,
            "negative": negative,
            "single_class": bool((positive + negative) > 0 and (positive == 0 or negative == 0)),
        }

    @staticmethod
    def _gate_status(
        *,
        total_linked_completed_trades: int,
        wins: int,
        linkage_healthy: bool,
        critical_feature_broken: bool,
        label_distribution_single_class: bool,
        feature_coverage_pct: float,
        training_ready: bool,
    ) -> str:
        if training_ready:
            return "ready_for_training"
        if total_linked_completed_trades == 0:
            return "not_ready"
        if (
            not linkage_healthy
            or critical_feature_broken
            or label_distribution_single_class
            or feature_coverage_pct < MIN_FEATURE_COVERAGE_PCT
        ):
            return "blocked"
        if total_linked_completed_trades < REQUIRED_TRADES or wins < REQUIRED_WINS:
            return "collecting_data"
        return "blocked"

    @staticmethod
    def _current_ml_status(
        *,
        gate: dict[str, Any],
        ml_mode: str,
        current_model_version: str | None,
    ) -> str:
        if not gate["training_ready"]:
            if gate["status"] == "collecting_data":
                return "collecting_data"
            return "not_ready"
        if gate["training_ready"] and not current_model_version:
            return "ready_for_training"
        if current_model_version and ml_mode == "disabled":
            return "ready_for_shadow_deployment"
        if current_model_version and ml_mode in {"shadow", "active"}:
            return "ready_for_live_promotion"
        return "not_ready"

    @staticmethod
    def _pct(part: int | float, total: int | float) -> float:
        if not total:
            return 0.0
        return round((float(part) / float(total)) * 100.0, 2)

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def _to_epoch(value: str | None) -> float | None:
        dt_value = MLAdminService._parse_dt(value)
        if dt_value is None:
            return None
        return dt_value.timestamp()

    @staticmethod
    def _session_for_timestamp(value: str | None) -> str:
        dt_value = MLAdminService._parse_dt(value)
        if dt_value is None:
            return "unknown"
        hour = dt_value.hour
        if 0 <= hour < 8:
            return "asia"
        if 8 <= hour < 16:
            return "london"
        return "ny"

    @staticmethod
    def _score_buckets() -> list[str]:
        return [
            "0.0-0.1",
            "0.1-0.2",
            "0.2-0.3",
            "0.3-0.4",
            "0.4-0.5",
            "0.5-0.6",
            "0.6-0.7",
            "0.7-0.8",
            "0.8-0.9",
            "0.9-1.0",
        ]

    @staticmethod
    def _bucket_for_score(score: float) -> str:
        score = min(max(score, 0.0), 0.999999)
        lower = math.floor(score * 10) / 10
        upper = lower + 0.1
        return f"{lower:.1f}-{upper:.1f}"

    @staticmethod
    def _bump_action_counter(target: dict[str, dict[str, int]], key: str, action: str) -> None:
        entry = target.setdefault(
            key,
            {"allow_count": 0, "shadow_count": 0, "block_count": 0, "skip_count": 0},
        )
        if action == "ALLOW":
            entry["allow_count"] += 1
        elif action == "SHADOW":
            entry["shadow_count"] += 1
        elif action == "BLOCK":
            entry["block_count"] += 1
        else:
            entry["skip_count"] += 1

    @staticmethod
    def _flatten_action_counters(source: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
        rows = [{"key": key, **value} for key, value in source.items()]
        rows.sort(key=lambda row: (row["allow_count"] + row["shadow_count"] + row["block_count"] + row["skip_count"]), reverse=True)
        return rows

    @staticmethod
    def _empty_group_stats() -> dict[str, Any]:
        return {
            "count": 0,
            "wins": 0,
            "losses": 0,
            "breakevens": 0,
            "total_pnl": 0.0,
            "average_pnl": 0.0,
        }

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    @staticmethod
    def _safe_nested(data: Any, *keys: Any) -> Any:
        current: Any = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
                current = current[key]
            else:
                return None
        return current

    @staticmethod
    def _loads_maybe(value: str | None) -> Any:
        if not value:
            return None
        try:
            return json.loads(value)
        except Exception:
            return value

    @staticmethod
    def _validation_method(metadata: dict[str, Any]) -> str | None:
        n_folds = metadata.get("n_folds")
        isotonic = metadata.get("isotonic_calibration")
        if n_folds is None and isotonic is None:
            return None
        method = f"walk_forward_{n_folds}_fold" if n_folds is not None else "walk_forward"
        if isotonic:
            method += "+isotonic_calibration"
        return method
