"""Canonical trading-evidence schema (Phases 9 and 11).

The lineage this schema exists to make queryable:

    runtime_session
        -> bot_run
            -> trading_cycle
                -> trading_decision          (exactly one per evaluated symbol)
                    -> execution_attempt      (one per approved decision)
                        -> order / fill(s)
                            -> position
                                -> position_events   (append-only)

Design rules:

* **Additive only.** Every statement is CREATE TABLE IF NOT EXISTS or an
  idempotent ALTER. No legacy trading evidence is dropped or rewritten.
* **Append-only where it is evidence.** position_events, risk_events,
  reconciliation_events and data_quality_events are never updated in place.
* **Unknown stays unknown.** Legacy rows carry provenance MIGRATED_LEGACY and
  evidence_quality PARTIAL rather than being backfilled with invented ids.
* **Provenance is a first-class filter.** Readiness and profitability reporting
  must be able to exclude replay, backtest, synthetic and validation rows
  without string matching.
"""
from __future__ import annotations

from typing import Any


# ── Provenance ───────────────────────────────────────────────────────────────
# What produced this evidence. Only ORGANIC_PROVENANCE may count toward
# readiness; everything else is observation, research or test scaffolding.

PAPER_FORWARD = "PAPER_FORWARD"
TESTNET = "TESTNET"
LIVE_MAINNET = "LIVE_MAINNET"
REPLAY = "REPLAY"
BACKTEST = "BACKTEST"
SYNTHETIC = "SYNTHETIC"
LEGACY_BACKFILL = "LEGACY_BACKFILL"
MIGRATED_LEGACY = "MIGRATED_LEGACY"
PAPER_FORWARD_VALIDATION = "PAPER_FORWARD_VALIDATION"
TEST_FIXTURE = "TEST_FIXTURE"

ALL_PROVENANCE = frozenset({
    PAPER_FORWARD, TESTNET, LIVE_MAINNET, REPLAY, BACKTEST, SYNTHETIC,
    LEGACY_BACKFILL, MIGRATED_LEGACY, PAPER_FORWARD_VALIDATION, TEST_FIXTURE,
})

#: Provenance that represents real forward runtime and may count as readiness
#: evidence. PAPER_FORWARD_VALIDATION is deliberately excluded: the Phase 12
#: smoke injects controlled opportunities and must never inflate readiness.
ORGANIC_PROVENANCE = frozenset({PAPER_FORWARD, TESTNET, LIVE_MAINNET})

#: Provenance that must never be mixed into performance or readiness reporting.
NON_ORGANIC_PROVENANCE = ALL_PROVENANCE - ORGANIC_PROVENANCE

#: How trustworthy the correlation ids on a row are.
EVIDENCE_COMPLETE = "COMPLETE"
EVIDENCE_PARTIAL = "PARTIAL"

#: Database roles. The runtime never guesses these from a filename.
DATABASE_ROLES = ("development", "paper", "research", "live")

#: How a database file on disk should be treated.
DB_CLASSIFICATIONS = ("ACTIVE", "FORENSIC", "ARCHIVED", "STALE_COPY", "RESEARCH", "BACKUP")


def _add_column_if_missing(conn: Any, table: str, column: str, decl: str) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def ensure_evidence_schema(db: Any) -> None:
    """Create/extend the canonical evidence tables. Safe to run repeatedly."""
    with db.connect() as conn:
        _runtime_lineage(conn)
        _market_and_decisions(conn)
        _execution_and_positions(conn)
        _event_streams(conn)
        _readiness(conn)
        _threshold_engine(conn)
        _indexes(conn)


# ── Phase 11 §25-27: runtime lineage ─────────────────────────────────────────


def _runtime_lineage(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_sessions (
            runtime_session_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            stopped_at TEXT,
            pid INTEGER,
            parent_pid INTEGER,
            python_executable TEXT,
            python_version TEXT,
            working_directory TEXT,
            code_revision TEXT,
            branch TEXT,
            working_tree_dirty INTEGER,
            database_role TEXT,
            database_path TEXT,
            schema_version INTEGER,
            process_execution_mode TEXT,
            environment_name TEXT,
            status TEXT NOT NULL DEFAULT 'RUNNING',
            shutdown_reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_runs (
            run_id TEXT PRIMARY KEY,
            runtime_session_id TEXT,
            bot_instance_id TEXT NOT NULL,
            user_id TEXT,
            policy_hash TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            execution_mode TEXT,
            broker_environment TEXT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL DEFAULT 'RUNNING',
            cycles INTEGER NOT NULL DEFAULT 0,
            decisions INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            fills INTEGER NOT NULL DEFAULT 0,
            positions_opened INTEGER NOT NULL DEFAULT 0,
            positions_closed INTEGER NOT NULL DEFAULT 0,
            primary_failure_reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_cycles (
            cycle_id TEXT PRIMARY KEY,
            run_id TEXT,
            runtime_session_id TEXT,
            bot_instance_id TEXT NOT NULL,
            policy_hash TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            started_at TEXT NOT NULL,
            completed_at TEXT,
            symbols_seen INTEGER NOT NULL DEFAULT 0,
            symbols_managed INTEGER NOT NULL DEFAULT 0,
            new_candle_evaluations INTEGER NOT NULL DEFAULT 0,
            no_new_candle_count INTEGER NOT NULL DEFAULT 0,
            no_opportunity_count INTEGER NOT NULL DEFAULT 0,
            quality_rejection_count INTEGER NOT NULL DEFAULT 0,
            risk_rejection_count INTEGER NOT NULL DEFAULT 0,
            execution_rejection_count INTEGER NOT NULL DEFAULT 0,
            approved_count INTEGER NOT NULL DEFAULT 0,
            execution_attempt_count INTEGER NOT NULL DEFAULT 0,
            fill_count INTEGER NOT NULL DEFAULT 0,
            open_count INTEGER NOT NULL DEFAULT 0,
            partial_close_count INTEGER NOT NULL DEFAULT 0,
            close_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            reason_counts_json TEXT
        )
        """
    )


# ── Phase 11 §28 + Phase 9: snapshots and the canonical decision ─────────────


def _market_and_decisions(conn: Any) -> None:
    # Snapshot lineage without duplicating gigabytes of identical candles:
    # the data_hash identifies the candle set, the row identifies the view.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_snapshots (
            market_snapshot_id TEXT PRIMARY KEY,
            bot_instance_id TEXT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            closed_candle_open_time INTEGER,
            closed_candle_close_time INTEGER,
            reference_price REAL,
            source TEXT,
            source_environment TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            fetched_at TEXT NOT NULL,
            data_hash TEXT,
            higher_timeframe TEXT,
            higher_timeframe_closed_candle_time INTEGER,
            higher_timeframe_aligned INTEGER,
            candle_count INTEGER
        )
        """
    )

    # The canonical decision. One finalized row per evaluated symbol, including
    # every early return (NO_NEW_CANDLE, regime block, risk rejection, ...).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_decisions (
            decision_id TEXT PRIMARY KEY,

            runtime_session_id TEXT,
            bot_instance_id TEXT NOT NULL,
            user_id TEXT,
            broker_account_id TEXT,
            run_id TEXT,
            cycle_id TEXT,
            market_snapshot_id TEXT,
            opportunity_id TEXT,
            policy_hash TEXT,

            symbol TEXT NOT NULL,
            market_type TEXT,
            timeframe TEXT,
            closed_candle_open_time INTEGER,
            closed_candle_close_time INTEGER,
            evaluated_at TEXT NOT NULL,

            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            evidence_quality TEXT NOT NULL DEFAULT 'COMPLETE',
            execution_mode TEXT,
            broker_environment TEXT,

            regime TEXT,
            regime_confidence REAL,
            component_signals_json TEXT,
            active_strategies_json TEXT,
            supporting_strategies_json TEXT,
            opposing_strategies_json TEXT,
            component_metadata_json TEXT,

            buy_score REAL,
            sell_score REAL,
            consensus_observed REAL,
            consensus_required REAL,
            raw_confidence REAL,

            threshold_base REAL,
            threshold_dynamic REAL,
            threshold_adaptive_modifier REAL,
            threshold_regime_modifier REAL,
            effective_entry_threshold REAL,

            quality_result TEXT,
            quality_reason TEXT,
            hard_veto_result TEXT,
            hard_veto_reason TEXT,
            risk_result TEXT,
            risk_reason TEXT,

            requested_margin REAL,
            approved_margin REAL,
            requested_notional REAL,
            approved_notional REAL,
            quantity REAL,
            leverage REAL,
            risk_amount REAL,
            stop_price REAL,
            target_price REAL,
            stop_distance REAL,
            reward_distance REAL,
            resolved_rr REAL,

            execution_feasibility_result TEXT,
            execution_feasibility_reason TEXT,
            entry_protection_result TEXT,
            entry_protection_reason TEXT,

            execution_attempt_id TEXT,
            order_id TEXT,
            fill_ids_json TEXT,
            position_id TEXT,

            final_action TEXT,
            primary_reason TEXT,
            secondary_reasons_json TEXT,
            legacy_reason TEXT,

            complete INTEGER NOT NULL DEFAULT 0,
            finalized_at TEXT
        )
        """
    )

    # One canonical ENTRY decision per (bot, symbol, timeframe, candle).
    #
    # Two exclusions matter:
    #   * partial (unfinalized) rows, so a crashed evaluation followed by a
    #     retry is not blocked by its own abandoned attempt;
    #   * NO_NEW_CANDLE heartbeats. Those carry the current candle's timestamp
    #     for diagnostics but are management ticks, not entry decisions. Without
    #     this exclusion the 10-second heartbeat's INSERT OR REPLACE silently
    #     overwrote the real evaluation for that candle -- observed in live
    #     runtime, where a genuine ENTRY_CONFIDENCE_BELOW_THRESHOLD row was
    #     replaced by the heartbeat that followed it.
    conn.execute("DROP INDEX IF EXISTS uq_trading_decisions_candle")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_decisions_candle
        ON trading_decisions (
            bot_instance_id, symbol, timeframe, closed_candle_close_time
        )
        WHERE closed_candle_close_time IS NOT NULL
          AND complete = 1
          AND primary_reason <> 'NO_NEW_CANDLE'
        """
    )


# ── Phase 11 §29-31: execution attempts, positions, position events ─────────


def _execution_and_positions(conn: Any) -> None:
    # Closes the gap between "decision approved" and "an order exists": an
    # attempt row is written even when the broker returns no order id at all.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_attempts (
            execution_attempt_id TEXT PRIMARY KEY,
            decision_id TEXT,
            bot_instance_id TEXT NOT NULL,
            user_id TEXT,
            run_id TEXT,
            cycle_id TEXT,
            execution_mode TEXT,
            broker_environment TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            symbol TEXT NOT NULL,
            requested_action TEXT,
            requested_qty REAL,
            requested_notional REAL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            result TEXT,
            broker_order_id TEXT,
            client_order_id TEXT,
            position_id TEXT,
            primary_reason TEXT,
            error_class TEXT,
            error_detail TEXT
        )
        """
    )
    # Broker execution lineage.  Requested sizing remains immutable evidence;
    # the executed fields are populated from the venue response/reconciliation.
    for column, decl in (
        ("executed_qty", "REAL"),
        ("avg_fill_price", "REAL"),
        # Fill resolution: what the create-order response said, what the
        # broker proved, and which source proved it.
        ("initial_response_executed_qty", "REAL"),
        ("resolved_executed_qty", "REAL"),
        ("fill_resolution_source", "TEXT"),
        ("fill_resolution_status", "TEXT"),
        ("fees", "REAL"),
        ("fee_asset", "TEXT"),
    ):
        _add_column_if_missing(conn, "execution_attempts", column, decl)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            position_id TEXT PRIMARY KEY,
            bot_instance_id TEXT NOT NULL,
            user_id TEXT,
            broker_account_id TEXT,
            run_id TEXT,
            decision_id TEXT,
            execution_attempt_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            execution_mode TEXT,
            broker_environment TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            original_qty REAL NOT NULL,
            remaining_qty REAL NOT NULL,
            realized_qty REAL NOT NULL DEFAULT 0,
            entry_price REAL,
            realized_pnl REAL NOT NULL DEFAULT 0,
            fees REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'OPEN',
            opened_at TEXT NOT NULL,
            updated_at TEXT,
            closed_at TEXT,
            close_reason TEXT
        )
        """
    )
    for column, decl in (
        ("leverage", "REAL NOT NULL DEFAULT 1"),
        ("committed_margin", "REAL NOT NULL DEFAULT 0"),
        ("requested_qty", "REAL"),
        ("broker_executed_qty", "REAL"),
        ("broker_position_mode", "TEXT"),
        ("reconciliation_reason", "TEXT"),
        ("last_reconciled_at", "TEXT"),
    ):
        _add_column_if_missing(conn, "positions", column, decl)
    # Append-only. Never UPDATE or DELETE a row here.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_events (
            event_id TEXT PRIMARY KEY,
            position_id TEXT NOT NULL,
            bot_instance_id TEXT NOT NULL,
            run_id TEXT,
            cycle_id TEXT,
            decision_id TEXT,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            quantity REAL,
            remaining_qty REAL,
            price REAL,
            fee REAL,
            realized_pnl REAL,
            stop_price REAL,
            target_price REAL,
            reason TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            detail_json TEXT
        )
        """
    )


#: Position lifecycle events (§31). Append-only vocabulary.
POSITION_EVENT_TYPES = (
    "OPENED", "PARTIAL_CLOSE", "TP1", "BREAK_EVEN_ACTIVATED", "TRAILING_ACTIVATED",
    "STOP_UPDATED", "TARGET_UPDATED", "FINAL_CLOSE", "DAILY_CLOSE",
    "KILL_SWITCH_CLOSE", "RECONCILIATION", "ERROR",
)

#: Bot lifecycle events (§34/§56). Every one must carry an actor and a reason.
BOT_LIFECYCLE_EVENT_TYPES = (
    "BOT_CREATED", "BOT_STARTED", "BOT_PAUSED", "BOT_STOPPED", "BOT_DELETED",
    "BOT_ARCHIVED", "BOT_REDEPLOYED", "BOT_REPLACED", "POLICY_CHANGED",
    "RUNNER_CREATED", "RUNNER_EVICTED", "APPROVAL_CHANGED", "BROKER_CHANGED",
    "OPERATOR_ACTION",
)

#: Data-quality findings (§35).
DATA_QUALITY_EVENT_TYPES = (
    "ORPHAN_FILL", "ORPHAN_ORDER", "MISSING_DECISION", "MISSING_EXECUTION_ATTEMPT",
    "INCOMPLETE_DECISION", "POSITION_QTY_MISMATCH", "POLICY_HASH_MISMATCH",
    "MODE_ENVIRONMENT_CONTRADICTION", "DUPLICATE_DECISION", "DUPLICATE_FILL",
    "LIFECYCLE_FLAT_WITHOUT_CLOSE",
)


# ── Phase 11 §32-35: event streams ──────────────────────────────────────────


def _event_streams(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_events (
            event_id TEXT PRIMARY KEY,
            bot_instance_id TEXT NOT NULL,
            run_id TEXT,
            cycle_id TEXT,
            decision_id TEXT,
            symbol TEXT,
            event_type TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            observed_value REAL,
            limit_value REAL,
            action TEXT,
            reason TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            detail_json TEXT
        )
        """
    )
    # Mismatches are recorded, never silently repaired.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliation_events (
            event_id TEXT PRIMARY KEY,
            bot_instance_id TEXT NOT NULL,
            run_id TEXT,
            cycle_id TEXT,
            symbol TEXT,
            position_id TEXT,
            occurred_at TEXT NOT NULL,
            execution_mode TEXT,
            broker_environment TEXT,
            scope TEXT,
            expected TEXT,
            observed TEXT,
            difference TEXT,
            action TEXT,
            reason TEXT,
            result TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',
            detail_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_quality_events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            severity TEXT NOT NULL DEFAULT 'WARNING',
            bot_instance_id TEXT,
            run_id TEXT,
            cycle_id TEXT,
            decision_id TEXT,
            subject_id TEXT,
            detected_at TEXT NOT NULL,
            detail TEXT,
            resolved_at TEXT,
            resolution TEXT
        )
        """
    )
    # Registry of database files seen next to the active one, so a stale
    # OneDrive copy is labelled rather than silently ranked by mtime or size.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS database_registry (
            database_path TEXT PRIMARY KEY,
            classification TEXT NOT NULL,
            database_role TEXT,
            size_bytes INTEGER,
            modified_at TEXT,
            schema_version INTEGER,
            registered_at TEXT NOT NULL,
            note TEXT
        )
        """
    )


# ── Phase 10: readiness approvals and Section A-E confirmations ─────────────


def _readiness(conn: Any) -> None:
    # readiness_approvals already exists from earlier work; extend it rather
    # than replacing it so approval history is preserved.
    for column, decl in (
        ("approval_status", "TEXT NOT NULL DEFAULT 'APPROVED'"),
        ("reviewer_role", "TEXT"),
        ("evidence_snapshot_id", "TEXT"),
        ("evidence_hash", "TEXT"),
        ("invalidated_at", "TEXT"),
        ("invalidation_reason", "TEXT"),
    ):
        try:
            _add_column_if_missing(conn, "readiness_approvals", column, decl)
        except Exception:
            # The table may not exist yet in a bare test DB; the readiness
            # module creates it, and this migration is safe to re-run.
            pass

    # Current review state per bot. Approval is never automatic: a bot only
    # reaches APPROVED_FOR_CONTROLLED_BETA through an explicit admin action.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS readiness_states (
            bot_instance_id TEXT PRIMARY KEY,
            state TEXT NOT NULL DEFAULT 'NOT_READY',
            policy_hash TEXT,
            evidence_hash TEXT,
            updated_at TEXT NOT NULL,
            updated_by TEXT,
            reason TEXT
        )
        """
    )
    # Each required section carries its own explicit confirmation. There is no
    # blanket "sections A-E confirmed" flag anywhere.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS readiness_section_confirmations (
            confirmation_id TEXT PRIMARY KEY,
            bot_instance_id TEXT NOT NULL,
            section TEXT NOT NULL,
            confirmed INTEGER NOT NULL DEFAULT 0,
            confirmed_by TEXT,
            confirmed_at TEXT,
            evidence_reference TEXT,
            code_revision TEXT,
            policy_hash TEXT,
            notes TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_readiness_section
        ON readiness_section_confirmations (bot_instance_id, section, policy_hash)
        """
    )


def _threshold_engine(conn: Any) -> None:
    """AdaptiveEntryThresholdEngine evidence and state.

    ``threshold_decisions`` stores every component of every threshold
    calculation, not just the resulting number. That is what makes the old
    failure detectable: with only the final value on record, a threshold pinned
    at 0.70 by a floor is indistinguishable from one a healthy engine chose.

    ``final_threshold`` is nullable on purpose. NOT_EVALUATED and HARD_BLOCKED
    rows carry NULL, never 0.0.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS threshold_decisions (
            threshold_decision_id TEXT PRIMARY KEY,

            bot_instance_id TEXT NOT NULL,
            run_id TEXT,
            cycle_id TEXT,
            decision_id TEXT,
            opportunity_id TEXT,
            market_snapshot_id TEXT,

            symbol TEXT NOT NULL,
            venue TEXT,
            market_type TEXT,
            timeframe TEXT,
            closed_candle_time INTEGER,
            decided_at TEXT NOT NULL,

            threshold_engine_version TEXT NOT NULL,
            threshold_mode TEXT NOT NULL,
            policy_hash TEXT,
            provenance TEXT NOT NULL DEFAULT 'PAPER_FORWARD',

            status TEXT NOT NULL,
            opportunity_confidence REAL,
            base_threshold REAL,

            regime TEXT,
            regime_adjustment REAL,
            volatility_score REAL,
            volatility_adjustment REAL,
            expert_agreement_score REAL,
            agreement_adjustment REAL,
            htf_alignment_score REAL,
            htf_adjustment REAL,
            market_quality_score REAL,
            market_quality_adjustment REAL,

            performance_score REAL,
            performance_adjustment REAL,
            performance_sample_size INTEGER,
            performance_status TEXT,
            distribution_percentile REAL,
            distribution_adjustment REAL,
            distribution_sample_size INTEGER,
            distribution_status TEXT,

            market_threshold REAL,
            calibration_adjustment REAL,
            raw_unclamped_threshold REAL,
            smoothed_threshold REAL,
            rate_limited_threshold REAL,
            final_threshold REAL,

            min_threshold REAL,
            max_threshold REAL,
            previous_threshold REAL,
            smoothing_applied INTEGER NOT NULL DEFAULT 0,
            rate_limit_applied INTEGER NOT NULL DEFAULT 0,
            clamp_applied INTEGER NOT NULL DEFAULT 0,

            passed INTEGER,
            reason TEXT,
            detail TEXT,
            reconciles INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS expert_evaluations (
            expert_evaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            threshold_decision_id TEXT,
            decision_id TEXT,
            bot_instance_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT,
            closed_candle_time INTEGER,
            recorded_at TEXT NOT NULL,

            strategy TEXT NOT NULL,
            eligible INTEGER NOT NULL,
            executed INTEGER NOT NULL,
            signal TEXT NOT NULL,
            confidence REAL,
            raw_score REAL,
            weight REAL,
            weighted_contribution REAL,
            reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS adaptive_threshold_state (
            bot_instance_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            previous_threshold REAL,
            last_candle_time INTEGER,
            distribution_samples_json TEXT,
            updated_at TEXT,
            engine_version TEXT,
            policy_hash TEXT,
            PRIMARY KEY (bot_instance_id, symbol, timeframe, strategy_version)
        )
        """
    )
    # The canonical decision row points at the threshold decision that governed
    # it, so the two can never drift apart in reporting.
    for column, decl in (
        ("threshold_decision_id", "TEXT"),
        ("threshold_status", "TEXT"),
        ("threshold_engine_version", "TEXT"),
        ("threshold_mode", "TEXT"),
    ):
        _add_column_if_missing(conn, "trading_decisions", column, decl)


def _indexes(conn: Any) -> None:
    """Indexes that the diagnostics API depends on for window queries."""
    for stmt in (
        "CREATE INDEX IF NOT EXISTS idx_td_bot_time ON trading_decisions(bot_instance_id, evaluated_at)",
        "CREATE INDEX IF NOT EXISTS idx_td_cycle ON trading_decisions(cycle_id)",
        "CREATE INDEX IF NOT EXISTS idx_td_run ON trading_decisions(run_id)",
        "CREATE INDEX IF NOT EXISTS idx_td_reason ON trading_decisions(bot_instance_id, primary_reason)",
        "CREATE INDEX IF NOT EXISTS idx_td_incomplete ON trading_decisions(complete, evaluated_at)",
        "CREATE INDEX IF NOT EXISTS idx_td_provenance ON trading_decisions(provenance, bot_instance_id)",
        "CREATE INDEX IF NOT EXISTS idx_ea_decision ON execution_attempts(decision_id)",
        "CREATE INDEX IF NOT EXISTS idx_ea_bot_time ON execution_attempts(bot_instance_id, started_at)",
        "CREATE INDEX IF NOT EXISTS idx_pos_bot_symbol ON positions(bot_instance_id, symbol, status)",
        "CREATE INDEX IF NOT EXISTS idx_pe_position ON position_events(position_id, occurred_at)",
        "CREATE INDEX IF NOT EXISTS idx_pe_bot_type ON position_events(bot_instance_id, event_type)",
        "CREATE INDEX IF NOT EXISTS idx_risk_bot_time ON risk_events(bot_instance_id, occurred_at)",
        "CREATE INDEX IF NOT EXISTS idx_recon_bot_time ON reconciliation_events(bot_instance_id, occurred_at)",
        "CREATE INDEX IF NOT EXISTS idx_dq_type_time ON data_quality_events(event_type, detected_at)",
        "CREATE INDEX IF NOT EXISTS idx_cycles_bot_time ON trading_cycles(bot_instance_id, started_at)",
        "CREATE INDEX IF NOT EXISTS idx_runs_bot ON bot_runs(bot_instance_id, started_at)",
        "CREATE INDEX IF NOT EXISTS idx_ms_symbol_time ON market_snapshots(symbol, timeframe, closed_candle_close_time)",
        "CREATE INDEX IF NOT EXISTS idx_thr_bot_time ON threshold_decisions(bot_instance_id, decided_at)",
        "CREATE INDEX IF NOT EXISTS idx_thr_symbol ON threshold_decisions(bot_instance_id, symbol, timeframe, closed_candle_time)",
        "CREATE INDEX IF NOT EXISTS idx_thr_status ON threshold_decisions(bot_instance_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_thr_decision ON threshold_decisions(decision_id)",
        "CREATE INDEX IF NOT EXISTS idx_expert_threshold ON expert_evaluations(threshold_decision_id)",
        "CREATE INDEX IF NOT EXISTS idx_expert_bot_symbol ON expert_evaluations(bot_instance_id, symbol, closed_candle_time)",
    ):
        conn.execute(stmt)
