"""Evidence retention and database growth (Section H, Step 3.14).

**The policy.** ``POLICY`` names every table of the database and puts it in exactly one class; ``CLASS_RULES``
says what may happen to a class. A table the policy does not name is an error -- ``unclassified_tables`` and
the test that calls it fail the build -- so a new table can be neither silently unprotected nor silently
unbounded: whoever adds it has to say which it is.

**What may be removed.** Only a table of class ``RAW_OPERATIONAL_DETAIL`` that also has an explicit
``PruneRule``, and only by ``run_retention``. Every other class is *protected*: no function of this module can
select one of its rows, whatever it is called with. Independently of the policy, a table that the database
itself guards with an append-only trigger is refused even if the policy were wrong about it, and no trigger is
ever dropped, disabled or bypassed here. The evidence the owner listed -- exchange execution audit evidence,
certification records, hypothesis history, holdout records, governance promotion history, mandatory financial
records, and whatever an unsettled or unknown execution still needs -- lies in protected classes.

**How a removal is made** (``run_retention``), so that a crash anywhere leaves either the rows or a verified
archive of them, never neither:

1. the rows older than the retention are *selected* -- never one that an account with open or unknown
   execution could still need, never one the rule says is still in use;
2. they are *exported* to a gzip archive in a directory the caller names (written to a temporary file, synced,
   renamed), outside any transaction;
3. ONE database transaction then re-reads the same rows (they must be byte-for-byte what was exported), reads
   the archive back from disk and checks its SHA-256 and content, writes the *summary* of what is about to go
   (rows per day, counts per status / reason / stage, sums and extremes of the measures) and any roll-up the
   table has, deletes exactly those rows, and records the run with the archive's hash in
   ``cati_retention_runs``. It commits all of that or none of it.

A run that died before step 3 committed changed nothing in the database: the next run exports again (the same
rows give the same file) and completes. A run that died after the commit is complete, and is recognised as
such. Running it twice removes nothing twice: the second run finds no rows, and the summary of a removed row
is written in the same transaction as its removal, so it is counted exactly once.

**Not in scope, ever:** the research register (``research/**``, a file outside the database) and the frozen
research evidence under ``docs/research``. This module opens no file outside the archive directory it is given
and deletes nothing there except its own unfinished ``.tmp`` files.

**Growth.** ``table_sizes`` and ``database_size`` measure rows and bytes per table (``dbstat`` when the SQLite
build has it, otherwise the summed payload of the rows, stated as such) and the file; ``growth_between`` turns
two measurements into growth per cycle and per account. The numbers of the synthetic run are in
``docs/implementation/evidence/step3/database_growth.json``.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from shared_lib.persistence.cati_operations_schema import RETENTION_SUMMARY_TABLE
from shared_lib.persistence.cati_platform_schema import CYCLE_METRIC_TABLE, CYCLE_TABLE, RETENTION_RUN_TABLE

RETENTION_VERSION = "1.0.0"
DAY_MS = 86_400_000
ARCHIVE_FORMAT = "cati-retention-archive/1"

# ── classes ──────────────────────────────────────────────────────────────────────────────────────────────
IMMUTABLE_GOVERNANCE = "IMMUTABLE_GOVERNANCE"
RESEARCH_GOVERNANCE = "RESEARCH_GOVERNANCE"
EXECUTION_AUDIT = "EXECUTION_AUDIT"
FINANCIAL_RECORD = "FINANCIAL_RECORD"
RECONCILIATION_STATE = "RECONCILIATION_STATE"
DECISION_SUMMARY = "DECISION_SUMMARY"
CUSTOMER_AND_COMPLIANCE = "CUSTOMER_AND_COMPLIANCE"
REFERENCE_MARKET_DATA = "REFERENCE_MARKET_DATA"
LEGACY_UNMANAGED = "LEGACY_UNMANAGED"
RAW_OPERATIONAL_DETAIL = "RAW_OPERATIONAL_DETAIL"
CACHE = "CACHE"


@dataclass(frozen=True)
class ClassRule:
    retention_class: str
    protected: bool
    rule: str


CLASS_RULES: Mapping[str, ClassRule] = MappingProxyType({r.retention_class: r for r in (
    ClassRule(IMMUTABLE_GOVERNANCE, True,
              "Kept for ever. Governance promotion history, kill-switch and scope records, certification records, "
              "manifests, operator emergency records and the record of every retention run. Corrected only by a later record."),
    ClassRule(RESEARCH_GOVERNANCE, True,
              "Kept for ever. Hypothesis history, holdout records, prospective research decisions and the "
              "research-to-production comparison records (the shadow book's fills, days and comparisons)."),
    ClassRule(EXECUTION_AUDIT, True,
              "Kept for ever. Every decision, plan, veto, attempt, order intent, fill, protection record, error and "
              "operator action that explains what was or was not sent to an exchange, and why."),
    ClassRule(FINANCIAL_RECORD, True,
              "Kept for ever. Income, transfers, billing, commissions and the daily equity history."),
    ClassRule(RECONCILIATION_STATE, True,
              "Current state that reconciliation, recovery or a lease needs. Bounded by the number of accounts, "
              "positions and cycles in flight, not by time; changed only by its owner, never by retention."),
    ClassRule(DECISION_SUMMARY, True,
              "Kept for ever. One row per cycle, per account of a cycle or per removed batch: the totals, errors "
              "and outcomes that stay after raw detail has gone."),
    ClassRule(CUSTOMER_AND_COMPLIANCE, True,
              "Never touched. Customers, connected accounts, credentials, bots, consents, KYC / AML, sessions, "
              "settings and the customer's own event history."),
    ClassRule(REFERENCE_MARKET_DATA, True,
              "Not pruned by this system. Market bars, funding, instruments and reference quotes: inputs of "
              "decisions (the sealed daily feed is itself append-only)."),
    ClassRule(LEGACY_UNMANAGED, True,
              "Not written by the CATI platform (legacy runner, signals, news, back-tests, dashboards). Left exactly "
              "as it is: any pruning needs its own reviewed rule and a move to RAW_OPERATIONAL_DETAIL."),
    ClassRule(RAW_OPERATIONAL_DETAIL, False,
              "Operational detail whose totals survive in a summary. Removable ONLY through a PruneRule: after its "
              "configured retention, summarised first, exported to a hashed archive, never while an open or unknown "
              "execution could need it. A table here without a PruneRule is managed by its own writer and is not "
              "touched by this module."),
    ClassRule(CACHE, False,
              "Rebuildable from its source. Refreshed and bounded by its owner; this module defines no removal for it."),
)})

PROTECTED_CLASSES: Tuple[str, ...] = tuple(c for c, r in CLASS_RULES.items() if r.protected)

# ── the policy: every table, one class ───────────────────────────────────────────────────────────────────
_BY_CLASS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (IMMUTABLE_GOVERNANCE, (
        # Section H Steps 5-8 (platform layer, go-live, launch operations)
        "beta_reviews", "golive_events", "incident_records", "ops_oncall_history",
        "ops_status_incident_updates", "ops_status_incidents", "platform_schema_migrations",
        "cati_adapter_cert_actions", "cati_adapter_cert_checks", "cati_adapter_cert_events", "cati_certification_runs",
        "cati_certification_stage_results", "cati_demo_certifications", "cati_emergency_events", "cati_family_events",
        "cati_governance_controls", "cati_ml_model_events", "cati_ml_models", "cati_promotion_phase_history",
        "cati_promotion_scopes", "cati_retention_runs", "dataset_manifests", "demo_transport_smoke",
        "market_activation_events", "universe_manifests")),
    (RESEARCH_GOVERNANCE, (
        "cati_experiment_registry", "cati_holdout_registry", "cati_ml_shadow_predictions", "cati_residual_decisions",
        "cati_residual_inputs", "cati_shadow_comparisons", "cati_shadow_days", "cati_shadow_fills", "cati_sim_account",
        "cati_sim_decisions", "cati_sim_fills", "cati_sim_orders", "cati_sim_positions")),
    (EXECUTION_AUDIT, (
        # Section H Steps 5-8 (platform layer, go-live, launch operations)
        "platform_assignment_history", "platform_job_attempts", "platform_jobs", "platform_order_intents",
        "platform_order_slots", "platform_symbol_budgets",
        "adaptive_daily_risk_decisions", "broker_audit_log", "canonical_trade_decisions", "cati_brake_events",
        "cati_brake_restarts", "cati_capital_plan_evidence", "cati_component_errors", "cati_decision_evidence",
        "cati_execution_attempts", "cati_execution_blocks", "cati_execution_evaluations", "cati_execution_events",
        "cati_execution_intents", "cati_exit_decisions", "cati_family_decisions", "cati_global_market_states",
        "cati_instrument_rule_changes", "cati_position_forecasts", "cati_production_fills", "cati_protection_replacements",
        "cati_protection_state", "cati_reservation_ledger", "cati_reservation_ledger_events", "cati_risk_decisions",
        "cati_trade_plans", "cati_user_stream_fills", "cati_user_stream_reconciliations", "entry_protection_events",
        "events", "execution_attempts", "invariant_violations", "order_failures", "position_events", "positions",
        "reconciliation_events", "risk_events", "slippage_monitoring", "trade_fills", "trades")),
    (FINANCIAL_RECORD, (
        # Section H Steps 5-8 (platform layer, go-live, launch operations)
        "billing_audit_log", "billing_payment_reviews", "track_record_daily", "track_record_publications",
        "account_equity_daily", "billing_customers", "billing_events", "billing_operator_grants", "broker_transfer_events",
        "broker_transfer_reconciliations", "broker_transfer_requests", "broker_transfer_settings", "cati_account_income",
        "commission_ledger", "commission_tiers", "equity_snapshots", "invoices", "monthly_snapshots", "pricing_intents",
        "revenue_snapshots", "subscriptions", "weekly_snapshots")),
    (RECONCILIATION_STATE, (
        # Section H Steps 5-8 (platform layer, go-live, launch operations)
        "platform_account_assignments", "platform_accounts", "platform_workers",
        "bot_daily_close_marks", "bot_daily_state", "cati_account_income_cursor", "cati_account_leases",
        "cati_account_period_risk", "cati_brake_state", "cati_emergency_requests", "cati_execution_backoff",
        "cati_feed_state", "cati_feed_symbols", "cati_instrument_rule_refreshes", "cati_portfolio_reservations",
        "cati_production_closes", "cati_production_daily_risk", "cati_production_protection",
        "cati_production_protection_uncertainty", "cati_production_state", "cati_rate_budgets", "cati_residual_tracker",
        "cati_shadow_accounts", "cati_shadow_positions", "cati_user_stream_cursors", "cati_user_stream_state", "daily_state",
        "notification_jobs", "notification_outbox", "pending_entries", "position_lifecycle_state", "protection_state",
        "runtime_ownership", "runtime_sessions")),
    (DECISION_SUMMARY, (
        "cati_platform_cycle_accounts", "cati_platform_cycles", "cati_production_decisions", "cati_retention_summaries",
        "cycle_decision_summaries")),
    (CUSTOMER_AND_COMPLIANCE, (
        # Section H Steps 5-8 (platform layer, go-live, launch operations)
        "admin_mfa_recovery_codes", "legal_acceptances", "legal_audit_log", "legal_country_eligibility",
        "legal_documents", "ops_oncall_members", "ops_oncall_rotations", "support_ticket_events",
        "support_ticket_messages", "support_tickets",
        "admin_roles", "admin_sessions", "admins", "affiliate_settings", "aml_alerts", "auth_audit_log", "auth_sessions",
        "bot_instances", "broker_accounts", "broker_auto_trading", "broker_credentials", "broker_credentials_v2",
        "commission_config", "database_registry", "deployment_confirmations", "deployment_consents", "email_verifications",
        "kyc_audit_log", "kyc_cases", "kyc_documents", "kyc_profiles", "kyc_requirements_config", "kyc_reviews",
        "kyc_selfie_checks", "kyc_submissions", "login_attempts", "notification_endpoints", "notification_preferences",
        "onboarding_profiles", "password_resets", "public_content_blocks", "readiness_approvals",
        "readiness_section_confirmations", "readiness_states", "risk_parameters", "strategies", "strategy_parameters",
        "strategy_versions", "system_settings", "telegram_link_codes", "user_events", "user_signal_preferences",
        "user_strategies", "user_strategy_configs", "users")),
    (REFERENCE_MARKET_DATA, (
        "benchmark_prices", "cati_feed_daily_bars", "cati_feed_funding", "cati_feed_metadata", "cati_feed_rejections",
        "cati_feed_revisions", "cati_feed_sealed_days", "economic_events", "fx_reference_ingest_log", "fx_reference_quotes",
        "fx_reference_repairs", "historical_candles", "market_candles", "market_feature_observations", "market_ingest_log",
        "market_snapshots", "universe_members", "universe_mode_migrations", "universe_snapshots", "venue_instruments")),
    (LEGACY_UNMANAGED, (
        "adaptive_threshold_state", "admin_ml_control_visibility_snapshot", "admin_ml_dashboard_snapshot",
        "admin_ml_drift_snapshot", "admin_ml_feature_completeness_snapshot", "admin_ml_linked_trade_snapshot",
        "admin_ml_validation_history_snapshot", "admin_profitability_daily_summary", "admin_profitability_sizing_events",
        "admin_profitability_symbol_summary", "alerts", "analytics_runs", "backtest_decision_logs", "backtest_equity_curve",
        "backtest_fills", "backtest_jobs", "backtest_performance", "backtest_runs", "bot_candle_evaluations", "bot_runs",
        "bot_symbol_state", "bot_system_events", "daily_activity_tracking", "daily_close_validation_reports",
        "daily_trade_counts", "data_quality_events", "decision_logs", "decision_traces",
        "dynamic_universe_shadow_diagnostics", "event_blackout_windows", "event_market_snapshots",
        "event_news_influence_decisions", "event_news_mode_decisions", "event_news_runtime_mode", "expert_evaluations",
        "external_signal_queue", "global_confidence_buckets", "global_regime_performance", "global_risk_blocks",
        "global_strategy_performance", "manual_news_imports", "market_event_reactions", "marketing_events",
        "marketing_sessions", "ml_action_runs", "ml_runtime_status", "ml_validation_history",
        "narrative_effectiveness_scores", "news_asset_mappings", "news_cluster_items", "news_clusters",
        "news_intelligence_signals", "news_market_reactions", "news_narratives", "news_narratives_new",
        "news_provider_health", "news_sentiment_scores", "news_sources", "raw_news_items",
        "real_time_news_provider_status", "run_summary", "runs", "shadow_trade_outcomes", "shadow_trades",
        "signal_candidates", "signal_delivery", "signal_notifications", "signal_operation_locks", "signal_outcomes",
        "signal_pair_metrics", "signal_pair_universe", "signal_performance", "signal_scan_results", "signal_scan_runs",
        "signal_system_settings", "strategy_performance", "symbol_state", "symbol_universe_promotion_decisions",
        "symbol_universe_rankings", "threshold_decisions", "trading_cycles", "trading_decisions", "trading_signals",
        "tradingview_alerts", "tradingview_processor_heartbeat", "tradingview_safety_lockouts",
        "tradingview_signal_decisions", "tradingview_webhooks")),
    (RAW_OPERATIONAL_DETAIL, (
        # Section H Steps 5-8 (platform layer, go-live, launch operations)
        "drill_venue_orders", "ops_alert_deliveries", "ops_alert_events", "ops_alerts",
        "account_equity_snapshots",     # PruneRule below: rolled up into account_equity_daily, then removed
        "cati_platform_cycle_metrics",  # PruneRule below
        "cati_feed_updates",            # one row per feed update run; no rule here: kept until its owner defines one
        "cati_rate_denials",            # counters, one row per (venue, scope, priority, reason): bounded by its key
        "cati_rate_reservations",       # sliding log pruned by its own writer (the reservation that follows)
    )),
    (CACHE, (
        "analytics_cache", "broker_transfers_cache", "cati_instrument_rules", "official_strategies_cache")),
)


def _build_policy() -> Mapping[str, str]:
    policy: Dict[str, str] = {}
    for retention_class, tables in _BY_CLASS:
        if retention_class not in CLASS_RULES:
            raise RuntimeError(f"RETENTION_CLASS_UNKNOWN:{retention_class}")
        for table in tables:
            if table in policy:
                raise RuntimeError(f"RETENTION_POLICY_CLASSIFIES_TWICE:{table}")
            policy[table] = retention_class
    return MappingProxyType(policy)


POLICY: Mapping[str, str] = _build_policy()


@dataclass(frozen=True)
class PruneRule:
    """How one RAW_OPERATIONAL_DETAIL table may lose rows. Nothing here is supplied by a caller."""
    table: str
    time_column: str                      # epoch milliseconds
    default_retention_days: int
    minimum_retention_days: int           # a caller may keep more, never less
    group_columns: Tuple[str, ...]        # the summary keeps the row count per value of each
    measure_columns: Tuple[str, ...]      # the summary keeps count / sum / min / max of each
    account_column: Optional[str] = None  # rows of an account with open or unknown execution are kept
    keep_while: Optional[str] = None      # SQL over alias t: rows that are still in use, kept whatever their age
    rollup: Optional[str] = None          # a roll-up into a kept table, applied in the removal's transaction


PRUNE_RULES: Mapping[str, PruneRule] = MappingProxyType({r.table: r for r in (
    PruneRule(table=CYCLE_METRIC_TABLE, time_column="recorded_at", default_retention_days=30, minimum_retention_days=7,
              group_columns=("stage",), measure_columns=("duration_ms", "items"), account_column="account_id",
              # the timings of a cycle that is not finished are part of its recovery: kept until it completes
              keep_while=f"EXISTS (SELECT 1 FROM {CYCLE_TABLE} c WHERE c.cycle_id = t.cycle_id AND c.status != 'COMPLETED')"),
    PruneRule(table="account_equity_snapshots", time_column="observed_at", default_retention_days=90,
              minimum_retention_days=30, group_columns=("reason", "source"), measure_columns=("equity",),
              account_column="broker_account_id", rollup="EQUITY_DAILY"),
)})

#: 0 keeps today's behaviour: every evaluation of every cycle is written. See ``evaluation_is_unchanged_repeat``.
EVALUATION_REPEAT_INTERVAL_MS = 0
#: evaluations that are always written, however often they repeat
EVALUATION_ALWAYS_RECORDED_STAGES: Tuple[str, ...] = ("DECISION_ACCEPTED", "BOUNDARY_RESULT", "EVALUATION_FAILED")

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ARCHIVE_NAME = re.compile(r"^ret_[0-9a-f]{24}\.jsonl\.gz$")
_PARTIAL_NAME = re.compile(r"^ret_[0-9a-f]{24}\.jsonl\.gz\.tmp$")


class RetentionError(RuntimeError):
    code = "RETENTION_ERROR"


class RetentionRefused(RetentionError):
    """The request would touch something retention may not touch. Nothing was changed."""
    code = "RETENTION_REFUSED"


class UnclassifiedTable(RetentionRefused):
    """A table the policy does not name. It is refused, and it fails the build until somebody classifies it."""
    code = "RETENTION_TABLE_UNCLASSIFIED"


class RetentionConflict(RetentionError):
    """The rows or the archive are not what was exported. Nothing was removed; run it again."""
    code = "RETENTION_CONFLICT"


# ═══════════════════════════════════════════════════════════════════════════════════════ classification
def classify(table: str) -> str:
    try:
        return POLICY[table]
    except KeyError:
        raise UnclassifiedTable(f"RETENTION_TABLE_UNCLASSIFIED:{table}") from None


def is_protected(table: str) -> bool:
    return CLASS_RULES[classify(table)].protected


def database_tables(conn: Any) -> List[str]:
    """Every table of the database except SQLite's own (``sqlite_*``), which no statement may change."""
    return [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]


def unclassified_tables(db: Any) -> List[str]:
    with db.connect() as conn:
        return [t for t in database_tables(conn) if t not in POLICY]


def guarded_tables(conn: Any) -> Dict[str, str]:
    """Tables the database itself protects: ``{table: "DELETE" | "UPDATE" | "DELETE,UPDATE"}`` for every table
    with a BEFORE trigger that raises. A guarded table is never a retention target, whatever the policy says."""
    out: Dict[str, set] = {}
    for table, sql in conn.execute("SELECT tbl_name, sql FROM sqlite_master WHERE type='trigger'"):
        text = " ".join(str(sql or "").upper().split())
        if "RAISE" not in text:
            continue
        for operation in ("DELETE", "UPDATE"):
            if f"BEFORE {operation}" in text:
                out.setdefault(table, set()).add(operation)
    return {table: ",".join(sorted(ops)) for table, ops in out.items()}


def policy_report(db: Any) -> Dict[str, Any]:
    """The policy against the database as it is: per-class totals, unclassified tables, and every table that
    is prunable at all."""
    with db.connect() as conn:
        tables, guarded = database_tables(conn), guarded_tables(conn)
    by_class: Dict[str, int] = {c: 0 for c in CLASS_RULES}
    for table in tables:
        if table in POLICY:
            by_class[POLICY[table]] += 1
    return {"policy_version": RETENTION_VERSION, "tables_in_database": len(tables), "tables_in_policy": len(POLICY),
            "by_class_in_database": by_class,
            "by_class_in_policy": {c: sum(1 for v in POLICY.values() if v == c) for c in CLASS_RULES},
            "unclassified": [t for t in tables if t not in POLICY],
            "protected_classes": list(PROTECTED_CLASSES),
            "prunable": sorted(PRUNE_RULES),
            "raw_detail_without_rule": sorted(t for t, c in POLICY.items() if c == RAW_OPERATIONAL_DETAIL and t not in PRUNE_RULES),
            "guarded_by_trigger": sorted(t for t in tables if "DELETE" in guarded.get(t, "")),
            "misclassified_guarded": sorted(t for t in PRUNE_RULES if t in guarded)}


def prune_rule(conn: Any, table: str) -> PruneRule:
    """The rule under which ``table`` may lose rows, or ``RetentionRefused``. The one gate of every removal."""
    if not isinstance(table, str) or not _IDENTIFIER.match(table):
        raise RetentionRefused("RETENTION_TABLE_NAME_INVALID")
    retention_class = classify(table)
    if CLASS_RULES[retention_class].protected:
        raise RetentionRefused(f"RETENTION_CLASS_PROTECTED:{retention_class}:{table}")
    rule = PRUNE_RULES.get(table)
    if retention_class != RAW_OPERATIONAL_DETAIL or rule is None:
        raise RetentionRefused(f"RETENTION_NO_PRUNE_RULE:{table}")
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if row is None:
        raise RetentionRefused(f"RETENTION_TABLE_ABSENT:{table}")
    if "WITHOUT ROWID" in " ".join(str(row[0] or "").upper().split()):
        raise RetentionRefused(f"RETENTION_TABLE_HAS_NO_ROWID:{table}")
    if table in guarded_tables(conn):
        raise RetentionRefused(f"RETENTION_TABLE_IS_GUARDED_BY_TRIGGER:{table}")   # the database's word beats the policy's
    return rule


# ═══════════════════════════════════════════════════════════════════════════ open and unknown execution
def accounts_with_open_execution(conn: Any) -> frozenset:
    """Every account that has an order, a close, a reservation, a protection or a position whose outcome is
    not settled. Nothing of such an account is removed. A source that cannot be read refuses the run."""
    tables = set(database_tables(conn))
    accounts: set = set()
    sources: List[Tuple[str, str, Tuple[Any, ...]]] = []
    if "cati_production_closes" in tables:
        sources.append(("cati_production_closes", "SELECT account_id FROM cati_production_closes WHERE status != 'CLOSED'", ()))
    if "cati_portfolio_reservations" in tables:
        sources.append(("cati_portfolio_reservations",
                        "SELECT broker_account_id FROM cati_portfolio_reservations WHERE status = 'RESOLUTION_PENDING'", ()))
    if "cati_production_protection_uncertainty" in tables:
        sources.append(("cati_production_protection_uncertainty", "SELECT account_id FROM cati_production_protection_uncertainty", ()))
    if "cati_execution_attempts" in tables:
        from app.trading_intelligence.contracts.execution import NO_POSITION

        settled = tuple(sorted(NO_POSITION)) + ("POSITION_CLOSED",)
        sources.append(("cati_execution_attempts",
                        "SELECT a.broker_account_id FROM cati_execution_attempts a WHERE a.sequence = (SELECT MAX(b.sequence) "
                        "FROM cati_execution_attempts b WHERE b.execution_attempt_id = a.execution_attempt_id) "
                        f"AND a.status NOT IN ({','.join('?' * len(settled))})", settled))
    if "cati_execution_intents" in tables:
        from shared_lib.persistence.cati_execution_schema import INTENT_OPEN_STATES

        sources.append(("cati_execution_intents",
                        f"SELECT account_id FROM cati_execution_intents WHERE state IN ({','.join('?' * len(INTENT_OPEN_STATES))})",
                        tuple(INTENT_OPEN_STATES)))
    if "cati_protection_state" in tables:
        from shared_lib.persistence.cati_execution_schema import PROTECTION_UNSETTLED

        sources.append(("cati_protection_state",
                        f"SELECT account_id FROM cati_protection_state WHERE status IN ({','.join('?' * len(PROTECTION_UNSETTLED))})",
                        tuple(PROTECTION_UNSETTLED)))
    try:
        for _, sql, args in sources:
            accounts.update(r[0] for r in conn.execute(sql, args) if r[0])
        if "cati_production_state" in tables:
            for account_id, document in conn.execute("SELECT account_id, document FROM cati_production_state"):
                positions = json.loads(document).get("positions") if document else None
                if positions is None or any(abs(float(p.get("positionAmt") or 0)) > 0 for p in positions):
                    accounts.add(account_id)             # an open position, or a book that could not be read
    except Exception as exc:  # noqa: BLE001 -- what cannot be shown to be settled is not settled
        raise RetentionRefused(f"OPEN_EXECUTION_STATE_UNREADABLE:{type(exc).__name__}") from exc
    return frozenset(accounts)


# ═══════════════════════════════════════════════════════════════════════════════════════════ the archive
def _plain(value: Any) -> Any:
    return {"__bytes__": value.hex()} if isinstance(value, (bytes, bytearray, memoryview)) else value


def _rows_digest(rows: Sequence[Sequence[Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps([_plain(v) for v in row], sort_keys=True, separators=(",", ":"), default=str).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_archive(path: Path, header: Mapping[str, Any], rows: Sequence[Sequence[Any]]) -> str:
    """Write the archive so that the same rows always give the same bytes, and so that the final name only
    ever names a complete, synced file. Returns its SHA-256."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".tmp")
    with open(partial, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as packed:
            packed.write(json.dumps(dict(header), sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
            for row in rows:
                packed.write(json.dumps([_plain(v) for v in row], sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n")
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(partial, path)
    return _file_sha256(path)


def read_archive(path: Any) -> Tuple[Dict[str, Any], List[List[Any]]]:
    """``(header, rows)`` of an archive. The first value of every row is its rowid in the table it came from."""
    with gzip.open(Path(path), "rb") as packed:
        lines = packed.read().decode("utf-8").splitlines()
    if not lines:
        raise RetentionConflict("RETENTION_ARCHIVE_EMPTY")
    header = json.loads(lines[0])
    rows = [[bytes.fromhex(v["__bytes__"]) if isinstance(v, dict) and set(v) == {"__bytes__"} else v for v in json.loads(line)]
            for line in lines[1:]]
    return header, rows


def verify_archive(path: Any, *, sha256: Optional[str] = None) -> Dict[str, Any]:
    """Read an archive back from disk and check it against itself (and against ``sha256`` when given)."""
    path = Path(path)
    if not path.is_file():
        raise RetentionConflict("RETENTION_ARCHIVE_MISSING")
    actual = _file_sha256(path)
    if sha256 is not None and actual != sha256:
        raise RetentionConflict("RETENTION_ARCHIVE_HASH_MISMATCH")
    try:
        header, rows = read_archive(path)
    except RetentionConflict:
        raise
    except Exception as exc:  # noqa: BLE001 -- truncated or not ours
        raise RetentionConflict(f"RETENTION_ARCHIVE_UNREADABLE:{type(exc).__name__}") from exc
    if header.get("format") != ARCHIVE_FORMAT or header.get("rows") != len(rows) or header.get("rows_sha256") != _rows_digest(rows):
        raise RetentionConflict("RETENTION_ARCHIVE_CONTENT_MISMATCH")
    return {"sha256": actual, "rows": len(rows), "rows_sha256": header["rows_sha256"], "table": header.get("table"),
            "columns": header.get("columns"), "run_id": header.get("run_id")}


# ═══════════════════════════════════════════════════════════════════════════════════════════ selection
def _candidates(conn: Any, rule: PruneRule, cutoff_ms: int, max_rows: int) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """Rows of the rule's table that may go: older than the cutoff, not kept by the rule, and not of an
    account with open or unknown execution. Oldest rowid first, at most ``max_rows``."""
    columns = ["rowid"] + [r[1] for r in conn.execute(f"PRAGMA table_info({rule.table})")]
    for name in (rule.time_column, rule.account_column, *rule.group_columns, *rule.measure_columns):
        if name is not None and name not in columns:
            raise RetentionRefused(f"RETENTION_RULE_COLUMN_ABSENT:{rule.table}.{name}")
    where = f"t.{rule.time_column} < ?"
    if rule.keep_while:
        where += f" AND NOT ({rule.keep_while})"
    open_accounts = accounts_with_open_execution(conn) if rule.account_column else frozenset()
    position = None if rule.account_column is None else columns.index(rule.account_column)
    rows: List[Tuple[Any, ...]] = []
    after = -1
    while len(rows) < max_rows:
        batch = conn.execute(f"SELECT t.rowid, t.* FROM {rule.table} t WHERE {where} AND t.rowid > ? ORDER BY t.rowid LIMIT ?",
                             (int(cutoff_ms), after, int(max_rows))).fetchall()
        if not batch:
            break
        after = batch[-1][0]
        for row in batch:
            if position is not None and row[position] in open_accounts:
                continue
            rows.append(tuple(row))
            if len(rows) >= max_rows:
                break
    return columns, rows


def _bucket(stamp_ms: Any) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(stamp_ms) / 1000))


def summarise(rule: PruneRule, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> Dict[str, Dict[str, Any]]:
    """What stays of ``rows``: per UTC day, how many there were, how many per value of each group column, and
    the count / sum / min / max of each measure. Pure."""
    index = {name: n for n, name in enumerate(columns)}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        stamp = int(row[index[rule.time_column]])
        day = out.setdefault(_bucket(stamp), {"rows": 0, "first_at": stamp, "last_at": stamp,
                                              "by": {c: {} for c in rule.group_columns},
                                              "measures": {c: {"count": 0, "sum": 0.0, "min": None, "max": None}
                                                           for c in rule.measure_columns}})
        day["rows"] += 1
        day["first_at"], day["last_at"] = min(day["first_at"], stamp), max(day["last_at"], stamp)
        for column in rule.group_columns:
            key = "" if row[index[column]] is None else str(row[index[column]])
            day["by"][column][key] = day["by"][column].get(key, 0) + 1
        for column in rule.measure_columns:
            value = row[index[column]]
            if value is None:
                continue
            measure = day["measures"][column]
            measure["count"] += 1
            measure["sum"] += float(value)
            measure["min"] = float(value) if measure["min"] is None else min(measure["min"], float(value))
            measure["max"] = float(value) if measure["max"] is None else max(measure["max"], float(value))
    return out


def _rollup_equity_daily(conn: Any, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> int:
    """Fold equity samples into ``account_equity_daily`` (open / high / low / close / samples per account, asset
    and UTC day), exactly as ``app.observability.account_recorder.prune`` defines the roll-up."""
    index = {name: n for n, name in enumerate(columns)}
    groups: Dict[Tuple[str, str, str], List[Tuple[int, float]]] = {}
    for row in rows:
        observed = int(row[index["observed_at"]])
        groups.setdefault((row[index["broker_account_id"]], row[index["asset"]], _bucket(observed)), []).append(
            (observed, float(row[index["equity"]])))
    for (account_id, asset, day), samples in groups.items():
        samples.sort()
        equities = [e for _, e in samples]
        existing = conn.execute("SELECT open, high, low, close, samples, first_observed_at, last_observed_at FROM account_equity_daily "
                                "WHERE broker_account_id=? AND day=? AND asset=?", (account_id, day, asset)).fetchone()
        if existing:
            open_, high, low, close, count, first, last = existing
            merged = (open_ if first <= samples[0][0] else equities[0], max(high, *equities), min(low, *equities),
                      close if last >= samples[-1][0] else equities[-1], count + len(samples), min(first, samples[0][0]),
                      max(last, samples[-1][0]))
        else:
            merged = (equities[0], max(equities), min(equities), equities[-1], len(samples), samples[0][0], samples[-1][0])
        conn.execute("INSERT OR REPLACE INTO account_equity_daily VALUES(?,?,?,?,?,?,?,?,?,?)", (account_id, day, asset, *merged))
    return len(groups)


def _apply_rollup(name: str, conn: Any, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> int:
    if name == "EQUITY_DAILY":
        return _rollup_equity_daily(conn, columns, rows)
    raise RetentionRefused(f"RETENTION_ROLLUP_UNKNOWN:{name}")


# ════════════════════════════════════════════════════════════════════════════════════════════ the run
def retention_days_for(table: str, configured: Optional[Mapping[str, int]] = None) -> int:
    """The raw-detail retention of ``table``: the configured number of days when there is one, else the
    rule's default. Never below the rule's minimum."""
    rule = PRUNE_RULES.get(table)
    if rule is None:
        raise RetentionRefused(f"RETENTION_NO_PRUNE_RULE:{table}")
    days = int((configured or {}).get(table, rule.default_retention_days))
    if days < rule.minimum_retention_days:
        raise RetentionRefused(f"RETENTION_BELOW_MINIMUM:{table}:{days}<{rule.minimum_retention_days}")
    return days


def plan_retention(db: Any, *, table: str, now_ms: int, retention_days: Optional[int] = None,
                   max_rows: int = 20_000) -> Dict[str, Any]:
    """What a run would remove. Reads only."""
    with db.connect() as conn:
        rule = prune_rule(conn, table)
        days = retention_days_for(table, None if retention_days is None else {table: retention_days})
        cutoff = int(now_ms) - days * DAY_MS
        columns, rows = _candidates(conn, rule, cutoff, int(max_rows))
    return {"table": table, "retention_days": days, "cutoff_ms": cutoff, "rows": len(rows),
            "first_rowid": rows[0][0] if rows else None, "last_rowid": rows[-1][0] if rows else None,
            "days": sorted(summarise(rule, columns, rows))}


def run_retention(db: Any, *, table: str, archive_dir: Any, now_ms: int, retention_days: Optional[int] = None,
                  max_rows: int = 20_000, fault: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Remove one batch of expired raw detail from ``table`` (see the module documentation for the order of
    things). Returns ``{"status": "NOOP" | "COMPLETED" | "ALREADY_COMPLETED", ...}``; ``more`` says whether
    another batch is waiting. ``fault`` is a test hook called with the name of each stage just completed."""
    fault = fault or (lambda stage: None)
    if int(max_rows) < 1:
        raise RetentionRefused("RETENTION_BATCH_INVALID")
    started = int(time.time() * 1000)
    with db.connect() as conn:
        rule = prune_rule(conn, table)                   # refuses everything that is not removable raw detail
        days = retention_days_for(table, None if retention_days is None else {table: retention_days})
        cutoff = int(now_ms) - days * DAY_MS
        columns, rows = _candidates(conn, rule, cutoff, int(max_rows) + 1)
        more, rows = len(rows) > int(max_rows), rows[:int(max_rows)]
        if not rows:
            return {"status": "NOOP", "table": table, "retention_days": days, "cutoff_ms": cutoff, "rows_removed": 0,
                    "more": False}
        digest = _rows_digest(rows)
        # The id is a function of the rows, so a run that is repeated after a crash exports to the same file.
        # ``generation`` only moves when identical rows were removed before and are back (a restored backup).
        generation = conn.execute(f"SELECT COUNT(*) FROM {RETENTION_RUN_TABLE} WHERE table_name=? AND detail LIKE ?",
                                  (table, f'%"rows_sha256": "{digest}"%')).fetchone()[0]
    run_id = "ret_" + hashlib.sha256(
        f"{table}|{rows[0][0]}|{rows[-1][0]}|{len(rows)}|{digest}|{generation}".encode("utf-8")).hexdigest()[:24]
    root = Path(archive_dir)
    relative = f"{table}/{run_id}.jsonl.gz"
    path = root / table / f"{run_id}.jsonl.gz"
    summaries = summarise(rule, columns, rows)

    # 2. export: no transaction is open and no lock is held while the file is written
    sha256 = _write_archive(path, {"format": ARCHIVE_FORMAT, "table": table, "columns": list(columns), "rows": len(rows),
                                   "rows_sha256": digest, "run_id": run_id, "retention_version": RETENTION_VERSION}, rows)
    fault("after_export")

    # 3. one transaction: verify, summarise, roll up, delete, record
    rowids = [r[0] for r in rows]
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        done = conn.execute(f"SELECT rows_removed, archive_sha256 FROM {RETENTION_RUN_TABLE} WHERE run_id=?", (run_id,)).fetchone()
        if done is not None:                             # another worker completed this very batch meanwhile
            return {"status": "ALREADY_COMPLETED", "table": table, "run_id": run_id, "rows_removed": 0,
                    "archive_path": relative, "archive_sha256": done[1], "more": more}
        prune_rule(conn, table)                          # under the write lock: still removable, still unguarded
        _, current = _candidates(conn, rule, cutoff, len(rows))
        if [r[0] for r in current] != rowids or _rows_digest(current) != digest:
            raise RetentionConflict("RETENTION_ROWS_CHANGED_SINCE_EXPORT")
        verified = verify_archive(path, sha256=sha256)   # read back from disk: what will remain is what was there
        if (verified["rows"], verified["rows_sha256"]) != (len(rows), digest):
            raise RetentionConflict("RETENTION_ARCHIVE_CONTENT_MISMATCH")
        now = int(time.time() * 1000)
        conn.executemany(
            f"INSERT INTO {RETENTION_SUMMARY_TABLE} (run_id, table_name, bucket, rows, first_at, last_at, summary, recorded_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [(run_id, table, day, s["rows"], s["first_at"], s["last_at"],
              json.dumps({"by": s["by"], "measures": s["measures"]}, sort_keys=True), now) for day, s in sorted(summaries.items())])
        fault("after_summary")
        rolled = _apply_rollup(rule.rollup, conn, columns, rows) if rule.rollup else 0
        removed = 0
        for offset in range(0, len(rowids), 500):
            chunk = rowids[offset:offset + 500]
            removed += conn.execute(f"DELETE FROM {rule.table} WHERE rowid IN ({','.join('?' * len(chunk))})", chunk).rowcount
        if removed != len(rows):
            raise RetentionConflict("RETENTION_DELETE_COUNT_MISMATCH")
        fault("after_delete")
        conn.execute(
            f"INSERT INTO {RETENTION_RUN_TABLE} (run_id, table_name, policy, cutoff_ms, rows_exported, rows_removed, archive_path, "
            "archive_sha256, status, started_at, completed_at, detail) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, table, f"{RAW_OPERATIONAL_DETAIL}/{RETENTION_VERSION}", cutoff, len(rows), removed, relative, sha256,
             "COMPLETED", started, now,
             json.dumps({"retention_days": days, "first_rowid": rowids[0], "last_rowid": rowids[-1], "rows_sha256": digest,
                         "summary_days": len(summaries), "rollup": rule.rollup, "rollup_groups": rolled}, sort_keys=True)))
    fault("after_commit")
    return {"status": "COMPLETED", "table": table, "run_id": run_id, "retention_days": days, "cutoff_ms": cutoff,
            "rows_exported": len(rows), "rows_removed": removed, "archive_path": relative, "archive_sha256": sha256,
            "summary_days": sorted(summaries), "more": more}


def run_all(db: Any, *, archive_dir: Any, now_ms: int, retention_days: Optional[Mapping[str, int]] = None,
            max_rows: int = 20_000, max_batches: int = 50) -> List[Dict[str, Any]]:
    """Every table that has a rule, batch after batch, each batch its own run. Tables that are not in this
    database are skipped."""
    out: List[Dict[str, Any]] = []
    with db.connect() as conn:
        present = set(database_tables(conn))
    for table in sorted(PRUNE_RULES):
        if table not in present:
            continue
        for _ in range(int(max_batches)):
            result = run_retention(db, table=table, archive_dir=archive_dir, now_ms=now_ms,
                                   retention_days=retention_days_for(table, retention_days), max_rows=max_rows)
            out.append(result)
            if not result["more"]:
                break
    return out


def recover(db: Any, archive_dir: Any) -> Dict[str, Any]:
    """After an interrupted cleanup: check every recorded run's archive, name the archives no run recorded
    (an export whose transaction never committed: its rows are still in the database) and clear this module's
    own unfinished ``.tmp`` files. Removes nothing else and changes nothing in the database."""
    root = Path(archive_dir)
    with db.connect() as conn:
        runs = {r[0]: (r[1], r[2], r[3], r[4]) for r in conn.execute(
            f"SELECT run_id, table_name, archive_path, archive_sha256, rows_removed FROM {RETENTION_RUN_TABLE}")}
        report: Dict[str, Any] = {"verified": [], "damaged": [], "orphans": [], "partials_removed": []}
        for run_id, (table, relative, sha256, _) in sorted(runs.items()):
            if not relative:
                continue
            try:
                verify_archive(root / relative, sha256=sha256)
                report["verified"].append(run_id)
            except RetentionConflict as exc:
                report["damaged"].append({"run_id": run_id, "table": table, "reason": str(exc)})
        for table in sorted(PRUNE_RULES):
            folder = root / table
            if not folder.is_dir():
                continue
            for item in sorted(folder.iterdir()):
                if _PARTIAL_NAME.match(item.name) and item.is_file():
                    item.unlink()                        # our own unfinished export: never a complete archive
                    report["partials_removed"].append(f"{table}/{item.name}")
                elif _ARCHIVE_NAME.match(item.name) and item.name[:-len(".jsonl.gz")] not in runs:
                    orphan: Dict[str, Any] = {"archive": f"{table}/{item.name}"}
                    try:
                        _, rows = read_archive(item)
                        ids = [r[0] for r in rows]
                        present = 0
                        for offset in range(0, len(ids), 500):
                            chunk = ids[offset:offset + 500]
                            present += conn.execute(f"SELECT COUNT(*) FROM {table} WHERE rowid IN ({','.join('?' * len(chunk))})",
                                                    chunk).fetchone()[0]
                        orphan.update(rows=len(rows), rows_still_in_database=present)
                    except Exception as exc:  # noqa: BLE001
                        orphan["unreadable"] = type(exc).__name__
                    report["orphans"].append(orphan)
    return report


# ══════════════════════════════════════════════════════════════════════════════════════════ measurement
def database_size(db: Any) -> Dict[str, int]:
    with db.connect() as conn:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        pages = int(conn.execute("PRAGMA page_count").fetchone()[0])
        free = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return {"page_size": page_size, "page_count": pages, "freelist_pages": free, "bytes": pages * page_size,
            "reclaimable_bytes": free * page_size}


def _dbstat_available(conn: Any) -> bool:
    try:
        conn.execute("SELECT 1 FROM dbstat LIMIT 1").fetchone()
        return True
    except Exception:  # noqa: BLE001 -- this SQLite was built without the dbstat table
        return False


def table_sizes(db: Any, tables: Optional[Iterable[str]] = None) -> Dict[str, Dict[str, Any]]:
    """``{table: {"rows", "bytes", "method"}}``. With ``dbstat`` the bytes are the pages of the table and of
    its indexes. Without it they are the summed payload of the rows (``payload_estimate``): no page or index
    overhead, so an under-estimate -- compare it with ``database_size`` for the whole file, which is exact."""
    out: Dict[str, Dict[str, Any]] = {}
    with db.connect() as conn:
        names = [t for t in (list(tables) if tables is not None else database_tables(conn))]
        exact = _dbstat_available(conn)
        pages: Dict[str, int] = {}
        if exact:
            owner = {r[0]: r[1] for r in conn.execute("SELECT name, tbl_name FROM sqlite_master WHERE type IN ('table','index')")}
            for name, size in conn.execute("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name"):
                pages[owner.get(name, name)] = pages.get(owner.get(name, name), 0) + int(size or 0)
        for table in names:
            if not _IDENTIFIER.match(table):
                continue
            rows = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            if exact:
                out[table] = {"rows": rows, "bytes": pages.get(table, 0), "method": "dbstat"}
                continue
            columns = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
            payload = " + ".join(f'COALESCE(LENGTH(CAST("{c}" AS BLOB)), 0)' for c in columns) or "0"
            size = conn.execute(f"SELECT COALESCE(SUM({payload}), 0) FROM {table}").fetchone()[0] if rows else 0
            out[table] = {"rows": rows, "bytes": int(size), "method": "payload_estimate"}
    return out


def growth_between(before: Mapping[str, Mapping[str, Any]], after: Mapping[str, Mapping[str, Any]], *, cycles: int,
                   accounts: int) -> Dict[str, Dict[str, Any]]:
    """Per table that grew: rows and bytes added, per cycle and per account of a cycle. Pure."""
    out: Dict[str, Dict[str, Any]] = {}
    for table, now in sorted(after.items()):
        was = before.get(table, {"rows": 0, "bytes": 0})
        rows, size = int(now["rows"]) - int(was["rows"]), int(now["bytes"]) - int(was["bytes"])
        if rows == 0 and size == 0:
            continue
        out[table] = {"retention_class": POLICY.get(table, "UNCLASSIFIED"), "prunable": table in PRUNE_RULES,
                      "rows_added": rows, "bytes_added": size, "method": now.get("method"),
                      "rows_per_cycle": round(rows / max(1, cycles), 3), "bytes_per_cycle": round(size / max(1, cycles), 1),
                      "rows_per_account_cycle": round(rows / max(1, cycles * accounts), 4),
                      "bytes_per_account_cycle": round(size / max(1, cycles * accounts), 1)}
    return out


def reclaim_space(db: Any) -> Dict[str, Any]:
    """``VACUUM``: give the pages that removals freed back to the file system. An operator's maintenance
    step, never run by a cycle: it rewrites the whole file under an exclusive lock and needs about as much
    free disk as the database is large. Refused while a trading runtime holds this database."""
    import sqlite3
    from datetime import datetime, timezone

    before = database_size(db)
    with db.connect() as conn:
        if "runtime_ownership" in set(database_tables(conn)):
            for (heartbeat,) in conn.execute("SELECT heartbeat_at FROM runtime_ownership WHERE released_at IS NULL"):
                try:
                    beat = datetime.fromisoformat(heartbeat)
                    beat = beat if beat.tzinfo else beat.replace(tzinfo=timezone.utc)
                    alive = (datetime.now(timezone.utc) - beat).total_seconds() < 150
                except (TypeError, ValueError):
                    alive = True                         # a lease that cannot be read is treated as held
                if alive:
                    raise RetentionRefused("RECLAIM_REFUSED_RUNTIME_HOLDS_THE_DATABASE")
    raw = sqlite3.connect(db.path, timeout=10, isolation_level=None)
    try:
        raw.execute("VACUUM")
    finally:
        raw.close()
    after = database_size(db)
    return {"bytes_before": before["bytes"], "bytes_after": after["bytes"], "bytes_returned": before["bytes"] - after["bytes"]}


# ═══════════════════════════════════════════════════════════ forward-looking: repeated evaluations
def evaluation_is_unchanged_repeat(conn: Any, *, account_id: str, result: Mapping[str, Any], now_ms: int,
                                   repeat_interval_ms: int = EVALUATION_REPEAT_INTERVAL_MS) -> bool:
    """Whether ``result`` -- one account's evaluation of one broker cycle -- only repeats the account's last
    stored evaluation and may therefore be written less often.

    ``cati_execution_evaluations`` is append-only by trigger and receives one row per account per cycle, the
    largest growth in the database. It cannot be pruned; it can be *written less*. With ``repeat_interval_ms``
    0 (the default) this function always answers False, which is today's behaviour: everything is written.
    With an interval, an evaluation is a skippable repeat only when ALL of these hold: it concerns the same
    decision, stage, reason and permission as the last stored one; that one is younger than the interval (so
    at least one row per interval is still written, as a heartbeat); and nothing about an order is in it -- no
    stage that is always recorded, no submission, plan, attempt or reservation, no recovery, no execution
    history, no open execution. Any change, any error and any veto that differs is therefore always written.
    """
    if int(repeat_interval_ms) <= 0:
        return False
    stage = result.get("stage") or "EVALUATED"
    if stage in EVALUATION_ALWAYS_RECORDED_STAGES or result.get("orders_submitted") or result.get("execution_portfolio_active"):
        return False
    if any(result.get(key) for key in ("trade_plan_id", "execution_attempt_id", "reservation_id", "broker_order_id",
                                       "boundary", "recovery", "execution_history")):
        return False
    row = conn.execute("SELECT decision_id, stage, reason, execution_permission, observed_at FROM cati_execution_evaluations "
                       "WHERE account_id=? ORDER BY observed_at DESC, rowid DESC LIMIT 1", (account_id,)).fetchone()
    if row is None:
        return False
    decision_id = (result.get("latest_cati_decision") or {}).get("decision_id")
    same = (row[0], row[1], row[2], row[3]) == (decision_id, stage, result.get("reason"), result.get("execution_permission"))
    return bool(same and 0 <= int(now_ms) - int(row[4]) < int(repeat_interval_ms))


__all__ = ["RETENTION_VERSION", "ARCHIVE_FORMAT", "DAY_MS", "IMMUTABLE_GOVERNANCE", "RESEARCH_GOVERNANCE", "EXECUTION_AUDIT",
           "FINANCIAL_RECORD", "RECONCILIATION_STATE", "DECISION_SUMMARY", "CUSTOMER_AND_COMPLIANCE", "REFERENCE_MARKET_DATA",
           "LEGACY_UNMANAGED", "RAW_OPERATIONAL_DETAIL", "CACHE", "ClassRule", "CLASS_RULES", "PROTECTED_CLASSES", "POLICY",
           "PruneRule", "PRUNE_RULES", "EVALUATION_REPEAT_INTERVAL_MS", "EVALUATION_ALWAYS_RECORDED_STAGES",
           "RetentionError", "UnclassifiedTable", "RetentionRefused", "RetentionConflict", "classify", "is_protected",
           "database_tables", "unclassified_tables", "guarded_tables", "policy_report", "prune_rule",
           "accounts_with_open_execution", "read_archive", "verify_archive", "summarise", "retention_days_for",
           "plan_retention", "run_retention", "run_all", "recover", "database_size", "table_sizes", "growth_between",
           "reclaim_space", "evaluation_is_unchanged_repeat"]
