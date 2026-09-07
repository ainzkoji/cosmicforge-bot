from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


MLMode = Literal["shadow", "active", "disabled"]
MLOverviewStatus = Literal[
    "not_ready",
    "collecting_data",
    "ready_for_training",
    "training_in_progress",
    "ready_for_shadow_deployment",
    "ready_for_live_promotion",
]
TrainingGateStatus = Literal["not_ready", "collecting_data", "ready_for_training", "blocked"]
FeatureHealthStatus = Literal["healthy", "partially_missing", "broken"]
DeployedMode = Literal["not_deployed", "shadow", "live", "rolled_back", "rejected"]
MLAlertLevel = Literal["success", "info", "warning", "danger"]
MLActionStatus = Literal["queued", "running", "succeeded", "failed", "blocked", "unsupported"]
MLActionKey = Literal[
    "rebuild_dataset",
    "run_training",
    "run_validation",
    "deploy_shadow",
    "promote_live",
    "rollback_shadow",
    "disable_ml",
]


class MLOverviewResponse(BaseModel):
    ml_enabled: bool
    ml_mode: MLMode
    current_model_version: str | None
    current_threshold: float | None
    current_hard_block_floor: float | None
    model_artifact_path: str | None
    encoder_path: str | None
    metadata_path: str | None
    last_model_load_time: str | None
    last_bot_restart_time: str | None
    current_ml_status: MLOverviewStatus
    runtime_loaded: bool = False
    runtime_load_error: str | None = None
    last_successful_score_timestamp: str | None = None
    contract_version: str | None = None
    schema_hash: str | None = None
    schema_compatible: bool = False
    configured_defaults: dict[str, Any] | None = None


class MLTrainingGateResponse(BaseModel):
    total_linked_completed_trades: int
    required_trades: int
    wins: int
    required_wins: int
    losses: int
    breakeven_trades: int
    excluded_open_positions: int
    trades_with_full_feature_coverage: int
    trades_missing_critical_features: int
    current_win_rate: float
    feature_coverage_pct: float
    linkage_healthy: bool
    label_distribution_single_class: bool
    training_ready: bool
    status: TrainingGateStatus
    dataset_schema_compatible: bool | None = None
    dataset_contract_version: str | None = None
    dataset_schema_hash: str | None = None
    dataset_path: str | None = None
    blocking_reasons: list[str] = []


class MLDashboardSummaryResponse(BaseModel):
    ml_mode: MLMode
    current_model_version: str | None
    total_linked_completed_trades: int
    wins: int
    feature_coverage_pct: float
    linkage_healthy: bool
    training_ready: bool
    status: TrainingGateStatus
    contract_version: str | None = None
    schema_hash: str | None = None
    schema_compatible: bool | None = None


class MLFeatureCompletenessItem(BaseModel):
    feature_name: str
    null_count_recent: int
    null_pct_recent: float
    null_count_lifetime: int
    null_pct_lifetime: float
    last_seen_populated_at: str | None
    status: FeatureHealthStatus


class MLFeatureCompletenessResponse(BaseModel):
    recent_window_size: int
    recent_window_basis: str
    recent_completeness_pct: float
    lifetime_completeness_pct: float
    features: list[MLFeatureCompletenessItem]
    broken_feature_count: int
    partially_missing_feature_count: int


class MLLinkageHealthResponse(BaseModel):
    post_fix_start: str | None
    total_post_fix_fills: int
    fills_with_non_null_run_id: int
    fills_with_non_null_cycle_id: int
    fills_with_non_null_position_id: int
    run_id_coverage_pct: float | None = None
    cycle_id_coverage_pct: float | None = None
    position_id_coverage_pct: float | None = None
    fully_linked_completed_trades: int
    fully_linked_completed_trades_pct: float
    orphan_open_fills: int
    unmatched_close_fills: int
    linkage_healthy: bool
    scope: str | None = None


class MLActivityActionCounts(BaseModel):
    key: str
    allow_count: int
    shadow_count: int
    block_count: int
    skip_count: int


class MLActivityScoreBucket(BaseModel):
    bucket: str
    count: int


class MLActivityRow(BaseModel):
    timestamp: str
    symbol: str | None
    side: str | None
    ml_score: float | None
    ml_action: str | None
    ml_model_version: str | None
    threshold: float | None
    regime: str | None
    session: str
    linkage_status: str


class MLActivityResponse(BaseModel):
    window_days: int
    page: int
    page_size: int
    total_recent_rows: int
    total_ml_scored_entries: int
    allow_count: int
    shadow_count: int
    block_count: int
    skip_count: int
    average_ml_score: float | None
    current_threshold: float | None
    current_hard_floor: float | None
    score_distribution: list[MLActivityScoreBucket]
    per_symbol_actions: list[MLActivityActionCounts]
    per_regime_actions: list[MLActivityActionCounts]
    per_session_actions: list[MLActivityActionCounts]
    recent_activity_rows: list[MLActivityRow]


class MLDecisionGroupStats(BaseModel):
    count: int
    wins: int
    losses: int
    breakevens: int
    total_pnl: float
    average_pnl: float


class MLShadowPerformanceResponse(BaseModel):
    window_days: int
    total_linked_completed_trades_with_ml_attribution: int
    decision_groups: dict[str, MLDecisionGroupStats]
    good_allows: int
    bad_allows: int
    good_blocks: int
    bad_blocks: int
    classification_logic: str


class MLValidationHistoryItem(BaseModel):
    model_version: str
    training_date: str | None
    dataset_used: str | None
    train_rows: int | None
    test_rows: int | None
    train_auc: float | None
    test_auc: float | None
    validation_method: str | None
    notes: str | None
    verdict: str
    deployed_mode: DeployedMode


class MLValidationHistoryResponse(BaseModel):
    items: list[MLValidationHistoryItem]
    source_note: str


class MLDroppedRowReason(BaseModel):
    reason: str
    count: int


class MLDatasetLabelDistribution(BaseModel):
    wins: int
    losses: int
    breakevens: int
    single_class: bool


class MLDatasetBuilderStatusResponse(BaseModel):
    dataset_source_date_range: dict[str, Any] | None
    linked_trade_count: int
    fully_usable_rows: int
    dropped_rows: int
    dropped_row_reasons: list[MLDroppedRowReason]
    feature_completeness_status: str
    label_distribution: MLDatasetLabelDistribution
    last_dataset_build_time: str | None
    last_dataset_path: str | None
    rebuild_dataset_allowed: bool
    source_note: str
    contract_version: str | None = None
    schema_hash: str | None = None
    schema_compatible: bool | None = None
    feature_null_counts: dict[str, Any] | None = None
    label_null_counts: dict[str, Any] | None = None
    class_balance: dict[str, Any] | None = None


class MLAlertItem(BaseModel):
    code: str
    level: MLAlertLevel
    title: str
    body: str


class MLAlertsResponse(BaseModel):
    generated_at: str
    items: list[MLAlertItem]


class MLActionDefinition(BaseModel):
    action_key: MLActionKey
    label: str
    supported: bool
    allowed: bool
    blocked_reason: str | None
    dangerous: bool
    requires_confirmation: bool
    confirmation_phrase: str
    dataset_path: str | None
    target_model_version: str | None
    log_path: str | None


class MLActionRun(BaseModel):
    id: str
    action_key: MLActionKey
    requested_by_admin_id: str | None
    requested_by_email: str | None
    note: str | None
    status: MLActionStatus
    reason: str | None
    supported: bool
    dataset_path: str | None
    target_model_version: str | None
    log_path: str | None
    created_at: str | None
    updated_at: str | None
    started_at: str | None
    finished_at: str | None
    result: Any = None
    log_tail: list[str]


class MLControlPanelResponse(BaseModel):
    readiness_status: TrainingGateStatus
    training_allowed_right_now: bool
    current_dataset_path: str | None
    target_output_model_version: str
    last_training_run_status: MLActionStatus | None
    last_training_run_logs: list[str]
    last_dataset_rebuild_status: MLActionStatus | None
    last_validation_run_status: MLActionStatus | None
    actions: list[MLActionDefinition]
    recent_action_runs: list[MLActionRun]


class MLDriftDistributionItem(BaseModel):
    key: str
    count: int
    pct: float
    average_pnl: float | None = None


class MLScoreBandPnlItem(BaseModel):
    bucket: str
    count: int
    average_pnl: float


class MLDriftMonitoringResponse(BaseModel):
    window_days: int
    live_win_rate: float | None
    historical_win_rate: float | None
    win_rate_delta: float | None
    live_score_distribution: list[MLActivityScoreBucket]
    training_score_distribution: list[MLActivityScoreBucket]
    symbol_distribution: list[MLDriftDistributionItem]
    regime_distribution: list[MLDriftDistributionItem]
    session_distribution: list[MLDriftDistributionItem]
    average_pnl_by_regime: list[dict[str, Any]]
    average_pnl_by_symbol: list[dict[str, Any]]
    average_pnl_by_score_band: list[MLScoreBandPnlItem]
    source_note: str


class MLActionRequest(BaseModel):
    confirmation_phrase: str
    note: str | None = None


class MLDashboardResponse(BaseModel):
    overview: MLOverviewResponse
    training_gate: MLTrainingGateResponse
    feature_completeness: dict[str, Any]
    linkage_health: MLLinkageHealthResponse
    activity_summary: dict[str, Any]
    shadow_performance: dict[str, Any]
    validation_history: dict[str, Any]
    dataset_builder_status: MLDatasetBuilderStatusResponse
    alerts: MLAlertsResponse
    control_panel: MLControlPanelResponse
    drift_monitoring: dict[str, Any]
