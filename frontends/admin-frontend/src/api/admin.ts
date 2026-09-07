// Admin API Client Functions
import { adminBotMonitorApiClient, adminDashboardApiClient, adminMLApiClient, adminProfitabilityApiClient, adminRevenueApiClient, adminTradingViewApiClient, apiClient } from "./client";

export type MLMode = "shadow" | "active" | "disabled";
export type MLOverviewStatus =
    | "not_ready"
    | "collecting_data"
    | "ready_for_training"
    | "training_in_progress"
    | "ready_for_shadow_deployment"
    | "ready_for_live_promotion";
export type MLTrainingGateStatus = "not_ready" | "collecting_data" | "ready_for_training" | "blocked";
export type MLFeatureHealthStatus = "healthy" | "partially_missing" | "broken";
export type MLValidationDeployedMode = "not_deployed" | "shadow" | "live" | "rolled_back" | "rejected";
export type MLAlertLevel = "success" | "info" | "warning" | "danger";
export type MLActionStatus = "queued" | "running" | "succeeded" | "failed" | "blocked" | "unsupported";
export type MLActionKey =
    | "rebuild_dataset"
    | "run_training"
    | "run_validation"
    | "deploy_shadow"
    | "promote_live"
    | "rollback_shadow"
    | "disable_ml";

export interface MLOverviewResponse {
    ml_enabled: boolean;
    ml_mode: MLMode;
    current_model_version: string | null;
    current_threshold: number | null;
    current_hard_block_floor: number | null;
    model_artifact_path: string | null;
    encoder_path: string | null;
    metadata_path: string | null;
    last_model_load_time: string | null;
    last_bot_restart_time: string | null;
    current_ml_status: MLOverviewStatus;
}

export interface MLTrainingGateResponse {
    total_linked_completed_trades: number;
    required_trades: number;
    wins: number;
    required_wins: number;
    losses: number;
    breakeven_trades: number;
    excluded_open_positions: number;
    trades_with_full_feature_coverage: number;
    trades_missing_critical_features: number;
    current_win_rate: number;
    feature_coverage_pct: number;
    linkage_healthy: boolean;
    label_distribution_single_class: boolean;
    training_ready: boolean;
    status: MLTrainingGateStatus;
}

export interface MLDashboardSummaryResponse {
    ml_mode: MLMode;
    current_model_version: string | null;
    total_linked_completed_trades: number;
    wins: number;
    feature_coverage_pct: number;
    linkage_healthy: boolean;
    training_ready: boolean;
    status: MLTrainingGateStatus;
}

export interface MLFeatureCompletenessItem {
    feature_name: string;
    null_count_recent: number;
    null_pct_recent: number;
    null_count_lifetime: number;
    null_pct_lifetime: number;
    last_seen_populated_at: string | null;
    status: MLFeatureHealthStatus;
}

export interface MLFeatureCompletenessResponse {
    recent_window_size: number;
    recent_window_basis: string;
    recent_completeness_pct: number;
    lifetime_completeness_pct: number;
    features: MLFeatureCompletenessItem[];
    broken_feature_count: number;
    partially_missing_feature_count: number;
}

export interface MLLinkageHealthResponse {
    post_fix_start: string | null;
    total_post_fix_fills: number;
    fills_with_non_null_run_id: number;
    fills_with_non_null_cycle_id: number;
    fills_with_non_null_position_id: number;
    fully_linked_completed_trades: number;
    fully_linked_completed_trades_pct: number;
    orphan_open_fills: number;
    unmatched_close_fills: number;
    linkage_healthy: boolean;
}

export interface MLActivityScoreBucket {
    bucket: string;
    count: number;
}

export interface MLActivityActionCounts {
    key: string;
    allow_count: number;
    shadow_count: number;
    block_count: number;
    skip_count: number;
}

export interface MLActivityRow {
    timestamp: string;
    symbol: string | null;
    side: string | null;
    ml_score: number | null;
    ml_action: string | null;
    ml_model_version: string | null;
    threshold: number | null;
    regime: string | null;
    session: string;
    linkage_status: string;
}

export interface MLActivityResponse {
    window_days: number;
    page: number;
    page_size: number;
    total_recent_rows: number;
    total_ml_scored_entries: number;
    allow_count: number;
    shadow_count: number;
    block_count: number;
    skip_count: number;
    average_ml_score: number | null;
    current_threshold: number | null;
    current_hard_floor: number | null;
    score_distribution: MLActivityScoreBucket[];
    per_symbol_actions: MLActivityActionCounts[];
    per_regime_actions: MLActivityActionCounts[];
    per_session_actions: MLActivityActionCounts[];
    recent_activity_rows: MLActivityRow[];
}

export interface MLDecisionGroupStats {
    count: number;
    wins: number;
    losses: number;
    breakevens: number;
    total_pnl: number;
    average_pnl: number;
}

export interface MLShadowPerformanceResponse {
    window_days: number;
    total_linked_completed_trades_with_ml_attribution: number;
    decision_groups: Record<string, MLDecisionGroupStats>;
    good_allows: number;
    bad_allows: number;
    good_blocks: number;
    bad_blocks: number;
    classification_logic: string;
}

export interface MLValidationHistoryItem {
    model_version: string;
    training_date: string | null;
    dataset_used: string | null;
    train_rows: number | null;
    test_rows: number | null;
    train_auc: number | null;
    test_auc: number | null;
    validation_method: string | null;
    notes: string | null;
    verdict: string;
    deployed_mode: MLValidationDeployedMode;
}

export interface MLValidationHistoryResponse {
    items: MLValidationHistoryItem[];
    source_note: string;
}

export interface MLDroppedRowReason {
    reason: string;
    count: number;
}

export interface MLDatasetLabelDistribution {
    wins: number;
    losses: number;
    breakevens: number;
    single_class: boolean;
}

export interface MLDatasetBuilderStatusResponse {
    dataset_source_date_range: Record<string, unknown> | null;
    linked_trade_count: number;
    fully_usable_rows: number;
    dropped_rows: number;
    dropped_row_reasons: MLDroppedRowReason[];
    feature_completeness_status: string;
    label_distribution: MLDatasetLabelDistribution;
    last_dataset_build_time: string | null;
    last_dataset_path: string | null;
    rebuild_dataset_allowed: boolean;
    source_note: string;
}

export interface MLAlertItem {
    code: string;
    level: MLAlertLevel;
    title: string;
    body: string;
}

export interface MLAlertsResponse {
    generated_at: string;
    items: MLAlertItem[];
}

export type TradingViewMode =
    | "ADVISORY_ONLY"
    | "EXTERNAL_SIGNAL_CANDIDATE"
    | "CONFIRMATION_ONLY"
    | "DISABLED";

export type TradingViewAlertStatus =
    | "ACCEPTED_ADVISORY"
    | "REJECTED"
    | "DUPLICATE"
    | "STALE"
    | "DISABLED_WEBHOOK"
    | "INVALID_SIGNATURE"
    | "INVALID_TOKEN"
    | "INVALID_SCHEMA"
    | "UNSUPPORTED_ACTION"
    | "UNSUPPORTED_SYMBOL"
    | "RATE_LIMITED";

export interface TradingViewWebhook {
    id: string;
    bot_id: string;
    name: string;
    mode: TradingViewMode;
    is_enabled: number;
    allowed_symbols: string[] | null;
    allowed_actions: string[];
    max_alert_age_seconds: number;
    rate_limit_per_minute: number;
    created_at: string;
    updated_at: string;
    last_used_at: string | null;
}

export interface TradingViewAlert {
    id: number;
    webhook_id: string | null;
    bot_id: string | null;
    alert_id: string | null;
    symbol_raw: string | null;
    symbol_normalized: string | null;
    action: string | null;
    side: string | null;
    timeframe: string | null;
    strategy_name: string | null;
    price: number | null;
    received_at: string;
    alert_timestamp: string | null;
    status: TradingViewAlertStatus;
    reject_reason: string | null;
    idempotency_key: string | null;
    source_ip: string | null;
    signature_valid: number | null;
    created_at: string;
}

export interface TradingViewDecision {
    id: number;
    alert_id: number;
    bot_id: string | null;
    symbol: string | null;
    action: string | null;
    mode: TradingViewMode;
    normalized_signal?: Record<string, unknown>;
    event_filter_result: string | null;
    policy_result: string | null;
    sizing_result: string | null;
    execution_result: string | null;
    decision_trace_id: string | null;
    final_status: string;
    final_reason: string;
    queue_id: string | null;
    created_at: string;
}

export interface ExternalSignalQueueRow {
    id: string;
    source: string;
    source_alert_id: string;
    bot_id: string;
    symbol: string;
    side: string | null;
    action: string;
    confidence: number | null;
    status: "PENDING" | "CLAIMED" | "PROCESSED" | "REJECTED" | "EXPIRED" | "DUPLICATE" | "FAILED";
    available_at: string;
    expires_at: string;
    claimed_at: string | null;
    processed_at: string | null;
    result: string | null;
    result_json?: Record<string, unknown> | null;
    created_at: string;
}

export interface TradingViewListResponse<T> {
    items: T[];
}

export interface MLActionDefinition {
    action_key: MLActionKey;
    label: string;
    supported: boolean;
    allowed: boolean;
    blocked_reason: string | null;
    dangerous: boolean;
    requires_confirmation: boolean;
    confirmation_phrase: string;
    dataset_path: string | null;
    target_model_version: string | null;
    log_path: string | null;
}

export interface MLActionRun {
    id: string;
    action_key: MLActionKey;
    requested_by_admin_id: string | null;
    requested_by_email: string | null;
    note: string | null;
    status: MLActionStatus;
    reason: string | null;
    supported: boolean;
    dataset_path: string | null;
    target_model_version: string | null;
    log_path: string | null;
    created_at: string | null;
    updated_at: string | null;
    started_at: string | null;
    finished_at: string | null;
    result: unknown;
    log_tail: string[];
}

export interface MLControlPanelResponse {
    readiness_status: MLTrainingGateStatus;
    training_allowed_right_now: boolean;
    current_dataset_path: string | null;
    target_output_model_version: string;
    last_training_run_status: MLActionStatus | null;
    last_training_run_logs: string[];
    last_dataset_rebuild_status: MLActionStatus | null;
    last_validation_run_status: MLActionStatus | null;
    actions: MLActionDefinition[];
    recent_action_runs: MLActionRun[];
}

export interface MLDriftDistributionItem {
    key: string;
    count: number;
    pct: number;
    average_pnl: number | null;
}

export interface MLScoreBandPnlItem {
    bucket: string;
    count: number;
    average_pnl: number;
}

export interface MLDriftMonitoringResponse {
    window_days: number;
    live_win_rate: number | null;
    historical_win_rate: number | null;
    win_rate_delta: number | null;
    live_score_distribution: MLActivityScoreBucket[];
    training_score_distribution: MLActivityScoreBucket[];
    symbol_distribution: MLDriftDistributionItem[];
    regime_distribution: MLDriftDistributionItem[];
    session_distribution: MLDriftDistributionItem[];
    average_pnl_by_regime: Array<{ key: string; average_pnl: number; count: number }>;
    average_pnl_by_symbol: Array<{ key: string; average_pnl: number; count: number }>;
    average_pnl_by_score_band: MLScoreBandPnlItem[];
    source_note: string;
}

export interface TriggerMLActionRequest {
    confirmation_phrase: string;
    note?: string;
}

export interface MLDashboardFeatureSummary {
    recent_completeness_pct: number;
    lifetime_completeness_pct: number;
    broken_feature_count: number;
    partially_missing_feature_count: number;
}

export interface MLDashboardActivitySummary {
    window_days: number;
    total_ml_scored_entries: number;
    allow_count: number;
    shadow_count: number;
    block_count: number;
    skip_count: number;
    average_ml_score: number | null;
    current_threshold: number | null;
    current_hard_floor: number | null;
    recent_activity_rows: MLActivityRow[];
}

export interface MLDashboardShadowSummary {
    window_days: number;
    total_linked_completed_trades_with_ml_attribution: number;
    decision_groups: Record<string, MLDecisionGroupStats>;
    good_allows: number;
    bad_allows: number;
    good_blocks: number;
    bad_blocks: number;
}

export interface MLDashboardValidationSummary {
    total_models: number;
    latest_model: MLValidationHistoryItem | null;
    source_note: string;
}

export interface MLDashboardResponse {
    overview: MLOverviewResponse;
    training_gate: MLTrainingGateResponse;
    feature_completeness: MLDashboardFeatureSummary;
    linkage_health: MLLinkageHealthResponse;
    activity_summary: MLDashboardActivitySummary;
    shadow_performance: MLDashboardShadowSummary;
    validation_history: MLDashboardValidationSummary;
    dataset_builder_status: MLDatasetBuilderStatusResponse;
    alerts: MLAlertsResponse;
    control_panel: MLControlPanelResponse;
    drift_monitoring: {
        window_days: number;
        live_win_rate: number | null;
        historical_win_rate: number | null;
        win_rate_delta: number | null;
        symbol_distribution: MLDriftDistributionItem[];
        regime_distribution: MLDriftDistributionItem[];
        session_distribution: MLDriftDistributionItem[];
        average_pnl_by_score_band: MLScoreBandPnlItem[];
        source_note: string;
    };
}

export interface ProfitabilityTrade {
    id: number | null;
    timestamp_utc: string | null;
    symbol: string | null;
    side: string | null;
    position_id: string | null;
    realized_pnl: number | null;
    r_multiple: number | null;
    exit_reason: string | null;
    trigger_source: string | null;
}

export interface ProfitabilityOverall {
    total_fills: number;
    total_trades: number;
    closed_trades: number;
    open_trades: number;
    raw_open_fills: number;
    raw_close_fills: number;
    position_linked_closed_trades: number;
    total_realized_pnl: number;
    win_rate_pct: number | null;
    average_win: number | null;
    average_loss: number | null;
    profit_factor: number | "inf" | null;
    average_r_multiple: number | null;
    best_trade: ProfitabilityTrade | null;
    worst_trade: ProfitabilityTrade | null;
}

export interface ProfitabilitySymbolRow {
    symbol: string;
    trades: number;
    win_rate_pct: number | null;
    total_pnl: number;
    average_pnl: number | null;
    average_r_multiple: number | null;
    sl_count: number;
    tp_count: number;
    time_exit_count: number;
    other_count: number;
}

export interface ProfitabilityRecentWindow {
    closed_trades: number;
    total_realized_pnl: number;
    win_rate_pct: number | null;
    average_pnl: number | null;
    profit_factor: number | "inf" | null;
}

export interface ProfitabilityRiskQuality {
    duplicate_order_id_action_symbol_groups: number;
    duplicate_exact_fill_groups: number;
    missing_run_id_count: number;
    missing_position_id_count: number;
    closed_fills_null_exit_reason: number;
    closed_fills_null_r_multiple: number;
    average_slippage_pct: number | null;
    biggest_abs_slippage_pct: number | null;
    biggest_slippage_fill: ProfitabilityTrade | null;
}

export interface SizingCapEvent {
    trace_id: string | null;
    timestamp_utc: string | null;
    symbol: string | null;
    position_id: string | null;
    signal: string | null;
    confidence: number | null;
    allocation_type: string | null;
    user_fixed_margin_usdt: number | null;
    base_margin_usdt: number | null;
    final_margin_usdt: number | null;
    base_notional_usdt: number | null;
    final_notional_usdt: number | null;
    leverage: number | null;
    cap_applied: boolean;
    cap_reason: string | null;
    atr_cap_margin_usdt: number | null;
    account_risk_pct: number | null;
    stop_distance_pct: number | null;
    theoretical_risk_usdt: number | null;
    theoretical_risk_pct: number | null;
    risk_warning: boolean;
    max_risk_capital: number | null;
    risk_level: string | null;
    risk_level_label: string | null;
    sizing_method: string | null;
    admin_message: string | null;
}

export interface ProfitabilityReportResponse {
    generated_at: string;
    scope: string;
    overall: ProfitabilityOverall;
    per_symbol: ProfitabilitySymbolRow[];
    recent: Record<"last_24h" | "last_48h" | "last_7d", ProfitabilityRecentWindow>;
    risk_execution_quality: ProfitabilityRiskQuality;
    sizing_cap_events: SizingCapEvent[];
}

export interface RevenueOverviewItem {
    date: string;
    subscription_revenue: number;
    commission_revenue: number;
    total_revenue: number;
}

export interface RevenueOverviewResponse {
    data: RevenueOverviewItem[];
}

export interface TradingPairDataPoint {
    symbol: string;
    trade_count: number;
    percentage: number;
}

export interface TopTradingPairsResponse {
    data: TradingPairDataPoint[];
}

// Dashboard Stats
export async function getAdminDashboardStats() {
    const response = await adminDashboardApiClient.get("/api/admin/dashboard/stats");
    return response.data;
}

export async function getRevenueOverview(timeframe: string = "12m") {
    const response = await adminDashboardApiClient.get<RevenueOverviewResponse>("/api/admin/dashboard/revenue-overview", {
        params: { timeframe }
    });
    return response.data;
}

export async function getTopTradingPairs() {
    const response = await adminDashboardApiClient.get<TopTradingPairsResponse>("/api/admin/dashboard/top-trading-pairs");
    return response.data;
}

// User Management
export async function listUsers(status?: string, limit: number = 50) {
    const response = await adminDashboardApiClient.get("/api/admin/users", {
        params: { status, limit }
    });
    return response.data;
}

export async function getUserDetails(userId: string) {
    const response = await apiClient.get(`/api/admin/users/${userId}`);
    return response.data;
}

export async function suspendUser(userId: string) {
    const response = await apiClient.post(`/api/admin/users/${userId}/suspend`);
    return response.data;
}

export async function activateUser(userId: string) {
    const response = await apiClient.post(`/api/admin/users/${userId}/activate`);
    return response.data;
}

// Revenue Analytics
export async function getRevenueAnalytics() {
    const response = await adminRevenueApiClient.get("/api/admin/revenue/overview");
    return response.data;
}

export async function getProfitabilityReport() {
    const response = await adminProfitabilityApiClient.get<ProfitabilityReportResponse>("/api/admin/profitability/report");
    return response.data;
}

// ── Affiliate & Broker Revenue ──────────────────────────────────────────────

export interface AffiliateSettings {
    id?: string;
    referral_commission_rate: number;
    cookie_duration_days: number;
    minimum_payout_amount: number;
    minimum_payout_currency: string;
    tiered_referrals_enabled: boolean;
    attribution_model: string;
    is_enabled: boolean;
    updated_at?: string | null;
}

export async function getAffiliateSettings(): Promise<AffiliateSettings> {
    const response = await apiClient.get<AffiliateSettings>("/api/admin/affiliate/settings");
    return response.data;
}

export async function updateAffiliateSettings(data: Omit<AffiliateSettings, "id" | "updated_at">): Promise<AffiliateSettings> {
    const response = await apiClient.put<AffiliateSettings>("/api/admin/affiliate/settings", data);
    return response.data;
}

// Audit Logs
export async function getAuditLogs(eventType?: string, limit: number = 100, source: string = "all") {
    const response = await apiClient.get("/api/admin/audit-logs", {
        params: { event_type: eventType, limit, source }
    });
    return response.data;
}

// Bot Monitoring
function asNumber(value: unknown, fallback = 0): number {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function asString(value: unknown, fallback = "unknown"): string {
    return value === null || value === undefined || value === "" ? fallback : String(value);
}

function parseJsonObject(value: unknown): Record<string, unknown> {
    if (!value) return {};
    if (typeof value === "object" && !Array.isArray(value)) return value as Record<string, unknown>;
    try {
        const parsed = JSON.parse(String(value));
        return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch {
        return {};
    }
}

function normalizeBotOverview(raw: any = {}) {
    return {
        ...raw,
        status: asString(raw.status, "unknown"),
        uptime_seconds: asNumber(raw.uptime_seconds),
        active_run_id: raw.active_run_id ?? null,
        active_positions: asNumber(raw.active_positions),
        daily_pnl: asNumber(raw.daily_pnl),
        daily_trades: asNumber(raw.daily_trades),
        recent_events_1h: asNumber(raw.recent_events_1h),
    };
}

function normalizeBotRun(raw: any = {}) {
    const startedAt = raw.started_at ?? raw.start_time ?? null;
    const stoppedAt = raw.stopped_at ?? raw.end_time ?? null;
    const trades = raw.trades ?? raw.trade_count ?? raw.total_trades ?? 0;
    const pnl = raw.realized_pnl ?? raw.pnl ?? 0;
    return {
        ...raw,
        run_id: asString(raw.run_id, "n/a"),
        started_at: startedAt,
        stopped_at: stoppedAt,
        start_time: startedAt,
        end_time: stoppedAt,
        mode: asString(raw.mode, "unknown"),
        status: asString(raw.status, "unknown"),
        trades: asNumber(trades),
        trade_count: asNumber(trades),
        realized_pnl: asNumber(pnl),
        pnl: asNumber(pnl),
    };
}

function normalizeBotRunSummary(raw: any = {}) {
    const totalTrades = raw.total_trades ?? raw.trades ?? raw.trade_count ?? 0;
    const pnl = raw.pnl ?? raw.realized_pnl ?? 0;
    const wins = asNumber(raw.win_trades);
    const losses = asNumber(raw.loss_trades);
    const countedTrades = wins + losses;
    const winRate = raw.win_rate ?? (countedTrades > 0 ? (wins / countedTrades) * 100 : 0);
    return {
        ...raw,
        total_trades: asNumber(totalTrades),
        trade_count: asNumber(totalTrades),
        trades: asNumber(totalTrades),
        pnl: asNumber(pnl),
        realized_pnl: asNumber(pnl),
        win_rate: asNumber(winRate),
        volume: asNumber(raw.volume),
    };
}

function normalizeBotEvent(raw: any = {}) {
    const createdAt = raw.created_at ?? raw.timestamp_utc ?? null;
    const details = raw.details ?? raw.details_json ?? "";
    return {
        ...raw,
        created_at: createdAt,
        timestamp_utc: raw.timestamp_utc ?? createdAt,
        event_type: asString(raw.event_type, "unknown"),
        details: typeof details === "string" ? details : JSON.stringify(details),
    };
}

function normalizeBotDecision(raw: any = {}) {
    const createdAt = raw.created_at ?? raw.ts ?? null;
    const action = raw.action ?? raw.intended_action ?? raw.signal ?? "unknown";
    const reason = raw.reason ?? raw.execution_status ?? raw.final_position ?? "n/a";
    return {
        ...raw,
        created_at: createdAt,
        ts: raw.ts ?? createdAt,
        symbol: raw.symbol ?? "n/a",
        action: asString(action, "unknown"),
        price: raw.price ?? null,
        reason: asString(reason, "n/a"),
        confidence: asNumber(raw.confidence),
    };
}

function normalizeBotRunsResponse(raw: any = {}) {
    const rows = Array.isArray(raw) ? raw : raw.runs ?? [];
    const runs = rows.map(normalizeBotRun);
    return {
        ...(Array.isArray(raw) ? {} : raw),
        runs,
        count: asNumber(raw.count, runs.length),
    };
}

function normalizeBotLiveTelemetry(raw: any = {}) {
    return {
        ...raw,
        positions: Array.isArray(raw.positions) ? raw.positions : [],
        latest_decisions: (raw.latest_decisions ?? []).map(normalizeBotDecision),
        latest_events: (raw.latest_events ?? []).map(normalizeBotEvent),
    };
}

function normalizeBotRunDetails(raw: any = {}) {
    const run = normalizeBotRun(raw.run ?? {});
    const summary = normalizeBotRunSummary(raw.summary ?? {});
    return {
        ...raw,
        run: {
            ...run,
            config: raw.run?.config ?? parseJsonObject(raw.run?.config_json),
        },
        summary,
        events: (raw.events ?? []).map(normalizeBotEvent),
        traces: (raw.traces ?? []).map(normalizeBotDecision),
    };
}

export async function getBotOverview() {
    const response = await adminBotMonitorApiClient.get("/api/admin/bot/overview");
    return normalizeBotOverview(response.data);
}

export async function getBotRuns(limit: number = 20) {
    const response = await adminBotMonitorApiClient.get("/api/admin/bot/runs", {
        params: { limit }
    });
    return normalizeBotRunsResponse(response.data);
}

export async function getBotRunDetails(runId: string) {
    const response = await adminBotMonitorApiClient.get(`/api/admin/bot/runs/${runId}`);
    return normalizeBotRunDetails(response.data);
}

export async function getBotLiveTelemetry() {
    const response = await adminBotMonitorApiClient.get("/api/admin/bot/live");
    return normalizeBotLiveTelemetry(response.data);
}

// Compliance
export async function getPendingKYC() {
    const response = await apiClient.get("/api/admin/compliance/kyc-pending");
    return response.data;
}

export async function getAMLFlags() {
    const response = await apiClient.get("/api/admin/compliance/aml-flags");
    return response.data;
}

export async function approveKYC(submissionId: string) {
    const response = await apiClient.post(`/api/admin/compliance/kyc/${submissionId}/approve`);
    return response.data;
}

export async function rejectKYC(submissionId: string, reason: string) {
    const response = await apiClient.post(`/api/admin/compliance/kyc/${submissionId}/reject`, { reason });
    return response.data;
}

// System Settings
export async function getSystemSettings() {
    const response = await apiClient.get("/api/admin/settings");
    return response.data;
}

export async function updateSystemSettings(settings: any) {
    const response = await apiClient.put("/api/admin/settings", settings);
    return response.data;
}

// ML Admin
export async function getMLDashboardSummary() {
    const response = await adminMLApiClient.get<MLDashboardSummaryResponse>("/api/admin/ml/dashboard-summary");
    return response.data;
}

export async function getMLOverview() {
    const response = await adminMLApiClient.get<MLOverviewResponse>("/api/admin/ml/overview");
    return response.data;
}

export async function getMLTrainingGate() {
    const response = await adminMLApiClient.get<MLTrainingGateResponse>("/api/admin/ml/training-gate");
    return response.data;
}

export async function getMLFeatureCompleteness(recentLimit: number = 500) {
    const response = await adminMLApiClient.get<MLFeatureCompletenessResponse>("/api/admin/ml/feature-completeness", {
        params: { recent_limit: recentLimit },
    });
    return response.data;
}

export async function getMLLinkageHealth() {
    const response = await adminMLApiClient.get<MLLinkageHealthResponse>("/api/admin/ml/linkage-health");
    return response.data;
}

export interface GetMLActivityParams {
    days?: number;
    page?: number;
    pageSize?: number;
}

export async function getMLActivity(params: GetMLActivityParams = {}) {
    const response = await adminMLApiClient.get<MLActivityResponse>("/api/admin/ml/activity", {
        params: {
            days: params.days ?? 30,
            page: params.page ?? 1,
            page_size: params.pageSize ?? 20,
        },
    });
    return response.data;
}

export async function getMLShadowPerformance(days: number = 90) {
    const response = await adminMLApiClient.get<MLShadowPerformanceResponse>("/api/admin/ml/shadow-performance", {
        params: { days },
    });
    return response.data;
}

export async function getMLValidationHistory(limit: number = 10) {
    const response = await adminMLApiClient.get<MLValidationHistoryResponse>("/api/admin/ml/validation-history", {
        params: { limit },
    });
    return response.data;
}

export async function getMLDatasetBuilderStatus() {
    const response = await adminMLApiClient.get<MLDatasetBuilderStatusResponse>("/api/admin/ml/dataset-builder-status");
    return response.data;
}

export async function getMLAlerts() {
    const response = await adminMLApiClient.get<MLAlertsResponse>("/api/admin/ml/alerts");
    return response.data;
}

export async function getMLControlPanel() {
    const response = await adminMLApiClient.get<MLControlPanelResponse>("/api/admin/ml/control-panel");
    return response.data;
}

export async function getMLDriftMonitoring(days: number = 30) {
    const response = await adminMLApiClient.get<MLDriftMonitoringResponse>("/api/admin/ml/drift-monitoring", {
        params: { days },
    });
    return response.data;
}

export async function triggerMLAction(actionKey: MLActionKey, payload: TriggerMLActionRequest) {
    const response = await apiClient.post<MLActionRun>(`/api/admin/ml/actions/${actionKey}`, payload);
    return response.data;
}

export async function getMLDashboard() {
    const response = await adminMLApiClient.get<MLDashboardResponse>("/api/admin/ml/dashboard");
    return response.data;
}

// ── FIX-F: ML Runtime Status ──────────────────────────────────────────────────

export interface MLPhaseIChecklistItem {
    id: string;
    label: string;
    met: boolean;
    detail: string;
    category: "data" | "execution" | "accumulation" | "performance" | "ml" | "safety" | "visibility";
}

export interface MLPhaseIReadiness {
    checklist: MLPhaseIChecklistItem[];
    met_count: number;
    total_count: number;
    overall_ready: boolean;
    overall_note: string;
}

export interface MLRuntimeStatusResponse {
    // Safety fields
    ml_enabled: boolean;
    ml_shadow_mode: boolean;
    safe_state_confirmed: boolean;
    safe_state_note: string;
    // Runtime identity
    ml_mode: MLMode;
    current_ml_status: MLOverviewStatus;
    model_version: string | null;
    contract_version: string;
    // Artifact paths
    model_artifact_path: string | null;
    encoder_path: string | null;
    metadata_path: string | null;
    // Load state
    model_load_error: string | null;
    last_model_load_time: string | null;
    last_score_timestamp: string | null;
    last_bot_restart_time: string | null;
    // Threshold
    current_threshold: number | null;
    current_hard_block_floor: number | null;
    // Document 1 readiness checklist
    phase_i_readiness: MLPhaseIReadiness;
    // Snapshot metadata
    snapshot_missing: boolean;
    snapshot_stale: boolean;
    generated_at: string;
    source_note: string;
}

export async function getMLRuntimeStatus() {
    const response = await adminMLApiClient.get<MLRuntimeStatusResponse>("/api/admin/ml/runtime-status");
    return response.data;
}

// TradingView Admin — 10 s per-request timeout prevents infinite hangs when backend is unresponsive
const TV_TIMEOUT = 10_000;

export async function getTradingViewWebhooks() {
    const response = await adminTradingViewApiClient.get<TradingViewListResponse<TradingViewWebhook>>(
        "/api/admin/tradingview/webhooks",
        { timeout: TV_TIMEOUT },
    );
    return response.data;
}

export async function getTradingViewAlerts() {
    const response = await adminTradingViewApiClient.get<TradingViewListResponse<TradingViewAlert>>(
        "/api/admin/tradingview/alerts",
        { timeout: TV_TIMEOUT },
    );
    return response.data;
}

export async function getTradingViewDecisions() {
    const response = await adminTradingViewApiClient.get<TradingViewListResponse<TradingViewDecision>>(
        "/api/admin/tradingview/decisions",
        { timeout: TV_TIMEOUT },
    );
    return response.data;
}

export async function getTradingViewExternalSignals() {
    const response = await adminTradingViewApiClient.get<TradingViewListResponse<ExternalSignalQueueRow>>(
        "/api/admin/tradingview/external-signals",
        { timeout: TV_TIMEOUT },
    );
    return response.data;
}
