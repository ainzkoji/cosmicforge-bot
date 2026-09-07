import { adminSignalsApiClient, apiClient } from "@/api/client";

export interface SignalPair {
    symbol: string;
    exchange: string;
    asset_class: string;
    quote_asset?: string | null;
    contract_type?: string | null;
    tier?: string | null;
    enabled?: number | boolean;
    whitelisted?: number | boolean;
    blacklisted?: number | boolean;
    blacklist_reason?: string | null;
    discovered_at?: string | null;
    last_seen_at?: string | null;
    created_at?: string | null;
    updated_at?: string | null;
    warning?: string;
}

export interface SignalPairMetrics {
    symbol: string;
    exchange: string;
    quote_volume_24h?: number | null;
    spread_percent?: number | null;
    bid_price?: number | null;
    ask_price?: number | null;
    candle_count?: number | null;
    atr_percent?: number | null;
    volatility_score?: number | null;
    liquidity_score?: number | null;
    spread_score?: number | null;
    reliability_score?: number | null;
    is_safe?: number | boolean;
    unsafe_reason?: string | null;
    last_updated?: string | null;
    tier?: string | null;
    enabled?: number | boolean;
    whitelisted?: number | boolean;
    blacklisted?: number | boolean;
}

export interface SignalScanRun {
    id: string;
    scan_type: string;
    started_at: string;
    ended_at?: string | null;
    symbols_discovered?: number | null;
    symbols_eligible?: number | null;
    symbols_scanned?: number | null;
    candidates_created?: number | null;
    signals_published?: number | null;
    errors?: string | null;
    status: string;
    created_at?: string | null;
    updated_at?: string | null;
    duration_seconds?: number | null;
}

export interface SignalScanResult {
    id: string;
    scan_run_id: string;
    symbol: string;
    was_scanned?: number | boolean;
    was_skipped?: number | boolean;
    skip_reason?: string | null;
    candidate_count?: number | null;
    accepted_count?: number | null;
    rejected_count?: number | null;
    published_count?: number | null;
    error?: string | null;
    created_at?: string | null;
}

export interface ListResponse<T> {
    items: T[];
    count: number;
    limit: number;
    offset: number;
}

export interface PairQueryParams {
    exchange?: string;
    asset_class?: string;
    quote_asset?: string;
    contract_type?: string;
    tier?: string;
    enabled?: boolean | number | string;
    whitelisted?: boolean | number | string;
    blacklisted?: boolean | number | string;
    search?: string;
    limit?: number;
    offset?: number;
}

export interface PairMetricsQueryParams {
    exchange?: string;
    is_safe?: boolean | number | string;
    min_volume?: number;
    max_spread?: number;
    symbol?: string;
    search?: string;
    limit?: number;
    offset?: number;
}

export interface RefreshPairsPayload {
    min_quote_volume_24h?: number;
    max_spread_percent?: number;
    quote_asset?: string;
    contract_type?: string;
    validate_candles?: boolean;
    candle_timeframe?: string;
    min_candles?: number;
}

export interface RefreshPairsSummary {
    scan_run_id?: string;
    exchange?: string;
    symbols_discovered: number;
    symbols_eligible: number;
    symbols_skipped: number;
    metrics_updated: number;
    errors: unknown[];
}

export interface ScanRunQueryParams {
    scan_type?: string;
    status?: string;
    limit?: number;
    offset?: number;
}

export interface ScanRunDetail {
    scan_run: SignalScanRun;
    results: SignalScanResult[];
}

function cleanParams(params: object = {}) {
    return Object.fromEntries(
        Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== "")
    );
}

export async function getAdminSignalPairs(params?: PairQueryParams): Promise<ListResponse<SignalPair>> {
    const response = await adminSignalsApiClient.get("/api/admin/signals/pairs", { params: cleanParams(params || {}) });
    return response.data;
}

export async function getAdminSignalPairMetrics(params?: PairMetricsQueryParams): Promise<ListResponse<SignalPairMetrics>> {
    const response = await adminSignalsApiClient.get("/api/admin/signals/pairs/metrics", { params: cleanParams(params || {}) });
    return response.data;
}

export async function enableAdminSignalPair(symbol: string): Promise<SignalPair> {
    const response = await apiClient.post(`/api/admin/signals/pairs/${symbol}/enable`);
    return response.data;
}

export async function disableAdminSignalPair(symbol: string): Promise<SignalPair> {
    const response = await apiClient.post(`/api/admin/signals/pairs/${symbol}/disable`);
    return response.data;
}

export async function blacklistAdminSignalPair({ symbol, reason }: { symbol: string; reason?: string }): Promise<SignalPair> {
    const response = await apiClient.post(`/api/admin/signals/pairs/${symbol}/blacklist`, { reason });
    return response.data;
}

export async function whitelistAdminSignalPair(symbol: string): Promise<SignalPair> {
    const response = await apiClient.post(`/api/admin/signals/pairs/${symbol}/whitelist`);
    return response.data;
}

export async function refreshAdminSignalPairs(payload?: RefreshPairsPayload): Promise<RefreshPairsSummary> {
    const response = await apiClient.post("/api/admin/signals/pairs/refresh", payload || {});
    return response.data;
}

export async function getAdminSignalScanRuns(params?: ScanRunQueryParams): Promise<ListResponse<SignalScanRun>> {
    const response = await adminSignalsApiClient.get("/api/admin/signals/scan-runs", { params: cleanParams(params || {}) });
    return response.data;
}

export async function getAdminSignalScanRunDetail(scanRunId: string): Promise<ScanRunDetail> {
    const response = await adminSignalsApiClient.get(`/api/admin/signals/scan-runs/${scanRunId}`);
    return response.data;
}
