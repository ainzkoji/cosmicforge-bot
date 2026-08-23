import { adminSignalsApiClient, apiClient } from "@/api/client";

export type AdminSignalStatus =
    | "PENDING_ENTRY"
    | "ACTIVE"
    | "EXPIRED"
    | "TP1_HIT"
    | "TP2_HIT"
    | "TP3_HIT"
    | "SL_HIT"
    | "CANCELLED"
    | "INVALIDATED";

export interface AdminTradingSignal {
    id: string;
    candidate_id?: string | null;
    asset_class: string;
    symbol: string;
    side: string;
    timeframe?: string | null;
    strategy_name?: string | null;
    entry_price: number;
    entry_zone_low?: number | null;
    entry_zone_high?: number | null;
    stop_loss: number;
    take_profit_1: number;
    take_profit_2?: number | null;
    take_profit_3?: number | null;
    risk_reward: number;
    confidence_score: number;
    signal_reason?: string | null;
    status: AdminSignalStatus | string;
    is_published: number | boolean;
    source?: string | null;
    dev_mode?: number | boolean;
    created_at: string;
    published_at?: string | null;
    expires_at?: string | null;
    invalidated_at?: string | null;
    tp1_hit_at?: string | null;
    tp2_hit_at?: string | null;
    tp3_hit_at?: string | null;
    sl_hit_at?: string | null;
    cancelled_at?: string | null;
    updated_at?: string | null;
}

export interface AdminSignalCandidate {
    id: string;
    asset_class: string;
    symbol: string;
    side: string;
    timeframe?: string | null;
    strategy_name?: string | null;
    entry_price?: number | null;
    entry_zone_low?: number | null;
    entry_zone_high?: number | null;
    stop_loss?: number | null;
    take_profit_1?: number | null;
    take_profit_2?: number | null;
    take_profit_3?: number | null;
    risk_reward?: number | null;
    confidence_score?: number | null;
    signal_reason?: string | null;
    rejection_reason?: string | null;
    source?: string | null;
    status: string;
    dev_mode?: number | boolean;
    created_at: string;
    updated_at?: string | null;
}

export interface AdminSignalListResponse<T> {
    items: T[];
    count: number;
    limit: number;
    offset: number;
}

export interface AdminSignalQueryParams {
    asset_class?: string;
    status?: string;
    symbol?: string;
    side?: string;
    is_published?: boolean | number | string;
    dev_mode?: boolean | number | string;
    limit?: number;
    offset?: number;
}

function cleanParams(params: AdminSignalQueryParams = {}) {
    return Object.fromEntries(
        Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== "")
    );
}

export async function getAdminSignals(params?: AdminSignalQueryParams): Promise<AdminSignalListResponse<AdminTradingSignal>> {
    const response = await adminSignalsApiClient.get("/api/admin/signals", { params: cleanParams(params) });
    return response.data;
}

export async function getAdminSignalCandidates(params?: AdminSignalQueryParams): Promise<AdminSignalListResponse<AdminSignalCandidate>> {
    const response = await adminSignalsApiClient.get("/api/admin/signals/candidates", { params: cleanParams(params) });
    return response.data;
}

export async function publishAdminSignal(signalId: string): Promise<AdminTradingSignal> {
    const response = await apiClient.post(`/api/admin/signals/${signalId}/publish`);
    return response.data;
}

export async function unpublishAdminSignal(signalId: string): Promise<AdminTradingSignal> {
    const response = await apiClient.post(`/api/admin/signals/${signalId}/unpublish`);
    return response.data;
}

export async function cancelAdminSignal(signalId: string): Promise<AdminTradingSignal> {
    const response = await apiClient.post(`/api/admin/signals/${signalId}/cancel`);
    return response.data;
}
