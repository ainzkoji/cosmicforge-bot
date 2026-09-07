import { apiClient, adminEventsApiClient } from "./client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type ImpactLevel = "HIGH" | "MEDIUM" | "LOW";

export interface EventOut {
    id: number;
    event_id: string;
    title: string;
    event_type: string;
    country_currency: string;
    impact_level: ImpactLevel;
    scheduled_utc: string;
    actual_val: number | null;
    forecast_val: number | null;
    previous_val: number | null;
    source: string;
    created_at: string;
    updated_at: string;
}

export interface BlackoutWindowOut {
    id: number;
    event_id: number;
    start_utc: string;
    end_utc: string;
    affected_symbols: string | null;
    is_global: boolean;
    reason: string;
    title: string;
    event_type: string;
    impact_level: ImpactLevel;
    country_currency: string;
}

export interface FeedStatusOut {
    last_sync_utc: string | null;
    staleness_hours: number | null;
    is_stale: boolean;
    stale_threshold_hours: number;
    active_blackout_count: number;
}

export interface SeedEventRequest {
    title: string;
    event_type: string;
    country_currency: string;
    impact_level: ImpactLevel;
    scheduled_utc: string;
    forecast_val?: number | null;
    previous_val?: number | null;
    source?: string;
}

export interface SeedEventResponse {
    event_id: string;
    rowid: number;
    message: string;
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

export async function getUpcomingEvents(
    days: number = 7,
    impact?: string
): Promise<EventOut[]> {
    const params: Record<string, string> = { days: String(days) };
    if (impact) params.impact = impact;
    const res = await adminEventsApiClient.get<EventOut[]>("/api/admin/events/upcoming", { params });
    return res.data;
}

export async function getActiveBlackouts(): Promise<BlackoutWindowOut[]> {
    const res = await adminEventsApiClient.get<BlackoutWindowOut[]>("/api/admin/events/active-blackouts");
    return res.data;
}

export async function getEventFeedStatus(): Promise<FeedStatusOut> {
    const res = await adminEventsApiClient.get<FeedStatusOut>("/api/admin/events/feed-status");
    return res.data;
}

export async function seedEvent(data: SeedEventRequest): Promise<SeedEventResponse> {
    const res = await apiClient.post<SeedEventResponse>("/api/admin/events/seed", data);
    return res.data;
}

export async function cleanupExpiredWindows(): Promise<{ deactivated: number; message: string }> {
    const res = await apiClient.post<{ deactivated: number; message: string }>(
        "/api/admin/events/cleanup-expired"
    );
    return res.data;
}
