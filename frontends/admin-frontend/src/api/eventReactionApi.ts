import { adminEventsApiClient } from "./client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type ReactionType =
    | "NO_REACTION"
    | "TREND_CONTINUATION"
    | "VOL_SPIKE"
    | "REVERSAL"
    | "WHIPSAW";

export const REACTION_LABELS: Record<ReactionType, string> = {
    NO_REACTION: "No Reaction",
    VOL_SPIKE: "Volatility Spike",
    TREND_CONTINUATION: "Trend Continuation",
    REVERSAL: "Reversal",
    WHIPSAW: "Whipsaw",
};

export type DataQuality =
    | "COMPLETE"
    | "PARTIAL"
    | "MISSING_PRE_EVENT_DATA"
    | "MISSING_POST_EVENT_DATA"
    | "EXCHANGE_DATA_ERROR"
    | "LOW_CONFIDENCE";

export interface ReactionOut {
    id?: number;
    event_id: string;
    symbol: string;
    exchange: string;
    pre_window_start_utc?: string | null;
    event_time_utc: string;
    post_window_end_utc?: string | null;
    price_before_event?: number | null;
    price_at_event?: number | null;
    price_after_5m?: number | null;
    price_after_15m?: number | null;
    price_after_30m?: number | null;
    price_after_60m?: number | null;
    max_move_pct?: number | null;
    min_move_pct?: number | null;
    net_move_pct?: number | null;
    direction_after_event?: string | null;
    continuation_or_reversal?: string | null;
    atr_before?: number | null;
    atr_after?: number | null;
    volatility_expansion_ratio?: number | null;
    candle_range_before?: number | null;
    candle_range_during?: number | null;
    realized_vol_before?: number | null;
    realized_vol_after?: number | null;
    average_volume_before?: number | null;
    event_volume?: number | null;
    volume_spike_ratio?: number | null;
    abnormal_volume_score?: number | null;
    spread_before?: number | null;
    spread_during?: number | null;
    spread_after?: number | null;
    spread_widening_ratio?: number | null;
    reaction_type: ReactionType;
    confidence_score?: number | null;
    data_quality: DataQuality;
    created_at: string;
    updated_at: string;
}

export interface SnapshotOut {
    id?: number;
    event_id: string;
    symbol: string;
    exchange: string;
    timestamp_utc: string;
    window_label: string;
    price?: number | null;
    volume?: number | null;
    candle_open?: number | null;
    candle_high?: number | null;
    candle_low?: number | null;
    candle_close?: number | null;
    atr?: number | null;
    spread?: number | null;
    source: string;
    created_at: string;
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

export async function getRecentReactions(
    days: number = 7,
    limit: number = 50
): Promise<ReactionOut[]> {
    const res = await adminEventsApiClient.get<ReactionOut[]>("/api/admin/events/reactions/recent", {
        params: { days: String(days), limit: String(limit) },
    });
    return res.data;
}

export async function getReactionsForEvent(eventId: string): Promise<ReactionOut[]> {
    const res = await adminEventsApiClient.get<ReactionOut[]>(
        `/api/admin/events/reactions/${eventId}`
    );
    return res.data;
}

export async function getSingleReaction(
    eventId: string,
    symbol: string
): Promise<ReactionOut> {
    const res = await adminEventsApiClient.get<ReactionOut>(
        `/api/admin/events/reactions/${eventId}/${symbol}`
    );
    return res.data;
}

export async function getEventSnapshots(
    eventId: string,
    symbol: string
): Promise<SnapshotOut[]> {
    const res = await adminEventsApiClient.get<SnapshotOut[]>(
        `/api/admin/events/snapshots/${eventId}/${symbol}`
    );
    return res.data;
}

export interface ReactionSummaryOut {
    total_reactions: number;
    by_type: { reaction_type: ReactionType; count: number }[];
    by_quality: { data_quality: DataQuality; count: number }[];
    latest_reaction_utc: string | null;
}

export async function getReactionSummary(days: number = 7): Promise<ReactionSummaryOut> {
    const res = await adminEventsApiClient.get<ReactionSummaryOut>("/api/admin/events/reactions/summary", {
        params: { days: String(days) },
    });
    return res.data;
}
