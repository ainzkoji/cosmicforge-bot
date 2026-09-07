import { REACTION_LABELS } from "@/api/eventReactionApi";
import type { ReactionOut, ReactionType, DataQuality } from "@/api/eventReactionApi";
import { useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";

const REACTION_COLORS: Record<ReactionType, string> = {
    NO_REACTION:         "#6b7280",
    VOL_SPIKE:           "#f59e0b",
    TREND_CONTINUATION:  "#22c55e",
    REVERSAL:            "#ef4444",
    WHIPSAW:             "#f97316",
};

const QUALITY_COLORS: Record<DataQuality, string> = {
    COMPLETE:                "#22c55e",
    PARTIAL:                 "#f59e0b",
    MISSING_PRE_EVENT_DATA:  "#f97316",
    MISSING_POST_EVENT_DATA: "#f97316",
    EXCHANGE_DATA_ERROR:     "#ef4444",
    LOW_CONFIDENCE:          "#8b5cf6",
};

function pct(v: number | null | undefined): string {
    if (v == null) return "—";
    return `${v >= 0 ? "+" : ""}${v.toFixed(2)}%`;
}

function ratio(v: number | null | undefined): string {
    if (v == null) return "—";
    return `${v.toFixed(2)}x`;
}

function fmt(v: number | null | undefined, digits = 4): string {
    if (v == null) return "—";
    return v.toFixed(digits);
}

interface Props {
    reaction: ReactionOut;
    eventTitle?: string;
}

export function EventReactionCard({ reaction, eventTitle }: Props) {
    const [expanded, setExpanded] = useState(false);
    const color = REACTION_COLORS[reaction.reaction_type] || "#6b7280";
    const qColor = QUALITY_COLORS[reaction.data_quality] || "#6b7280";

    return (
        <div
            className="rounded-lg border p-4 space-y-3"
            style={{ borderColor: `${color}44`, background: "#1e2130" }}
        >
            {/* Header */}
            <div className="flex items-start justify-between gap-2">
                <div>
                    <p className="text-sm font-semibold text-white">
                        {reaction.symbol} · <span className="text-xs text-gray-400">{reaction.exchange}</span>
                    </p>
                    {eventTitle && (
                        <p className="text-xs text-gray-400 mt-0.5 truncate max-w-xs">{eventTitle}</p>
                    )}
                </div>
                <div className="flex gap-2 items-center flex-shrink-0">
                    <span
                        className="px-2 py-0.5 rounded text-xs font-bold"
                        style={{ background: `${color}22`, color }}
                    >
                        {REACTION_LABELS[reaction.reaction_type]}
                    </span>
                    <span
                        className="px-2 py-0.5 rounded text-xs font-medium"
                        style={{ background: `${qColor}22`, color: qColor }}
                    >
                        {reaction.data_quality.replace(/_/g, " ")}
                    </span>
                </div>
            </div>

            {/* Key metrics row */}
            <div className="grid grid-cols-4 gap-2 text-xs">
                <MetricCell label="Net Move" value={pct(reaction.net_move_pct)} />
                <MetricCell label="Max Move" value={pct(reaction.max_move_pct)} />
                <MetricCell label="Vol Expansion" value={ratio(reaction.volatility_expansion_ratio)} />
                <MetricCell label="Vol Spike" value={ratio(reaction.volume_spike_ratio)} />
            </div>

            {/* Confidence bar */}
            {reaction.confidence_score != null && (
                <div className="space-y-0.5">
                    <div className="flex justify-between text-xs text-gray-400">
                        <span>Confidence</span>
                        <span>{(reaction.confidence_score * 100).toFixed(0)}%</span>
                    </div>
                    <div className="h-1.5 rounded-full bg-gray-700">
                        <div
                            className="h-1.5 rounded-full"
                            style={{
                                width: `${reaction.confidence_score * 100}%`,
                                background: color,
                            }}
                        />
                    </div>
                </div>
            )}

            {/* Expand/collapse raw metrics */}
            <button
                className="flex items-center gap-1 text-xs text-gray-400 hover:text-gray-200"
                onClick={() => setExpanded(v => !v)}
            >
                {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                {expanded ? "Hide" : "Show"} raw metrics
            </button>

            {expanded && (
                <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-gray-300 pt-1 border-t border-gray-700">
                    <RawRow label="Price before" value={fmt(reaction.price_before_event)} />
                    <RawRow label="Price at event" value={fmt(reaction.price_at_event)} />
                    <RawRow label="Price +5m" value={fmt(reaction.price_after_5m)} />
                    <RawRow label="Price +15m" value={fmt(reaction.price_after_15m)} />
                    <RawRow label="Price +30m" value={fmt(reaction.price_after_30m)} />
                    <RawRow label="Price +60m" value={fmt(reaction.price_after_60m)} />
                    <RawRow label="Direction" value={reaction.direction_after_event || "—"} />
                    <RawRow label="Continuation" value={reaction.continuation_or_reversal || "—"} />
                    <RawRow label="ATR before" value={fmt(reaction.atr_before)} />
                    <RawRow label="ATR after" value={fmt(reaction.atr_after)} />
                    <RawRow label="Avg vol before" value={fmt(reaction.average_volume_before, 0)} />
                    <RawRow label="Event volume" value={fmt(reaction.event_volume, 0)} />
                    <RawRow label="Spread widen" value={ratio(reaction.spread_widening_ratio)} />
                    <RawRow label="Event time" value={new Date(reaction.event_time_utc).toLocaleString("en-GB", { timeZone: "UTC" }) + " UTC"} />
                </div>
            )}
        </div>
    );
}

function MetricCell({ label, value }: { label: string; value: string }) {
    return (
        <div className="flex flex-col gap-0.5">
            <span className="text-gray-500 uppercase tracking-wide" style={{ fontSize: "10px" }}>{label}</span>
            <span className="text-white font-mono font-semibold">{value}</span>
        </div>
    );
}

function RawRow({ label, value }: { label: string; value: string }) {
    return (
        <>
            <span className="text-gray-500">{label}</span>
            <span className="font-mono text-white">{value}</span>
        </>
    );
}
