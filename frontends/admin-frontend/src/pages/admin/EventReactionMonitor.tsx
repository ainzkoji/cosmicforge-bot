import { useState } from "react";
import { Activity, BarChart2, AlertCircle, RefreshCw, ChevronDown } from "lucide-react";
import { useRecentReactionsQuery, useReactionsForEventQuery, useEventSnapshotsQuery } from "@/hooks/useEventReaction";
import { useUpcomingEventsQuery } from "@/hooks/useEventCalendar";
import { EventReactionCard } from "@/components/admin/events/EventReactionCard";
import { ReactionTimeline } from "@/components/admin/events/ReactionTimeline";
import { VolatilityReactionChart } from "@/components/admin/events/VolatilityReactionChart";
import { REACTION_LABELS } from "@/api/eventReactionApi";
import type { ReactionOut, DataQuality } from "@/api/eventReactionApi";

const QUALITY_COLOR: Record<DataQuality, string> = {
    COMPLETE:                "#22c55e",
    PARTIAL:                 "#f59e0b",
    MISSING_PRE_EVENT_DATA:  "#f97316",
    MISSING_POST_EVENT_DATA: "#f97316",
    EXCHANGE_DATA_ERROR:     "#ef4444",
    LOW_CONFIDENCE:          "#8b5cf6",
};

function StabilizationBadge({ reaction }: { reaction: ReactionOut | undefined }) {
    if (!reaction) return null;
    const type = reaction.reaction_type;
    let label = "STABLE";
    let color = "#22c55e";
    if (type === "VOL_SPIKE" || type === "WHIPSAW") {
        label = "STILL_VOLATILE";
        color = "#f59e0b";
    } else if (type === "REVERSAL") {
        label = "REVERSAL";
        color = "#ef4444";
    } else if (reaction.data_quality !== "COMPLETE") {
        label = "DATA_INCOMPLETE";
        color = "#6b7280";
    }
    return (
        <span
            className="px-2 py-0.5 rounded text-xs font-bold"
            style={{ background: `${color}22`, color }}
        >
            {label}
        </span>
    );
}

export default function EventReactionMonitor() {
    const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
    const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
    const [days, setDays] = useState(7);

    const recentQuery = useRecentReactionsQuery(days, 100);
    const eventReactionsQuery = useReactionsForEventQuery(selectedEventId);
    const snapshotsQuery = useEventSnapshotsQuery(selectedEventId, selectedSymbol);
    const upcomingQuery = useUpcomingEventsQuery(30);

    const reactions = eventReactionsQuery.data ?? recentQuery.data ?? [];
    const isLoading = recentQuery.isLoading || eventReactionsQuery.isLoading;

    // Past events with seeded data
    const pastEvents = (upcomingQuery.data ?? []).filter(e => {
        try { return new Date(e.scheduled_utc) < new Date(); } catch { return false; }
    });

    // Top affected symbols (ranked by max move %)
    const topSymbols = [...reactions]
        .filter(r => r.max_move_pct != null)
        .sort((a, b) => Math.abs(b.max_move_pct!) - Math.abs(a.max_move_pct!))
        .slice(0, 8);

    const selectedReaction = selectedSymbol
        ? reactions.find(r => r.symbol === selectedSymbol)
        : undefined;

    return (
        <div className="space-y-6 p-4 text-sm">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <Activity className="text-blue-400" size={20} />
                    <h1 className="text-lg font-bold text-white">Market Reaction Monitor</h1>
                    <span className="px-2 py-0.5 rounded text-xs bg-blue-900 text-blue-300">
                        Phase 2 — Observer
                    </span>
                </div>
                <div className="flex items-center gap-2">
                    <select
                        className="bg-gray-800 text-gray-300 border border-gray-700 rounded px-2 py-1 text-xs"
                        value={days}
                        onChange={e => { setDays(Number(e.target.value)); setSelectedEventId(null); }}
                    >
                        {[1, 3, 7, 14, 30].map(d => (
                            <option key={d} value={d}>Last {d} day{d > 1 ? "s" : ""}</option>
                        ))}
                    </select>
                    <button
                        className="flex items-center gap-1 px-2 py-1 rounded bg-gray-800 text-gray-300 border border-gray-700 text-xs hover:bg-gray-700"
                        onClick={() => { recentQuery.refetch(); eventReactionsQuery.refetch(); }}
                    >
                        <RefreshCw size={12} /> Refresh
                    </button>
                </div>
            </div>

            {/* Event selector */}
            <div className="bg-gray-900 rounded-lg border border-gray-700 p-4 space-y-2">
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide">Select Event</p>
                <div className="flex flex-wrap gap-2">
                    <button
                        className={`px-3 py-1 rounded text-xs border ${!selectedEventId ? "border-blue-500 text-blue-300 bg-blue-900/20" : "border-gray-700 text-gray-400 hover:border-gray-500"}`}
                        onClick={() => { setSelectedEventId(null); setSelectedSymbol(null); }}
                    >
                        All recent
                    </button>
                    {pastEvents.slice(0, 12).map(ev => (
                        <button
                            key={ev.event_id}
                            className={`px-3 py-1 rounded text-xs border ${selectedEventId === ev.event_id ? "border-blue-500 text-blue-300 bg-blue-900/20" : "border-gray-700 text-gray-400 hover:border-gray-500"}`}
                            onClick={() => { setSelectedEventId(ev.event_id); setSelectedSymbol(null); }}
                        >
                            {ev.title.slice(0, 28)} ({ev.country_currency})
                        </button>
                    ))}
                </div>
            </div>

            {isLoading && (
                <div className="flex items-center gap-2 text-gray-400 text-sm">
                    <RefreshCw size={14} className="animate-spin" /> Loading reaction data…
                </div>
            )}

            {!isLoading && reactions.length === 0 && (
                <div className="flex items-center gap-2 text-gray-500 bg-gray-900 rounded-lg border border-gray-700 p-6">
                    <AlertCircle size={16} />
                    <span>No market reaction records found. Reaction tracking is implemented but has not captured runtime data yet.</span>
                </div>
            )}

            {reactions.length > 0 && (
                <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
                    {/* Left: Charts */}
                    <div className="xl:col-span-2 space-y-4">
                        {/* Volatility chart */}
                        <div className="bg-gray-900 rounded-lg border border-gray-700 p-4">
                            <div className="flex items-center gap-2 mb-3">
                                <BarChart2 size={14} className="text-orange-400" />
                                <span className="text-xs font-semibold text-gray-300 uppercase tracking-wide">
                                    ATR Expansion
                                </span>
                            </div>
                            <VolatilityReactionChart reactions={reactions} />
                        </div>

                        {/* Timeline for selected symbol */}
                        {selectedReaction && (
                            <div className="bg-gray-900 rounded-lg border border-gray-700 p-4">
                                <div className="flex items-center gap-2 mb-3">
                                    <Activity size={14} className="text-green-400" />
                                    <span className="text-xs font-semibold text-gray-300 uppercase tracking-wide">
                                        Price / Volume Timeline — {selectedSymbol}
                                    </span>
                                    <StabilizationBadge reaction={selectedReaction} />
                                </div>
                                {snapshotsQuery.isLoading ? (
                                    <div className="text-gray-500 text-xs">Loading snapshots…</div>
                                ) : (
                                    <ReactionTimeline
                                        snapshots={snapshotsQuery.data ?? []}
                                        symbol={selectedSymbol ?? ""}
                                    />
                                )}
                            </div>
                        )}
                    </div>

                    {/* Right: Top symbols + reaction cards */}
                    <div className="space-y-4">
                        {/* Top affected symbols */}
                        <div className="bg-gray-900 rounded-lg border border-gray-700 p-4">
                            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">
                                Top Affected Symbols
                            </p>
                            <table className="w-full text-xs">
                                <thead>
                                    <tr className="text-gray-500 text-left">
                                        <th className="pb-1">Symbol</th>
                                        <th className="pb-1">Max Move</th>
                                        <th className="pb-1">Vol Spike</th>
                                        <th className="pb-1">Type</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {topSymbols.map(r => (
                                        <tr
                                            key={`${r.event_id}-${r.symbol}`}
                                            className={`cursor-pointer hover:bg-gray-800 rounded ${selectedSymbol === r.symbol ? "bg-gray-800" : ""}`}
                                            onClick={() => {
                                                setSelectedSymbol(r.symbol);
                                                if (!selectedEventId) setSelectedEventId(r.event_id);
                                            }}
                                        >
                                            <td className="py-0.5 pr-2 font-mono text-white">{r.symbol}</td>
                                            <td className="py-0.5 pr-2 font-mono">
                                                <span className={r.max_move_pct! >= 0 ? "text-green-400" : "text-red-400"}>
                                                    {r.max_move_pct != null ? `${r.max_move_pct >= 0 ? "+" : ""}${r.max_move_pct.toFixed(2)}%` : "—"}
                                                </span>
                                            </td>
                                            <td className="py-0.5 pr-2 font-mono text-blue-300">
                                                {r.volume_spike_ratio != null ? `${r.volume_spike_ratio.toFixed(1)}×` : "—"}
                                            </td>
                                            <td className="py-0.5 text-gray-400" style={{ fontSize: "10px" }}>
                                                {REACTION_LABELS[r.reaction_type]}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>

                        {/* Reaction cards */}
                        <div className="space-y-2 max-h-96 overflow-y-auto pr-1">
                            {reactions.slice(0, 20).map(r => (
                                <div
                                    key={`${r.event_id}-${r.symbol}`}
                                    onClick={() => { setSelectedSymbol(r.symbol); if (!selectedEventId) setSelectedEventId(r.event_id); }}
                                    className="cursor-pointer"
                                >
                                    <EventReactionCard reaction={r} />
                                </div>
                            ))}
                        </div>

                        {/* Data quality summary */}
                        <div className="bg-gray-900 rounded-lg border border-gray-700 p-4">
                            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
                                Data Quality
                            </p>
                            {Object.entries(
                                reactions.reduce<Record<string, number>>((acc, r) => {
                                    acc[r.data_quality] = (acc[r.data_quality] ?? 0) + 1;
                                    return acc;
                                }, {})
                            ).map(([q, count]) => (
                                <div key={q} className="flex justify-between text-xs py-0.5">
                                    <span style={{ color: QUALITY_COLOR[q as DataQuality] ?? "#9ca3af" }}>
                                        {q.replace(/_/g, " ")}
                                    </span>
                                    <span className="text-gray-400 font-mono">{count}</span>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
