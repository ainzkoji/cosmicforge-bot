import { useState } from "react";
import { AlertTriangle, Calendar, Clock, RefreshCw, Plus, Trash2 } from "lucide-react";
import {
    useActiveBlackoutsQuery,
    useCleanupExpiredMutation,
    useEventFeedStatusQuery,
    useSeedEventMutation,
    useUpcomingEventsQuery,
} from "@/hooks/useEventCalendar";
import type { ImpactLevel, SeedEventRequest } from "@/api/eventCalendarApi";

const IMPACT_COLORS: Record<ImpactLevel, string> = {
    HIGH: "#ef4444",
    MEDIUM: "#f59e0b",
    LOW: "#6b7280",
};

function ImpactBadge({ level }: { level: ImpactLevel }) {
    return (
        <span
            className="px-2 py-0.5 rounded text-xs font-semibold"
            style={{ background: `${IMPACT_COLORS[level]}22`, color: IMPACT_COLORS[level] }}
        >
            {level}
        </span>
    );
}

function formatUtc(iso: string): string {
    try {
        return new Date(iso).toLocaleString("en-GB", {
            day: "2-digit", month: "short", year: "numeric",
            hour: "2-digit", minute: "2-digit", timeZone: "UTC",
        }) + " UTC";
    } catch {
        return iso;
    }
}

const EMPTY_FORM: SeedEventRequest = {
    title: "",
    event_type: "",
    country_currency: "USD",
    impact_level: "HIGH",
    scheduled_utc: "",
    forecast_val: null,
    previous_val: null,
    source: "manual",
};

export default function EventCalendar() {
    const [lookAheadDays, setLookAheadDays] = useState(7);
    const [showSeedForm, setShowSeedForm] = useState(false);
    const [form, setForm] = useState<SeedEventRequest>(EMPTY_FORM);

    const { data: upcoming, isLoading: upcomingLoading, refetch: refetchUpcoming } = useUpcomingEventsQuery(lookAheadDays);
    const { data: blackouts, isLoading: blackoutsLoading } = useActiveBlackoutsQuery();
    const { data: feedStatus } = useEventFeedStatusQuery();
    const seedMutation = useSeedEventMutation();
    const cleanupMutation = useCleanupExpiredMutation();

    const handleSeed = async (e: React.FormEvent) => {
        e.preventDefault();
        await seedMutation.mutateAsync(form);
        setForm(EMPTY_FORM);
        setShowSeedForm(false);
    };

    return (
        <div className="p-6 space-y-6" style={{ color: "var(--admin-text-primary)" }}>
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold">Event Calendar</h1>
                    <p className="text-sm mt-1" style={{ color: "var(--admin-text-muted)" }}>
                        Scheduled high-impact events and active trading blackout windows
                    </p>
                </div>
                <div className="flex gap-2">
                    <button
                        onClick={() => cleanupMutation.mutate()}
                        disabled={cleanupMutation.isPending}
                        className="flex items-center gap-2 px-3 py-2 rounded text-sm"
                        style={{ background: "var(--admin-card-bg)", border: "1px solid var(--admin-border-color)" }}
                    >
                        <Trash2 className="w-4 h-4" />
                        Clean expired
                    </button>
                    <button
                        onClick={() => setShowSeedForm(!showSeedForm)}
                        className="flex items-center gap-2 px-3 py-2 rounded text-sm"
                        style={{ background: "#6366f1", color: "#fff" }}
                    >
                        <Plus className="w-4 h-4" />
                        Seed event
                    </button>
                </div>
            </div>

            {/* Feed status */}
            {feedStatus && (
                <div
                    className="flex items-center gap-3 px-4 py-3 rounded-lg border"
                    style={{
                        background: feedStatus.is_stale ? "rgba(245,158,11,0.1)" : "rgba(34,197,94,0.08)",
                        borderColor: feedStatus.is_stale ? "rgba(245,158,11,0.4)" : "rgba(34,197,94,0.3)",
                        color: feedStatus.is_stale ? "#f59e0b" : "#22c55e",
                    }}
                >
                    <Clock className="w-4 h-4 flex-shrink-0" />
                    <span className="text-sm">
                        {feedStatus.is_stale
                            ? `Feed stale — last sync ${feedStatus.staleness_hours != null ? `${Math.round(feedStatus.staleness_hours)}h ago` : "unknown"}`
                            : `Feed fresh — last sync ${feedStatus.staleness_hours != null ? `${Math.round(feedStatus.staleness_hours)}h ago` : "just now"}`}
                        {" · "}
                        {feedStatus.active_blackout_count} active blackout{feedStatus.active_blackout_count !== 1 ? "s" : ""}
                    </span>
                </div>
            )}

            {/* Seed form */}
            {showSeedForm && (
                <div
                    className="p-5 rounded-lg border"
                    style={{ background: "var(--admin-card-bg)", borderColor: "var(--admin-border-color)" }}
                >
                    <h2 className="text-sm font-semibold mb-4">Manually Seed Economic Event</h2>
                    <form onSubmit={handleSeed} className="grid grid-cols-2 gap-4">
                        <div className="col-span-2">
                            <label className="text-xs mb-1 block" style={{ color: "var(--admin-text-muted)" }}>Title</label>
                            <input
                                required
                                value={form.title}
                                onChange={(e) => setForm((f) => ({ ...f, title: e.target.value }))}
                                className="w-full px-3 py-2 rounded text-sm"
                                style={{ background: "var(--admin-bg)", border: "1px solid var(--admin-border-color)", color: "inherit" }}
                                placeholder="e.g. US CPI (MoM)"
                            />
                        </div>
                        <div>
                            <label className="text-xs mb-1 block" style={{ color: "var(--admin-text-muted)" }}>Event Type</label>
                            <input
                                required
                                value={form.event_type}
                                onChange={(e) => setForm((f) => ({ ...f, event_type: e.target.value }))}
                                className="w-full px-3 py-2 rounded text-sm"
                                style={{ background: "var(--admin-bg)", border: "1px solid var(--admin-border-color)", color: "inherit" }}
                                placeholder="CPI, FOMC, NFP, FED_SPEECH..."
                            />
                        </div>
                        <div>
                            <label className="text-xs mb-1 block" style={{ color: "var(--admin-text-muted)" }}>Currency</label>
                            <input
                                required
                                value={form.country_currency}
                                onChange={(e) => setForm((f) => ({ ...f, country_currency: e.target.value }))}
                                className="w-full px-3 py-2 rounded text-sm"
                                style={{ background: "var(--admin-bg)", border: "1px solid var(--admin-border-color)", color: "inherit" }}
                                placeholder="USD"
                            />
                        </div>
                        <div>
                            <label className="text-xs mb-1 block" style={{ color: "var(--admin-text-muted)" }}>Impact Level</label>
                            <select
                                value={form.impact_level}
                                onChange={(e) => setForm((f) => ({ ...f, impact_level: e.target.value as ImpactLevel }))}
                                className="w-full px-3 py-2 rounded text-sm"
                                style={{ background: "var(--admin-bg)", border: "1px solid var(--admin-border-color)", color: "inherit" }}
                            >
                                <option value="HIGH">HIGH</option>
                                <option value="MEDIUM">MEDIUM</option>
                                <option value="LOW">LOW</option>
                            </select>
                        </div>
                        <div>
                            <label className="text-xs mb-1 block" style={{ color: "var(--admin-text-muted)" }}>Scheduled UTC</label>
                            <input
                                required
                                type="datetime-local"
                                value={form.scheduled_utc.replace("Z", "").replace("+00:00", "")}
                                onChange={(e) => setForm((f) => ({ ...f, scheduled_utc: e.target.value + ":00+00:00" }))}
                                className="w-full px-3 py-2 rounded text-sm"
                                style={{ background: "var(--admin-bg)", border: "1px solid var(--admin-border-color)", color: "inherit" }}
                            />
                        </div>
                        <div className="col-span-2 flex gap-3">
                            <button
                                type="submit"
                                disabled={seedMutation.isPending}
                                className="px-4 py-2 rounded text-sm font-semibold"
                                style={{ background: "#6366f1", color: "#fff" }}
                            >
                                {seedMutation.isPending ? "Seeding..." : "Seed Event"}
                            </button>
                            <button
                                type="button"
                                onClick={() => { setShowSeedForm(false); setForm(EMPTY_FORM); }}
                                className="px-4 py-2 rounded text-sm"
                                style={{ background: "var(--admin-bg)", border: "1px solid var(--admin-border-color)" }}
                            >
                                Cancel
                            </button>
                        </div>
                        {seedMutation.isSuccess && (
                            <p className="col-span-2 text-xs text-green-400">{seedMutation.data?.message}</p>
                        )}
                        {seedMutation.isError && (
                            <p className="col-span-2 text-xs text-red-400">
                                Failed to seed event. Check the form fields.
                            </p>
                        )}
                    </form>
                </div>
            )}

            {/* Active blackouts */}
            <div
                className="rounded-lg border p-5"
                style={{ background: "var(--admin-card-bg)", borderColor: "var(--admin-border-color)" }}
            >
                <div className="flex items-center gap-2 mb-4">
                    <AlertTriangle className="w-4 h-4 text-red-400" />
                    <h2 className="text-sm font-semibold">Active Blackout Windows</h2>
                    {blackouts && <span className="text-xs px-2 py-0.5 rounded bg-red-500/20 text-red-400">{blackouts.length}</span>}
                </div>
                {blackoutsLoading ? (
                    <p className="text-sm" style={{ color: "var(--admin-text-muted)" }}>Loading...</p>
                ) : blackouts && blackouts.length > 0 ? (
                    <div className="space-y-2">
                        {blackouts.map((w) => (
                            <div
                                key={w.id}
                                className="flex items-start gap-3 p-3 rounded"
                                style={{ background: "rgba(239,68,68,0.08)", border: "1px solid rgba(239,68,68,0.25)" }}
                            >
                                <div className="flex-1">
                                    <div className="flex items-center gap-2">
                                        <span className="text-sm font-semibold text-red-400">{w.title}</span>
                                        <ImpactBadge level={w.impact_level} />
                                        <span className="text-xs" style={{ color: "var(--admin-text-muted)" }}>{w.country_currency}</span>
                                    </div>
                                    <div className="text-xs mt-1" style={{ color: "var(--admin-text-muted)" }}>
                                        {formatUtc(w.start_utc)} → {formatUtc(w.end_utc)}
                                        {" · "}
                                        {w.is_global ? "Global block" : `Symbols: ${w.affected_symbols || "n/a"}`}
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                ) : (
                    <p className="text-sm" style={{ color: "var(--admin-text-muted)" }}>
                        No blackout windows have been generated yet.
                    </p>
                )}
            </div>

            {/* Upcoming events */}
            <div
                className="rounded-lg border p-5"
                style={{ background: "var(--admin-card-bg)", borderColor: "var(--admin-border-color)" }}
            >
                <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center gap-2">
                        <Calendar className="w-4 h-4" style={{ color: "var(--admin-text-muted)" }} />
                        <h2 className="text-sm font-semibold">Upcoming Events</h2>
                    </div>
                    <div className="flex items-center gap-2">
                        <select
                            value={lookAheadDays}
                            onChange={(e) => setLookAheadDays(Number(e.target.value))}
                            className="text-xs px-2 py-1 rounded"
                            style={{ background: "var(--admin-bg)", border: "1px solid var(--admin-border-color)", color: "inherit" }}
                        >
                            <option value={1}>Next 24h</option>
                            <option value={3}>Next 3 days</option>
                            <option value={7}>Next 7 days</option>
                            <option value={14}>Next 14 days</option>
                        </select>
                        <button
                            onClick={() => refetchUpcoming()}
                            className="p-1 rounded"
                            style={{ color: "var(--admin-text-muted)" }}
                        >
                            <RefreshCw className="w-4 h-4" />
                        </button>
                    </div>
                </div>
                {upcomingLoading ? (
                    <p className="text-sm" style={{ color: "var(--admin-text-muted)" }}>Loading...</p>
                ) : upcoming && upcoming.length > 0 ? (
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr style={{ borderBottom: "1px solid var(--admin-border-color)" }}>
                                    {["Title", "Type", "Currency", "Impact", "Scheduled UTC", "Forecast", "Previous"].map((h) => (
                                        <th key={h} className="text-left pb-2 pr-4 text-xs font-medium" style={{ color: "var(--admin-text-muted)" }}>
                                            {h}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {upcoming.map((ev) => (
                                    <tr
                                        key={ev.id}
                                        style={{ borderBottom: "1px solid var(--admin-border-color)" }}
                                    >
                                        <td className="py-2 pr-4 font-medium">{ev.title}</td>
                                        <td className="py-2 pr-4 text-xs" style={{ color: "var(--admin-text-muted)" }}>{ev.event_type}</td>
                                        <td className="py-2 pr-4 text-xs">{ev.country_currency}</td>
                                        <td className="py-2 pr-4"><ImpactBadge level={ev.impact_level} /></td>
                                        <td className="py-2 pr-4 text-xs">{formatUtc(ev.scheduled_utc)}</td>
                                        <td className="py-2 pr-4 text-xs" style={{ color: "var(--admin-text-muted)" }}>
                                            {ev.forecast_val != null ? ev.forecast_val : "—"}
                                        </td>
                                        <td className="py-2 text-xs" style={{ color: "var(--admin-text-muted)" }}>
                                            {ev.previous_val != null ? ev.previous_val : "—"}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                ) : (
                    <p className="text-sm" style={{ color: "var(--admin-text-muted)" }}>
                        No economic events have been ingested yet.
                    </p>
                )}
            </div>
        </div>
    );
}
