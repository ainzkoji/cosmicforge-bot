import { AlertTriangle, CheckCircle, Clock } from "lucide-react";
import { useActiveBlackoutsQuery, useEventFeedStatusQuery } from "@/hooks/useEventCalendar";

function formatTimeLeft(endUtc: string): string {
    try {
        const end = new Date(endUtc);
        const now = new Date();
        const diffMs = end.getTime() - now.getTime();
        if (diffMs <= 0) return "ending";
        const mins = Math.floor(diffMs / 60000);
        if (mins < 60) return `${mins}m left`;
        const hrs = Math.floor(mins / 60);
        const rem = mins % 60;
        return rem > 0 ? `${hrs}h ${rem}m left` : `${hrs}h left`;
    } catch {
        return "";
    }
}

function formatUtc(iso: string): string {
    try {
        return new Date(iso).toLocaleString("en-GB", {
            day: "2-digit",
            month: "short",
            hour: "2-digit",
            minute: "2-digit",
            timeZone: "UTC",
        }) + " UTC";
    } catch {
        return iso;
    }
}

export function EventBlackoutBanner() {
    const { data: blackouts, isLoading: blackoutsLoading } = useActiveBlackoutsQuery();
    const { data: feedStatus } = useEventFeedStatusQuery();

    if (blackoutsLoading) return null;

    const hasActiveBlackout = blackouts && blackouts.length > 0;
    const isFeedStale = feedStatus?.is_stale && feedStatus?.last_sync_utc !== null;

    if (!hasActiveBlackout && !isFeedStale) return null;

    return (
        <div className="mb-4 space-y-2">
            {isFeedStale && (
                <div
                    className="flex items-center gap-3 px-4 py-3 rounded-lg border"
                    style={{
                        background: "rgba(245, 158, 11, 0.1)",
                        borderColor: "rgba(245, 158, 11, 0.4)",
                        color: "#f59e0b",
                    }}
                >
                    <Clock className="w-4 h-4 flex-shrink-0" />
                    <span className="text-sm font-medium">
                        Event calendar feed is stale (
                        {feedStatus?.staleness_hours != null
                            ? `${Math.round(feedStatus.staleness_hours)}h`
                            : "unknown"}{" "}
                        since last sync). Bot may be applying failsafe block.
                    </span>
                </div>
            )}

            {hasActiveBlackout &&
                blackouts.map((w) => (
                    <div
                        key={w.id}
                        className="flex items-start gap-3 px-4 py-3 rounded-lg border"
                        style={{
                            background: "rgba(239, 68, 68, 0.1)",
                            borderColor: "rgba(239, 68, 68, 0.4)",
                            color: "#ef4444",
                        }}
                    >
                        <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                        <div className="flex-1 min-w-0">
                            <div className="text-sm">
                                <span className="font-semibold">EVENT BLACKOUT ACTIVE - </span>
                                {w.title} ({w.country_currency} {w.impact_level})
                                {w.is_global ? " - Global block (all symbols)" : ` - ${w.affected_symbols || "symbol-specific"}`}
                                {" - "}
                                {formatTimeLeft(w.end_utc)}
                            </div>
                            <div className="text-xs mt-1 opacity-90">
                                Entries blocked: yes - Window: {formatUtc(w.start_utc)} to {formatUtc(w.end_utc)} - Reason: {w.reason}
                            </div>
                        </div>
                    </div>
                ))}

            {!hasActiveBlackout && !isFeedStale && (
                <div
                    className="flex items-center gap-3 px-4 py-3 rounded-lg border"
                    style={{
                        background: "rgba(34, 197, 94, 0.08)",
                        borderColor: "rgba(34, 197, 94, 0.3)",
                        color: "#22c55e",
                    }}
                >
                    <CheckCircle className="w-4 h-4" />
                    <span className="text-sm">No active event blackouts.</span>
                </div>
            )}
        </div>
    );
}
