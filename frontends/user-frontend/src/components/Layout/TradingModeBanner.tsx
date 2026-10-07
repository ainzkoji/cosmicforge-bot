import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Shield } from "lucide-react";
import { api } from "@/api/client";
import { summarizeTradingMode } from "@/utils/tradingMode";

/**
 * Persistent indicator of whether the user has any bot in LIVE (real-money)
 * mode. Uses the same query key as the bots list so the request is shared.
 * "Demo/Paper only" is shown only when the list loaded successfully and every
 * bot is paper; a live bot that is stopped still shows a live indicator.
 */
export function TradingModeBanner() {
    const { data: bots, isLoading, isError } = useQuery({
        queryKey: ["botInstances"],
        queryFn: () => api.getBotInstances(),
        refetchInterval: 30_000,
    });

    if (isLoading) return null;

    if (isError || !Array.isArray(bots)) {
        return (
            <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-medium bg-amber-500/10 text-amber-400 border-b border-amber-500/20">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                Trading mode unknown — your bots could not be loaded.
            </div>
        );
    }

    // Fail towards LIVE: any bot that is not positively paper/demo (including an
    // unknown or missing mode) counts as live, whether or not it is running.
    const summary = summarizeTradingMode(bots);
    const plural = summary.liveCount === 1 ? "" : "s";

    if (summary.kind === "live_running") {
        return (
            <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-bold bg-red-600 text-white border-b border-red-700">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                <span className="uppercase tracking-wider">LIVE — real money</span>
                <span className="font-medium opacity-90">
                    {summary.liveCount} bot{plural} in live mode, {summary.liveActiveCount} running
                </span>
            </div>
        );
    }

    if (summary.kind === "live_stopped") {
        return (
            <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-bold bg-red-500/15 text-red-400 border-b border-red-500/30">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                <span className="uppercase tracking-wider">LIVE bot configured (stopped)</span>
                <span className="font-medium opacity-90">
                    {summary.liveCount} bot{plural} set to live mode, none running. Starting one trades real money.
                </span>
            </div>
        );
    }

    // Only reached when the list loaded and every bot is paper/demo.
    return (
        <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-medium bg-blue-500/10 text-blue-300 border-b border-blue-500/20">
            <Shield className="w-3.5 h-3.5 shrink-0" />
            <span className="uppercase tracking-wider font-bold">Demo/Paper only</span>
            <span className="opacity-80">No bot is set to live mode.</span>
        </div>
    );
}
