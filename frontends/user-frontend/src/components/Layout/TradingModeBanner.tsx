import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Shield } from "lucide-react";
import { api } from "@/api/client";

/**
 * Persistent indicator of whether the user has any bot in LIVE (real-money)
 * mode. Uses the same query key as the bots list so the request is shared.
 */
export function TradingModeBanner() {
    const { data: bots, isLoading, isError } = useQuery({
        queryKey: ["botInstances"],
        queryFn: () => api.getBotInstances(),
        refetchInterval: 30_000,
    });

    if (isLoading) return null;

    if (isError || !bots) {
        return (
            <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-medium bg-amber-500/10 text-amber-400 border-b border-amber-500/20">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                Trading mode unknown — your bots could not be loaded.
            </div>
        );
    }

    const liveBots = bots.filter((bot) => bot.mode === "live" && bot.status !== "stopped");
    const runningLive = liveBots.filter((bot) => bot.status === "active").length;

    if (liveBots.length > 0) {
        return (
            <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-bold bg-red-600 text-white border-b border-red-700">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                <span className="uppercase tracking-wider">LIVE — real money</span>
                <span className="font-medium opacity-90">
                    {liveBots.length} bot{liveBots.length === 1 ? "" : "s"} in live mode, {runningLive} running
                </span>
            </div>
        );
    }

    return (
        <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-medium bg-blue-500/10 text-blue-300 border-b border-blue-500/20">
            <Shield className="w-3.5 h-3.5 shrink-0" />
            <span className="uppercase tracking-wider font-bold">Demo/Paper only</span>
            <span className="opacity-80">No bot is set to live mode.</span>
        </div>
    );
}
