import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Shield } from "lucide-react";
import { api } from "@/api/client";
import { indexAccountEnvironments, summarizeTradingMode } from "@/utils/tradingMode";

/**
 * Persistent indicator of whether any of the user's bots can trade REAL money.
 *
 * A bot's `mode: "live"` only means it executes at a broker; the environment
 * of its broker ACCOUNT decides whether the money is real. A live-mode bot on
 * a DEMO/testnet account is shown as "DEMO exchange — no real money".
 *
 * Uses the same query keys as the bots list and the broker accounts list so
 * both requests are shared. Fails towards LIVE: when the broker accounts
 * cannot be loaded, or a bot's account or its environment is unknown, that bot
 * counts as real money. "Demo/Paper only" is shown only when both lists
 * loaded successfully.
 */
export function TradingModeBanner() {
    const { data: bots, isLoading, isError } = useQuery({
        queryKey: ["botInstances"],
        queryFn: () => api.getBotInstances(),
        refetchInterval: 30_000,
    });
    const { data: brokerData, isLoading: accountsLoading, isError: accountsError } = useQuery({
        queryKey: ["broker-accounts"],
        queryFn: api.getBrokerAccounts,
    });

    if (isLoading || accountsLoading) return null;

    if (isError || !Array.isArray(bots)) {
        return (
            <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-medium bg-amber-500/10 text-amber-400 border-b border-amber-500/20">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                Trading mode unknown — your bots could not be loaded.
            </div>
        );
    }

    // Accounts not loaded => null => every broker-executing bot counts as real money.
    const accountList = brokerData?.accounts;
    const environments = !accountsError && Array.isArray(accountList) ? indexAccountEnvironments(accountList) : null;
    const summary = summarizeTradingMode(bots, environments);
    const plural = summary.liveCount === 1 ? "" : "s";
    const accountsNote = environments === null ? " Broker account environments could not be loaded, so they are treated as real money." : "";

    if (summary.kind === "live_running") {
        return (
            <div role="status" className="flex flex-wrap items-center gap-2 px-6 py-1.5 text-xs font-bold bg-red-600 text-white border-b border-red-700">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                <span className="uppercase tracking-wider">LIVE — real money</span>
                <span className="font-medium opacity-90">
                    {summary.liveCount} bot{plural} on a live (or unverified) exchange account, {summary.liveActiveCount} running.{accountsNote}
                </span>
            </div>
        );
    }

    if (summary.kind === "live_stopped") {
        return (
            <div role="status" className="flex flex-wrap items-center gap-2 px-6 py-1.5 text-xs font-bold bg-red-500/15 text-red-400 border-b border-red-500/30">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                <span className="uppercase tracking-wider">Real-money bot configured (stopped)</span>
                <span className="font-medium opacity-90">
                    {summary.liveCount} bot{plural} on a live (or unverified) exchange account, none running. Starting one trades real money.{accountsNote}
                </span>
            </div>
        );
    }

    if (summary.kind === "demo_exchange") {
        const demoPlural = summary.demoCount === 1 ? "" : "s";
        return (
            <div role="status" className="flex flex-wrap items-center gap-2 px-6 py-1.5 text-xs font-medium bg-blue-500/10 text-blue-300 border-b border-blue-500/20">
                <Shield className="w-3.5 h-3.5 shrink-0" />
                <span className="uppercase tracking-wider font-bold">DEMO exchange — no real money</span>
                <span className="opacity-80">
                    {summary.demoCount} bot{demoPlural} trading on a demo exchange account, {summary.demoActiveCount} running. No bot can trade real money.
                </span>
            </div>
        );
    }

    // Only reached when the bots list loaded and every bot is paper.
    return (
        <div role="status" className="flex items-center gap-2 px-6 py-1.5 text-xs font-medium bg-blue-500/10 text-blue-300 border-b border-blue-500/20">
            <Shield className="w-3.5 h-3.5 shrink-0" />
            <span className="uppercase tracking-wider font-bold">Demo/Paper only</span>
            <span className="opacity-80">No bot trades on an exchange account.</span>
        </div>
    );
}
