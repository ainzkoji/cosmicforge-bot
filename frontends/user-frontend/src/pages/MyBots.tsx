
import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api/client";
import type { BotInstance } from "../api/client";
import {
    Plus, TrendingUp, Activity, AlertTriangle, Square, X
} from "lucide-react";
import { BotInstanceRow } from "@/components/BotInstance/BotInstanceRow";
import { ConfirmationDialog } from "@/components/UI/ConfirmationDialog";
import { LiveTradingConfirmDialog } from "@/components/UI/LiveTradingConfirmDialog";

interface StopAllResult {
    id: string;
    label: string;
    ok: boolean;
    message: string;
}

function errorMessage(err: unknown, fallback: string): string {
    return err instanceof Error && err.message ? err.message : fallback;
}

function botLabel(bot: BotInstance): string {
    const symbol = bot.symbols?.[0] || "multi-symbol";
    return `${bot.name || bot.strategy_id} · ${symbol} · ${bot.mode.toUpperCase()} · ${bot.id.slice(0, 8)}`;
}

export default function MyBots() {
    const queryClient = useQueryClient();
    const [filterStatus, setFilterStatus] = useState("all");
    const [filterMode, setFilterMode] = useState("all"); // paper/live

    // Dialog State
    const [confirmAction, setConfirmAction] = useState<{ type: 'stop' | 'delete', id: string } | null>(null);
    // A LIVE (real-money) bot is only started after an explicit confirmation.
    const [liveStartBot, setLiveStartBot] = useState<BotInstance | null>(null);
    const [liveStartError, setLiveStartError] = useState<string | null>(null);
    // Server error from the last start/pause/stop/delete action.
    const [actionError, setActionError] = useState<string | null>(null);
    // Stop-all flow
    const [stopAllOpen, setStopAllOpen] = useState(false);
    const [stopAllRunning, setStopAllRunning] = useState(false);
    const [stopAllResults, setStopAllResults] = useState<StopAllResult[] | null>(null);

    // Fetch Bots
    const { data: bots = [], isLoading, isError, error: botsError, refetch } = useQuery({
        queryKey: ['botInstances'],
        queryFn: async () => {
            return api.getBotInstances();
        },
        refetchInterval: 5000 // Poll every 5s for status updates
    });

    // Fetch Brokers (for badges)
    const { data: brokersData } = useQuery({
        queryKey: ["broker-accounts"],
        queryFn: api.getBrokerAccounts,
    });
    const brokerAccounts = brokersData?.accounts || [];

    // Filtering
    const filteredBots = bots.filter((bot) => {
        if (filterStatus !== 'all' && bot.status !== filterStatus) return false;
        if (filterMode !== 'all' && bot.mode !== filterMode) return false;
        return true;
    });

    // Stats Calculation
    const activeBots = bots.filter(b => b.status === 'active').length;
    // PnL calculation would need real data field in BotInstance (e.g. realized_pnl). 
    // The interface has total_trades but not PnL. I'll omit PnL or mock it if not available.
    // The interface I defined didn't have PnL. I'll stick to what I have.

    // Mutations
    const startMutation = useMutation({
        mutationFn: api.startBotInstance,
        onMutate: () => { setActionError(null); setLiveStartError(null); },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
            setLiveStartBot(null);
        },
        onError: (err: Error) => {
            const message = errorMessage(err, 'Failed to start bot instance');
            // Keep the live confirmation open and show the server's reason inside it.
            if (liveStartBot) setLiveStartError(message);
            else setActionError(`Could not start bot: ${message}`);
        }
    });

    const pauseMutation = useMutation({
        mutationFn: api.pauseBotInstance,
        onMutate: () => setActionError(null),
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['botInstances'] }),
        onError: (err: Error) => setActionError(`Could not pause bot: ${errorMessage(err, 'Failed to pause bot instance')}`)
    });

    const stopMutation = useMutation({
        mutationFn: api.stopBotInstance,
        onMutate: () => setActionError(null),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
            setConfirmAction(null);
        },
        onError: (err: Error) => {
            setConfirmAction(null);
            setActionError(`Could not stop bot: ${errorMessage(err, 'Failed to stop bot instance')}`);
        }
    });

    const deleteMutation = useMutation({
        mutationFn: api.deleteBotInstance,
        onMutate: () => setActionError(null),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
            setConfirmAction(null);
        },
        onError: (err: Error) => {
            setConfirmAction(null);
            setActionError(`Could not delete bot: ${errorMessage(err, 'Failed to delete bot instance')}`);
        }
    });

    // Paper/demo bots start with one click; LIVE bots require confirmation first.
    const handleStart = (id: string) => {
        const bot = bots.find((b) => b.id === id);
        if (bot && bot.mode === 'live') {
            setLiveStartError(null);
            setLiveStartBot(bot);
            return;
        }
        startMutation.mutate(id);
    };

    // Every bot that is not already stopped (active, paused or in error).
    const stoppableBots = bots.filter((b) => b.status !== 'stopped');

    const handleStopAll = async () => {
        const targets = stoppableBots;
        setStopAllRunning(true);
        setActionError(null);
        const settled = await Promise.allSettled(targets.map((b) => api.stopBotInstance(b.id)));
        setStopAllResults(settled.map((result, index) => ({
            id: targets[index].id,
            label: botLabel(targets[index]),
            ok: result.status === 'fulfilled',
            message: result.status === 'fulfilled'
                ? 'Stopped'
                : errorMessage(result.reason, 'Failed to stop bot instance'),
        })));
        setStopAllRunning(false);
        setStopAllOpen(false);
        queryClient.invalidateQueries({ queryKey: ['botInstances'] });
    };

    const liveStartAccount = liveStartBot
        ? brokerAccounts.find((a) => a.id === liveStartBot.broker_account_id)
        : undefined;
    const stopAllFailures = stopAllResults ? stopAllResults.filter((r) => !r.ok).length : 0;

    return (
        <div className="space-y-6 text-foreground animate-in fade-in duration-500 max-w-7xl mx-auto px-4 md:px-6 py-8">
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Bot Instances</h1>
                    <p className="text-muted-foreground mt-1">Manage and monitor your active trading instances.</p>
                </div>
                <div className="flex flex-wrap items-center gap-3">
                    <button
                        type="button"
                        onClick={() => setStopAllOpen(true)}
                        disabled={stoppableBots.length === 0 || stopAllRunning}
                        className="flex items-center gap-2 px-5 py-2.5 rounded-xl font-bold border border-red-500/40 text-red-500 hover:bg-red-500/10 transition-all disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                        <Square className="w-4 h-4" /> Stop all bots
                    </button>
                    <Link
                        to="/dashboard/auto-pilot"
                        className="flex items-center gap-2 px-5 py-2.5 bg-primary text-primary-foreground rounded-xl font-bold hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/20"
                    >
                        <Plus className="w-5 h-5" /> Deploy New Bot
                    </Link>
                </div>
            </div>

            {/* Action error (server message) */}
            {actionError && (
                <div role="alert" className="flex items-start justify-between gap-4 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-500">
                    <div className="flex items-start gap-2">
                        <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
                        <span>{actionError}</span>
                    </div>
                    <button type="button" onClick={() => setActionError(null)} aria-label="Dismiss" className="shrink-0 hover:opacity-70">
                        <X className="w-4 h-4" />
                    </button>
                </div>
            )}

            {/* Stop-all results, per bot */}
            {stopAllResults && (
                <div
                    role={stopAllFailures > 0 ? "alert" : "status"}
                    className={`rounded-xl border px-4 py-3 text-sm ${stopAllFailures > 0 ? 'border-red-500/30 bg-red-500/10' : 'border-border bg-card'}`}
                >
                    <div className="flex items-start justify-between gap-4">
                        <div className="font-bold">
                            {stopAllFailures > 0
                                ? `Stop all: ${stopAllFailures} of ${stopAllResults.length} bot(s) could NOT be stopped`
                                : `Stop all: ${stopAllResults.length} bot(s) stopped`}
                        </div>
                        <button type="button" onClick={() => setStopAllResults(null)} aria-label="Dismiss" className="shrink-0 hover:opacity-70">
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                    <ul className="mt-2 space-y-1">
                        {stopAllResults.map((result) => (
                            <li key={result.id} className="flex flex-wrap justify-between gap-2">
                                <span className="font-mono text-xs">{result.label}</span>
                                <span className={result.ok ? 'text-green-500' : 'text-red-500 font-bold'}>
                                    {result.ok ? 'Stopped' : `Failed: ${result.message}`}
                                </span>
                            </li>
                        ))}
                    </ul>
                    <p className="mt-2 text-xs text-muted-foreground">
                        Stopping a bot does not close open positions. Check your exchange account for any positions that are still open.
                    </p>
                </div>
            )}

            {/* Stats Overview */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {/* For PnL we might need a separate endpoint or field. Placeholder for now. */}
                <div className="bg-card border border-border p-6 rounded-2xl flex items-center justify-between shadow-sm">
                    <div>
                        <div className="text-muted-foreground text-sm font-medium mb-1">Active Instances</div>
                        <div className="text-3xl font-bold">{activeBots} <span className="text-muted-foreground text-lg font-normal">/ {bots.length}</span></div>
                    </div>
                    <div className="w-12 h-12 rounded-full bg-blue-500/10 flex items-center justify-center text-blue-500">
                        <Activity className="w-6 h-6" />
                    </div>
                </div>
                {/* Add more stats if API supports it */}
                <div className="bg-card border border-border p-6 rounded-2xl flex items-center justify-between shadow-sm">
                    <div>
                        <div className="text-muted-foreground text-sm font-medium mb-1">Total Trades (All Time)</div>
                        <div className="text-3xl font-bold">{bots.reduce((acc, b) => acc + (b.performance?.trades_count || b.total_trades || 0), 0)}</div>
                    </div>
                    <div className="w-12 h-12 rounded-full bg-green-500/10 flex items-center justify-center text-green-500">
                        <TrendingUp className="w-6 h-6" />
                    </div>
                </div>
                <div className="bg-card border border-border p-6 rounded-2xl flex items-center justify-between shadow-sm">
                    <div>
                        <div className="text-muted-foreground text-sm font-medium mb-1">Error State</div>
                        <div className="text-3xl font-bold text-red-500">{bots.filter(b => b.status === 'error').length}</div>
                    </div>
                    <div className="w-12 h-12 rounded-full bg-red-500/10 flex items-center justify-center text-red-500">
                        <AlertTriangle className="w-6 h-6" />
                    </div>
                </div>
            </div>

            {/* Controls */}
            <div className="flex flex-col md:flex-row justify-between items-center gap-4 bg-card border border-border p-2 rounded-xl">
                <div className="flex bg-muted/50 p-1 rounded-lg w-full md:w-auto overflow-x-auto">
                    {['all', 'active', 'paused', 'stopped', 'error'].map(status => (
                        <button
                            key={status}
                            onClick={() => setFilterStatus(status)}
                            className={`px-4 py-1.5 rounded-md text-sm font-bold capitalize transition-all whitespace-nowrap ${filterStatus === status
                                ? 'bg-background shadow text-foreground'
                                : 'text-muted-foreground hover:text-foreground hover:bg-white/5'
                                }`}
                        >
                            {status}
                        </button>
                    ))}
                </div>

                <div className="flex items-center gap-2 w-full md:w-auto">
                    <select
                        value={filterMode}
                        onChange={(e) => setFilterMode(e.target.value)}
                        className="bg-muted/50 border-transparent rounded-lg px-3 py-1.5 text-sm outline-none focus:ring-2 focus:ring-primary/20"
                    >
                        <option value="all">All Modes</option>
                        <option value="paper">Paper Trading</option>
                        <option value="live">Live Trading</option>
                    </select>
                </div>
            </div>

            {/* List */}
            <div className="space-y-4">
                {isLoading ? (
                    [1, 2, 3].map(i => <div key={i} className="h-24 bg-card/50 animate-pulse rounded-xl border border-white/5" />)
                ) : isError ? (
                    <div role="alert" className="text-center py-16 border border-red-500/30 bg-red-500/5 rounded-2xl">
                        <AlertTriangle className="w-10 h-10 text-red-500 mx-auto mb-4" />
                        <h3 className="text-xl font-bold mb-2">Could not load your bots</h3>
                        <p className="text-muted-foreground mb-2">{errorMessage(botsError, 'The bot list is unavailable.')}</p>
                        <p className="text-muted-foreground text-sm mb-6">
                            Bots that were already running may still be running. Check your exchange account if in doubt.
                        </p>
                        <button type="button" onClick={() => refetch()} className="text-primary font-bold hover:underline">
                            Try again
                        </button>
                    </div>
                ) : filteredBots.length > 0 ? (
                    filteredBots.map((bot) => (
                        <BotInstanceRow
                            key={bot.id}
                            bot={bot}
                            brokers={brokerAccounts}
                            onStart={handleStart}
                            onPause={(id) => pauseMutation.mutate(id)}
                            onStop={(id) => setConfirmAction({ type: 'stop', id })}
                            onDelete={(id) => setConfirmAction({ type: 'delete', id })}
                            onViewLogs={(id) => console.log("View logs", id)} // Placeholder for now
                            isProcessing={startMutation.isPending || pauseMutation.isPending || stopAllRunning}
                        />
                    ))
                ) : (
                    <div className="text-center py-20 border-2 border-dashed border-white/5 rounded-2xl">
                        <div className="w-16 h-16 bg-muted/50 rounded-full flex items-center justify-center mx-auto mb-4">
                            <Activity className="w-8 h-8 text-muted-foreground opacity-50" />
                        </div>
                        <h3 className="text-xl font-bold mb-2">No bot instances found</h3>
                        <p className="text-muted-foreground mb-6">You haven't deployed any strategies yet.</p>
                        <Link to="/dashboard/auto-pilot" className="text-primary font-bold hover:underline">
                            Deploy Auto Pilot
                        </Link>
                    </div>
                )}
            </div>

            {/* Confirmation Dialogs */}
            <ConfirmationDialog
                isOpen={confirmAction?.type === 'stop'}
                onClose={() => setConfirmAction(null)}
                onConfirm={() => confirmAction && stopMutation.mutate(confirmAction.id)}
                title="Stop Bot Instance?"
                message="The bot will stop and open no new trades. This does NOT close open positions: they stay on your exchange account, the stopped bot no longer manages them, and you must monitor or close them yourself. You can start the bot again later."
                confirmLabel="Stop Instance"
                isLoading={stopMutation.isPending}
            />

            <ConfirmationDialog
                isOpen={stopAllOpen}
                onClose={() => { if (!stopAllRunning) setStopAllOpen(false); }}
                onConfirm={handleStopAll}
                title={`Stop all ${stoppableBots.length} bot(s)?`}
                message="Every bot that is not already stopped will be stopped and will open no new trades. This does NOT close open positions: they stay on your exchange account, the stopped bots no longer manage them, and you must monitor or close them yourself."
                confirmLabel="Stop all bots"
                isLoading={stopAllRunning}
            >
                <ul className="max-h-40 overflow-y-auto rounded-lg border border-border divide-y divide-border text-xs font-mono">
                    {stoppableBots.map((bot) => (
                        <li key={bot.id} className="px-3 py-2">{botLabel(bot)} · {bot.status}</li>
                    ))}
                </ul>
            </ConfirmationDialog>

            <LiveTradingConfirmDialog
                isOpen={liveStartBot !== null}
                onClose={() => { if (!startMutation.isPending) setLiveStartBot(null); }}
                onConfirm={() => liveStartBot && startMutation.mutate(liveStartBot.id)}
                title="Start LIVE bot?"
                confirmLabel="Start live bot"
                broker={liveStartAccount?.broker_id || liveStartBot?.broker_id || "unknown"}
                account={liveStartAccount?.label || liveStartBot?.broker_account_id || "unknown"}
                environment={liveStartAccount?.environment}
                details={liveStartBot ? [
                    { label: "Bot", value: liveStartBot.name || liveStartBot.strategy_id },
                    { label: "Symbols", value: liveStartBot.symbols?.length ? liveStartBot.symbols.join(", ") : "Multi-symbol" },
                    {
                        label: "Trade amount",
                        value: liveStartBot.allocation_type === 'percent_balance'
                            ? `${liveStartBot.allocation_value}% of equity`
                            : `${liveStartBot.allocation_value} (fixed)`
                    },
                ] : []}
                isLoading={startMutation.isPending}
                error={liveStartError}
            />

            <ConfirmationDialog
                isOpen={confirmAction?.type === 'delete'}
                onClose={() => setConfirmAction(null)}
                onConfirm={() => confirmAction && deleteMutation.mutate(confirmAction.id)}
                title="Delete Bot Instance?"
                message="Are you sure you want to remove this bot instance? All history and logs will be permanently deleted."
                confirmLabel="Delete Forever"
                isLoading={deleteMutation.isPending}
            />
        </div>
    );
}
