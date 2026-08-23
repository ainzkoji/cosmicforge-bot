
import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, BotInstance } from "../api/client";
import {
    Plus, TrendingUp, Activity, AlertTriangle, Filter, RotateCcw
} from "lucide-react";
import { BotInstanceRow } from "@/components/BotInstance/BotInstanceRow";
import { ConfirmationDialog } from "@/components/UI/ConfirmationDialog";
import { Loader2 } from "lucide-react";

export default function MyBots() {
    const queryClient = useQueryClient();
    const [filterStatus, setFilterStatus] = useState("all");
    const [filterMode, setFilterMode] = useState("all"); // paper/live

    // Dialog State
    const [confirmAction, setConfirmAction] = useState<{ type: 'stop' | 'delete', id: string } | null>(null);

    // Fetch Bots
    const { data: bots = [], isLoading } = useQuery({
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
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['botInstances'] })
    });

    const pauseMutation = useMutation({
        mutationFn: api.pauseBotInstance,
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['botInstances'] })
    });

    const stopMutation = useMutation({
        mutationFn: api.stopBotInstance,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
            setConfirmAction(null);
        }
    });

    const deleteMutation = useMutation({
        mutationFn: api.deleteBotInstance,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
            setConfirmAction(null);
        }
    });

    return (
        <div className="space-y-6 text-foreground animate-in fade-in duration-500 max-w-7xl mx-auto px-4 md:px-6 py-8">
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Bot Instances</h1>
                    <p className="text-muted-foreground mt-1">Manage and monitor your active trading instances.</p>
                </div>
                <Link
                    to="/dashboard/auto-pilot"
                    className="flex items-center gap-2 px-5 py-2.5 bg-primary text-primary-foreground rounded-xl font-bold hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/20"
                >
                    <Plus className="w-5 h-5" /> Deploy New Bot
                </Link>
            </div>

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
                ) : filteredBots.length > 0 ? (
                    filteredBots.map((bot) => (
                        <BotInstanceRow
                            key={bot.id}
                            bot={bot}
                            brokers={brokerAccounts}
                            onStart={(id) => startMutation.mutate(id)}
                            onPause={(id) => pauseMutation.mutate(id)}
                            onStop={(id) => setConfirmAction({ type: 'stop', id })}
                            onDelete={(id) => setConfirmAction({ type: 'delete', id })}
                            onViewLogs={(id) => console.log("View logs", id)} // Placeholder for now
                            isProcessing={startMutation.isPending || pauseMutation.isPending}
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
                message="This will gracefully close all open positions and stop the bot. This action cannot be undone."
                confirmLabel="Stop Instance"
                isLoading={stopMutation.isPending}
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
