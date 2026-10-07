import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/client";
import {
    ArrowLeft,
    Play,
    Pause,
    Square,
    Activity,
    Settings,
    TrendingUp,
    Clock,
    AlertTriangle,
    Terminal,
    FileText
} from "lucide-react";
import { StatusBadge } from "@/components/BotInstance/StatusBadge";
import { BotHealthBadge } from "@/components/BotInstance/BotHealthBadge";
import { useState } from "react";
import { ConfirmationDialog } from "@/components/UI/ConfirmationDialog";
import { LiveTradingConfirmDialog } from "@/components/UI/LiveTradingConfirmDialog";
import { CopyableId } from "@/components/UI/CopyableId";

function errorMessage(err: unknown, fallback: string): string {
    return err instanceof Error && err.message ? err.message : fallback;
}

export default function BotDetails() {
    const { id } = useParams<{ id: string }>();
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const [activeTab, setActiveTab] = useState<'overview' | 'logs' | 'history'>('overview');

    // Actions State
    const [actionLoading, setActionLoading] = useState<string | null>(null);
    const [confirmAction, setConfirmAction] = useState<{ type: 'stop' | 'delete', isOpen: boolean }>({ type: 'stop', isOpen: false });
    // Server error from the last start/pause/stop/delete action.
    const [actionError, setActionError] = useState<string | null>(null);
    // A LIVE (real-money) bot is only started after an explicit confirmation.
    const [liveStartOpen, setLiveStartOpen] = useState(false);
    const [liveStartError, setLiveStartError] = useState<string | null>(null);

    const { data: brokersData } = useQuery({
        queryKey: ['broker-accounts'],
        queryFn: api.getBrokerAccounts,
        staleTime: 1000 * 60 * 5
    });

    const { data: bot, isLoading, error } = useQuery({
        queryKey: ['bot', id],
        queryFn: () => api.getBotDetails(id!),
        enabled: !!id,
        refetchInterval: 5000 // Live updates
    });

    const startMutation = useMutation({
        mutationFn: api.startBotInstance,
        onMutate: () => { setActionLoading('start'); setActionError(null); setLiveStartError(null); },
        onSettled: () => setActionLoading(null),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['bot', id] });
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
            setLiveStartOpen(false);
        },
        onError: (err: Error) => {
            const message = errorMessage(err, 'Failed to start bot instance');
            if (liveStartOpen) setLiveStartError(message);
            else setActionError(`Could not start bot: ${message}`);
        }
    });

    const pauseMutation = useMutation({
        mutationFn: api.pauseBotInstance,
        onMutate: () => { setActionLoading('pause'); setActionError(null); },
        onSettled: () => setActionLoading(null),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['bot', id] });
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
        },
        onError: (err: Error) => setActionError(`Could not pause bot: ${errorMessage(err, 'Failed to pause bot instance')}`)
    });

    const stopMutation = useMutation({
        mutationFn: api.stopBotInstance,
        onMutate: () => { setActionLoading('stop'); setActionError(null); },
        onSettled: () => {
            setActionLoading(null);
            setConfirmAction({ ...confirmAction, isOpen: false });
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['bot', id] });
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
        },
        onError: (err: Error) => setActionError(`Could not stop bot: ${errorMessage(err, 'Failed to stop bot instance')}`)
    });

    const deleteMutation = useMutation({
        mutationFn: api.deleteBotInstance,
        onMutate: () => { setActionLoading('delete'); setActionError(null); },
        onSettled: () => {
            setActionLoading(null);
            setConfirmAction({ ...confirmAction, isOpen: false });
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['botInstances'] });
            navigate('/dashboard/bots');
        },
        onError: (err: Error) => setActionError(`Could not delete bot: ${errorMessage(err, 'Failed to delete bot instance')}`)
    });

    if (isLoading) {
        return (
            <div className="flex items-center justify-center min-h-[50vh]">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-500" />
            </div>
        );
    }

    if (error || !bot) {
        return (
            <div className="text-center py-12">
                <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
                <h3 className="text-xl font-bold text-white mb-2">Failed to load bot details</h3>
                <p className="text-gray-400 mb-6">{(error as Error)?.message || "Bot not found"}</p>
                <button
                    onClick={() => navigate('/dashboard/bots')}
                    className="px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg text-white transition-colors"
                >
                    Back to Dashboard
                </button>
            </div>
        );
    }

    const brokerAccount = (brokersData?.accounts || []).find((a) => a.id === bot.broker_account_id);

    // Paper/demo bots start with one click; LIVE bots require confirmation first.
    const handleStart = () => {
        if (bot.mode === 'live') {
            setLiveStartError(null);
            setLiveStartOpen(true);
            return;
        }
        startMutation.mutate(bot.id);
    };

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div className="flex items-center gap-4">
                    <button
                        onClick={() => navigate('/dashboard/bots')}
                        className="p-2 hover:bg-white/5 rounded-lg text-gray-400 hover:text-white transition-colors"
                    >
                        <ArrowLeft className="w-5 h-5" />
                    </button>
                    <div>
                        <div className="flex items-center gap-3 mb-1">
                            <h1 className="text-2xl font-bold text-white">{bot.name}</h1>
                            <StatusBadge status={bot.status} />
                            <BotHealthBadge status={bot.bot_health_status} />
                        </div>
                        <div className="flex items-center gap-2 text-sm text-gray-400">
                            <span>{bot.strategy?.name}</span>
                            <span>•</span>
                            <span className="uppercase">{bot.market?.symbol}</span>
                            <span>•</span>
                            <span className="capitalize">{bot.market?.type}</span>
                        </div>
                    </div>
                </div>

                <div className="flex items-center gap-2">
                    {bot.status !== 'active' && (
                        <button
                            onClick={handleStart}
                            disabled={actionLoading === 'start'}
                            className="flex items-center gap-2 px-4 py-2 bg-green-500/10 text-green-400 border border-green-500/20 rounded-lg hover:bg-green-500/20 disabled:opacity-50 transition-colors"
                        >
                            <Play className="w-4 h-4" />
                            Start
                        </button>
                    )}
                    {bot.status === 'active' && (
                        <button
                            onClick={() => pauseMutation.mutate(bot.id)}
                            disabled={actionLoading === 'pause'}
                            className="flex items-center gap-2 px-4 py-2 bg-yellow-500/10 text-yellow-400 border border-yellow-500/20 rounded-lg hover:bg-yellow-500/20 disabled:opacity-50 transition-colors"
                        >
                            <Pause className="w-4 h-4" />
                            Pause
                        </button>
                    )}
                    <button
                        onClick={() => setConfirmAction({ type: 'stop', isOpen: true })}
                        disabled={actionLoading === 'stop' || bot.status === 'stopped'}
                        className="flex items-center gap-2 px-4 py-2 bg-gray-800 text-gray-400 border border-white/10 rounded-lg hover:bg-gray-700 disabled:opacity-50 transition-colors"
                    >
                        <Square className="w-4 h-4" />
                        Stop
                    </button>
                    <button
                        onClick={() => navigate(`/dashboard/bots/${bot.id}/edit`)}
                        className="flex items-center gap-2 px-4 py-2 bg-blue-500/10 text-blue-400 border border-blue-500/20 rounded-lg hover:bg-blue-500/20 transition-colors"
                    >
                        <Settings className="w-4 h-4" />
                        Edit
                    </button>
                </div>
            </div>

            {actionError && (
                <div role="alert" className="flex items-start gap-2 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-400">
                    <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
                    <span className="flex-1">{actionError}</span>
                    <button type="button" onClick={() => setActionError(null)} className="shrink-0 underline hover:no-underline">
                        Dismiss
                    </button>
                </div>
            )}

            {/* Overview Stats */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <StatCard
                    label="Total PnL"
                    value={`${(bot.performance?.total_pnl ?? 0) >= 0 ? '+' : ''}${(bot.performance?.total_pnl ?? 0).toFixed(2)}`}
                    subValue={`${(bot.performance?.pnl_percent ?? 0) > 0 ? '+' : ''}${(bot.performance?.pnl_percent ?? 0).toFixed(2)}%`}
                    color={(bot.performance?.total_pnl ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}
                    icon={TrendingUp}
                />
                <StatCard
                    label="Win Rate"
                    value={`${bot.performance?.win_rate ?? 0}%`}
                    subValue={`${bot.performance?.trades_count ?? 0} Trades`}
                    color="text-blue-400"
                    icon={Activity}
                />
                <StatCard
                    label="Status"
                    value={bot.status.toUpperCase()}
                    subValue={bot.started_at ? `Started ${new Date(bot.started_at).toLocaleString()}` : 'Not started'}
                    color="text-purple-400"
                    icon={Clock}
                />
                <StatCard
                    label="Active Positions"
                    value={String(bot.active_positions ?? 0)}
                    subValue="As reported by the bot"
                    color="text-yellow-400"
                    icon={FileText}
                />
            </div>

            {/* Content Tabs */}
            <div className="bg-[#111122] border border-white/5 rounded-xl overflow-hidden">
                <div className="flex items-center border-b border-white/5">
                    <TabButton
                        active={activeTab === 'overview'}
                        onClick={() => setActiveTab('overview')}
                        label="Overview"
                    />
                    <TabButton
                        active={activeTab === 'logs'}
                        onClick={() => setActiveTab('logs')}
                        label="Logs"
                    />
                    <TabButton
                        active={activeTab === 'history'}
                        onClick={() => setActiveTab('history')}
                        label="Trade History"
                    />
                </div>

                <div className="p-6 min-h-[400px]">
                    {activeTab === 'overview' && (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                            <div>
                                <h3 className="text-lg font-semibold text-white mb-4">Bot Health</h3>
                                <div className="space-y-3 bg-black/20 p-4 rounded-xl border border-white/5 mb-8">
                                    <div className="flex items-center justify-between">
                                        <span className="text-gray-400">Status</span>
                                        <BotHealthBadge status={bot.bot_health_status} />
                                    </div>
                                    <div className="text-sm text-gray-200">
                                        {bot.bot_health_message || "No health status reported yet."}
                                    </div>
                                    {bot.bot_health_recommended_action && (
                                        <div className="text-sm text-gray-400">
                                            <span className="font-medium text-gray-300">Recommended action:</span>{" "}
                                            {bot.bot_health_recommended_action}
                                        </div>
                                    )}
                                    <div className="text-xs text-gray-500">
                                        Updated{" "}
                                        {bot.bot_health_updated_at
                                            ? new Date(bot.bot_health_updated_at).toLocaleString()
                                            : "-"}
                                    </div>
                                </div>

                                <h3 className="text-lg font-semibold text-white mb-4">Configuration</h3>
                                <div className="space-y-4">
                                    <ConfigItem label="Bot ID" value={<CopyableId id={bot.id} className="bg-white/5 border-white/10" />} />
                                    <ConfigItem label="Strategy" value={bot.strategy?.name} />
                                    <ConfigItem
                                        label="Broker"
                                        value={
                                            // Ideally we resolve this name from a list of accounts
                                            // For now, let's just make it clear, or use a hook to fetch if needed.
                                            // But since we didn't add the accounts query here yet, I'll add a lightweight Resolver or just show the ID for now, 
                                            // or better: The user wants to see "Binance" or "Bybit".
                                            // The BotInstance usually doesn't carry the broker name directly in the default schema manifest?
                                            // Actually, the user requirement is "show broker provider badge (Binance/Bybit)".
                                            // I will infer it from the market type or account ID prefix if possible, 
                                            // BUT safer is to relying on backend. 
                                            // The broker_service.py creates IDs like "brk_...". 
                                            // Let's rely on adding the useQuery for accounts.

                                            <BrokerNameResolver accountId={bot.broker_account_id} />
                                        }
                                    />
                                    <ConfigItem label="Market" value={bot.market?.symbol} />
                                    <ConfigItem label="Mode" value={bot.mode} />
                                    <ConfigItem
                                        label="Trade Amount"
                                        value={bot.allocation_type === 'percent_balance'
                                            ? `${bot.allocation_value}% of Equity`
                                            : `$${bot.allocation_value} (Fixed)`}
                                    />
                                    <ConfigItem label="Created At" value={bot.created_at ? new Date(bot.created_at).toLocaleDateString() : '-'} />
                                </div>
                                
                                <h3 className="text-lg font-semibold text-white mt-8 mb-4">Support & Debug</h3>
                                <div className="space-y-4 bg-black/20 p-4 rounded-xl border border-white/5">
                                    <ConfigItem label="Bot Instance ID" value={<CopyableId id={bot.id} maxLength={32} className="bg-transparent border-none p-0 text-gray-300 hover:text-white" />} />
                                    <ConfigItem label="Broker Account ID" value={<CopyableId id={bot.broker_account_id} maxLength={32} className="bg-transparent border-none p-0 text-gray-300 hover:text-white" />} />
                                    <ConfigItem label="Status" value={<StatusBadge status={bot.status} />} />
                                    <ConfigItem label="Execution Mode" value={<span className={`text-xs px-2 py-0.5 rounded uppercase font-bold tracking-wider ${bot.mode === 'live' ? 'bg-red-500/10 text-red-500' : 'bg-blue-500/10 text-blue-500'}`}>{bot.mode}</span>} />
                                    {bot.block_category && (
                                        <ConfigItem label="Blocked Reason" value={<span className="text-red-400 text-sm max-w-xs text-right block">{bot.block_reason_detail || bot.block_category}</span>} />
                                    )}
                                    <ConfigItem label="Last Run" value={<span className="text-gray-400 text-sm font-mono">{bot.last_run_at ? new Date(bot.last_run_at).toLocaleString() : 'Never'}</span>} />
                                    <ConfigItem label="Created At" value={<span className="text-gray-400 text-sm font-mono">{bot.created_at ? new Date(bot.created_at).toLocaleString() : '-'}</span>} />
                                </div>
                            </div>
                            <div>
                                <h3 className="text-lg font-semibold text-white mb-4">Performance Chart</h3>
                                <div className="h-64 bg-white/5 rounded-lg flex items-center justify-center border border-white/5 border-dashed">
                                    <span className="text-gray-500">Performance Chart Placeholder</span>
                                </div>
                            </div>
                        </div>
                    )}

                    {activeTab === 'logs' && (
                        <div className="bg-black/50 rounded-lg p-4 font-mono text-sm h-[400px] overflow-y-auto border border-white/10">
                            <div className="flex items-center gap-2 text-gray-500 mb-4 pb-2 border-b border-white/10">
                                <Terminal className="w-4 h-4" />
                                <span>System Logs</span>
                            </div>
                            <div className="space-y-2 text-gray-300">
                                <p className="text-yellow-500 italic">Log streaming is not available yet. No log lines are shown here.</p>
                            </div>
                        </div>
                    )}

                    {activeTab === 'history' && (
                        <div className="text-center text-gray-500 py-12">
                            <FileText className="w-12 h-12 mx-auto mb-4 opacity-50" />
                            <p>No trading history available yet.</p>
                        </div>
                    )}
                </div>
            </div>

            {/* Confirmation Dialogs */}
            <ConfirmationDialog
                isOpen={confirmAction.isOpen && confirmAction.type === 'stop'}
                onClose={() => setConfirmAction({ ...confirmAction, isOpen: false })}
                onConfirm={() => stopMutation.mutate(bot.id)}
                title="Stop Bot Instance?"
                message="The bot will stop and open no new trades. This does NOT close open positions: they stay on your exchange account, the stopped bot no longer manages them, and you must monitor or close them yourself. You can start the bot again later."
                confirmLabel="Stop Bot"
                variant="danger"
                isLoading={actionLoading === 'stop'}
            />

            <LiveTradingConfirmDialog
                isOpen={liveStartOpen}
                onClose={() => { if (!startMutation.isPending) setLiveStartOpen(false); }}
                onConfirm={() => startMutation.mutate(bot.id)}
                title="Start LIVE bot?"
                confirmLabel="Start live bot"
                broker={brokerAccount?.broker_id || bot.broker_id || "unknown"}
                account={brokerAccount?.label || bot.broker_account_id}
                environment={brokerAccount?.environment}
                details={[
                    { label: "Bot", value: bot.name || bot.strategy_id },
                    { label: "Symbols", value: bot.symbols?.length ? bot.symbols.join(", ") : "Multi-symbol" },
                    {
                        label: "Trade amount",
                        value: bot.allocation_type === 'percent_balance'
                            ? `${bot.allocation_value}% of equity`
                            : `${bot.allocation_value} (fixed)`
                    },
                ]}
                isLoading={startMutation.isPending}
                error={liveStartError}
            />

            {/* Assuming Delete is not directly on this page, or we want it here? Added button for 'stop', 'pause', 'start'. 'delete' usually in settings or here. */}
        </div>
    );
}

function StatCard({ label, value, subValue, color, icon: Icon }: any) {
    return (
        <div className="bg-[#111122] border border-white/5 p-4 rounded-xl">
            <div className="flex items-start justify-between mb-2">
                <span className="text-gray-400 text-sm">{label}</span>
                <div className={`p-2 rounded-lg bg-white/5 ${color}`}>
                    <Icon className="w-4 h-4" />
                </div>
            </div>
            <div className={`text-2xl font-bold ${color} mb-1`}>{value}</div>
            <div className="text-sm text-gray-500">{subValue}</div>
        </div>
    );
}

function TabButton({ active, onClick, label }: any) {
    return (
        <button
            onClick={onClick}
            className={`px-6 py-4 text-sm font-medium border-b-2 transition-colors ${active
                ? 'border-purple-500 text-white'
                : 'border-transparent text-gray-400 hover:text-white hover:border-white/10'
                }`}
        >
            {label}
        </button>
    );
}

function ConfigItem({ label, value }: any) {
    return (
        <div className="flex items-center justify-between py-3 border-b border-white/5 last:border-0">
            <span className="text-gray-400">{label}</span>
            <span className="text-white font-medium">{value}</span>
        </div>
    );
}

function BrokerNameResolver({ accountId }: { accountId: string }) {
    const { data } = useQuery({
        queryKey: ['broker-accounts'],
        queryFn: api.getBrokerAccounts,
        staleTime: 1000 * 60 * 5 // Cache for 5 mins
    });

    if (!data) return <span className="text-gray-500 text-xs font-mono">{accountId?.substring(0, 8)}...</span>;

    const account = data.accounts.find((a: any) => a.id === accountId);
    // Fallback to bot.broker_id if account not found in list (e.g. error) or while loading if passed
    const brokerId = account?.broker_id || "unknown";

    const getBadgeColor = (bid: string) => {
        switch (bid) {
            case 'binance': return 'bg-[#FCD535]/10 text-[#FCD535] border-[#FCD535]/20';
            case 'bybit': return 'bg-orange-500/10 text-orange-500 border-orange-500/20';
            case 'bingx': return 'bg-blue-500/10 text-blue-500 border-blue-500/20';
            default: return 'bg-white/10 text-gray-400 border-white/10';
        }
    };

    if (!account) return <span className="text-gray-500">Unknown ({accountId?.substring(0, 6)})</span>;

    return (
        <span className="flex items-center gap-2">
            <span className={`text-xs px-2 py-0.5 rounded border font-bold uppercase ${getBadgeColor(brokerId)}`}>
                {brokerId}
            </span>
            <span className="font-semibold text-white text-sm">{account.label || "Account"}</span>
            <span className="text-xs text-gray-500 uppercase">({account.environment || "live"})</span>
        </span>
    );
}
