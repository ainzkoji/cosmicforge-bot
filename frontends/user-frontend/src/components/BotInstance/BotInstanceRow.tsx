
import { Link } from 'react-router-dom';
import { Play, Pause, Square, Trash2, FileText, Settings, ExternalLink, MoreHorizontal } from 'lucide-react';
import { StatusBadge } from './StatusBadge';
import { BotHealthBadge } from './BotHealthBadge';
import { BotInstance } from '@/api/client';
import { useState } from 'react';
import { CopyableId } from '../UI/CopyableId';

interface BotInstanceRowProps {
    bot: BotInstance;
    onStart: (id: string) => void;
    onPause: (id: string) => void;
    onStop: (id: string) => void;
    onDelete: (id: string) => void;
    onViewLogs: (id: string) => void;
    isProcessing?: boolean;
    brokers?: any[]; // List of broker accounts for lookup
}

export const BotInstanceRow = ({ bot, onStart, onPause, onStop, onDelete, onViewLogs, isProcessing = false, brokers = [] }: BotInstanceRowProps) => {
    const [showActions, setShowActions] = useState(false);

    const formatDate = (dateString: string) => {
        if (!dateString) return '-';
        return new Date(dateString).toLocaleString(undefined, {
            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
        });
    };

    const broker = brokers.find(b => b.id === bot.broker_account_id);

    return (
        <div className="group bg-card border border-border rounded-xl p-4 hover:border-primary/30 hover:shadow-lg transition-all mb-3 relative overflow-hidden">
            {/* Background gradient for active bots */}
            {bot.status === 'active' && (
                <div className="absolute inset-0 bg-gradient-to-r from-green-500/5 to-transparent pointer-events-none" />
            )}

            <div className="flex flex-col md:flex-row gap-4 items-start md:items-center relative z-10">

                {/* Status & Identity */}
                <div className="flex items-center gap-4 flex-1 min-w-0">
                    <div className="flex flex-col gap-1">
                        <StatusBadge status={bot.status} />
                        <BotHealthBadge status={bot.bot_health_status} />
                    </div>

                    <div className="min-w-0">
                        <Link to={`/dashboard/bots/${bot.id}`} className="font-bold text-lg hover:text-primary transition-colors flex items-center gap-2 truncate">
                            {bot.strategy_id === 'master_ensemble' ? 'Auto Pilot (Master Ensemble)' : bot.strategy_id}
                        </Link>
                        <div className="flex items-center gap-3 mt-1.5 mb-1">
                            <CopyableId id={bot.id} label="Bot ID" />
                            <CopyableId id={bot.broker_account_id} label="Broker ID" maxLength={8} />
                        </div>
                        <div className="flex items-center gap-2 text-sm text-muted-foreground mt-0.5 flex-wrap">
                            <div className="flex items-center gap-1.5 font-medium text-foreground/80 bg-muted/40 px-1.5 py-0.5 rounded border border-border/50">
                                {broker ? (
                                    <>
                                        {/* Simple icon mapping based on ID/Name if logo not avail in Account object. Usually account has broker_id. */}
                                        <span className="capitalize">{broker.broker_id}</span>
                                        <span className="text-border">|</span>
                                        <span className="font-mono text-xs text-muted-foreground">{broker.masked_key?.substring(0, 8)}...</span>
                                    </>
                                ) : (
                                    <span>🏦 {bot.broker_account_id}</span>
                                )}
                            </div>
                            <span>•</span>
                            <span className="flex items-center gap-1">
                                {bot.market_type === 'CRYPTO' ? '🪙' : '💱'} {bot.market_type}
                            </span>
                            <span>•</span>
                            <span className="font-mono text-xs">{bot.symbols?.[0] || 'Multi-Symbol'}</span>
                            <span>•</span>
                            <span className={`text-[10px] px-1.5 py-0.5 rounded uppercase font-bold tracking-wider ${bot.mode === 'live' ? 'bg-red-500/10 text-red-500' : 'bg-blue-500/10 text-blue-500'
                                }`}>
                                {bot.mode}
                            </span>
                        </div>
                        {bot.block_category && (
                            <div className="mt-2 flex items-center gap-1.5 text-xs text-red-400 bg-red-500/10 px-2 py-1 rounded border border-red-500/20 w-fit">
                                <span className="text-lg">⚠</span>
                                <span>Blocked: {bot.block_reason_detail || bot.block_category}</span>
                            </div>
                        )}
                        {bot.bot_health_message && (
                            <div className="mt-2 text-xs text-muted-foreground">
                                <div>{bot.bot_health_message}</div>
                                {bot.bot_health_recommended_action && (
                                    <div className="mt-1">Recommended: {bot.bot_health_recommended_action}</div>
                                )}
                                {bot.bot_health_updated_at && (
                                    <div className="mt-1">Updated: {formatDate(bot.bot_health_updated_at)}</div>
                                )}
                            </div>
                        )}
                    </div>
                </div>

                {/* Metrics */}
                <div className="flex gap-6 md:gap-8 items-center text-sm">
                    <div className="hidden sm:block text-right">
                        <div className="text-muted-foreground text-xs">Positions</div>
                        <div className="font-mono font-bold">{bot.active_positions}</div>
                    </div>
                    <div className="hidden sm:block text-right">
                        <div className="text-muted-foreground text-xs">PnL</div>
                        <div className={`font-mono font-bold ${(bot.performance?.pnl || 0) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                            {(bot.performance?.pnl || 0) >= 0 ? '+' : ''}{(bot.performance?.pnl || 0).toFixed(2)} USDT
                        </div>
                    </div>
                    <div className="hidden sm:block text-right">
                        <div className="text-muted-foreground text-xs">Trades</div>
                        <div className="font-mono font-bold text-muted-foreground">{bot.total_trades}</div>
                    </div>
                    <div className="hidden md:block text-right">
                        <div className="text-muted-foreground text-xs">Last Run</div>
                        <div className="font-mono text-xs text-muted-foreground">{formatDate(bot.last_run_at || '')}</div>
                    </div>
                </div>

                {/* Actions */}
                <div className="flex items-center gap-1 md:gap-2 w-full md:w-auto justify-end border-t md:border-t-0 border-border pt-4 md:pt-0 mt-2 md:mt-0">
                    {bot.status === 'active' ? (
                        <button
                            onClick={() => onPause(bot.id)}
                            disabled={isProcessing}
                            className="p-2 text-amber-500 hover:bg-amber-500/10 rounded-lg transition-colors tooltip-trigger"
                            title="Pause"
                        >
                            <Pause className="w-5 h-5" />
                        </button>
                    ) : (
                        <button
                            onClick={() => onStart(bot.id)}
                            disabled={isProcessing} // Also disabled if stopped? Usually can restart active/paused, maybe not stopped depending on logic
                            className="p-2 text-green-500 hover:bg-green-500/10 rounded-lg transition-colors tooltip-trigger"
                            title="Start"
                        >
                            <Play className="w-5 h-5" />
                        </button>
                    )}

                    <button
                        onClick={() => onStop(bot.id)}
                        disabled={bot.status === 'stopped' || isProcessing}
                        className="p-2 text-muted-foreground hover:text-foreground hover:bg-white/5 rounded-lg transition-colors disabled:opacity-30"
                        title="Stop"
                    >
                        <Square className="w-5 h-5" />
                    </button>

                    <button
                        onClick={() => onViewLogs(bot.id)}
                        className="p-2 text-muted-foreground hover:text-blue-500 hover:bg-blue-500/10 rounded-lg transition-colors hidden sm:block"
                        title="View Logs"
                    >
                        <FileText className="w-5 h-5" />
                    </button>

                    <Link
                        to={`/dashboard/bots/${bot.id}/edit`}
                        className="p-2 text-muted-foreground hover:text-foreground hover:bg-white/5 rounded-lg transition-colors"
                        title="Settings"
                    >
                        <Settings className="w-5 h-5" />
                    </Link>

                    <div className="h-4 w-px bg-border mx-1" />

                    <button
                        onClick={() => onDelete(bot.id)}
                        disabled={bot.status === 'active' || isProcessing}
                        className="p-2 text-red-500/50 hover:text-red-500 hover:bg-red-500/10 rounded-lg transition-colors disabled:opacity-30 disabled:hover:bg-transparent"
                        title="Delete"
                    >
                        <Trash2 className="w-5 h-5" />
                    </button>
                </div>
            </div>
        </div>
    );
};
