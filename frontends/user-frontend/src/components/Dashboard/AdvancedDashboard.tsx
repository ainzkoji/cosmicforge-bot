import { Link, useNavigate } from "react-router-dom";
import {
    TrendingUp, TrendingDown, Activity, Zap, DollarSign, BarChart3,
    Play, Pause, Settings, Plus, ArrowUpRight, ArrowDownRight,
    Bot, Wallet, Target, AlertCircle, Eye, EyeOff, RefreshCw, Sparkles, Brain,
    MoreHorizontal, Calendar, Filter, Download, Trophy, CheckCircle2, X
} from "lucide-react";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { motion } from "framer-motion";
import { CumulativePnlChart } from "@/components/analytics/CumulativePnlChart";
import type { ReconciliationResponse } from "@/api/client";

function formatActivityDate(value: string | undefined | null): string {
    if (!value) return 'N/A';
    const d = new Date(value);
    if (isNaN(d.getTime())) return 'N/A';
    return d.toLocaleString();
}

export function AdvancedDashboard() {
    const navigate = useNavigate();
    const [hideBalances, setHideBalances] = useState(false);
    const [showReconciliation, setShowReconciliation] = useState(false);

    const { data: portfolioData, isLoading: isPortfolioLoading, error: portfolioError } = useQuery({
        queryKey: ['portfolio-summary'],
        queryFn: api.getPortfolioSummary,
        refetchInterval: 5000,
    });

    const { data: brokerData, isLoading: isBrokersLoading } = useQuery({
        queryKey: ['broker-accounts'],
        queryFn: api.getBrokerAccounts,
    });

    const { data: closedTradesData, isLoading: isClosedTradesLoading } = useQuery({
        queryKey: ['dashboard-recent-closed-trades'],
        queryFn: () => api.getPositionHistory('ALL', 'closed', 1, 5, undefined, undefined, undefined, undefined, undefined, 'closed_at'),
        staleTime: 60_000,
    });

    const { data: reconciliationData, isLoading: isReconciliationLoading } = useQuery({
        queryKey: ['analytics-reconciliation', 30],
        queryFn: () => api.getReconciliation(30),
        staleTime: 60_000,
        refetchInterval: 30_000,
    });

    const recentClosedTrades = closedTradesData?.items ?? [];
    const perfSummary = closedTradesData?.summary;

    const reconciliation = reconciliationData as ReconciliationResponse | undefined;

    const isLoading = isPortfolioLoading || isBrokersLoading;

    if (isLoading) {
        return (
            <div className="flex items-center justify-center min-h-[400px]">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
            </div>
        );
    }

    if (portfolioError) {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] text-muted-foreground">
                <AlertCircle className="w-10 h-10 mb-2 text-red-500" />
                <p>Failed to load portfolio data.</p>
                <button onClick={() => window.location.reload()} className="mt-4 px-4 py-2 text-sm bg-primary/10 text-primary rounded-lg hover:bg-primary/20 transition-colors">
                    Retry
                </button>
            </div>
        );
    }

    const { summary, positions, recent_activity, allocation } = portfolioData || {};
    // Check for connected vs draft accounts
    const accounts = brokerData?.accounts || [];
    const hasConnectedBrokers = accounts.some((a: any) => a.status === 'connected');
    const hasDraftBrokers = accounts.some((a: any) => a.status === 'draft');

    // Show CTA if no connected brokers.
    const showConnectBrokerCTA = !hasConnectedBrokers;

    // Customize CTA based on drafts
    const ctaTitle = hasDraftBrokers ? "Resume Broker Setup" : "Connect Your First Broker";
    const ctaDescription = hasDraftBrokers
        ? "You have a broker setup in progress. Complete it to start tracking your portfolio."
        : "Your dashboard is looking a bit empty. Connect an exchange account (like Binance) to start tracking your portfolio and running strategies.";
    const ctaButtonText = hasDraftBrokers ? "Resume Setup" : "Connect Broker";

    // Fallback for visual stability if data is partial
    const totalEquity = summary?.total_portfolio_value || 0;
    const dayChange = summary?.total_day_pnl || 0;
    const dayChangePercent = summary?.total_day_pnl_percent || 0;
    // Allocation field names differ across backend versions (Cash/Invested vs USDT/Positions).
    // Prefer current backend values (Cash/Invested), fallback to legacy labels if present.
    const investedAmount =
        allocation?.find((a: any) => a.asset === 'Invested')?.value ??
        allocation?.find((a: any) => a.asset === 'Positions')?.value ??
        0;
    const cashAmount =
        allocation?.find((a: any) => a.asset === 'Cash')?.value ??
        allocation?.find((a: any) => a.asset === 'USDT')?.value ??
        0;

    function reconciliationExplanation(expl: string | undefined): string {
        switch (expl) {
            case 'open_unrealized_pnl':
                return "Portfolio Value is broker equity and includes unrealized PnL from open positions. Completed Trades shows only closed realized PnL.";
            case 'deposits_withdrawals':
                return "Portfolio Value can change due to deposits/withdrawals and broker-side adjustments. Completed Trades shows only closed realized PnL.";
            case 'missing_persistence_or_other_balance_components':
                return "Some balance movement may not be explained by closed-trade PnL (e.g., missing persistence, funding/fees, or broker-side adjustments).";
            case 'closed_realized_pnl_matches_equity_change':
                return "Closed-trade net PnL matches equity change for this window.";
            default:
                return "Portfolio Value is broker-reported equity. It may include wallet balance, unrealized PnL, deposits, demo balance changes, and other broker-side adjustments. Completed Trades only shows closed realized trades.";
        }
    }

    return (
        <div className="space-y-6 animate-in fade-in duration-500 text-foreground">
            {/* Header Bar */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 py-2">
                <div>
                    <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
                        <LayoutDashboardIcon className="w-6 h-6 text-primary" />
                        Dashboard
                    </h1>
                    <p className="text-sm text-muted-foreground">Overview of your automated portfolio</p>
                </div>

                <div className="flex items-center gap-3 bg-card/50 backdrop-blur-md p-1.5 rounded-xl border border-border/50">
                    <button className="px-4 py-2 text-xs font-semibold bg-primary/10 text-primary rounded-lg border border-primary/20 hover:bg-primary/20 transition-colors">
                        Live
                    </button>
                    <div className="h-4 w-px bg-border mx-1" />
                    <button className="flex items-center gap-2 px-3 py-2 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors">
                        <Calendar className="w-3.5 h-3.5" /> Today
                    </button>
                    <button className="p-2 hover:bg-muted rounded-lg transition-colors text-muted-foreground hover:text-foreground">
                        <Filter className="w-4 h-4" />
                    </button>
                </div>
            </div>

            {/* Empty State / Connect Broker CTA */}
            {showConnectBrokerCTA && (
                <div className="bg-gradient-to-br from-blue-500/10 to-purple-500/10 border border-blue-500/20 rounded-2xl p-8 flex flex-col items-center justify-center text-center space-y-4 shadow-xl">
                    <div className="p-4 bg-blue-500/20 rounded-full mb-2">
                        <Wallet className="w-8 h-8 text-blue-400" />
                    </div>
                    <h2 className="text-2xl font-bold text-white">{ctaTitle}</h2>
                    <p className="text-muted-foreground max-w-md">
                        {ctaDescription}
                    </p>
                    <Link to="/dashboard/brokers" className="px-6 py-2.5 bg-blue-600 hover:bg-blue-500 text-white font-semibold rounded-lg transition-all shadow-lg hover:shadow-blue-500/25">
                        {ctaButtonText}
                    </Link>
                </div>
            )}

            {!showConnectBrokerCTA && (
                <>
                    {/* Top Metrics Row */}
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                        {/* Main Balance Card */}
                        <div className="col-span-1 md:col-span-2 bg-[#0F1218] border border-white/5 rounded-2xl p-6 relative overflow-hidden group shadow-xl shadow-black/20">
                            <div className="absolute top-0 right-0 p-6 opacity-30">
                                <div className="w-24 h-24 bg-primary/20 rounded-full blur-3xl -mr-10 -mt-10" />
                            </div>
                            <div className="relative z-10 flex flex-col justify-between h-full">
                                <div className="flex justify-between items-start mb-6">
                                    <div>
                                        <h3
                                            className="text-sm font-medium text-muted-foreground mb-1"
                                            title="Broker-reported account value/equity. May include wallet balance, unrealized PnL, deposits, and broker-side adjustments."
                                        >
                                            Total Portfolio Value
                                        </h3>
                                        <div className="text-4xl font-mono font-bold text-white tracking-tight flex items-baseline gap-2">
                                            {hideBalances ? "•••••" : `$${totalEquity.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                                            <span className={`text-lg font-sans font-semibold px-2 py-0.5 rounded-full border ${dayChangePercent >= 0 ? 'bg-green-500/10 text-green-500 border-green-500/20' : 'bg-red-500/10 text-red-500 border-red-500/20'}`}>
                                                {dayChangePercent >= 0 ? '+' : ''}{dayChangePercent}%
                                            </span>
                                        </div>
                                    </div>
                                    <button onClick={() => setHideBalances(!hideBalances)} className="text-muted-foreground hover:text-white transition-colors">
                                        {hideBalances ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                                    </button>
                                </div>

                                <div className="grid grid-cols-3 gap-8 pt-4 border-t border-white/5">
                                    <div>
                                        <div className="text-xs text-muted-foreground mb-1">Daily P&L</div>
                                        <div className={`font-mono font-semibold ${dayChange >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                            {dayChange >= 0 ? '+' : ''}${Math.abs(dayChange).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-xs text-muted-foreground mb-1">Invested</div>
                                        <div className="font-mono font-semibold text-white">${investedAmount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                                    </div>
                                    <div>
                                        <div className="text-xs text-muted-foreground mb-1">Cash</div>
                                        <div className="font-mono font-semibold text-white">${cashAmount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Bot Performance Summary */}
                        <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 flex flex-col justify-between hover:border-white/10 transition-colors shadow-lg">
                            <div className="flex justify-between items-center mb-4">
                                <span className="text-sm font-medium text-muted-foreground">Active Strategies</span>
                                <Activity className="w-5 h-5 text-purple-500" />
                            </div>
                            <div className="flex items-end gap-2 mb-2">
                                <div className="text-3xl font-bold text-white">{summary?.active_strategies || 0}</div>
                                <span className="text-sm text-muted-foreground mb-1">running</span>
                            </div>
                            <div className="w-full bg-muted/20 rounded-full h-1.5 overflow-hidden">
                                <div className="bg-purple-500 h-full w-3/4 animate-pulse" />
                            </div>
                            <div className="mt-4 flex gap-2">
                                <div className="flex-1 bg-white/5 rounded-lg p-2 text-center">
                                    <div className="text-xs text-muted-foreground">Trade Win Rate</div>
                                    <div className="text-sm font-bold text-white">
                                        {isClosedTradesLoading ? '…' : perfSummary?.win_rate != null ? `${perfSummary.win_rate.toFixed(1)}%` : 'N/A'}
                                    </div>
                                </div>
                                <div className="flex-1 bg-white/5 rounded-lg p-2 text-center">
                                    <div className="text-xs text-muted-foreground">Open Pos</div>
                                    <div className="text-sm font-bold text-white">{summary?.open_positions_count || 0}</div>
                                </div>
                            </div>
                        </div>

                        {/* PnL Reconciliation */}
                        <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 flex flex-col justify-between hover:border-white/10 transition-colors shadow-lg">
                            <div className="flex justify-between items-center mb-4">
                                <span className="text-sm font-medium text-muted-foreground">PnL Reconciliation</span>
                                <BarChart3 className="w-5 h-5 text-blue-400" />
                            </div>

                            {isReconciliationLoading ? (
                                <div className="text-sm text-muted-foreground">Loadingâ€¦</div>
                            ) : reconciliation?.account ? (
                                <div className="space-y-2.5">
                                    <div className="flex justify-between items-center">
                                        <span className="text-xs text-muted-foreground">Broker Equity</span>
                                        <span className="text-sm font-mono font-semibold text-white">
                                            ${reconciliation.account.equity.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                                        </span>
                                    </div>
                                    <div className="flex justify-between items-center">
                                        <span className="text-xs text-muted-foreground">Wallet Balance</span>
                                        <span className="text-sm font-mono font-semibold text-white">
                                            ${reconciliation.account.wallet_balance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                                        </span>
                                    </div>
                                    <div className="flex justify-between items-center">
                                        <span className="text-xs text-muted-foreground">Unrealized PnL</span>
                                        <span className={`text-sm font-mono font-semibold ${reconciliation.account.unrealized_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {reconciliation.account.unrealized_pnl >= 0 ? '+' : ''}${Math.abs(reconciliation.account.unrealized_pnl).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                                        </span>
                                    </div>
                                    <div className="border-t border-white/5 my-2" />
                                    <div className="flex justify-between items-center">
                                        <span className="text-xs text-muted-foreground" title="Closed realized PnL after fees from trade fills.">Completed Trades Net PnL</span>
                                        <span className={`text-sm font-mono font-bold ${(reconciliation.trade_fills?.net_pnl ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {(reconciliation.trade_fills?.net_pnl ?? 0) >= 0 ? '+' : ''}${Math.abs(reconciliation.trade_fills?.net_pnl ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                                        </span>
                                    </div>
                                    <div className="text-[11px] leading-snug text-muted-foreground pt-1">
                                        {reconciliationExplanation(reconciliation.difference?.likely_explanation)}
                                    </div>
                                </div>
                            ) : (
                                <div className="text-sm text-muted-foreground">Reconciliation unavailable</div>
                            )}

                            <button
                                onClick={() => setShowReconciliation(true)}
                                className="mt-4 w-full py-2.5 text-sm font-medium text-primary border border-primary/20 rounded-lg hover:bg-primary/10 transition-colors flex items-center justify-center gap-2"
                            >
                                View Reconciliation <ArrowUpRight className="w-3.5 h-3.5" />
                            </button>
                        </div>

                        {/* Market Compass / AI Insight */}
                        <div className="bg-gradient-to-br from-indigo-500/10 to-purple-500/10 border border-indigo-500/20 rounded-2xl p-6 flex flex-col justify-between hover:border-indigo-500/40 transition-colors shadow-lg relative overflow-hidden">
                            <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(to_bottom,transparent,black)]" />
                            <div className="relative z-10">
                                <div className="flex items-center gap-2 mb-3">
                                    <div className="p-1.5 bg-indigo-500/20 rounded-lg">
                                        <Sparkles className="w-4 h-4 text-indigo-400" />
                                    </div>
                                    <span className="text-sm font-bold text-indigo-300">AI Market Insights</span>
                                </div>
                                <div className="text-sm font-semibold text-indigo-200/80 mb-2">
                                    Not yet active
                                </div>
                                <p className="text-xs text-indigo-200/50 leading-relaxed">
                                    AI Market Insights are not active yet. Insights will appear once signal intelligence is connected.
                                </p>
                            </div>
                        </div>
                    </div>

                    {/* Charts Section */}
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Cumulative closed-trade Net PnL Chart */}
                        <div className="lg:col-span-2 bg-[#0F1218] border border-white/5 rounded-2xl p-6 shadow-xl min-w-0 overflow-hidden">
                            <div className="flex justify-between items-center mb-6">
                                <div>
                                    <h2 className="text-lg font-bold text-white">Performance Overview</h2>
                                    <p className="text-xs text-muted-foreground">Cumulative closed-trade net PnL (after fees)</p>
                                </div>
                            </div>
                            <div className="w-full min-w-0 min-h-[320px]">
                                <CumulativePnlChart darkBackground={true} />
                            </div>
                        </div>

                        {/* Asset Allocation Donut */}
                        <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 shadow-xl flex flex-col">
                            <div className="flex justify-between items-center mb-6">
                                <h2 className="text-lg font-bold text-white">Allocation</h2>
                                <button className="p-1 hover:bg-white/5 rounded"><MoreHorizontal className="w-4 h-4 text-muted-foreground" /></button>
                            </div>

                            <div className="flex-1 flex items-center justify-center relative">
                                {/* SVG Donut */}
                                <div className="relative w-48 h-48">
                                    <svg viewBox="0 0 100 100" className="transform -rotate-90 w-full h-full">
                                        <circle cx="50" cy="50" r="40" fill="none" stroke="#1F2937" strokeWidth="12" />
                                        {/* Simple segment for now - full dynamic donut requires calculating offsets */}
                                        <circle cx="50" cy="50" r="40" fill="none" stroke="#3B82F6" strokeWidth="12" strokeDasharray="100 251" strokeDashoffset="-0" />
                                    </svg>
                                    <div className="absolute inset-0 flex flex-col items-center justify-center">
                                        <span className="text-2xl font-bold text-white">{allocation?.length || 0}</span>
                                        <span className="text-xs text-muted-foreground uppercase tracking-wider">Assets</span>
                                    </div>
                                </div>
                            </div>

                            <div className="mt-6 space-y-3">
                                {allocation?.slice(0, 3).map((item, idx) => (
                                    <div key={idx} className="flex justify-between items-center text-sm">
                                        <div className="flex items-center gap-2">
                                            <div className="w-3 h-3 rounded-full bg-blue-500" />
                                            <span className="text-gray-300">{item.asset}</span>
                                        </div>
                                        <span className="font-mono text-white">{item.percent}%</span>
                                    </div>
                                ))}
                                {(!allocation || allocation.length === 0) && (
                                    <div className="text-center text-xs text-muted-foreground">No allocation data</div>
                                )}
                            </div>
                        </div>
                    </div>

                    {/* Bottom Section - Tables */}
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Active Strategies Table */}
                        <div className="lg:col-span-2 bg-[#0F1218] border border-white/5 rounded-2xl overflow-hidden shadow-xl">
                            <div className="p-6 border-b border-white/5 flex justify-between items-center">
                                <h2 className="text-lg font-bold text-white">Open Positions</h2>
                                <button className="text-xs text-primary hover:text-primary/80 transition-colors">View All</button>
                            </div>
                            <div className="overflow-x-auto">
                                <table className="w-full">
                                    <thead className="bg-white/5 text-xs text-muted-foreground uppercase tracking-wider font-semibold">
                                        <tr>
                                            <th className="px-6 py-4 text-left">Pair</th>
                                            <th className="px-6 py-4 text-left">Side</th>
                                            <th className="px-6 py-4 text-right">Entry Price</th>
                                            <th className="px-6 py-4 text-right">Size</th>
                                            <th className="px-6 py-4 text-right">P&L</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-white/5">
                                        {positions?.map((pos) => (
                                            <tr key={`${pos.account_id}-${pos.symbol}`} className="hover:bg-white/5 transition-colors group">
                                                <td className="px-6 py-4">
                                                    <div className="flex items-center gap-3">
                                                        <div className="w-8 h-8 rounded-full bg-white/10 flex items-center justify-center font-bold text-[10px] text-white">
                                                            {pos.symbol.substring(0, 3)}
                                                        </div>
                                                        <div>
                                                            <div className="font-bold text-white text-sm">{pos.symbol}</div>
                                                            <div className="text-[10px] text-muted-foreground">{pos.broker}</div>
                                                        </div>
                                                    </div>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <span className={`text-xs font-semibold px-2 py-0.5 rounded ${pos.side === 'LONG' ? 'bg-green-500/10 text-green-500' : 'bg-red-500/10 text-red-500'}`}>
                                                        {pos.side}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4 text-right font-mono text-sm text-white">
                                                    {pos.entry_price != null ? `$${Number(pos.entry_price).toLocaleString()}` : 'N/A'}
                                                </td>
                                                <td className="px-6 py-4 text-right font-mono text-sm text-white">{pos.size}</td>
                                                <td className="px-6 py-4 text-right">
                                                    <div className={`font-mono text-sm font-bold ${Number(pos.pnl ?? 0) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                                        {Number(pos.pnl ?? 0) >= 0 ? '+' : ''}${Math.abs(Number(pos.pnl ?? 0)).toLocaleString()}
                                                    </div>
                                                    <div className={`text-[10px] ${Number(pos.roi_percent ?? 0) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                                        {pos.roi_percent != null ? `${Number(pos.roi_percent) >= 0 ? '+' : ''}${pos.roi_percent}%` : '—'}
                                                    </div>
                                                </td>
                                            </tr>
                                        ))}
                                        {positions?.length === 0 && (
                                            <tr>
                                                <td colSpan={5} className="px-6 py-8 text-center text-sm text-muted-foreground">
                                                    No open positions
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        {/* Portfolio Activity */}
                        <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 shadow-xl">
                            <h2 className="text-lg font-bold text-white mb-6">Portfolio Activity</h2>
                            <div className="space-y-6">
                                {recent_activity?.slice(0, 5).map((activity, i) => (
                                    <div key={i} className="flex gap-4 relative">
                                        {i !== (recent_activity?.length || 0) - 1 && <div className="absolute top-8 left-[14px] w-px h-full bg-white/5" />}
                                        <div className={`w-7 h-7 rounded-full flex items-center justify-center z-10 ${activity.side === 'BUY' || activity.side === 'LONG' ? 'bg-green-500/10 text-green-500' : 'bg-red-500/10 text-red-500'}`}>
                                            {activity.side === 'BUY' || activity.side === 'LONG' ? <ArrowUpRight className="w-4 h-4" /> : <ArrowDownRight className="w-4 h-4" />}
                                        </div>
                                        <div className="flex-1">
                                            <div className="flex justify-between items-start mb-1">
                                                <span className="text-sm font-bold text-white">{activity.action} {activity.symbol}</span>
                                                <span className="text-xs text-muted-foreground">{formatActivityDate(activity.timestamp)}</span>
                                            </div>
                                            <div className="flex justify-between items-center text-xs">
                                                <span className="text-gray-400">
                                                    {activity.qty ?? '—'} @ {activity.price != null ? `$${Number(activity.price).toLocaleString()}` : 'N/A'}
                                                </span>
                                                {(activity.pnl != null || activity.realized_pnl != null) && Number(activity.pnl ?? activity.realized_pnl) !== 0 && (
                                                    <span className={`font-mono ${Number(activity.pnl ?? activity.realized_pnl) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                                        {Number(activity.pnl ?? activity.realized_pnl) >= 0 ? '+' : ''}${Number(activity.pnl ?? activity.realized_pnl).toLocaleString()}
                                                    </span>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                ))}
                                {recent_activity?.length === 0 && (
                                    <div className="text-center text-sm text-muted-foreground py-4">No recent activity</div>
                                )}
                            </div>
                            <button
                                onClick={() => navigate('/dashboard/analytics?tab=trades&sub=completed')}
                                className="w-full mt-6 py-2 text-sm text-muted-foreground border border-white/10 rounded-lg hover:bg-white/5 hover:text-white transition-colors flex items-center justify-center gap-1.5"
                            >
                                View Full Trade History <ArrowUpRight className="w-3.5 h-3.5" />
                            </button>
                        </div>
                    </div>

                    {/* Recent Closed Trades + Trading Performance */}
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Recent Closed Trades */}
                        <div className="lg:col-span-2 bg-[#0F1218] border border-white/5 rounded-2xl overflow-hidden shadow-xl">
                            <div className="p-6 border-b border-white/5 flex justify-between items-center">
                                <div>
                                    <h2 className="text-lg font-bold text-white">Recent Closed Trades</h2>
                                    <p className="text-xs text-muted-foreground mt-0.5">Last completed positions from trade history</p>
                                </div>
                                <button
                                    onClick={() => navigate('/dashboard/analytics?tab=trades&sub=completed')}
                                    className="flex items-center gap-1 text-xs text-primary hover:text-primary/80 transition-colors"
                                >
                                    View Full History <ArrowUpRight className="w-3 h-3" />
                                </button>
                            </div>
                            <div className="overflow-x-auto">
                                <table className="w-full">
                                    <thead className="bg-white/5 text-xs text-muted-foreground uppercase tracking-wider font-semibold">
                                        <tr>
                                            <th className="px-6 py-4 text-left">Symbol</th>
                                            <th className="px-6 py-4 text-left">Side</th>
                                            <th className="px-6 py-4 text-left whitespace-nowrap">Closed</th>
                                            <th className="px-6 py-4 text-right whitespace-nowrap">Realized PnL</th>
                                            <th className="px-6 py-4 text-right whitespace-nowrap">Net PnL</th>
                                            <th className="px-6 py-4 text-center">Result</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-white/5">
                                        {isClosedTradesLoading ? (
                                            Array.from({ length: 3 }).map((_, i) => (
                                                <tr key={i} className="animate-pulse">
                                                    <td className="px-6 py-4"><div className="h-3 bg-white/10 rounded w-20" /></td>
                                                    <td className="px-6 py-4"><div className="h-4 bg-white/10 rounded w-12" /></td>
                                                    <td className="px-6 py-4"><div className="h-3 bg-white/10 rounded w-28" /></td>
                                                    <td className="px-6 py-4 text-right"><div className="h-3 bg-white/10 rounded w-16 ml-auto" /></td>
                                                    <td className="px-6 py-4 text-right"><div className="h-3 bg-white/10 rounded w-16 ml-auto" /></td>
                                                    <td className="px-6 py-4"><div className="h-4 bg-white/10 rounded w-16 mx-auto" /></td>
                                                </tr>
                                            ))
                                        ) : recentClosedTrades.length > 0 ? (
                                            recentClosedTrades.map((pos) => (
                                                <tr
                                                    key={pos.position_id}
                                                    className="hover:bg-white/5 transition-colors cursor-pointer"
                                                    onClick={() => navigate('/dashboard/analytics?tab=trades&sub=completed')}
                                                >
                                                    <td className="px-6 py-4">
                                                        <div className="font-bold text-white text-sm">{pos.symbol}</div>
                                                        <div className="text-[10px] text-muted-foreground">{pos.broker_account_id || '—'}</div>
                                                    </td>
                                                    <td className="px-6 py-4">
                                                        <span className={`text-xs font-semibold px-2 py-0.5 rounded ${pos.side === 'LONG' || pos.side === 'BUY' ? 'bg-green-500/10 text-green-500' : 'bg-red-500/10 text-red-500'}`}>
                                                            {pos.side}
                                                        </span>
                                                    </td>
                                                    <td className="px-6 py-4 text-xs text-muted-foreground whitespace-nowrap">
                                                        {formatActivityDate(pos.closed_at)}
                                                    </td>
                                                    <td className={`px-6 py-4 text-right font-mono text-sm font-semibold whitespace-nowrap ${pos.realized_pnl >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                                        {pos.realized_pnl >= 0 ? '+' : '-'}${Math.abs(pos.realized_pnl).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                                                    </td>
                                                    <td className={`px-6 py-4 text-right font-mono text-sm font-bold whitespace-nowrap ${(pos.net_pnl ?? pos.realized_pnl) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                                        {pos.net_pnl != null
                                                            ? ((pos.net_pnl >= 0 ? '+' : '-') + '$' + Math.abs(pos.net_pnl).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }))
                                                            : 'N/A'}
                                                    </td>
                                                    <td className="px-6 py-4 text-center whitespace-nowrap">
                                                        {pos.result === 'winner' && (
                                                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-green-500/10 text-green-400 border border-green-500/20">
                                                                <Trophy className="w-3 h-3" /> Winner
                                                            </span>
                                                        )}
                                                        {pos.result === 'loser' && (
                                                            <span className="inline-flex px-2 py-0.5 rounded-full text-xs font-semibold bg-red-500/10 text-red-400 border border-red-500/20">
                                                                Loser
                                                            </span>
                                                        )}
                                                        {pos.result === 'breakeven' && (
                                                            <span className="inline-flex px-2 py-0.5 rounded-full text-xs font-semibold bg-white/5 text-muted-foreground border border-white/10">
                                                                Even
                                                            </span>
                                                        )}
                                                    </td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={6} className="px-6 py-10 text-center">
                                                    <div className="flex flex-col items-center gap-2">
                                                        <CheckCircle2 className="w-8 h-8 text-muted-foreground/30" />
                                                        <p className="text-sm text-muted-foreground">No closed trades yet</p>
                                                        <p className="text-xs text-muted-foreground/70">Completed positions will appear here once your bot closes trades.</p>
                                                        <button
                                                            onClick={() => navigate('/dashboard/analytics?tab=trades&sub=completed')}
                                                            className="mt-2 px-4 py-1.5 text-xs text-primary border border-primary/20 rounded-lg hover:bg-primary/10 transition-colors"
                                                        >
                                                            Go to Trade History
                                                        </button>
                                                    </div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        {/* Trading Performance Summary */}
                        <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 shadow-xl flex flex-col gap-5">
                            <div className="flex justify-between items-center">
                                <h2 className="text-lg font-bold text-white">Trading Performance</h2>
                                <BarChart3 className="w-4 h-4 text-muted-foreground" />
                            </div>

                            {isClosedTradesLoading ? (
                                <div className="space-y-3 animate-pulse flex-1">
                                    {Array.from({ length: 6 }).map((_, i) => (
                                        <div key={i} className="flex justify-between">
                                            <div className="h-3 bg-white/10 rounded w-24" />
                                            <div className="h-3 bg-white/10 rounded w-16" />
                                        </div>
                                    ))}
                                </div>
                            ) : perfSummary ? (
                                <div className="space-y-2.5 flex-1">
                                    <PerfRow label="Realized PnL" value={perfSummary.total_realized_pnl} type="money_signed" />
                                    <PerfRow label="Net PnL (after fees)" value={perfSummary.total_net_pnl} type="money_signed" />
                                    <PerfRow label="Total Fees" value={perfSummary.total_fees} type="fees" />
                                    <div className="border-t border-white/5 my-1" />
                                    <PerfRow label="Closed Trades" value={perfSummary.closed_count} type="count" />
                                    <PerfRow label="Winners" value={perfSummary.winners} type="count_green" />
                                    <PerfRow label="Losers" value={perfSummary.losers} type="count_red" />
                                    <PerfRow label="Win Rate" value={perfSummary.win_rate} type="percent" />
                                </div>
                            ) : (
                                <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground py-4">
                                    No performance data yet
                                </div>
                            )}

                            <button
                                onClick={() => navigate('/dashboard/analytics?tab=trades&sub=completed')}
                                className="w-full py-2.5 text-sm font-medium text-primary border border-primary/20 rounded-lg hover:bg-primary/10 transition-colors flex items-center justify-center gap-2"
                            >
                                View Full Trade History <ArrowUpRight className="w-3.5 h-3.5" />
                            </button>
                        </div>
                    </div>
                </>
            )}

            {/* Reconciliation Drawer */}
            {showReconciliation && (
                <>
                    <div className="fixed inset-0 z-40 bg-black/40 backdrop-blur-sm" onClick={() => setShowReconciliation(false)} />
                    <div className="fixed right-0 top-0 bottom-0 z-50 w-full max-w-md bg-card border-l border-border shadow-2xl flex flex-col overflow-hidden">
                        <div className="flex items-center justify-between px-6 py-4 border-b border-border flex-shrink-0">
                            <div>
                                <h2 className="text-lg font-bold">Reconciliation</h2>
                                <p className="text-sm text-muted-foreground">Account vs closed-trade PnL</p>
                            </div>
                            <button onClick={() => setShowReconciliation(false)} className="p-2 rounded-lg hover:bg-muted text-muted-foreground hover:text-foreground transition-colors">
                                <X className="w-4 h-4" />
                            </button>
                        </div>

                        <div className="flex-1 overflow-y-auto px-6 py-4 space-y-6">
                            {isReconciliationLoading ? (
                                <div className="text-sm text-muted-foreground">Loadingâ€¦</div>
                            ) : !reconciliation ? (
                                <div className="text-sm text-muted-foreground">Reconciliation unavailable</div>
                            ) : (
                                <>
                                    <div>
                                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Account Balance</h3>
                                        <div className="bg-background rounded-lg border border-border p-4 space-y-2">
                                            <div className="flex justify-between"><span className="text-xs text-muted-foreground">Broker Equity</span><span className="font-mono text-sm">${reconciliation.account.equity.toFixed(2)}</span></div>
                                            <div className="flex justify-between"><span className="text-xs text-muted-foreground">Wallet Balance</span><span className="font-mono text-sm">${reconciliation.account.wallet_balance.toFixed(2)}</span></div>
                                            <div className="flex justify-between"><span className="text-xs text-muted-foreground">Unrealized PnL</span><span className={`font-mono text-sm ${reconciliation.account.unrealized_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>{reconciliation.account.unrealized_pnl >= 0 ? '+' : ''}${Math.abs(reconciliation.account.unrealized_pnl).toFixed(2)}</span></div>
                                            {reconciliation.account.balance_change && (
                                                <div className="pt-2 border-t border-border/60 text-[11px] text-muted-foreground space-y-1">
                                                    <div>Window: {reconciliation.account.balance_change.from_ts} â†’ {reconciliation.account.balance_change.to_ts}</div>
                                                    <div>Equity change: {reconciliation.account.balance_change.equity_change >= 0 ? '+' : ''}{reconciliation.account.balance_change.equity_change.toFixed(2)}</div>
                                                    <div>Wallet change: {reconciliation.account.balance_change.wallet_balance_change >= 0 ? '+' : ''}{reconciliation.account.balance_change.wallet_balance_change.toFixed(2)}</div>
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                    <div>
                                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Closed Trade Totals</h3>
                                        <div className="bg-background rounded-lg border border-border p-4 space-y-2">
                                            <div className="flex justify-between"><span className="text-xs text-muted-foreground">Closed positions</span><span className="font-mono text-sm">{reconciliation.trade_fills.closed_positions}</span></div>
                                            <div className="flex justify-between"><span className="text-xs text-muted-foreground">Gross realized PnL</span><span className="font-mono text-sm">{reconciliation.trade_fills.gross_realized_pnl >= 0 ? '+' : ''}${Math.abs(reconciliation.trade_fills.gross_realized_pnl).toFixed(2)}</span></div>
                                            <div className="flex justify-between"><span className="text-xs text-muted-foreground">Fees</span><span className="font-mono text-sm text-red-400">-${Math.abs(reconciliation.trade_fills.fees).toFixed(2)}</span></div>
                                            <div className="flex justify-between"><span className="text-xs text-muted-foreground">Net PnL</span><span className={`font-mono text-sm font-bold ${reconciliation.trade_fills.net_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>{reconciliation.trade_fills.net_pnl >= 0 ? '+' : ''}${Math.abs(reconciliation.trade_fills.net_pnl).toFixed(2)}</span></div>
                                        </div>
                                    </div>

                                    <div>
                                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Explanation</h3>
                                        <div className="bg-background rounded-lg border border-border p-4 text-sm text-muted-foreground leading-relaxed">
                                            {reconciliationExplanation(reconciliation.difference?.likely_explanation)}
                                        </div>
                                    </div>

                                    <div>
                                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Data Quality</h3>
                                        <div className="bg-background rounded-lg border border-border p-4 space-y-2">
                                            <div className="text-xs text-muted-foreground">Scope Mode</div>
                                            <div className="font-mono text-xs text-muted-foreground break-all">{reconciliation.metadata?.scope_mode}</div>
                                            {(reconciliation.metadata?.warnings?.length ?? 0) > 0 && (
                                                <div className="pt-2 border-t border-border/60 space-y-1">
                                                    {reconciliation.metadata.warnings.map((w, i) => (
                                                        <div key={i} className="text-xs text-yellow-300/90 flex items-start gap-2">
                                                            <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
                                                            <span>{w}</span>
                                                        </div>
                                                    ))}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </>
                            )}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}

function PerfRow({ label, value, type }: {
    label: string;
    value: number | null | undefined;
    type: 'money_signed' | 'fees' | 'count' | 'count_green' | 'count_red' | 'percent';
}) {
    let display = '—';
    let colorClass = 'text-white';

    if (value != null) {
        const abs = Math.abs(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        if (type === 'money_signed') {
            display = (value >= 0 ? '+$' : '-$') + abs;
            colorClass = value >= 0 ? 'text-green-400' : 'text-red-400';
        } else if (type === 'fees') {
            display = '-$' + abs;
            colorClass = 'text-red-400';
        } else if (type === 'count') {
            display = value.toString();
        } else if (type === 'count_green') {
            display = value.toString();
            colorClass = 'text-green-400';
        } else if (type === 'count_red') {
            display = value.toString();
            colorClass = 'text-red-400';
        } else if (type === 'percent') {
            display = (value >= 0 ? '+' : '') + value.toFixed(1) + '%';
            colorClass = value >= 50 ? 'text-green-400' : value < 40 ? 'text-red-400' : 'text-white';
        }
    }

    return (
        <div className="flex justify-between items-center">
            <span className="text-xs text-muted-foreground">{label}</span>
            <span className={`text-sm font-mono font-semibold ${colorClass}`}>{display}</span>
        </div>
    );
}

// Icon helper since I can't import layout-dashboard easily if it's not exported or if I want a specific one
function LayoutDashboardIcon(props: any) {
    return (
        <svg
            {...props}
            xmlns="http://www.w3.org/2000/svg"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
        >
            <rect width="7" height="9" x="3" y="3" rx="1" />
            <rect width="7" height="5" x="14" y="3" rx="1" />
            <rect width="7" height="9" x="14" y="12" rx="1" />
            <rect width="7" height="5" x="3" y="16" rx="1" />
        </svg>
    )
}
