import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import {
    Wallet, TrendingUp, TrendingDown, DollarSign,
    ArrowUpRight, ArrowDownRight, Download, Upload,
    RefreshCw, AlertCircle, Building2, Eye, EyeOff
} from "lucide-react";
import { Link } from "react-router-dom";

// Account Card Component with Real Data
function AccountCard({ account, hideBalances }: { account: any; hideBalances: boolean }) {
    const { data: summary, isLoading } = useQuery({
        queryKey: ["broker-capital", account.id],
        queryFn: () => api.getBrokerSummary(account.id),
        refetchInterval: 30000,
        retry: false
    });

    const formatCurrency = (value: number) => {
        if (hideBalances) return "****";
        return `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    };

    // Extract real data from API
    const capital = summary?.capital;
    const totalEquity = capital?.total_equity || 0;
    const availableBalance = capital?.available_balance || 0;
    const openPositions = capital?.position_count || 0;
    const marginUsed = capital?.initial_margin || 0;

    return (
        <div className="bg-card border border-border/50 rounded-2xl p-5 hover:border-primary/20 transition-colors">
            <div className="flex items-center gap-3 mb-4">
                <div className="w-10 h-10 bg-white rounded-lg flex items-center justify-center">
                    <Building2 className="w-6 h-6 text-black/20" />
                </div>
                <div>
                    <h3 className="font-bold capitalize">{account.label || account.broker_id}</h3>
                    <p className="text-xs text-muted-foreground">{account.broker_id}</p>
                </div>
            </div>

            {isLoading ? (
                <div className="space-y-3">
                    <div className="h-4 bg-muted animate-pulse rounded w-1/2"></div>
                    <div className="h-8 bg-muted animate-pulse rounded"></div>
                    <div className="grid grid-cols-2 gap-3">
                        <div className="h-12 bg-muted animate-pulse rounded"></div>
                        <div className="h-12 bg-muted animate-pulse rounded"></div>
                    </div>
                </div>
            ) : summary?.error ? (
                <div className="text-xs text-red-400 p-3 bg-red-500/10 rounded-lg border border-red-500/20">
                    Error: {summary.error}
                </div>
            ) : (
                <div className="space-y-3">
                    <div>
                        <p className="text-xs text-muted-foreground">Total Equity</p>
                        <p className="text-xl font-bold">{formatCurrency(totalEquity)}</p>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                        <div>
                            <p className="text-xs text-muted-foreground">Open Positions</p>
                            <p className="text-sm font-semibold">{openPositions}</p>
                        </div>
                        <div>
                            <p className="text-xs text-muted-foreground">Margin Used</p>
                            <p className="text-sm font-semibold">{formatCurrency(marginUsed)}</p>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

export default function Portfolio() {
    const [hideBalances, setHideBalances] = useState(false);

    const { data: portfolioData, isLoading, error } = useQuery({
        queryKey: ['portfolio-summary'],
        queryFn: api.getPortfolioSummary,

        refetchInterval: 5000,
    });

    const { data: transactionsData } = useQuery({
        queryKey: ['portfolio-transactions'],
        queryFn: api.getPortfolioTransactions,
        refetchInterval: 30000,
    });

    const { data: brokerData } = useQuery({
        queryKey: ['broker-accounts'],
        queryFn: api.getBrokerAccounts,
    });

    if (isLoading) {
        return (
            <div className="flex items-center justify-center min-h-[400px]">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
            </div>
        );
    }

    if (error) {
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

    const { summary, positions, allocation, account_details } = portfolioData || {};
    const accounts = brokerData?.accounts || [];
    const connectedAccounts = accounts.filter((a: any) => a.status === 'connected');

    // Use account_details if available, otherwise fall back to allocation/summary
    // Use account_details if available, otherwise fall back to allocation/summary
    const totalBalance = account_details?.margin_balance || summary?.total_portfolio_value || 0;
    const availableCash = account_details?.available_balance || allocation?.find(a => a.asset === 'Cash')?.value || 0;
    // Invested amount (Initial Margin)
    const marginUsed = account_details?.initial_margin || account_details?.margin_used || allocation?.find(a => a.asset === 'Invested')?.value || 0;
    // Maintenance Margin (Minimum required)
    const maintenanceMargin = account_details?.maintenance_margin || 0;

    const unrealizedPnL = account_details?.unrealized_pnl || summary?.total_day_pnl || 0;
    const walletBalance = account_details?.wallet_balance || (totalBalance - unrealizedPnL);
    const pnlPercent = summary?.total_day_pnl_percent || 0;

    // Margin Ratio %
    const marginRatio = account_details?.margin_ratio_percent || (totalBalance > 0 && maintenanceMargin > 0 ? (maintenanceMargin / totalBalance) * 100 : 0);

    // Group positions by account_id for per-broker breakdown
    const positionsByAccount = (positions || []).reduce((acc: any, pos: any) => {
        if (!acc[pos.account_id]) {
            acc[pos.account_id] = [];
        }
        acc[pos.account_id].push(pos);
        return acc;
    }, {});

    const formatCurrency = (value: number) => {
        if (hideBalances) return "****";
        return `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    };

    return (
        <div className="space-y-6 p-6">
            {/* Header */}
            <div className="flex justify-between items-center">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight flex items-center gap-2">
                        <Wallet className="w-8 h-8 text-primary" />
                        Portfolio
                    </h1>
                    <p className="text-sm text-muted-foreground mt-1">Complete overview of your wallet and assets</p>
                </div>
                <button
                    onClick={() => setHideBalances(!hideBalances)}
                    className="p-2 hover:bg-muted rounded-lg transition-colors"
                    title={hideBalances ? "Show balances" : "Hide balances"}
                >
                    {hideBalances ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                </button>
            </div>

            {/* Wallet Overview */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <div className="bg-gradient-to-br from-blue-500/10 to-purple-500/10 border border-blue-500/20 rounded-2xl p-6">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-2">
                        <Wallet className="w-4 h-4 text-blue-500" />
                        Margin Balance
                    </div>
                    <div className="text-3xl font-bold">{formatCurrency(totalBalance)}</div>
                    <p className="text-xs text-green-500 mt-1">{pnlPercent >= 0 ? '+' : ''}{pnlPercent.toFixed(2)}%</p>
                </div>

                {/* Cash / Balance */}
                <div className="bg-card border border-border/50 rounded-2xl p-6">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-2">
                        <DollarSign className="w-4 h-4 text-green-500" />
                        Cash (Balance)
                    </div>
                    <div className="text-3xl font-bold">{formatCurrency(availableCash)}</div>
                    <p className="text-xs text-muted-foreground mt-1">Available</p>
                </div>

                {/* Maintenance Margin / Invested */}
                <div className="bg-card border border-border/50 rounded-2xl p-6">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-2">
                        <TrendingUp className="w-4 h-4 text-orange-500" />
                        Maintenance Margin
                    </div>
                    <div className="text-3xl font-bold">{formatCurrency(maintenanceMargin)}</div>
                    <p className="text-xs text-muted-foreground mt-1">Required</p>
                </div>

                {/* Unrealized PnL */}
                <div className={`bg-card border rounded-2xl p-6 ${unrealizedPnL >= 0 ? 'border-green-500/20' : 'border-red-500/20'}`}>
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-2">
                        {unrealizedPnL >= 0 ? (
                            <ArrowUpRight className="w-4 h-4 text-green-500" />
                        ) : (
                            <ArrowDownRight className="w-4 h-4 text-red-500" />
                        )}
                        Unrealized PnL
                    </div>
                    <div className={`text-3xl font-bold ${unrealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {formatCurrency(Math.abs(unrealizedPnL))}
                    </div>
                    <p className={`text-xs mt-1 ${unrealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {pnlPercent >= 0 ? '+' : ''}{pnlPercent.toFixed(2)}% today
                    </p>
                </div>
            </div>



            {/* Detailed Account Information (Binance Style) */}
            <div className="bg-card border border-border/50 rounded-2xl p-6">
                <h2 className="text-lg font-bold mb-4">Account</h2>

                {/* Margin Ratio Section */}
                <div className="mb-6">
                    <h3 className="text-sm font-semibold text-muted-foreground mb-3">Margin Ratio</h3>
                    <div className="space-y-3">
                        <div className="flex justify-between items-center">
                            <span className="text-sm text-muted-foreground">USDT Cross</span>
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 rounded-full bg-green-500"></div>
                                <span className="text-sm font-semibold text-green-500">
                                    {marginRatio.toFixed(2)}%
                                </span>
                            </div>
                        </div>
                        <div className="flex justify-between items-center">
                            <span className="text-sm text-muted-foreground">Maintenance Margin</span>
                            <span className="text-sm font-semibold">{formatCurrency(maintenanceMargin)}</span>
                        </div>
                        <div className="flex justify-between items-center">
                            <span className="text-sm text-muted-foreground">Margin Balance</span>
                            <span className="text-sm font-semibold">{formatCurrency(totalBalance)}</span>
                        </div>
                    </div>
                </div>

                {/* Single-Asset Mode Button */}
                <button className="w-full py-2.5 bg-muted hover:bg-muted/80 text-foreground rounded-lg font-semibold text-sm transition-colors mb-6">
                    Single-Asset Mode
                </button>

                {/* USDT Section */}
                <div>
                    <h3 className="text-sm font-semibold mb-3 flex items-center gap-2">
                        USDT
                        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                        </svg>
                    </h3>
                    <div className="space-y-3">
                        <div className="flex justify-between items-center">
                            <span className="text-sm text-muted-foreground">Balance</span>
                            <span className="text-sm font-semibold">{formatCurrency(availableCash)}</span>
                        </div>
                        <div className="flex justify-between items-center">
                            <span className="text-sm text-muted-foreground">Unrealized PNL</span>
                            <span className={`text-sm font-semibold ${unrealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                {formatCurrency(unrealizedPnL)}
                            </span>
                        </div>
                    </div>
                </div>
            </div>

            {/* Per-Broker Breakdown */}
            <div>
                <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
                    <Building2 className="w-5 h-5 text-primary" />
                    Accounts Breakdown
                </h2>

                {connectedAccounts.length === 0 ? (
                    <div className="bg-card border border-dashed border-border/40 rounded-2xl p-12 text-center">
                        <Building2 className="w-12 h-12 text-muted-foreground/50 mx-auto mb-4" />
                        <h3 className="text-lg font-semibold mb-2">No Connected Accounts</h3>
                        <p className="text-muted-foreground mb-6">Connect a broker to see your account breakdown</p>
                        <Link to="/dashboard/brokers" className="inline-flex px-6 py-2.5 bg-primary text-primary-foreground rounded-lg font-semibold hover:bg-primary/90 transition-colors">
                            Connect Broker
                        </Link>
                    </div>
                ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {connectedAccounts.map((account: any) => {
                            return <AccountCard key={account.id} account={account} hideBalances={hideBalances} />;
                        })}
                    </div>
                )}
            </div>

            {/* Transaction History */}
            <div>
                <h2 className="text-xl font-bold mb-4">Transaction History</h2>
                <div className="bg-card border border-border/50 rounded-2xl overflow-hidden">
                    <div className="overflow-x-auto">
                        <table className="w-full">
                            <thead className="bg-muted/50 border-b border-border/50">
                                <tr>
                                    <th className="text-left p-4 text-sm font-semibold text-muted-foreground">Date/Time</th>
                                    <th className="text-left p-4 text-sm font-semibold text-muted-foreground">Account</th>
                                    <th className="text-left p-4 text-sm font-semibold text-muted-foreground">Type</th>
                                    <th className="text-left p-4 text-sm font-semibold text-muted-foreground">Asset</th>
                                    <th className="text-right p-4 text-sm font-semibold text-muted-foreground">Amount</th>
                                    <th className="text-left p-4 text-sm font-semibold text-muted-foreground">Status</th>
                                    <th className="text-left p-4 text-sm font-semibold text-muted-foreground">TxID</th>
                                </tr>
                            </thead>
                            <tbody>

                                {transactionsData?.transactions.map((tx: any, idx: number) => (
                                    <tr key={tx.tx_id || idx} className="border-b border-border/50 bg-card hover:bg-muted/30 transition-colors">
                                        <td className="p-4 text-sm text-foreground">{new Date(tx.timestamp).toLocaleString()}</td>
                                        <td className="p-4 text-sm text-foreground">
                                            <div className="font-semibold">{tx.broker}</div>
                                            <div className="text-xs text-muted-foreground">{tx.account_id}</div>
                                        </td>
                                        <td className="p-4 text-sm">
                                            <span className={`px-2 py-0.5 rounded text-xs font-semibold ${tx.type === 'DEPOSIT' ? 'bg-green-500/10 text-green-500' : 'bg-orange-500/10 text-orange-500'
                                                }`}>
                                                {tx.type}
                                            </span>
                                        </td>
                                        <td className="p-4 text-sm text-foreground font-medium">{tx.asset}</td>
                                        <td className={`p-4 text-sm text-right font-mono font-bold ${tx.type === 'DEPOSIT' ? 'text-green-500' : 'text-foreground'
                                            }`}>
                                            {tx.type === 'DEPOSIT' ? '+' : '-'}{tx.amount.toLocaleString()}
                                        </td>
                                        <td className="p-4 text-sm">
                                            <span className={`flex items-center gap-1.5 ${tx.status === 'SUCCESS' ? 'text-green-500' :
                                                tx.status === 'PENDING' ? 'text-yellow-500' : 'text-red-500'
                                                }`}>
                                                <span className={`w-1.5 h-1.5 rounded-full ${tx.status === 'SUCCESS' ? 'bg-green-500' :
                                                    tx.status === 'PENDING' ? 'bg-yellow-500' : 'bg-red-500'
                                                    }`} />
                                                {tx.status}
                                            </span>
                                        </td>
                                        <td className="p-4 text-sm text-muted-foreground font-mono text-xs truncate max-w-[120px]" title={tx.tx_id}>
                                            {tx.tx_id || '-'}
                                        </td>
                                    </tr>
                                ))}
                                {(!transactionsData?.transactions || transactionsData.transactions.length === 0) && (
                                    <tr>
                                        <td colSpan={7} className="p-8 text-center text-muted-foreground">
                                            No recent transactions found
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    );
}
