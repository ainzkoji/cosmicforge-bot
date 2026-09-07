import { useState } from 'react';
import {
    BarChart3, Download, PieChart, TrendingUp,
    ArrowUpRight, Printer, FileText, Landmark,
    DollarSign, Activity, Loader2
} from 'lucide-react';
import { motion } from 'framer-motion';
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';

export default function Analytics() {
    const [timeframe, setTimeframe] = useState<'1M' | '3M' | 'YTD' | 'ALL'>('YTD');
    const [activeTab, setActiveTab] = useState<'performance' | 'tax' | 'benchmarks'>('performance');

    // Fetch Overview Stats
    const overviewQuery = useQuery({
        queryKey: ['analytics-overview', timeframe],
        queryFn: () => api.getAnalyticsOverview(timeframe),
        refetchInterval: 30000 // Refresh every 30s
    });

    // Fetch Leaderboard (for asset allocation or other widgets later)
    const leaderboardQuery = useQuery({
        queryKey: ['analytics-leaderboard'],
        queryFn: () => api.getAnalyticsLeaderboard(5)
    });

    const data = overviewQuery.data || {
        total_profit: 0,
        total_trades: 0,
        win_rate: 0,
        profit_factor: 0,
        profit_change_pct: 0,
        sharpe_ratio: 0
    };

    // Mock Monthly PnL for chart (until we have a timeline API)
    const monthlyPnL = [
        { month: 'Jan', value: 1200 },
        { month: 'Feb', value: -400 },
        { month: 'Mar', value: 2100 },
        { month: 'Apr', value: 850 },
        { month: 'May', value: 1600 },
        { month: 'Jun', value: 3200 },
    ];

    if (overviewQuery.isLoading && !overviewQuery.data) {
        return (
            <div className="flex h-[50vh] items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
        );
    }

    if (overviewQuery.isError) {
        return (
            <div className="p-8 text-center bg-destructive/10 text-destructive rounded-xl">
                <p>Failed to load analytics data. Please try again later.</p>
            </div>
        );
    }

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="max-w-[1600px] mx-auto space-y-8"
        >
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Analytics & Reports</h1>
                    <p className="text-muted-foreground">Comprehensive insights into your trading performance.</p>
                </div>
                <div className="flex items-center gap-2">
                    <div className="flex bg-muted rounded-lg p-1">
                        {(['1M', '3M', 'YTD', 'ALL'] as const).map((tf) => (
                            <button
                                key={tf}
                                onClick={() => setTimeframe(tf)}
                                className={`px-3 py-1 rounded-md text-sm font-medium transition-all ${timeframe === tf
                                    ? 'bg-background shadow text-foreground'
                                    : 'text-muted-foreground hover:text-foreground'
                                    }`}
                            >
                                {tf}
                            </button>
                        ))}
                    </div>
                    <button className="flex items-center gap-2 px-4 py-2 bg-card border border-border rounded-lg hover:bg-muted transition-colors">
                        <Printer className="w-4 h-4" />
                        <span>Print</span>
                    </button>
                    <button className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg shadow hover:shadow-lg transition-all">
                        <Download className="w-4 h-4" />
                        <span>Export</span>
                    </button>
                </div>
            </div>

            {/* Navigation Tabs */}
            <div className="border-b border-border">
                <div className="flex gap-8">
                    {[
                        { id: 'performance', label: 'Performance', icon: TrendingUp },
                        { id: 'tax', label: 'Tax Reporting', icon: Landmark },
                        { id: 'benchmarks', label: 'Benchmarks', icon: BarChart3 },
                    ].map((tab) => (
                        <button
                            key={tab.id}
                            onClick={() => setActiveTab(tab.id as any)}
                            className={`flex items-center gap-2 pb-4 text-sm font-medium transition-colors border-b-2 ${activeTab === tab.id
                                ? 'border-primary text-primary'
                                : 'border-transparent text-muted-foreground hover:text-foreground'
                                }`}
                        >
                            <tab.icon className="w-4 h-4" />
                            {tab.label}
                        </button>
                    ))}
                </div>
            </div>

            {/* Content Areas */}
            {activeTab === 'performance' && (
                <div className="space-y-6">
                    {/* KPI Cards */}
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                        <div className="bg-card border border-border rounded-xl p-6">
                            <div className="flex items-center justify-between mb-2">
                                <span className="text-sm font-medium text-muted-foreground">Net Profit</span>
                                <DollarSign className="w-4 h-4 text-green-500" />
                            </div>
                            <div className="text-2xl font-bold font-mono">
                                ${data.total_profit.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                            </div>
                            <div className="flex items-center gap-1 text-xs text-green-500 mt-1">
                                <ArrowUpRight className="w-3 h-3" />
                                <span>+{data.profit_change_pct}%</span>
                            </div>
                        </div>
                        <div className="bg-card border border-border rounded-xl p-6">
                            <div className="flex items-center justify-between mb-2">
                                <span className="text-sm font-medium text-muted-foreground">Win Rate</span>
                                <PieChart className="w-4 h-4 text-blue-500" />
                            </div>
                            <div className="text-2xl font-bold">{data.win_rate}%</div>
                            <div className="text-xs text-muted-foreground mt-1">Out of {data.total_trades} trades</div>
                        </div>
                        <div className="bg-card border border-border rounded-xl p-6">
                            <div className="flex items-center justify-between mb-2">
                                <span className="text-sm font-medium text-muted-foreground">Profit Factor</span>
                                <TrendingUp className="w-4 h-4 text-purple-500" />
                            </div>
                            <div className="text-2xl font-bold">{data.profit_factor}</div>
                            <div className="text-xs text-muted-foreground mt-1">Gross Profit / Gross Loss</div>
                        </div>
                        <div className="bg-card border border-border rounded-xl p-6">
                            <div className="flex items-center justify-between mb-2">
                                <span className="text-sm font-medium text-muted-foreground">Sharpe Ratio</span>
                                <Activity className="w-4 h-4 text-amber-500" />
                            </div>
                            <div className="text-2xl font-bold">{data.sharpe_ratio}</div>
                            <div className="text-xs text-muted-foreground mt-1">Risk-adjusted return</div>
                        </div>
                    </div>

                    {/* Charts Section */}
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        <div className="lg:col-span-2 bg-card border border-border rounded-xl p-6">
                            <h3 className="text-lg font-bold mb-6">Equity Curve</h3>
                            {/* Placeholder for Advanced Chart (e.g., TradingView or Recharts) */}
                            <div className="h-80 bg-muted/20 rounded-lg flex items-center justify-center border border-dashed border-border">
                                <p className="text-muted-foreground">Interactive Chart Integration Area</p>
                            </div>
                        </div>
                        <div className="bg-card border border-border rounded-xl p-6">
                            <h3 className="text-lg font-bold mb-6">Monthly P&L</h3>
                            <div className="space-y-4">
                                {monthlyPnL.map((item) => (
                                    <div key={item.month} className="space-y-1">
                                        <div className="flex justify-between text-sm">
                                            <span>{item.month}</span>
                                            <span className={item.value >= 0 ? 'text-green-500' : 'text-red-500'}>
                                                {item.value >= 0 ? '+' : ''}${item.value}
                                            </span>
                                        </div>
                                        <div className="h-2 bg-muted rounded-full overflow-hidden">
                                            <div
                                                className={`h-full ${item.value >= 0 ? 'bg-green-500' : 'bg-red-500'}`}
                                                style={{ width: `${Math.min(Math.abs(item.value) / 40, 100)}%` }} // Simple scale
                                            />
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>

                        {/* Row 2: Asset Allocation (Mocked for now, or use Leaderboard) */}
                        <div className="lg:col-span-1 bg-card border border-border rounded-xl p-6">
                            <h3 className="text-lg font-bold mb-6">Asset Allocation</h3>
                            <div className="flex flex-col items-center">
                                {/* Simple CSS Pie Chart Mock */}
                                <div className="relative w-48 h-48 mb-6">
                                    <svg viewBox="0 0 100 100" className="w-full h-full transform -rotate-90">
                                        <circle cx="50" cy="50" r="40" fill="transparent" stroke="#3b82f6" strokeWidth="20" strokeDasharray="150 251" />
                                        <circle cx="50" cy="50" r="40" fill="transparent" stroke="#8b5cf6" strokeWidth="20" strokeDasharray="70 251" strokeDashoffset="-150" />
                                        <circle cx="50" cy="50" r="40" fill="transparent" stroke="#10b981" strokeWidth="20" strokeDasharray="31 251" strokeDashoffset="-220" />
                                    </svg>
                                    <div className="absolute inset-0 flex items-center justify-center flex-col">
                                        <span className="text-xs text-muted-foreground">Total Value</span>
                                        <span className="font-bold">$42,500</span>
                                    </div>
                                </div>
                                <div className="w-full space-y-3">
                                    {/* Using leaderboard data to show top strategies if available? */}
                                    {leaderboardQuery.data && leaderboardQuery.data.length > 0 ? (
                                        leaderboardQuery.data.slice(0, 3).map((item: any, i: number) => (
                                            <div key={item.strategy + i} className="flex justify-between items-center text-sm">
                                                <div className="flex items-center gap-2">
                                                    <div className={`w-3 h-3 rounded-full ${i === 0 ? 'bg-blue-500' : i === 1 ? 'bg-violet-500' : 'bg-emerald-500'}`} />
                                                    <span>{item.strategy} ({item.symbol})</span>
                                                </div>
                                                <span className="font-bold">{item.win_rate}% WR</span>
                                            </div>
                                        ))
                                    ) : (
                                        <div className="text-center text-sm text-muted-foreground">No active strategies</div>
                                    )}
                                </div>
                            </div>
                        </div>

                        <div className="lg:col-span-2 bg-card border border-border rounded-xl p-6">
                            <h3 className="text-lg font-bold mb-6">Advanced Risk Metrics</h3>
                            <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
                                <div>
                                    <div className="text-sm text-muted-foreground mb-1">Max Drawdown</div>
                                    <div className="text-2xl font-bold text-red-500">-11.2%</div>
                                    <div className="h-1 w-full bg-red-500/20 rounded-full mt-2">
                                        <div className="h-full bg-red-500 w-[30%] rounded-full" />
                                    </div>
                                </div>
                                <div>
                                    <div className="text-sm text-muted-foreground mb-1">Volatility (30d)</div>
                                    <div className="text-2xl font-bold">4.2%</div>
                                    <div className="h-1 w-full bg-blue-500/20 rounded-full mt-2">
                                        <div className="h-full bg-blue-500 w-[60%] rounded-full" />
                                    </div>
                                </div>
                                <div>
                                    <div className="text-sm text-muted-foreground mb-1">Sortino Ratio</div>
                                    <div className="text-2xl font-bold text-green-500">2.1</div>
                                    <div className="h-1 w-full bg-green-500/20 rounded-full mt-2">
                                        <div className="h-full bg-green-500 w-[80%] rounded-full" />
                                    </div>
                                </div>
                                <div>
                                    <div className="text-sm text-muted-foreground mb-1">Alpha</div>
                                    <div className="text-2xl font-bold text-foreground">+0.05</div>
                                    <div className="h-1 w-full bg-muted/20 rounded-full mt-2">
                                        <div className="h-full bg-foreground w-[50%] rounded-full" />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {activeTab === 'tax' && (
                <div className="max-w-4xl">
                    <div className="bg-card border border-border rounded-xl p-8 text-center mb-8">
                        <FileText className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
                        <h2 className="text-xl font-bold mb-2">Tax Reports Generation</h2>
                        <p className="text-muted-foreground mb-6">
                            Generate compliant tax reports for your jurisdiction.
                            Supported formats: FIFO, LIFO, HIFO.
                        </p>
                        <div className="flex justify-center gap-4">
                            <button className="px-6 py-2 bg-primary text-primary-foreground rounded-lg font-medium">
                                Generate 2025 Report
                            </button>
                            <button className="px-6 py-2 border border-border hover:bg-muted rounded-lg font-medium">
                                Configure Tax Settings
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {activeTab === 'benchmarks' && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="bg-card border border-border rounded-xl p-6">
                        <h3 className="text-lg font-bold mb-4">vs. Bitcoin Holding</h3>
                        <div className="space-y-4">
                            <div className="flex justify-between items-end">
                                <div className="text-sm text-muted-foreground">Your Strategy</div>
                                <div className="text-xl font-bold text-green-500">+{data.profit_change_pct}%</div>
                            </div>
                            <div className="flex justify-between items-end">
                                <div className="text-sm text-muted-foreground">Buy & Hold BTC</div>
                                <div className="text-xl font-bold text-blue-500">+8.2%</div>
                            </div>
                            <div className="h-64 bg-muted/20 rounded-lg mt-4 flex items-center justify-center">
                                <span className="text-xs text-muted-foreground">Comparison Chart Area</span>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </motion.div>
    );
}
