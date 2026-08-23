import React, { useState, useEffect } from 'react';
import { DollarSign, ArrowUpRight, PieChart, TrendingUp, Activity, Loader2, Download, FileText, Hash, Wallet, AlertTriangle } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { EquityCurve } from '@/components/analytics/EquityCurve';
import { api } from '@/api/client';

interface AnalyticsOverviewProps {
    timeframe: string;
}

export function AnalyticsOverview({ timeframe }: AnalyticsOverviewProps) {
    const [isExporting, setIsExporting] = useState(false);
    const [exportMenuOpen, setExportMenuOpen] = useState(false);

    const query = useQuery({
        queryKey: ['analytics-overview', timeframe],
        queryFn: () => api.getAnalyticsOverview(timeframe),
    });

    const handleExportPDF = async () => {
        setIsExporting(true);
        setExportMenuOpen(false);
        try {
            const token = localStorage.getItem('access_token');
            const response = await fetch(`/api/analytics/export/pdf?timeframe=${timeframe}`, {
                headers: {
                    'Authorization': `Bearer ${token}`
                }
            });

            if (!response.ok) throw new Error('Export failed');

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `analytics_overview_${timeframe}_${new Date().toISOString().split('T')[0]}.pdf`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            console.error('Export failed:', error);
            alert('Failed to export PDF. Please try again.');
        } finally {
            setIsExporting(false);
        }
    };

    // Close dropdown when clicking outside
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement;
            if (exportMenuOpen && !target.closest('.relative')) {
                setExportMenuOpen(false);
            }
        };

        document.addEventListener('click', handleClickOutside);
        return () => document.removeEventListener('click', handleClickOutside);
    }, [exportMenuOpen]);



    const data = query.data;

    if (query.isLoading && !data) {
        return (
            <div className="space-y-6 animate-pulse">
                <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4">
                    {Array.from({ length: 6 }).map((_, i) => (
                        <div key={i} className="bg-card border border-border rounded-xl p-6">
                            <div className="h-3 bg-muted rounded w-20 mb-3" />
                            <div className="h-7 bg-muted rounded w-24 mb-2" />
                            <div className="h-3 bg-muted rounded w-16" />
                        </div>
                    ))}
                </div>
                <div className="h-80 bg-card border border-border rounded-xl" />
            </div>
        );
    }

    if (query.isError || !data) {
        return (
            <div className="flex flex-col items-center justify-center py-16 gap-4">
                <div className="w-12 h-12 rounded-full bg-destructive/10 flex items-center justify-center">
                    <AlertTriangle className="w-5 h-5 text-destructive" />
                </div>
                <div className="text-center">
                    <p className="font-medium text-foreground">Failed to load analytics overview</p>
                    <p className="text-sm text-muted-foreground mt-1">Check your connection and try again.</p>
                </div>
                <button
                    onClick={() => query.refetch()}
                    className="px-4 py-2 bg-background border border-border rounded-lg hover:bg-muted text-foreground text-sm transition-colors"
                >
                    Retry
                </button>
            </div>
        );
    }

    return (
        <div className="space-y-6">
            {/* Export Button */}
            <div className="flex justify-end mb-4">
                <div className="relative">
                    <button
                        onClick={() => setExportMenuOpen(!exportMenuOpen)}
                        disabled={isExporting}
                        className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                        {isExporting ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" />
                                <span>Exporting...</span>
                            </>
                        ) : (
                            <>
                                <Download className="w-4 h-4" />
                                <span>Export</span>
                            </>
                        )}
                    </button>

                    {exportMenuOpen && !isExporting && (
                        <div className="absolute right-0 mt-2 w-48 bg-card border border-border rounded-lg shadow-lg z-10">
                            <button
                                onClick={handleExportPDF}
                                className="w-full flex items-center gap-2 px-4 py-2 hover:bg-muted text-left rounded-t-lg transition-colors"
                            >
                                <FileText className="w-4 h-4" />
                                <span>Export as PDF</span>
                            </button>
                        </div>
                    )}
                </div>
            </div>

            {/* KPI Cards — 6 across */}
            <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4">
                {/* Net Profit */}
                <div className="bg-card border border-border rounded-xl p-5">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Net Profit</span>
                        <DollarSign className="w-4 h-4 text-green-500" />
                    </div>
                    <div className={`text-xl font-bold font-mono ${(data.total_profit ?? 0) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {(data.total_profit ?? 0) >= 0 ? '+' : ''}${(data.total_profit ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                    </div>
                    <div className="flex items-center gap-1 text-xs mt-1 text-muted-foreground">
                        {data.profit_change_pct == null ? (
                            <span>vs prev period: N/A</span>
                        ) : (
                            <>
                                <ArrowUpRight className={`w-3 h-3 ${data.profit_change_pct < 0 ? 'rotate-180 text-red-500' : 'text-green-500'}`} />
                                <span className={data.profit_change_pct >= 0 ? 'text-green-500' : 'text-red-500'}>
                                    {data.profit_change_pct > 0 ? '+' : ''}{data.profit_change_pct.toFixed(1)}%
                                </span>
                            </>
                        )}
                    </div>
                </div>
                {/* Win Rate */}
                <div className="bg-card border border-border rounded-xl p-5">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Win Rate</span>
                        <PieChart className="w-4 h-4 text-blue-500" />
                    </div>
                    <div className="text-xl font-bold">{(data.win_rate ?? 0).toFixed(1)}%</div>
                    <div className="text-xs text-muted-foreground mt-1">{data.wins ?? 0}W / {data.losses ?? 0}L</div>
                </div>
                {/* Profit Factor */}
                <div className="bg-card border border-border rounded-xl p-5">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Profit Factor</span>
                        <TrendingUp className="w-4 h-4 text-purple-500" />
                    </div>
                    <div className="text-xl font-bold">{(data.profit_factor ?? 0).toFixed(2)}</div>
                    <div className="text-xs text-muted-foreground mt-1">Gross profit / loss</div>
                </div>
                {/* Total Trades */}
                <div className="bg-card border border-border rounded-xl p-5">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Total Trades</span>
                        <Hash className="w-4 h-4 text-sky-500" />
                    </div>
                    <div className="text-xl font-bold">{data.total_trades ?? 0}</div>
                    <div className="text-xs text-muted-foreground mt-1">Fills recorded</div>
                </div>
                {/* Fees */}
                <div className="bg-card border border-border rounded-xl p-5">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Total Fees</span>
                        <Wallet className="w-4 h-4 text-orange-400" />
                    </div>
                    <div className="text-xl font-bold font-mono text-foreground">
                        ${(data.fees_total ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                    </div>
                    <div className="text-xs text-muted-foreground mt-1">Trading costs</div>
                </div>
                {/* Max Drawdown */}
                <div className="bg-card border border-border rounded-xl p-5">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Max Drawdown</span>
                        <Activity className="w-4 h-4 text-red-500" />
                    </div>
                    <div className="text-xl font-bold text-red-500">
                        {(data.max_drawdown ?? 0).toFixed(2)}%
                    </div>
                    <div className="text-xs text-muted-foreground mt-1">Peak-to-trough</div>
                </div>
            </div>

            {/* Equity Curve & Monthly P&L */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2 bg-card border border-border rounded-xl p-6">
                    <h3 className="text-lg font-bold mb-6">Equity Curve</h3>
                    <EquityCurve />
                </div>
                <div className="bg-card border border-border rounded-xl p-6">
                    <h3 className="text-lg font-bold mb-6">Monthly P&L</h3>
                    <div className="space-y-4">
                        {(!data.monthly_pnl || data.monthly_pnl.length === 0) ? (
                            <p className="text-sm text-muted-foreground">No monthly data available.</p>
                        ) : (
                            data.monthly_pnl.map((item: any) => (
                                <div key={item.month} className="space-y-1">
                                    <div className="flex justify-between text-sm">
                                        <span>{item.month}</span>
                                        <span className={item.value >= 0 ? 'text-green-500' : 'text-red-500'}>
                                            {item.value >= 0 ? '+' : ''}${item.value.toLocaleString()}
                                        </span>
                                    </div>
                                    <div className="h-2 bg-muted rounded-full overflow-hidden">
                                        <div
                                            className={`h-full ${item.value >= 0 ? 'bg-green-500' : 'bg-red-500'}`}
                                            style={{ width: `${Math.min(Math.abs(item.value) / 1000 * 100, 100)}%` }}
                                        />
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* Asset Allocation Pie Chart */}
                <div className="lg:col-span-1 bg-card border border-border rounded-xl p-6">
                    <h3 className="text-lg font-bold mb-6">Asset Allocation</h3>
                    {(!data.asset_allocation || data.asset_allocation.length === 0) ? (
                        <div className="flex h-48 items-center justify-center text-muted-foreground text-sm">
                            No assets allocated
                        </div>
                    ) : (
                        <div className="flex flex-col items-center">
                            <div className="relative w-48 h-48 mb-6">
                                <svg viewBox="0 0 100 100" className="w-full h-full transform -rotate-90">
                                    <circle cx="50" cy="50" r="40" fill="transparent" stroke="#e2e8f0" strokeWidth="20" />
                                    {(data.asset_allocation ?? []).slice(0, 3).map((item: any, i: number, arr: any[]) => {
                                        const total = (data.asset_allocation ?? []).reduce((acc: number, curr: any) => acc + curr.value_usdt, 0);
                                        const val = item.value_usdt;
                                        const pct = (val / total) * 100;
                                        const dashArray = `${(pct / 100) * 251} 251`;
                                        let offset = 0;
                                        for (let j = 0; j < i; j++) {
                                            const prevVal = arr[j].value_usdt;
                                            offset += (prevVal / total) * 251;
                                        }

                                        return (
                                            <circle
                                                key={i}
                                                cx="50" cy="50" r="40"
                                                fill="transparent"
                                                stroke={item.color || ['#3b82f6', '#8b5cf6', '#10b981'][i % 3]}
                                                strokeWidth="20"
                                                strokeDasharray={dashArray}
                                                strokeDashoffset={-offset}
                                            />
                                        );
                                    })}
                                </svg>
                                <div className="absolute inset-0 flex items-center justify-center flex-col">
                                    <span className="text-xs text-muted-foreground">Total Value</span>
                                    <span className="font-bold">
                                        ${data.asset_allocation.reduce((acc: number, curr: any) => acc + curr.value_usdt, 0).toLocaleString()}
                                    </span>
                                </div>
                            </div>
                            <div className="w-full space-y-3">
                                {(data.asset_allocation ?? []).map((item: any, i: number) => (
                                    <div key={i} className="flex justify-between items-center text-sm">
                                        <div className="flex items-center gap-2">
                                            <div
                                                className="w-3 h-3 rounded-full"
                                                style={{ backgroundColor: item.color || ['#3b82f6', '#8b5cf6', '#10b981'][i % 3] }}
                                            />
                                            <span>{item.symbol ?? item.label}</span>
                                        </div>
                                        <span className="font-bold">${item.value_usdt.toLocaleString()}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>

                {/* Risk Metrics */}
                <div className="lg:col-span-2 bg-card border border-border rounded-xl p-6">
                    <h3 className="text-lg font-bold mb-6">Advanced Risk Metrics</h3>
                    <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
                        <div>
                            <div className="text-sm text-muted-foreground mb-1">Sharpe Ratio</div>
                            {data.sharpe_ratio == null ? (
                                <div className="text-2xl font-bold text-muted-foreground">N/A</div>
                            ) : (
                                <>
                                    <div className="text-2xl font-bold">{data.sharpe_ratio.toFixed(2)}</div>
                                    <div className="h-1 w-full bg-amber-500/20 rounded-full mt-2">
                                        <div className="h-full bg-amber-500 rounded-full" style={{ width: `${Math.min(Math.abs(data.sharpe_ratio) * 20, 100)}%` }} />
                                    </div>
                                </>
                            )}
                        </div>
                        <div>
                            <div className="text-sm text-muted-foreground mb-1">Volatility (30d)</div>
                            {data.risk_metrics?.volatility_30d == null ? (
                                <div className="text-2xl font-bold text-muted-foreground">N/A</div>
                            ) : (
                                <>
                                    <div className="text-2xl font-bold">{data.risk_metrics.volatility_30d.toFixed(2)}%</div>
                                    <div className="h-1 w-full bg-blue-500/20 rounded-full mt-2">
                                        <div className="h-full bg-blue-500 rounded-full" style={{ width: `${Math.min(data.risk_metrics.volatility_30d * 2, 100)}%` }} />
                                    </div>
                                </>
                            )}
                        </div>
                        <div>
                            <div className="text-sm text-muted-foreground mb-1">Sortino Ratio</div>
                            {data.risk_metrics?.sortino_ratio == null ? (
                                <div className="text-2xl font-bold text-muted-foreground">N/A</div>
                            ) : (
                                <>
                                    <div className="text-2xl font-bold text-green-500">{data.risk_metrics.sortino_ratio.toFixed(2)}</div>
                                    <div className="h-1 w-full bg-green-500/20 rounded-full mt-2">
                                        <div className="h-full bg-green-500 rounded-full" style={{ width: `${Math.min(data.risk_metrics.sortino_ratio * 20, 100)}%` }} />
                                    </div>
                                </>
                            )}
                        </div>
                        <div>
                            <div className="text-sm text-muted-foreground mb-1">Alpha</div>
                            {data.risk_metrics?.alpha == null ? (
                                <div className="text-2xl font-bold text-muted-foreground">N/A</div>
                            ) : (
                                <>
                                    <div className="text-2xl font-bold text-foreground">
                                        {data.risk_metrics.alpha > 0 ? '+' : ''}{data.risk_metrics.alpha.toFixed(2)}
                                    </div>
                                    <div className="h-1 w-full bg-muted/20 rounded-full mt-2">
                                        <div className="h-full bg-foreground rounded-full" style={{ width: `${Math.min(Math.abs(data.risk_metrics.alpha) * 10, 100)}%` }} />
                                    </div>
                                </>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

