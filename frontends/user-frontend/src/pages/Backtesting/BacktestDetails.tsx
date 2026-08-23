import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation } from '@tanstack/react-query';
import { BacktestAPI, BacktestRun } from '@/api/backtest';
import {
    Button, Badge, Card, CardHeader, CardTitle, CardContent,
    Table, TableBody, TableCell, TableHead, TableHeader, TableRow
} from '@/components/UI/SimpleUI';
import {
    ResponsiveContainer,
    AreaChart,
    Area,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend
} from 'recharts';
import { ArrowLeft, Ban, Download, Loader2 } from 'lucide-react';

const KPICard = ({ title, value, subtext, color = "text-foreground" }: { title: string, value: string, subtext?: string, color?: string }) => (
    <Card>
        <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">{title}</div>
            <div className={`text-2xl font-bold ${color}`}>{value}</div>
            {subtext && <p className="text-xs text-muted-foreground mt-1">{subtext}</p>}
        </CardContent>
    </Card>
);

export default function BacktestDetails() {
    const { id } = useParams<{ id: string }>();
    const navigate = useNavigate();
    const [activeTab, setActiveTab] = useState("overview");
    const [fillsPage, setFillsPage] = useState(1);

    // Helper for date formatting
    const formatDate = (dateStr: string) => {
        try {
            return new Date(dateStr).toLocaleDateString() + ' ' + new Date(dateStr).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        } catch (e) {
            return dateStr;
        }
    };

    // 1. Fetch Run Details (poll if running)
    const { data: run, refetch, isLoading: isLoadingRun } = useQuery({
        queryKey: ['backtest', id],
        queryFn: () => BacktestAPI.get(id!),
        enabled: !!id,
        refetchInterval: (query) => {
            const data = query.state.data;
            if (!data) return false;
            return ['pending', 'processing', 'running'].includes(data.status) ? 2000 : false;
        }
    });

    // 2. Fetch Equity Curve (only if run exists)
    const { data: equityData } = useQuery({
        queryKey: ['backtest-equity', id],
        queryFn: () => BacktestAPI.getEquityCurve(id!),
        enabled: !!id && !!run && run.status !== 'pending' && run.status !== 'failed',
    });

    // 3. Fetch Fills (paginated)
    const { data: fillsData, isLoading: isLoadingFills } = useQuery({
        queryKey: ['backtest-fills', id, fillsPage],
        queryFn: () => BacktestAPI.getFills(id!, { page: fillsPage, size: 50 }),
        enabled: !!id && activeTab === 'trades'
    });

    // Cancel Mutation
    const cancelMutation = useMutation({
        mutationFn: () => BacktestAPI.cancel(id!),
        onSuccess: () => {
            refetch();
            alert("Cancellation requested");
        }
    });

    const handleDownload = (format: "csv" | "json") => {
        if (!id) return;
        const url = BacktestAPI.getExportUrl(id, format);
        window.open(url, '_blank');
    };

    if (isLoadingRun) return <div className="flex justify-center p-20"><Loader2 className="animate-spin h-8 w-8" /></div>;
    if (!run) return <div className="p-20 text-center">Simulation not found</div>;

    const isRunning = ['pending', 'processing', 'running'].includes(run.status);

    return (
        <div className="container mx-auto px-4 py-8 space-y-6">
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between md:items-center gap-4">
                <div className="flex items-center gap-4">
                    <Button variant="ghost" size="icon" onClick={() => navigate('/dashboard/backtests')}>
                        <ArrowLeft className="h-5 w-5" />
                    </Button>
                    <div>
                        <div className="flex items-center gap-2">
                            <h1 className="text-2xl font-bold">{run.name}</h1>
                            <Badge variant={isRunning ? "secondary" : "outline"} className={isRunning ? "animate-pulse" : ""}>
                                {run.status}
                            </Badge>
                        </div>
                        <p className="text-sm text-muted-foreground">
                            {run.strategy_id} • {run.timeframe} • {run.symbols.join(', ')}
                        </p>
                    </div>
                </div>
                <div className="flex gap-2">
                    {isRunning && (
                        <Button variant="destructive" size="sm" onClick={() => cancelMutation.mutate()} disabled={cancelMutation.isPending}>
                            <Ban className="mr-2 h-4 w-4" /> Stop
                        </Button>
                    )}
                    <Button variant="outline" size="sm" onClick={() => handleDownload('csv')}>
                        <Download className="mr-2 h-4 w-4" /> CSV
                    </Button>
                    <Button variant="outline" size="sm" onClick={() => handleDownload('json')}>
                        <Download className="mr-2 h-4 w-4" /> JSON
                    </Button>
                </div>
            </div>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <KPICard
                    title="Net PnL"
                    value={run.metrics.net_pnl.toLocaleString('en-US', { style: 'currency', currency: 'USD' })}
                    color={run.metrics.net_pnl >= 0 ? "text-green-600" : "text-red-600"}
                    subtext={`Gross: ${run.metrics.gross_pnl.toFixed(2)} | Fees: ${run.metrics.total_fees.toFixed(2)}`}
                />
                <KPICard
                    title="Win Rate"
                    value={`${(run.metrics.win_rate * 100).toFixed(1)}%`}
                    subtext={`${run.metrics.total_trades} total trades`}
                />
                <KPICard
                    title="Max Drawdown"
                    value={`${(run.metrics.max_drawdown * 100).toFixed(2)}%`}
                    color="text-red-600"
                />
                <KPICard
                    title="Capital"
                    value={(run.initial_capital + run.metrics.net_pnl).toLocaleString('en-US', { style: 'currency', currency: 'USD' })}
                    subtext={`Initial: ${run.initial_capital.toLocaleString()}`}
                />
            </div>

            {/* Custom Tabs */}
            <div className="space-y-4">
                <div className="flex space-x-1 rounded-lg bg-muted p-1">
                    {["overview", "trades", "logs"].map((tab) => (
                        <button
                            key={tab}
                            onClick={() => setActiveTab(tab)}
                            className={`flex-1 rounded-md px-3 py-2 text-sm font-medium transition-all ${activeTab === tab
                                ? "bg-white text-foreground shadow"
                                : "text-muted-foreground hover:bg-white/50 hover:text-foreground"
                                }`}
                        >
                            {tab.charAt(0).toUpperCase() + tab.slice(1)}
                            {tab === "trades" && ` (${run.metrics.total_trades})`}
                        </button>
                    ))}
                </div>

                {/* Tab Content: Overview */}
                {activeTab === "overview" && (
                    <div className="space-y-6">
                        <Card>
                            <CardHeader>
                                <CardTitle>Equity Curve</CardTitle>
                            </CardHeader>
                            <CardContent className="h-[400px]">
                                {equityData?.datapoints && equityData.datapoints.length > 0 ? (
                                    <ResponsiveContainer width="100%" height="100%">
                                        <AreaChart data={equityData.datapoints}>
                                            <defs>
                                                <linearGradient id="colorEquity" x1="0" y1="0" x2="0" y2="1">
                                                    <stop offset="5%" stopColor="#8884d8" stopOpacity={0.8} />
                                                    <stop offset="95%" stopColor="#8884d8" stopOpacity={0} />
                                                </linearGradient>
                                            </defs>
                                            <CartesianGrid strokeDasharray="3 3" vertical={false} />
                                            <XAxis
                                                dataKey="timestamp"
                                                tickFormatter={(val) => new Date(val).toLocaleDateString(undefined, { month: 'numeric', day: 'numeric' })}
                                                minTickGap={30}
                                            />
                                            <YAxis domain={['auto', 'auto']} />
                                            <Tooltip
                                                labelFormatter={(val) => new Date(val).toLocaleString()}
                                                formatter={(value: any) => [Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD' }), "Equity"]}
                                            />
                                            <Legend />
                                            <Area
                                                type="monotone"
                                                dataKey="equity"
                                                stroke="#8884d8"
                                                fillOpacity={1}
                                                fill="url(#colorEquity)"
                                            />
                                        </AreaChart>
                                    </ResponsiveContainer>
                                ) : (
                                    <div className="h-full flex items-center justify-center text-muted-foreground">
                                        No equity data available yet.
                                    </div>
                                )}
                            </CardContent>
                        </Card>

                        {/* Monthly Performance Table */}
                        <Card>
                            <CardHeader>
                                <CardTitle>Monthly Performance</CardTitle>
                            </CardHeader>
                            <CardContent>
                                {!equityData?.datapoints || equityData.datapoints.length === 0 ? (
                                    <div className="text-muted-foreground text-sm">No performance data available.</div>
                                ) : (
                                    (() => {
                                        // Calculate Monthly Returns
                                        const monthlyData: Record<string, { start: number, end: number, pnl: number, pct: number }> = {};

                                        // Sort datapoints just in case
                                        const sorted = [...equityData.datapoints].sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

                                        sorted.forEach(pt => {
                                            const date = new Date(pt.timestamp);
                                            const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

                                            if (!monthlyData[key]) {
                                                monthlyData[key] = { start: pt.equity, end: pt.equity, pnl: 0, pct: 0 };
                                            }
                                            monthlyData[key].end = pt.equity;
                                        });

                                        Object.keys(monthlyData).forEach(key => {
                                            const m = monthlyData[key];
                                            m.pnl = m.end - m.start;
                                            m.pct = ((m.end - m.start) / m.start) * 100;
                                        });

                                        // Group by Year
                                        const years: Record<string, typeof monthlyData> = {};
                                        Object.keys(monthlyData).sort().forEach(key => {
                                            const year = key.split('-')[0];
                                            if (!years[year]) years[year] = {};
                                            years[year][key] = monthlyData[key];
                                        });

                                        return (
                                            <div className="space-y-6">
                                                {Object.keys(years).sort().reverse().map(year => (
                                                    <div key={year}>
                                                        <h4 className="text-sm font-semibold mb-2">{year}</h4>
                                                        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2">
                                                            {Object.keys(years[year]).map(monthKey => {
                                                                const m = years[year][monthKey];
                                                                const monthName = new Date(monthKey + "-01").toLocaleDateString('en-US', { month: 'short' });
                                                                const isPos = m.pct >= 0;
                                                                return (
                                                                    <div key={monthKey} className={`p-3 rounded-lg border flex flex-col items-center ${isPos ? "bg-green-500/5 border-green-500/20" : "bg-red-500/5 border-red-500/20"}`}>
                                                                        <span className="text-xs text-muted-foreground uppercase">{monthName}</span>
                                                                        <span className={`font-bold ${isPos ? "text-green-500" : "text-red-500"}`}>
                                                                            {m.pct > 0 ? "+" : ""}{m.pct.toFixed(2)}%
                                                                        </span>
                                                                        <span className="text-[10px] text-muted-foreground">
                                                                            {m.pnl > 0 ? "+" : ""}{m.pnl.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 })}
                                                                        </span>
                                                                    </div>
                                                                );
                                                            })}
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        );
                                    })()
                                )}
                            </CardContent>
                        </Card>
                    </div>
                )}

                {/* Tab Content: Trades */}
                {activeTab === "trades" && (
                    <Card>
                        <CardContent className="pt-6">
                            {isLoadingFills ? (
                                <div>Loading trades...</div>
                            ) : !fillsData || fillsData.items.length === 0 ? (
                                <div className="text-center py-8 text-muted-foreground">No trades executed.</div>
                            ) : (
                                <>
                                    <Table>
                                        <TableHeader>
                                            <TableRow>
                                                <TableHead>Time</TableHead>
                                                <TableHead>Symbol</TableHead>
                                                <TableHead>Side</TableHead>
                                                <TableHead className="text-right">Price</TableHead>
                                                <TableHead className="text-right">Qty</TableHead>
                                                <TableHead className="text-right">Fee</TableHead>
                                                <TableHead className="text-right">PnL</TableHead>
                                            </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                            {fillsData.items.map((fill, idx) => (
                                                <TableRow key={idx}>
                                                    <TableCell>{formatDate(fill.timestamp)}</TableCell>
                                                    <TableCell>{fill.symbol}</TableCell>
                                                    <TableCell>
                                                        <span className={fill.side === 'BUY' ? "text-green-600 font-bold" : "text-red-600 font-bold"}>
                                                            {fill.side}
                                                        </span>
                                                    </TableCell>
                                                    <TableCell className="text-right">{fill.price.toFixed(2)}</TableCell>
                                                    <TableCell className="text-right">{fill.quantity.toFixed(4)}</TableCell>
                                                    <TableCell className="text-right">{fill.fee_usdt.toFixed(2)}</TableCell>
                                                    <TableCell className="text-right">
                                                        {fill.pnl ? (
                                                            <span className={fill.pnl >= 0 ? "text-green-600" : "text-red-600"}>
                                                                {fill.pnl.toFixed(2)}
                                                            </span>
                                                        ) : '-'}
                                                    </TableCell>
                                                </TableRow>
                                            ))}
                                        </TableBody>
                                    </Table>
                                    <div className="flex justify-end mt-4 gap-2">
                                        <Button
                                            variant="outline"
                                            size="sm"
                                            onClick={() => setFillsPage(p => Math.max(1, p - 1))}
                                            disabled={fillsPage === 1}
                                        >
                                            Previous
                                        </Button>
                                        <span className="flex items-center text-sm">
                                            Page {fillsPage} of {Math.ceil(fillsData.total / fillsData.size)}
                                        </span>
                                        <Button
                                            variant="outline"
                                            size="sm"
                                            onClick={() => setFillsPage(p => p + 1)}
                                            disabled={fillsPage >= Math.ceil(fillsData.total / fillsData.size)}
                                        >
                                            Next
                                        </Button>
                                    </div>
                                </>
                            )}
                        </CardContent>
                    </Card>
                )}

                {/* Tab Content: Logs */}
                {activeTab === "logs" && (
                    <Card>
                        <CardHeader>
                            <CardTitle>Run Configuration</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <pre className="bg-muted p-4 rounded-lg overflow-auto text-xs">
                                {JSON.stringify(run, null, 2)}
                            </pre>
                        </CardContent>
                    </Card>
                )}
            </div>
        </div>
    );
}
