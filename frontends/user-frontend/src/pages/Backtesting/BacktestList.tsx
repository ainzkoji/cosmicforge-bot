import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { BacktestAPI } from '@/api/backtest';
import {
    Card, CardHeader, CardTitle, CardContent,
    Button, Badge,
    Table, TableBody, TableCell, TableHead, TableHeader, TableRow
} from '@/components/UI/SimpleUI';
import { Plus, RefreshCw, AlertCircle } from 'lucide-react';

const StatusBadge = ({ status }: { status: string }) => {
    switch (status) {
        case 'completed':
            return <Badge className="bg-green-500 hover:bg-green-600">Completed</Badge>;
        case 'processing':
        case 'running':
            return <Badge variant="secondary" className="animate-pulse bg-blue-500 text-white">Running</Badge>;
        case 'pending':
            return <Badge variant="outline" className="text-yellow-600 border-yellow-600">Pending</Badge>;
        case 'failed':
            return <Badge variant="destructive">Failed</Badge>;
        case 'cancelled':
            return <Badge variant="secondary" className="text-gray-500">Cancelled</Badge>;
        default:
            return <Badge variant="secondary">{status}</Badge>;
    }
};

const PnLText = ({ value }: { value: number }) => {
    const isPositive = value >= 0;
    return (
        <span className={isPositive ? "text-green-600 font-medium" : "text-red-600 font-medium"}>
            {isPositive ? "+" : ""}{value.toLocaleString('en-US', { style: 'currency', currency: 'USD' })}
        </span>
    );
};

export default function BacktestList() {
    const navigate = useNavigate();
    const [page] = useState(1);

    // Helper for date formatting since date-fns is missing
    const formatDate = (dateStr: string) => {
        try {
            return new Date(dateStr).toLocaleDateString() + ' ' + new Date(dateStr).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        } catch (e) {
            return dateStr;
        }
    };

    const { data, isLoading, isError, refetch, isFetching } = useQuery({
        queryKey: ['backtests', page],
        queryFn: () => BacktestAPI.list({ page, size: 20 }),
        refetchInterval: 5000 // Auto-refresh list
    });

    return (
        <div className="space-y-6 container mx-auto px-4 py-8">
            <div className="flex justify-between items-center">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Simulation History</h1>
                    <p className="text-muted-foreground mt-2">
                        Review past simulation runs and system-managed strategy performance.
                    </p>
                </div>
                <Button onClick={() => navigate('/dashboard/backtests/new')}>
                    <Plus className="mr-2 h-4 w-4" /> Run Simulation
                </Button>
            </div>

            <Card>
                <CardHeader className="flex flex-row items-center justify-between">
                    <CardTitle>History</CardTitle>
                    <Button variant="ghost" size="sm" onClick={() => refetch()} disabled={isFetching}>
                        <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
                    </Button>
                </CardHeader>
                <CardContent>
                    {isLoading ? (
                        <div className="flex justify-center p-8">Loading...</div>
                    ) : isError ? (
                        <div className="flex items-center text-red-500 p-4 bg-red-50 rounded-md">
                            <AlertCircle className="mr-2 h-5 w-5" />
                            Failed to load backtests.
                        </div>
                    ) : !data || data.items.length === 0 ? (
                        <div className="text-center py-12 text-muted-foreground">
                            No simulations found. Run one!
                        </div>
                    ) : (
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead>Runs</TableHead>
                                    <TableHead>Strategy</TableHead>
                                    <TableHead>Symbols</TableHead>
                                    <TableHead>Date Range</TableHead>
                                    <TableHead>Status</TableHead>
                                    <TableHead className="text-right">Net PnL</TableHead>
                                    <TableHead className="text-right">Win Rate</TableHead>
                                    <TableHead></TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {data.items.map((run) => (
                                    <TableRow
                                        key={run.id}
                                        className="cursor-pointer hover:bg-muted/50 transition-colors"
                                        onClick={() => navigate(`/dashboard/backtests/${run.id}`)}
                                    >
                                        <TableCell>
                                            <div className="font-medium">{run.name}</div>
                                            <div className="text-xs text-muted-foreground">
                                                {formatDate(run.created_at)}
                                            </div>
                                        </TableCell>
                                        <TableCell>{run.strategy_id}</TableCell>
                                        <TableCell>
                                            <div className="flex gap-1 flex-wrap">
                                                {run.symbols.slice(0, 3).map(s => (
                                                    <Badge key={s} variant="outline" className="text-xs">{s}</Badge>
                                                ))}
                                                {run.symbols.length > 3 && (
                                                    <span className="text-xs text-muted-foreground">+{run.symbols.length - 3}</span>
                                                )}
                                            </div>
                                        </TableCell>
                                        <TableCell className="text-sm">
                                            {new Date(run.start_date).toLocaleDateString()}
                                            <br />
                                            <span className='text-muted-foreground transition-arrow'>→</span>
                                            {new Date(run.end_date).toLocaleDateString()}
                                        </TableCell>
                                        <TableCell><StatusBadge status={run.status} /></TableCell>
                                        <TableCell className="text-right">
                                            {run.status === 'completed' ? (
                                                <PnLText value={run.metrics.net_pnl} />
                                            ) : '-'}
                                        </TableCell>
                                        <TableCell className="text-right">
                                            {run.status === 'completed' ? (
                                                <span>{(run.metrics.win_rate * 100).toFixed(1)}%</span>
                                            ) : '-'}
                                        </TableCell>
                                        <TableCell className="text-right">
                                            <Button variant="ghost" size="sm">View</Button>
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}
