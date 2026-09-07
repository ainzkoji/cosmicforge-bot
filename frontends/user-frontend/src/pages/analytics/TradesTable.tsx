import React, { useState, useMemo } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { api, type AnalyticsTrade, type TradesTableResponse } from '@/api/client';
import {
    ChevronLeft, ChevronRight, Search, X, RefreshCw,
    Download, TrendingUp, TrendingDown, BarChart3,
} from 'lucide-react';

// --- Formatting helpers ---

function formatTradeDate(value: string | undefined | null): string {
    if (!value) return 'N/A';
    const d = new Date(value);
    if (isNaN(d.getTime())) return 'N/A';
    return d.toLocaleString();
}

function formatMoney(value: number, decimals = 2): string {
    return '$' + value.toLocaleString(undefined, {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
    });
}

function formatQty(value: number | null | undefined): string {
    if (value == null) return '—';
    return value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 8 });
}

// --- Summary Card ---

function SummaryCard({
    label,
    value,
    format,
    color = 'neutral',
}: {
    label: string;
    value: number | null;
    format: 'money_signed' | 'money' | 'count' | 'percent';
    color?: 'green' | 'red' | 'neutral';
}) {
    let display: string;
    if (value == null) {
        display = 'N/A';
    } else {
        switch (format) {
            case 'money_signed': display = (value >= 0 ? '+' : '') + formatMoney(value); break;
            case 'money':        display = formatMoney(value); break;
            case 'count':        display = value.toString(); break;
            case 'percent':      display = value.toFixed(1) + '%'; break;
        }
    }

    const colorClass =
        value == null     ? 'text-muted-foreground' :
        color === 'green' ? 'text-green-400' :
        color === 'red'   ? 'text-red-400' :
                            'text-foreground';

    return (
        <div className="bg-card border border-border rounded-xl p-4">
            <div className="text-xs font-medium text-muted-foreground mb-1.5 truncate">{label}</div>
            <div className={`text-base font-bold font-mono leading-tight ${colorClass}`}>{display}</div>
        </div>
    );
}

// --- Filter types ---

type FilterPreset = 'all' | 'open' | 'closed' | 'winners' | 'losers';
type SideFilter = 'all' | 'BUY' | 'SELL';

const FILTER_PRESETS: { id: FilterPreset; label: string }[] = [
    { id: 'all',     label: 'All' },
    { id: 'open',    label: 'Open' },
    { id: 'closed',  label: 'Closed' },
    { id: 'winners', label: 'Winners' },
    { id: 'losers',  label: 'Losers' },
];

function presetToParams(preset: FilterPreset): { action?: string; result?: string } {
    switch (preset) {
        case 'open':    return { action: 'OPEN' };
        case 'closed':  return { action: 'CLOSE' };
        case 'winners': return { result: 'winner' };
        case 'losers':  return { result: 'loser' };
        default:        return {};
    }
}

// --- Empty state ---

function getEmptyMessage(preset: FilterPreset, symbolFilter: string): { title: string; sub: string } {
    if (symbolFilter) return {
        title: 'No trades match your filters.',
        sub: 'Try a different symbol or reset filters.',
    };
    switch (preset) {
        case 'open':    return { title: 'No open positions found.', sub: 'Open entries will appear here once your bot opens a position.' };
        case 'closed':  return { title: 'Closed trades will appear here once positions are exited.', sub: 'Your bot has not closed any positions yet.' };
        case 'winners': return { title: 'No winning closed trades found.', sub: 'Winners appear once a closed trade has positive realized PnL.' };
        case 'losers':  return { title: 'No losing closed trades found.', sub: 'Losers appear once a closed trade has negative realized PnL.' };
        default:        return { title: 'No trades found yet.', sub: 'Trades will appear here as your bot executes positions.' };
    }
}

function EmptyState({ preset, symbolFilter }: { preset: FilterPreset; symbolFilter: string }) {
    const { title, sub } = getEmptyMessage(preset, symbolFilter);
    const Icon =
        preset === 'winners' ? TrendingUp :
        preset === 'losers'  ? TrendingDown :
                               BarChart3;

    return (
        <div className="flex flex-col items-center justify-center gap-3 py-12">
            <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center">
                <Icon className="w-5 h-5 text-muted-foreground" />
            </div>
            <div className="text-center">
                <p className="font-medium text-foreground text-sm">{title}</p>
                <p className="text-xs text-muted-foreground mt-1 max-w-xs mx-auto">{sub}</p>
            </div>
        </div>
    );
}

// --- Loading skeleton ---

function TableSkeleton() {
    return (
        <div className="rounded-xl border border-border bg-card overflow-hidden">
            <div className="overflow-x-auto">
                <table className="w-full text-sm">
                    <thead className="bg-muted/40 border-b border-border">
                        <tr>
                            {Array.from({ length: 12 }).map((_, i) => (
                                <th key={i} className="px-4 py-3">
                                    <div className="h-3 bg-muted rounded animate-pulse w-14" />
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                        {Array.from({ length: 8 }).map((_, i) => (
                            <tr key={i} className="animate-pulse">
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-28" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-20" /></td>
                                <td className="px-4 py-3.5"><div className="h-5 bg-muted rounded-full w-14" /></td>
                                <td className="px-4 py-3.5"><div className="h-5 bg-muted rounded-full w-12" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-14 ml-auto" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-20 ml-auto" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-20 ml-auto" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-14 ml-auto" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-20 ml-auto" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-20 ml-auto" /></td>
                                <td className="px-4 py-3.5"><div className="h-3 bg-muted rounded w-16" /></td>
                                <td className="px-4 py-3.5"><div className="h-6 bg-muted rounded-md w-10 ml-auto" /></td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

// --- Trade Detail Drawer ---

function TradeDetailDrawer({ trade, onClose }: { trade: AnalyticsTrade | null; onClose: () => void }) {
    if (!trade) return null;

    const isClosed = trade.action === 'CLOSE';
    const pnl = trade.realized_pnl;
    const netPnl = trade.net_pnl != null
        ? trade.net_pnl
        : (pnl != null && trade.fee != null ? pnl - trade.fee : null);

    const rows: [string, string][] = [
        ['Trade ID',    trade.trade_id || trade.id || 'N/A'],
        ['Symbol',      trade.symbol   || 'N/A'],
        ['Action',      trade.action   ?? 'N/A'],
        ['Side',        trade.side     || 'N/A'],
        ['Broker',      trade.broker_id          || 'N/A'],
        ['Account',     trade.broker_account_id  || 'N/A'],
        ['Bot Instance', trade.bot_instance_id   ?? 'N/A'],
        ['Timestamp',   trade.timestamp ? formatTradeDate(trade.timestamp) : 'N/A'],
        ['Quantity',    trade.qty   != null ? formatQty(trade.qty)  : 'N/A'],
        ['Price',       trade.price != null ? formatMoney(trade.price) : 'N/A'],
        ['Value',       trade.value != null ? formatMoney(trade.value) : 'N/A'],
        ['Fee',         trade.fee   != null ? formatMoney(trade.fee)   : 'N/A'],
        ['Realized PnL', isClosed
            ? (pnl != null ? `${pnl >= 0 ? '+' : ''}${formatMoney(pnl)}` : 'N/A')
            : 'Open — position not yet closed'],
        ['Net PnL', isClosed
            ? (netPnl != null ? `${netPnl >= 0 ? '+' : ''}${formatMoney(netPnl)}` : 'N/A')
            : 'Open — position not yet closed'],
    ];

    return (
        <>
            <div
                className="fixed inset-0 bg-black/60 backdrop-blur-sm z-40"
                onClick={onClose}
            />
            <div className="fixed right-0 top-0 h-full w-full max-w-[420px] bg-card border-l border-border z-50 flex flex-col shadow-2xl">
                {/* Header */}
                <div className="flex items-start justify-between px-6 py-5 border-b border-border">
                    <div>
                        <h2 className="text-base font-bold tracking-tight">{trade.symbol} Trade Details</h2>
                        <p className="text-xs text-muted-foreground mt-0.5">{formatTradeDate(trade.timestamp)}</p>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-1.5 hover:bg-muted rounded-lg transition-colors"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>

                {/* Status badges */}
                <div className="flex items-center gap-2 px-6 py-3 border-b border-border bg-muted/20 flex-wrap">
                    <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold border ${
                        trade.action === 'OPEN'
                            ? 'bg-blue-500/10 text-blue-400 border-blue-500/25'
                            : 'bg-muted/60 text-muted-foreground border-border'
                    }`}>
                        {trade.action ?? 'UNKNOWN'}
                    </span>
                    <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold border ${
                        trade.side === 'BUY'
                            ? 'bg-green-500/10 text-green-400 border-green-500/25'
                            : 'bg-red-500/10 text-red-400 border-red-500/25'
                    }`}>
                        {trade.side || 'N/A'}
                    </span>
                    {isClosed && pnl != null && (
                        <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold border ml-auto ${
                            pnl >= 0
                                ? 'bg-green-500/10 text-green-400 border-green-500/25'
                                : 'bg-red-500/10 text-red-400 border-red-500/25'
                        }`}>
                            {pnl >= 0 ? 'Winner' : 'Loser'}
                        </span>
                    )}
                </div>

                {/* Field list */}
                <div className="flex-1 overflow-y-auto px-6 py-2">
                    {rows.map(([label, value]) => {
                        const isPnlField = label.includes('PnL');
                        const isPositive = isPnlField && isClosed && value.startsWith('+');
                        const isNegative = isPnlField && isClosed && value.startsWith('-');
                        return (
                            <div key={label} className="flex justify-between items-start py-3 border-b border-border/50 last:border-0 gap-4">
                                <span className="text-sm text-muted-foreground shrink-0">{label}</span>
                                <span className={`text-sm font-medium font-mono text-right break-all ${
                                    isPositive ? 'text-green-400' :
                                    isNegative ? 'text-red-400' :
                                    'text-foreground'
                                }`}>
                                    {value}
                                </span>
                            </div>
                        );
                    })}
                </div>

                {/* Footer */}
                <div className="px-6 py-4 border-t border-border">
                    <p className="text-xs text-muted-foreground text-center">
                        Data sourced from broker execution records.
                    </p>
                </div>
            </div>
        </>
    );
}

// --- Main component ---

interface TradesTableProps {
    timeframe: string;
}

export function TradesTable({ timeframe }: TradesTableProps) {
    const [page, setPage] = useState(1);
    const [pageSize] = useState(20);
    const [symbolFilter, setSymbolFilter] = useState('');
    const [filterPreset, setFilterPreset] = useState<FilterPreset>('all');
    const [sideFilter, setSideFilter] = useState<SideFilter>('all');
    const [selectedTrade, setSelectedTrade] = useState<AnalyticsTrade | null>(null);

    const { action, result } = presetToParams(filterPreset);

    const query = useQuery<TradesTableResponse>({
        queryKey: ['analytics-trades', timeframe, page, pageSize, symbolFilter, filterPreset],
        queryFn: () => api.getAnalyticsTrades(
            timeframe, page, pageSize,
            undefined, undefined,
            symbolFilter || undefined,
            action, result,
        ),
        placeholderData: keepPreviousData,
    });

    // Client-side side filter on the loaded page
    const trades: AnalyticsTrade[] = useMemo(() => {
        const base = query.data?.trades ?? [];
        if (sideFilter === 'all') return base;
        return base.filter(t => t.side === sideFilter);
    }, [query.data?.trades, sideFilter]);

    const pagination = query.data?.pagination ?? {};
    const totalPages = pagination.total_pages ?? 1;

    // Page-level aggregates (current page only)
    const closedTrades = trades.filter(t => t.action === 'CLOSE');
    const winners     = closedTrades.filter(t => (t.realized_pnl ?? 0) > 0);
    const losers      = closedTrades.filter(t => (t.realized_pnl ?? 0) < 0);
    const realizedPnl = closedTrades.reduce((sum, t) => sum + (t.realized_pnl ?? 0), 0);
    const totalFees   = trades.reduce((sum, t) => sum + (t.fee ?? 0), 0);
    const pageWinRate = closedTrades.length > 0 ? (winners.length / closedTrades.length) * 100 : null;

    const hasActiveFilters = filterPreset !== 'all' || symbolFilter !== '' || sideFilter !== 'all';

    const resetFilters = () => {
        setFilterPreset('all');
        setSymbolFilter('');
        setSideFilter('all');
        setPage(1);
    };

    if (query.isError) {
        return (
            <div className="flex flex-col items-center justify-center py-16 gap-4">
                <div className="w-12 h-12 rounded-full bg-red-500/10 flex items-center justify-center">
                    <X className="w-5 h-5 text-red-500" />
                </div>
                <div className="text-center">
                    <p className="font-medium text-foreground">Failed to load trade history</p>
                    <p className="text-sm text-muted-foreground mt-1">Check your connection and try again.</p>
                </div>
                <button
                    onClick={() => query.refetch()}
                    className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg text-sm hover:bg-primary/90 transition-colors"
                >
                    <RefreshCw className="w-4 h-4" />
                    Retry
                </button>
            </div>
        );
    }

    return (
        <>
            <TradeDetailDrawer trade={selectedTrade} onClose={() => setSelectedTrade(null)} />

            <div className="space-y-5">
                {/* Section header */}
                <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-3">
                    <div>
                        <h2 className="text-xl font-bold tracking-tight">Trade History</h2>
                        <p className="text-sm text-muted-foreground mt-0.5">
                            All opened and closed trades — realized PnL, fees, and broker execution records.
                        </p>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                        <button
                            onClick={() => query.refetch()}
                            disabled={query.isFetching}
                            title="Refresh trades"
                            className="flex items-center gap-1.5 px-3 py-2 text-sm bg-card border border-border rounded-lg hover:bg-muted transition-colors disabled:opacity-50"
                        >
                            <RefreshCw className={`w-3.5 h-3.5 ${query.isFetching ? 'animate-spin' : ''}`} />
                            Refresh
                        </button>
                        <button
                            disabled
                            title="CSV export not yet available"
                            className="flex items-center gap-1.5 px-3 py-2 text-sm bg-card border border-border rounded-lg opacity-40 cursor-not-allowed"
                        >
                            <Download className="w-3.5 h-3.5" />
                            Export CSV
                        </button>
                    </div>
                </div>

                {/* Summary cards */}
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
                    {query.isLoading && !query.data ? (
                        Array.from({ length: 6 }).map((_, i) => (
                            <div key={i} className="bg-card border border-border rounded-xl p-4 animate-pulse">
                                <div className="h-3 bg-muted rounded w-20 mb-2" />
                                <div className="h-5 bg-muted rounded w-16" />
                            </div>
                        ))
                    ) : (
                        <>
                            <SummaryCard
                                label="Realized PnL"
                                value={closedTrades.length > 0 ? realizedPnl : null}
                                format="money_signed"
                                color={closedTrades.length > 0 ? (realizedPnl >= 0 ? 'green' : 'red') : 'neutral'}
                            />
                            <SummaryCard label="Closed Trades" value={closedTrades.length} format="count" />
                            <SummaryCard
                                label="Winners"
                                value={winners.length}
                                format="count"
                                color={winners.length > 0 ? 'green' : 'neutral'}
                            />
                            <SummaryCard
                                label="Losers"
                                value={losers.length}
                                format="count"
                                color={losers.length > 0 ? 'red' : 'neutral'}
                            />
                            <SummaryCard
                                label="Win Rate"
                                value={pageWinRate}
                                format="percent"
                                color={pageWinRate == null ? 'neutral' : pageWinRate >= 50 ? 'green' : 'red'}
                            />
                            <SummaryCard label="Total Fees" value={totalFees} format="money" />
                        </>
                    )}
                </div>
                {!query.isLoading && (
                    <p className="text-xs text-muted-foreground -mt-2">Based on visible rows</p>
                )}

                {/* Filter bar */}
                <div className="flex flex-wrap gap-2.5 items-center">
                    {/* Preset pills */}
                    <div className="flex gap-0.5 bg-muted/50 rounded-lg p-1">
                        {FILTER_PRESETS.map(fp => (
                            <button
                                key={fp.id}
                                onClick={() => { setFilterPreset(fp.id); setPage(1); }}
                                className={`px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${
                                    filterPreset === fp.id
                                        ? 'bg-background shadow-sm text-foreground'
                                        : 'text-muted-foreground hover:text-foreground'
                                }`}
                            >
                                {fp.label}
                            </button>
                        ))}
                    </div>

                    {/* Side filter */}
                    <div className="flex gap-0.5 bg-muted/50 rounded-lg p-1">
                        {(['all', 'BUY', 'SELL'] as SideFilter[]).map(s => (
                            <button
                                key={s}
                                onClick={() => { setSideFilter(s); setPage(1); }}
                                className={`px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${
                                    sideFilter === s
                                        ? 'bg-background shadow-sm text-foreground'
                                        : 'text-muted-foreground hover:text-foreground'
                                }`}
                            >
                                {s === 'all' ? 'Both Sides' : s}
                            </button>
                        ))}
                    </div>

                    {/* Symbol search */}
                    <div className="relative min-w-[160px] flex-1 max-w-xs">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
                        <input
                            type="text"
                            placeholder="Search symbol…"
                            value={symbolFilter}
                            onChange={e => { setSymbolFilter(e.target.value); setPage(1); }}
                            className="w-full pl-8 pr-8 py-2 bg-background border border-border rounded-lg text-xs focus:outline-none focus:ring-1 focus:ring-primary"
                        />
                        {symbolFilter && (
                            <button
                                onClick={() => { setSymbolFilter(''); setPage(1); }}
                                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                            >
                                <X className="w-3 h-3" />
                            </button>
                        )}
                    </div>

                    {/* Reset */}
                    {hasActiveFilters && (
                        <button
                            onClick={resetFilters}
                            className="flex items-center gap-1.5 px-3 py-2 text-xs text-muted-foreground hover:text-foreground border border-border rounded-lg hover:bg-muted transition-colors"
                        >
                            <X className="w-3 h-3" />
                            Reset
                        </button>
                    )}
                </div>

                {/* Table */}
                {query.isLoading && !query.data ? (
                    <TableSkeleton />
                ) : (
                    <div className="rounded-xl border border-border bg-card overflow-hidden">
                        <div className="overflow-x-auto">
                            <table className="w-full text-sm text-left">
                                <thead className="bg-muted/40 border-b border-border">
                                    <tr className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
                                        <th className="px-4 py-3 whitespace-nowrap">Date & Time</th>
                                        <th className="px-4 py-3">Symbol</th>
                                        <th className="px-4 py-3">Action</th>
                                        <th className="px-4 py-3">Side</th>
                                        <th className="px-4 py-3 text-right">Qty</th>
                                        <th className="px-4 py-3 text-right">Price</th>
                                        <th className="px-4 py-3 text-right">Value</th>
                                        <th className="px-4 py-3 text-right">Fee</th>
                                        <th className="px-4 py-3 text-right">Realized PnL</th>
                                        <th className="px-4 py-3 text-right">Net PnL</th>
                                        <th className="px-4 py-3">Broker</th>
                                        <th className="px-4 py-3 text-right">Details</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-border">
                                    {trades.length === 0 ? (
                                        <tr>
                                            <td colSpan={12}>
                                                <EmptyState preset={filterPreset} symbolFilter={symbolFilter} />
                                            </td>
                                        </tr>
                                    ) : (
                                        trades.map((trade, idx) => {
                                            const isClosed = trade.action === 'CLOSE';
                                            const pnl    = trade.realized_pnl;
                                            const netPnl = trade.net_pnl != null
                                                ? trade.net_pnl
                                                : (pnl != null && trade.fee != null ? pnl - trade.fee : null);

                                            return (
                                                <tr
                                                    key={trade.trade_id || trade.id || String(idx)}
                                                    onClick={() => setSelectedTrade(trade)}
                                                    className="hover:bg-muted/30 cursor-pointer transition-colors"
                                                >
                                                    <td className="px-4 py-3.5 whitespace-nowrap text-xs text-muted-foreground">
                                                        {formatTradeDate(trade.timestamp)}
                                                    </td>
                                                    <td className="px-4 py-3.5">
                                                        <span className="font-semibold text-foreground tracking-wide">
                                                            {trade.symbol || '—'}
                                                        </span>
                                                    </td>
                                                    <td className="px-4 py-3.5">
                                                        {trade.action ? (
                                                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold border ${
                                                                trade.action === 'OPEN'
                                                                    ? 'bg-blue-500/10 text-blue-400 border-blue-500/20'
                                                                    : 'bg-muted/60 text-muted-foreground border-border'
                                                            }`}>
                                                                {trade.action}
                                                            </span>
                                                        ) : <span className="text-muted-foreground">—</span>}
                                                    </td>
                                                    <td className="px-4 py-3.5">
                                                        {trade.side ? (
                                                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold border ${
                                                                trade.side === 'BUY'
                                                                    ? 'bg-green-500/10 text-green-400 border-green-500/20'
                                                                    : 'bg-red-500/10 text-red-400 border-red-500/20'
                                                            }`}>
                                                                {trade.side}
                                                            </span>
                                                        ) : <span className="text-muted-foreground">—</span>}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-right font-mono text-xs">
                                                        {formatQty(trade.qty)}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-right font-mono text-xs">
                                                        {trade.price != null ? formatMoney(trade.price) : '—'}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-right font-mono text-xs">
                                                        {trade.value != null ? formatMoney(trade.value) : '—'}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-right font-mono text-xs text-muted-foreground">
                                                        {trade.fee != null ? formatMoney(trade.fee) : '—'}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-right font-mono text-xs font-semibold">
                                                        {isClosed ? (
                                                            pnl != null ? (
                                                                <span className={pnl >= 0 ? 'text-green-400' : 'text-red-400'}>
                                                                    {pnl >= 0 ? '+' : ''}{formatMoney(pnl)}
                                                                </span>
                                                            ) : '—'
                                                        ) : (
                                                            <span className="text-muted-foreground font-normal italic">Open</span>
                                                        )}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-right font-mono text-xs font-semibold">
                                                        {isClosed ? (
                                                            netPnl != null ? (
                                                                <span className={netPnl >= 0 ? 'text-green-400' : 'text-red-400'}>
                                                                    {netPnl >= 0 ? '+' : ''}{formatMoney(netPnl)}
                                                                </span>
                                                            ) : '—'
                                                        ) : (
                                                            <span className="text-muted-foreground font-normal italic">Open</span>
                                                        )}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-xs text-muted-foreground">
                                                        {trade.broker_id || '—'}
                                                    </td>
                                                    <td className="px-4 py-3.5 text-right">
                                                        <button
                                                            onClick={e => { e.stopPropagation(); setSelectedTrade(trade); }}
                                                            className="px-2.5 py-1 text-xs border border-border rounded-md hover:bg-muted hover:text-foreground text-muted-foreground transition-colors"
                                                        >
                                                            View
                                                        </button>
                                                    </td>
                                                </tr>
                                            );
                                        })
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                )}

                {/* Pagination */}
                {!query.isLoading && trades.length > 0 && (
                    <div className="flex items-center justify-between text-sm">
                        <span className="text-xs text-muted-foreground">
                            Page {page} of {totalPages} &bull; {pagination.total_items ?? 0} total records
                        </span>
                        <div className="flex gap-1.5">
                            <button
                                onClick={() => setPage(p => Math.max(1, p - 1))}
                                disabled={page <= 1}
                                className="flex items-center gap-1 px-3 py-1.5 border border-border rounded-lg hover:bg-muted disabled:opacity-40 transition-colors text-xs"
                            >
                                <ChevronLeft className="w-3.5 h-3.5" />
                                Prev
                            </button>
                            <button
                                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                                disabled={page >= totalPages}
                                className="flex items-center gap-1 px-3 py-1.5 border border-border rounded-lg hover:bg-muted disabled:opacity-40 transition-colors text-xs"
                            >
                                Next
                                <ChevronRight className="w-3.5 h-3.5" />
                            </button>
                        </div>
                    </div>
                )}
            </div>
        </>
    );
}
