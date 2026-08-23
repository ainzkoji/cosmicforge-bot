import React, { useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { api, type PositionRecord } from '@/api/client';
import {
    ChevronLeft, ChevronRight, Search, X, RefreshCw,
    Clock, Eye, AlertTriangle,
} from 'lucide-react';

// --- Helpers (duplicated locally to keep components self-contained) ---

function formatDate(ts: string | null | undefined): string {
    if (!ts) return '—';
    const d = new Date(ts);
    return isNaN(d.getTime()) ? '—' : d.toLocaleString();
}

function fmt$(val: number | null | undefined): string {
    if (val == null) return '—';
    const abs = Math.abs(val).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return (val < 0 ? '-$' : '$') + abs;
}

function formatQty(val: number | null | undefined): string {
    if (val == null) return '—';
    return val.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 8 });
}

function formatPrice(val: number | null | undefined): string {
    if (val == null) return '—';
    return '$' + val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 8 });
}

function formatDurationSince(openedAt: string | null | undefined): string {
    if (!openedAt) return '—';
    const d = new Date(openedAt);
    if (isNaN(d.getTime())) return '—';
    const seconds = Math.floor((Date.now() - d.getTime()) / 1000);
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
    if (seconds < 86400) {
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        return m > 0 ? `${h}h ${m}m` : `${h}h`;
    }
    const d2 = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    return h > 0 ? `${d2}d ${h}h` : `${d2}d`;
}

function SideBadge({ side }: { side: string }) {
    const isLong = side === 'LONG' || side === 'BUY';
    return (
        <span className={`inline-flex px-2 py-0.5 rounded text-xs font-bold ${
            isLong
                ? 'bg-green-500/10 text-green-400 border border-green-500/20'
                : 'bg-red-500/10 text-red-400 border border-red-500/20'
        }`}>
            {side}
        </span>
    );
}

function TableSkeleton() {
    return (
        <div className="space-y-0">
            {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="flex gap-4 items-center px-4 py-3 border-b border-border/50 animate-pulse">
                    <div className="h-3 bg-muted rounded w-32 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-20 flex-shrink-0" />
                    <div className="h-5 bg-muted rounded w-12 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-16 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-20 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-14 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded flex-1" />
                </div>
            ))}
        </div>
    );
}

// --- Position detail drawer (simplified for open positions) ---

function OpenPositionDrawer({ position, onClose }: {
    position: PositionRecord | null;
    onClose: () => void;
}) {
    if (!position) return null;

    return (
        <>
            <div className="fixed inset-0 z-40 bg-black/40 backdrop-blur-sm" onClick={onClose} />
            <div className="fixed right-0 top-0 bottom-0 z-50 w-full max-w-md bg-card border-l border-border shadow-2xl flex flex-col overflow-hidden">
                <div className="flex items-center justify-between px-6 py-4 border-b border-border flex-shrink-0">
                    <div>
                        <h2 className="text-lg font-bold">{position.symbol}</h2>
                        <p className="text-sm text-blue-400 flex items-center gap-1"><Clock className="w-3 h-3" /> Open Position</p>
                    </div>
                    <button onClick={onClose} className="p-2 rounded-lg hover:bg-muted text-muted-foreground hover:text-foreground transition-colors">
                        <X className="w-4 h-4" />
                    </button>
                </div>

                <div className="flex-1 overflow-y-auto px-6 py-4 space-y-6">
                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Trade Info</h3>
                        <div className="bg-background rounded-lg border border-border overflow-hidden">
                            {[
                                ['Position ID', <span className="font-mono text-xs text-muted-foreground break-all">{position.position_id}</span>],
                                ['Symbol', <span className="font-semibold">{position.symbol}</span>],
                                ['Side', <SideBadge side={position.side} />],
                                ['Opened', formatDate(position.opened_at)],
                                ['Open For', formatDurationSince(position.opened_at)],
                            ].map(([label, value], i, arr) => (
                                <div key={i} className={`flex justify-between items-center px-4 py-2.5 ${i < arr.length - 1 ? 'border-b border-border/50' : ''}`}>
                                    <span className="text-sm text-muted-foreground">{label as string}</span>
                                    <span className="text-sm font-medium text-right">{value as React.ReactNode}</span>
                                </div>
                            ))}
                        </div>
                    </div>

                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Entry</h3>
                        <div className="bg-background rounded-lg border border-border overflow-hidden">
                            {[
                                ['Entry Price (avg)', formatPrice(position.entry_price)],
                                ['Entry Qty', formatQty(position.open_qty)],
                                ['Fill Count', `${position.open_count} fill${position.open_count !== 1 ? 's' : ''}`],
                                ['Entry Value', fmt$(position.entry_price != null && position.open_qty != null ? position.entry_price * position.open_qty : null)],
                            ].map(([label, value], i, arr) => (
                                <div key={i} className={`flex justify-between items-center px-4 py-2.5 ${i < arr.length - 1 ? 'border-b border-border/50' : ''}`}>
                                    <span className="text-sm text-muted-foreground">{label}</span>
                                    <span className="text-sm font-medium font-mono">{value}</span>
                                </div>
                            ))}
                        </div>
                    </div>

                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Exit</h3>
                        <div className="bg-background rounded-lg border border-border overflow-hidden">
                            <div className="px-4 py-3">
                                <p className="text-sm text-muted-foreground italic">Open — position not yet closed</p>
                            </div>
                        </div>
                    </div>

                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Fees</h3>
                        <div className="bg-background rounded-lg border border-border overflow-hidden">
                            <div className="flex justify-between items-center px-4 py-2.5">
                                <span className="text-sm text-muted-foreground">Entry Fees</span>
                                <span className="text-sm font-medium text-red-400">-{fmt$(position.total_fees)}</span>
                            </div>
                        </div>
                    </div>

                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Metadata</h3>
                        <div className="bg-background rounded-lg border border-border overflow-hidden">
                            {[
                                ['Bot Instance', <span className="font-mono text-xs text-muted-foreground">{position.bot_instance_id || '—'}</span>],
                                ['Broker Account', <span className="font-mono text-xs text-muted-foreground">{position.broker_account_id || '—'}</span>],
                                ['Run ID', <span className="font-mono text-xs text-muted-foreground">{position.run_id || '—'}</span>],
                            ].map(([label, value], i, arr) => (
                                <div key={i} className={`flex justify-between items-center px-4 py-2.5 ${i < arr.length - 1 ? 'border-b border-border/50' : ''}`}>
                                    <span className="text-sm text-muted-foreground">{label as string}</span>
                                    <span className="text-sm font-medium text-right">{value as React.ReactNode}</span>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
        </>
    );
}

// --- Main component ---

export function OpenPositions({ timeframe }: { timeframe: string }) {
    const [page, setPage] = useState(1);
    const [symbolInput, setSymbolInput] = useState('');
    const [symbolFilter, setSymbolFilter] = useState('');
    const [selectedPosition, setSelectedPosition] = useState<PositionRecord | null>(null);

    const { data, isLoading, isError, refetch, isFetching } = useQuery({
        queryKey: ['positions-history', 'open', timeframe, symbolFilter, page],
        queryFn: () => api.getPositionHistory(
            timeframe,
            'open',
            page,
            20,
            symbolFilter || undefined,
        ),
        placeholderData: keepPreviousData,
        staleTime: 15_000,
        refetchInterval: 60_000,
    });

    const items: PositionRecord[] = data?.items ?? [];
    const summary = data?.summary;
    const pagination = data?.pagination;
    const totalPages = pagination?.total_pages ?? 1;

    const handleSymbolSearch = () => {
        setPage(1);
        setSymbolFilter(symbolInput.trim().toUpperCase());
    };

    const handleClearSymbol = () => {
        setSymbolInput('');
        setSymbolFilter('');
        setPage(1);
    };

    return (
        <div className="space-y-6">
            {/* Summary cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                {[
                    { label: 'Open Positions', value: summary?.open_count ?? null },
                    { label: 'Symbols', value: null as null },
                ].slice(0, 1).map((_, i) => (
                    isLoading
                        ? <div key={i} className="bg-card border border-border rounded-xl p-4 animate-pulse"><div className="h-3 bg-muted rounded w-20 mb-2" /><div className="h-5 bg-muted rounded w-16" /></div>
                        : null
                ))}
                {!isLoading && (
                    <>
                        <div className="bg-card border border-border rounded-xl p-4">
                            <div className="text-xs font-medium text-muted-foreground mb-1.5">Open Positions</div>
                            <div className="text-base font-bold text-blue-400">{summary?.open_count ?? 0}</div>
                        </div>
                        <div className="bg-card border border-border rounded-xl p-4">
                            <div className="text-xs font-medium text-muted-foreground mb-1.5">Entry Fees Paid</div>
                            <div className="text-base font-bold font-mono text-red-400">
                                {summary ? `-$${summary.total_fees.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}
                            </div>
                        </div>
                    </>
                )}
            </div>

            {/* Filter bar */}
            <div className="flex items-center gap-2">
                <div className="relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground" />
                    <input
                        type="text"
                        value={symbolInput}
                        onChange={e => setSymbolInput(e.target.value.toUpperCase())}
                        onKeyDown={e => e.key === 'Enter' && handleSymbolSearch()}
                        placeholder="Symbol..."
                        className="pl-8 pr-8 py-1.5 text-sm bg-muted border border-border rounded-lg w-36 focus:outline-none focus:ring-1 focus:ring-primary placeholder:text-muted-foreground"
                    />
                    {symbolInput && (
                        <button onClick={handleClearSymbol} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                            <X className="w-3.5 h-3.5" />
                        </button>
                    )}
                </div>
                {symbolInput && (
                    <button onClick={handleSymbolSearch} className="px-3 py-1.5 text-sm bg-primary text-primary-foreground rounded-lg">
                        Search
                    </button>
                )}
                <button
                    onClick={() => refetch()}
                    disabled={isFetching}
                    className="ml-auto p-1.5 text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
                    title="Refresh"
                >
                    <RefreshCw className={`w-4 h-4 ${isFetching ? 'animate-spin' : ''}`} />
                </button>
            </div>

            {/* Table */}
            <div className="bg-card border border-border rounded-xl overflow-hidden">
                {isError ? (
                    <div className="flex flex-col items-center justify-center py-16 gap-3">
                        <AlertTriangle className="w-8 h-8 text-red-400" />
                        <p className="text-muted-foreground">Failed to load open positions.</p>
                        <button onClick={() => refetch()} className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-lg">Retry</button>
                    </div>
                ) : (
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="border-b border-border bg-muted/30">
                                    <th className="text-left px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Opened</th>
                                    <th className="text-left px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Symbol</th>
                                    <th className="text-left px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Side</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Qty</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Entry Price</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Entry Value</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Open For</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Fees Paid</th>
                                    <th className="text-center px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Details</th>
                                </tr>
                            </thead>
                            <tbody>
                                {isLoading ? (
                                    <tr><td colSpan={9}><TableSkeleton /></td></tr>
                                ) : items.length === 0 ? (
                                    <tr>
                                        <td colSpan={9}>
                                            <div className="flex flex-col items-center justify-center py-16 gap-3">
                                                <Clock className="w-10 h-10 text-muted-foreground/30" />
                                                <p className="font-medium text-muted-foreground">No open positions</p>
                                                <p className="text-sm text-muted-foreground/70">Active positions will appear here.</p>
                                            </div>
                                        </td>
                                    </tr>
                                ) : (
                                    items.map((pos) => (
                                        <tr
                                            key={pos.position_id}
                                            onClick={() => setSelectedPosition(pos)}
                                            className="border-b border-border/50 hover:bg-muted/30 cursor-pointer transition-colors"
                                        >
                                            <td className="px-4 py-3 text-xs text-muted-foreground whitespace-nowrap">{formatDate(pos.opened_at)}</td>
                                            <td className="px-4 py-3 font-semibold whitespace-nowrap">{pos.symbol}</td>
                                            <td className="px-4 py-3 whitespace-nowrap"><SideBadge side={pos.side} /></td>
                                            <td className="px-4 py-3 text-right font-mono whitespace-nowrap">{formatQty(pos.open_qty)}</td>
                                            <td className="px-4 py-3 text-right font-mono whitespace-nowrap">{formatPrice(pos.entry_price)}</td>
                                            <td className="px-4 py-3 text-right font-mono whitespace-nowrap text-muted-foreground">
                                                {pos.entry_price != null && pos.open_qty != null
                                                    ? fmt$(pos.entry_price * pos.open_qty)
                                                    : '—'}
                                            </td>
                                            <td className="px-4 py-3 text-right text-blue-400 whitespace-nowrap">{formatDurationSince(pos.opened_at)}</td>
                                            <td className="px-4 py-3 text-right font-mono text-red-400 whitespace-nowrap">-{fmt$(pos.total_fees)}</td>
                                            <td className="px-4 py-3 text-center whitespace-nowrap">
                                                <button
                                                    onClick={e => { e.stopPropagation(); setSelectedPosition(pos); }}
                                                    className="p-1.5 rounded-lg hover:bg-muted text-muted-foreground hover:text-foreground transition-colors"
                                                >
                                                    <Eye className="w-4 h-4" />
                                                </button>
                                            </td>
                                        </tr>
                                    ))
                                )}
                            </tbody>
                        </table>
                    </div>
                )}

                {/* Pagination */}
                {!isLoading && !isError && totalPages > 1 && (
                    <div className="flex items-center justify-between px-4 py-3 border-t border-border bg-muted/20">
                        <span className="text-xs text-muted-foreground">
                            Page {pagination?.page ?? 1} of {totalPages}
                        </span>
                        <div className="flex items-center gap-1">
                            <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page <= 1} className="p-1.5 rounded-lg hover:bg-muted disabled:opacity-40 transition-colors">
                                <ChevronLeft className="w-4 h-4" />
                            </button>
                            <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="p-1.5 rounded-lg hover:bg-muted disabled:opacity-40 transition-colors">
                                <ChevronRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                )}
            </div>

            <OpenPositionDrawer position={selectedPosition} onClose={() => setSelectedPosition(null)} />
        </div>
    );
}
