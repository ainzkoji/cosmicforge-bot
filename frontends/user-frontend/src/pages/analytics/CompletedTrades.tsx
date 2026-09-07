import React, { useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { api, type PositionRecord, type PositionSummary } from '@/api/client';
import {
    ChevronLeft, ChevronRight, Search, X, RefreshCw,
    TrendingUp, TrendingDown, Trophy, Minus, Clock, Eye,
    AlertTriangle, Info, Download, Copy, Check, ChevronDown, ChevronUp,
} from 'lucide-react';

// --- Helpers ---

function formatDate(ts: string | null | undefined): string {
    if (!ts) return '—';
    const d = new Date(ts);
    return isNaN(d.getTime()) ? '—' : d.toLocaleString();
}

function fmt$(val: number | null | undefined, showSign = false): string {
    if (val == null) return 'N/A';
    const abs = Math.abs(val).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
    if (showSign) return (val >= 0 ? '+$' : '-$') + abs;
    return (val < 0 ? '-$' : '$') + abs;
}

function formatPct(val: number | null | undefined): string {
    if (val == null) return 'N/A';
    return (val >= 0 ? '+' : '') + val.toFixed(2) + '%';
}

function formatQty(val: number | null | undefined): string {
    if (val == null) return '—';
    return val.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 8 });
}

function formatDuration(seconds: number | null | undefined): string {
    if (seconds == null) return '—';
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
    if (seconds < 86400) {
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        return m > 0 ? `${h}h ${m}m` : `${h}h`;
    }
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    return h > 0 ? `${d}d ${h}h` : `${d}d`;
}

function formatPrice(val: number | null | undefined): string {
    if (val == null) return '—';
    return '$' + val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 8 });
}

function getStartDateForRange(range: DateRange): string | undefined {
    if (range === 'all') return undefined;
    const days = range === '7d' ? 7 : range === '30d' ? 30 : 90;
    return new Date(Date.now() - days * 24 * 3600 * 1000).toISOString();
}

// --- Sub-components ---

function SummaryCard({ label, value, format, color = 'neutral', sub }: {
    label: string;
    value: number | null;
    format: 'money_signed' | 'money' | 'count' | 'percent';
    color?: 'green' | 'red' | 'neutral';
    sub?: string;
}) {
    let display: string;
    if (value == null) {
        display = 'N/A';
    } else {
        switch (format) {
            case 'money_signed': display = fmt$(value, true); break;
            case 'money':        display = fmt$(value); break;
            case 'count':        display = value.toString(); break;
            case 'percent':      display = value.toFixed(1) + '%'; break;
        }
    }

    const colorClass =
        value == null                    ? 'text-muted-foreground' :
        color === 'green'                ? 'text-green-400' :
        color === 'red'                  ? 'text-red-400' :
        format === 'money_signed' && value > 0 ? 'text-green-400' :
        format === 'money_signed' && value < 0 ? 'text-red-400' :
                                           'text-foreground';

    return (
        <div className="bg-card border border-border rounded-xl p-4">
            <div className="text-xs font-medium text-muted-foreground mb-1.5 truncate">{label}</div>
            <div className={`text-base font-bold font-mono leading-tight ${colorClass}`}>{display}</div>
            {sub && <div className="text-xs text-muted-foreground mt-1">{sub}</div>}
        </div>
    );
}

function SummaryCardSkeleton() {
    return (
        <div className="bg-card border border-border rounded-xl p-4 animate-pulse">
            <div className="h-3 bg-muted rounded w-20 mb-2" />
            <div className="h-5 bg-muted rounded w-24" />
        </div>
    );
}

function TableSkeleton() {
    return (
        <div className="space-y-0">
            {Array.from({ length: 8 }).map((_, i) => (
                <div key={i} className="flex gap-4 items-center px-4 py-3 border-b border-border/50 animate-pulse">
                    <div className="h-3 bg-muted rounded w-32 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-20 flex-shrink-0" />
                    <div className="h-5 bg-muted rounded w-12 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-16 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-20 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-20 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded w-14 flex-shrink-0" />
                    <div className="h-3 bg-muted rounded flex-1" />
                    <div className="h-3 bg-muted rounded flex-1" />
                    <div className="h-5 bg-muted rounded w-16 flex-shrink-0" />
                </div>
            ))}
        </div>
    );
}

function ResultBadge({ result }: { result: PositionRecord['result'] }) {
    if (!result) return null;
    if (result === 'winner') return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-green-500/10 text-green-400 border border-green-500/20">
            <Trophy className="w-3 h-3" /> Winner
        </span>
    );
    if (result === 'loser') return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-red-500/10 text-red-400 border border-red-500/20">
            <TrendingDown className="w-3 h-3" /> Loser
        </span>
    );
    return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-muted/60 text-muted-foreground border border-border">
            <Minus className="w-3 h-3" /> Breakeven
        </span>
    );
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

// --- Copy Button ---
function CopyButton({ text, label = 'Copy' }: { text: string; label?: string }) {
    const [copied, setCopied] = useState(false);

    const handleCopy = () => {
        navigator.clipboard.writeText(text).then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
        });
    };

    return (
        <button
            onClick={handleCopy}
            className="inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded bg-muted hover:bg-muted/80 text-muted-foreground hover:text-foreground transition-colors"
            title={`Copy ${label}`}
        >
            {copied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
            {copied ? 'Copied' : label}
        </button>
    );
}

// --- Collapsible Fill IDs ---
function FillIdList({ ids, label }: { ids: string[]; label: string }) {
    const [expanded, setExpanded] = useState(false);
    const COLLAPSE_THRESHOLD = 3;
    const shouldCollapse = ids.length > COLLAPSE_THRESHOLD;
    const visible = expanded || !shouldCollapse ? ids : ids.slice(0, COLLAPSE_THRESHOLD);

    if (ids.length === 0) return <span className="text-muted-foreground text-xs italic">None</span>;

    return (
        <div className="space-y-1">
            {visible.map((id, i) => (
                <div key={i} className="flex items-center gap-1">
                    <span className="font-mono text-xs text-muted-foreground">{id}</span>
                    <CopyButton text={id} />
                </div>
            ))}
            {shouldCollapse && (
                <button
                    onClick={() => setExpanded(!expanded)}
                    className="flex items-center gap-1 text-xs text-primary hover:text-primary/80 transition-colors mt-0.5"
                >
                    {expanded
                        ? <><ChevronUp className="w-3 h-3" /> Show less</>
                        : <><ChevronDown className="w-3 h-3" /> +{ids.length - COLLAPSE_THRESHOLD} more</>
                    }
                </button>
            )}
        </div>
    );
}

// --- Lifecycle Timeline ---
function LifecycleTimeline({ position }: { position: PositionRecord }) {
    const isClosed = position.status === 'CLOSED';
    const steps = [
        { label: 'Opened', time: formatDate(position.opened_at), done: true },
        { label: 'Closed', time: isClosed ? formatDate(position.closed_at) : null, done: isClosed },
        { label: 'Duration', time: isClosed ? formatDuration(position.duration_seconds) : null, done: isClosed },
    ];

    return (
        <div className="flex items-start gap-0">
            {steps.map((step, i) => (
                <React.Fragment key={i}>
                    <div className="flex flex-col items-center min-w-0 flex-1">
                        <div className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${step.done ? 'bg-primary' : 'bg-muted-foreground/30'}`} />
                        <div className="text-xs font-medium text-foreground mt-1">{step.label}</div>
                        <div className="text-[10px] text-muted-foreground mt-0.5 text-center leading-tight">
                            {step.time ?? <span className="italic">—</span>}
                        </div>
                    </div>
                    {i < steps.length - 1 && (
                        <div className={`h-px w-8 mt-1.5 flex-shrink-0 ${step.done && steps[i + 1].done ? 'bg-primary' : 'bg-muted-foreground/20'}`} />
                    )}
                </React.Fragment>
            ))}
        </div>
    );
}

// --- Position Drawer ---

function PositionDrawer({ position, onClose }: {
    position: PositionRecord | null;
    onClose: () => void;
}) {
    if (!position) return null;

    const isClosed = position.status === 'CLOSED';
    const isSynthetic = String(position.position_id || '').startsWith('fill_');
    const isUnlinkedCloseFill = isSynthetic && isClosed && (position.open_count ?? 0) === 0 && (position.close_count ?? 0) > 0;

    const entryRows: { label: string; value: React.ReactNode }[] = [
        { label: 'Entry Price (avg)', value: formatPrice(position.entry_price) },
        { label: 'Entry Qty', value: formatQty(position.open_qty) },
        { label: 'Fill Count', value: `${position.open_count} fill${position.open_count !== 1 ? 's' : ''}` },
    ];

    const exitRows: { label: string; value: React.ReactNode }[] = isClosed ? [
        { label: 'Exit Price (avg)', value: formatPrice(position.exit_price) },
        { label: 'Exit Qty', value: formatQty(position.close_qty) },
        { label: 'Fill Count', value: `${position.close_count} fill${position.close_count !== 1 ? 's' : ''}` },
    ] : [
        { label: 'Exit', value: <span className="text-muted-foreground italic">Open — not yet closed</span> },
    ];

    const pnlRows: { label: string; value: React.ReactNode; highlight?: boolean }[] = isClosed ? [
        { label: 'Realized PnL', value: <span className={position.realized_pnl >= 0 ? 'text-green-400' : 'text-red-400'}>{fmt$(position.realized_pnl, true)}</span> },
        { label: 'Total Fees', value: <span className="text-red-400">-{fmt$(position.total_fees)}</span> },
        { label: 'Net PnL', value: <span className={`font-bold ${(position.net_pnl ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>{fmt$(position.net_pnl, true)}</span>, highlight: true },
        { label: 'Return %', value: <span className={(position.realized_pnl_percent ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}>{formatPct(position.realized_pnl_percent)}</span> },
        { label: 'R-Multiple', value: position.r_multiple != null ? `${position.r_multiple.toFixed(2)}R` : 'N/A' },
    ] : [
        { label: 'Realized PnL', value: <span className="text-muted-foreground italic">Open</span> },
        { label: 'Total Fees', value: fmt$(position.total_fees) },
        { label: 'Net PnL', value: <span className="text-muted-foreground italic">Open</span> },
    ];

    const metaRows: { label: string; value: React.ReactNode }[] = [
        { label: 'Bot Instance', value: <span className="font-mono text-xs text-muted-foreground">{position.bot_instance_id || '—'}</span> },
        { label: 'Broker Account', value: <span className="font-mono text-xs text-muted-foreground">{position.broker_account_id || '—'}</span> },
        { label: 'Run ID', value: <span className="font-mono text-xs text-muted-foreground">{position.run_id || '—'}</span> },
    ];

    return (
        <>
            <div className="fixed inset-0 z-40 bg-black/40 backdrop-blur-sm" onClick={onClose} />
            <div className="fixed right-0 top-0 bottom-0 z-50 w-full max-w-md bg-card border-l border-border shadow-2xl flex flex-col overflow-hidden">
                <div className="flex items-center justify-between px-6 py-4 border-b border-border flex-shrink-0">
                    <div>
                        <h2 className="text-lg font-bold">{position.symbol}</h2>
                        <p className="text-sm text-muted-foreground">Position Detail</p>
                    </div>
                    <div className="flex items-center gap-2">
                        {isUnlinkedCloseFill && (
                            <span className="text-[11px] px-2 py-1 rounded-full border border-yellow-500/30 bg-yellow-500/10 text-yellow-300/90 font-medium">
                                Unlinked close fill
                            </span>
                        )}
                        {isClosed && <ResultBadge result={position.result} />}
                        <button onClick={onClose} className="p-2 rounded-lg hover:bg-muted text-muted-foreground hover:text-foreground transition-colors">
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                </div>

                <div className="flex-1 overflow-y-auto px-6 py-4 space-y-6">
                    {/* Lifecycle Timeline */}
                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Lifecycle</h3>
                        <div className="bg-background rounded-lg border border-border p-4">
                            <LifecycleTimeline position={position} />
                        </div>
                    </div>

                    {/* Position ID + Copy */}
                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Position ID</h3>
                        <div className="bg-background rounded-lg border border-border p-3 flex items-center justify-between gap-2">
                            <span className="font-mono text-xs text-muted-foreground break-all">{position.position_id}</span>
                            <CopyButton text={position.position_id} label="ID" />
                        </div>
                        {isUnlinkedCloseFill && (
                            <div className="mt-2 text-[11px] leading-snug text-muted-foreground flex items-start gap-2">
                                <Info className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />
                                <span>
                                    This record came from a CLOSE fill that did not have a position ID. It is included so realized PnL remains complete.
                                </span>
                            </div>
                        )}
                    </div>

                    {/* Side + Status */}
                    <div className="flex gap-3">
                        <div className="flex-1 bg-background rounded-lg border border-border p-3 text-center">
                            <div className="text-xs text-muted-foreground mb-1">Side</div>
                            <SideBadge side={position.side} />
                        </div>
                        <div className="flex-1 bg-background rounded-lg border border-border p-3 text-center">
                            <div className="text-xs text-muted-foreground mb-1">Status</div>
                            {isClosed
                                ? <span className="text-sm text-muted-foreground font-medium">Closed</span>
                                : <span className="text-sm text-blue-400 font-medium flex items-center justify-center gap-1"><Clock className="w-3 h-3" />Open</span>
                            }
                        </div>
                    </div>

                    <DrawerSection title="Entry" rows={entryRows} />
                    <DrawerSection title="Exit" rows={exitRows} />
                    <DrawerSection title="PnL Breakdown" rows={pnlRows} />
                    <DrawerSection title="Metadata" rows={metaRows} />

                    {/* Fill IDs */}
                    <div>
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Fill IDs</h3>
                        <div className="bg-background rounded-lg border border-border overflow-hidden">
                            <div className="px-4 py-3 border-b border-border/50">
                                <div className="text-xs text-muted-foreground mb-2">Open Fills ({position.open_count})</div>
                                <FillIdList ids={position.open_fill_ids} label="fill" />
                            </div>
                            <div className="px-4 py-3">
                                <div className="text-xs text-muted-foreground mb-2">Close Fills ({position.close_count})</div>
                                <FillIdList ids={position.close_fill_ids} label="fill" />
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </>
    );
}

function DrawerSection({ title, rows }: {
    title: string;
    rows: { label: string; value: React.ReactNode; highlight?: boolean }[];
}) {
    return (
        <div>
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">{title}</h3>
            <div className="bg-background rounded-lg border border-border overflow-hidden">
                {rows.map((row, i) => (
                    <div key={i} className={`flex justify-between items-center px-4 py-2.5 ${
                        i < rows.length - 1 ? 'border-b border-border/50' : ''
                    } ${row.highlight ? 'bg-muted/30' : ''}`}>
                        <span className="text-sm text-muted-foreground">{row.label}</span>
                        <span className="text-sm font-medium text-right">{row.value}</span>
                    </div>
                ))}
            </div>
        </div>
    );
}

// --- Filter types ---

type ResultFilter = 'all' | 'winner' | 'loser' | 'breakeven';
type SideFilter = 'all' | 'LONG' | 'SHORT';
type DateRange = 'all' | '7d' | '30d' | '90d';

type SortOption = {
    label: string;
    sort_by: string;
    sort_order: 'ASC' | 'DESC';
};

const SORT_OPTIONS: SortOption[] = [
    { label: 'Newest Closed',  sort_by: 'closed_at',   sort_order: 'DESC' },
    { label: 'Oldest Closed',  sort_by: 'closed_at',   sort_order: 'ASC'  },
    { label: 'Highest Net PnL', sort_by: 'net_pnl',   sort_order: 'DESC' },
    { label: 'Lowest Net PnL',  sort_by: 'net_pnl',   sort_order: 'ASC'  },
    { label: 'Longest Duration', sort_by: 'duration_seconds', sort_order: 'DESC' },
];

const RESULT_FILTERS: { id: ResultFilter; label: string }[] = [
    { id: 'all',       label: 'All Closed'  },
    { id: 'winner',    label: 'Winners'     },
    { id: 'loser',     label: 'Losers'      },
    { id: 'breakeven', label: 'Breakeven'   },
];

// --- Main component ---

export function CompletedTrades({ timeframe }: { timeframe: string }) {
    const [page, setPage] = useState(1);
    const [filterResult, setFilterResult] = useState<ResultFilter>('all');
    const [sideFilter, setSideFilter] = useState<SideFilter>('all');
    const [dateRange, setDateRange] = useState<DateRange>('all');
    const [sortIdx, setSortIdx] = useState(0);
    const [symbolInput, setSymbolInput] = useState('');
    const [symbolFilter, setSymbolFilter] = useState('');
    const [selectedPosition, setSelectedPosition] = useState<PositionRecord | null>(null);
    const [isExporting, setIsExporting] = useState(false);

    const sort = SORT_OPTIONS[sortIdx];
    const startDate = getStartDateForRange(dateRange);

    const activeFilterCount = [
        filterResult !== 'all',
        sideFilter !== 'all',
        dateRange !== 'all',
        sortIdx !== 0,
        symbolFilter !== '',
    ].filter(Boolean).length;

    const { data, isLoading, isError, refetch, isFetching } = useQuery({
        queryKey: ['positions-history', 'closed', timeframe, filterResult, sideFilter, dateRange, sortIdx, symbolFilter, page],
        queryFn: () => api.getPositionHistory(
            timeframe,
            'closed',
            page,
            20,
            symbolFilter || undefined,
            sideFilter !== 'all' ? sideFilter : undefined,
            filterResult !== 'all' ? filterResult : undefined,
            undefined,
            undefined,
            sort.sort_by,
            sort.sort_order,
            startDate,
        ),
        placeholderData: keepPreviousData,
        staleTime: 30_000,
    });

    const summary: PositionSummary | undefined = data?.summary;
    const items: PositionRecord[] = data?.items ?? [];
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

    const handleReset = () => {
        setFilterResult('all');
        setSideFilter('all');
        setDateRange('all');
        setSortIdx(0);
        setSymbolInput('');
        setSymbolFilter('');
        setPage(1);
    };

    const handleExport = async () => {
        try {
            setIsExporting(true);
            const blob = await api.exportCompletedTrades(
                timeframe,
                symbolFilter || undefined,
                sideFilter !== 'all' ? sideFilter : undefined,
                filterResult !== 'all' ? filterResult : undefined,
                undefined,
                undefined,
                startDate,
            );
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `cosmicforge-completed-trades-${new Date().toISOString().slice(0, 10)}.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (err) {
            console.error('Export failed:', err);
        } finally {
            setIsExporting(false);
        }
    };

    const pnlColor = (val: number | null | undefined) =>
        val == null ? 'text-muted-foreground' :
        val > 0 ? 'text-green-400' :
        val < 0 ? 'text-red-400' : 'text-muted-foreground';

    return (
        <div className="space-y-6">
            {/* Summary cards */}
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
                {isLoading ? (
                    Array.from({ length: 6 }).map((_, i) => <SummaryCardSkeleton key={i} />)
                ) : (
                    <>
                        <SummaryCard label="Total Closed"  value={summary?.closed_count ?? null} format="count" />
                        <SummaryCard label="Winners"       value={summary?.winners ?? null}       format="count" color="green" />
                        <SummaryCard label="Losers"        value={summary?.losers ?? null}        format="count" color="red" />
                        <SummaryCard
                            label="Win Rate"
                            value={summary?.win_rate ?? null}
                            format="percent"
                            color={(summary?.win_rate ?? 0) >= 50 ? 'green' : (summary?.win_rate ?? 0) < 40 ? 'red' : 'neutral'}
                        />
                        <SummaryCard label="Realized PnL"       value={summary?.total_realized_pnl ?? null} format="money_signed" />
                        <SummaryCard
                            label="Net PnL (after fees)"
                            value={summary?.total_net_pnl ?? null}
                            format="money_signed"
                            sub={summary ? `Fees: ${fmt$(summary.total_fees)}` : undefined}
                        />
                    </>
                )}
            </div>

            {/* Filter bar — row 1: result + side + date */}
            <div className="flex flex-col gap-2">
                <div className="flex flex-wrap gap-2 items-center">
                    {/* Result filter pills */}
                    <div className="flex gap-1">
                        {RESULT_FILTERS.map((f) => (
                            <button
                                key={f.id}
                                onClick={() => { setFilterResult(f.id); setPage(1); }}
                                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                                    filterResult === f.id
                                        ? 'bg-primary text-primary-foreground shadow'
                                        : 'bg-muted text-muted-foreground hover:text-foreground hover:bg-muted/80'
                                }`}
                            >
                                {f.label}
                            </button>
                        ))}
                    </div>

                    <div className="h-4 w-px bg-border" />

                    {/* Side filter */}
                    {(['all', 'LONG', 'SHORT'] as SideFilter[]).map(s => (
                        <button
                            key={s}
                            onClick={() => { setSideFilter(s); setPage(1); }}
                            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                                sideFilter === s
                                    ? s === 'all' ? 'bg-primary text-primary-foreground shadow'
                                        : s === 'LONG' ? 'bg-green-500/20 text-green-400 border border-green-500/30 shadow'
                                        : 'bg-red-500/20 text-red-400 border border-red-500/30 shadow'
                                    : 'bg-muted text-muted-foreground hover:text-foreground hover:bg-muted/80'
                            }`}
                        >
                            {s === 'all' ? 'Both Sides' : s}
                        </button>
                    ))}

                    <div className="h-4 w-px bg-border" />

                    {/* Date range */}
                    {(['all', '7d', '30d', '90d'] as DateRange[]).map(r => (
                        <button
                            key={r}
                            onClick={() => { setDateRange(r); setPage(1); }}
                            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                                dateRange === r
                                    ? 'bg-primary text-primary-foreground shadow'
                                    : 'bg-muted text-muted-foreground hover:text-foreground hover:bg-muted/80'
                            }`}
                        >
                            {r === 'all' ? 'All Time' : `Last ${r}`}
                        </button>
                    ))}
                </div>

                {/* Row 2: symbol search, sort, export, reset, refresh */}
                <div className="flex flex-wrap items-center gap-2">
                    {/* Symbol search */}
                    <div className="relative">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground" />
                        <input
                            type="text"
                            value={symbolInput}
                            onChange={e => setSymbolInput(e.target.value.toUpperCase())}
                            onKeyDown={e => e.key === 'Enter' && handleSymbolSearch()}
                            placeholder="Symbol..."
                            className="pl-8 pr-8 py-1.5 text-xs bg-muted border border-border rounded-lg w-32 focus:outline-none focus:ring-1 focus:ring-primary placeholder:text-muted-foreground"
                        />
                        {symbolInput && (
                            <button onClick={handleClearSymbol} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                                <X className="w-3 h-3" />
                            </button>
                        )}
                    </div>
                    {symbolInput && (
                        <button onClick={handleSymbolSearch} className="px-3 py-1.5 text-xs bg-primary text-primary-foreground rounded-lg">
                            Search
                        </button>
                    )}

                    {/* Sort */}
                    <select
                        value={sortIdx}
                        onChange={e => { setSortIdx(Number(e.target.value)); setPage(1); }}
                        className="px-2 py-1.5 text-xs bg-muted border border-border rounded-lg text-foreground focus:outline-none focus:ring-1 focus:ring-primary cursor-pointer"
                    >
                        {SORT_OPTIONS.map((opt, i) => (
                            <option key={i} value={i}>{opt.label}</option>
                        ))}
                    </select>

                    <div className="ml-auto flex items-center gap-2">
                        {/* Export CSV */}
                        <button
                            onClick={handleExport}
                            disabled={isExporting}
                            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-muted border border-border rounded-lg hover:bg-muted/80 text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
                            title="Export as CSV"
                        >
                            <Download className="w-3.5 h-3.5" />
                            {isExporting ? 'Exporting…' : 'Export CSV'}
                        </button>

                        {/* Reset */}
                        {activeFilterCount > 0 && (
                            <button
                                onClick={handleReset}
                                className="flex items-center gap-1 px-3 py-1.5 text-xs text-muted-foreground hover:text-foreground border border-border rounded-lg hover:bg-muted transition-colors"
                            >
                                <RefreshCw className="w-3 h-3" />
                                Reset
                                <span className="ml-0.5 px-1.5 py-0.5 text-[10px] font-bold bg-primary text-primary-foreground rounded-full leading-none">
                                    {activeFilterCount}
                                </span>
                            </button>
                        )}

                        {/* Refresh */}
                        <button
                            onClick={() => refetch()}
                            disabled={isFetching}
                            className="p-1.5 text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
                            title="Refresh"
                        >
                            <RefreshCw className={`w-4 h-4 ${isFetching ? 'animate-spin' : ''}`} />
                        </button>
                    </div>
                </div>
            </div>

            {/* Table */}
            <div className="bg-card border border-border rounded-xl overflow-hidden">
                {isError ? (
                    <div className="flex flex-col items-center justify-center py-16 gap-3">
                        <AlertTriangle className="w-8 h-8 text-red-400" />
                        <p className="text-muted-foreground">Failed to load position history.</p>
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
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Exit Price</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Duration</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Realized PnL</th>
                                    <th className="text-right px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Net PnL</th>
                                    <th className="text-center px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Result</th>
                                    <th className="text-center px-4 py-3 text-xs font-semibold text-muted-foreground whitespace-nowrap">Details</th>
                                </tr>
                            </thead>
                            <tbody>
                                {isLoading ? (
                                    <tr>
                                        <td colSpan={11}><TableSkeleton /></td>
                                    </tr>
                                ) : items.length === 0 ? (
                                    <tr>
                                        <td colSpan={11}>
                                            <div className="flex flex-col items-center justify-center py-16 gap-3">
                                                <TrendingUp className="w-10 h-10 text-muted-foreground/30" />
                                                <p className="font-medium text-muted-foreground">No completed trades found</p>
                                                <p className="text-sm text-muted-foreground/70">
                                                    {activeFilterCount > 0 ? 'Try adjusting your filters.' : 'Closed positions will appear here once trades complete.'}
                                                </p>
                                                {activeFilterCount > 0 && (
                                                    <button onClick={handleReset} className="text-sm text-primary hover:text-primary/80 transition-colors">
                                                        Clear all filters
                                                    </button>
                                                )}
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
                                            <td className="px-4 py-3 text-right font-mono whitespace-nowrap">{formatPrice(pos.exit_price)}</td>
                                            <td className="px-4 py-3 text-right text-muted-foreground whitespace-nowrap">{formatDuration(pos.duration_seconds)}</td>
                                            <td className={`px-4 py-3 text-right font-mono font-semibold whitespace-nowrap ${pnlColor(pos.realized_pnl)}`}>
                                                {fmt$(pos.realized_pnl, true)}
                                            </td>
                                            <td className={`px-4 py-3 text-right font-mono font-bold whitespace-nowrap ${pnlColor(pos.net_pnl)}`}>
                                                {fmt$(pos.net_pnl, true)}
                                            </td>
                                            <td className="px-4 py-3 text-center whitespace-nowrap">
                                                <ResultBadge result={pos.result} />
                                            </td>
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
                            Page {pagination?.page ?? 1} of {totalPages} — {pagination?.total_items ?? 0} positions
                        </span>
                        <div className="flex items-center gap-1">
                            <button
                                onClick={() => setPage(p => Math.max(1, p - 1))}
                                disabled={page <= 1}
                                className="p-1.5 rounded-lg hover:bg-muted disabled:opacity-40 transition-colors"
                            >
                                <ChevronLeft className="w-4 h-4" />
                            </button>
                            <button
                                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                                disabled={page >= totalPages}
                                className="p-1.5 rounded-lg hover:bg-muted disabled:opacity-40 transition-colors"
                            >
                                <ChevronRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                )}

                {/* Diagnostics note */}
                {(() => {
                    const noPos = data?.metadata?.fills_without_position_id;
                    if (!noPos || noPos <= 0) return null;
                    return (
                        <div className="flex items-start gap-2 px-4 py-2 border-t border-border bg-muted/10 text-xs text-muted-foreground">
                            <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0 mt-0.5 text-yellow-300/90" />
                            <div className="leading-snug">
                                <div className="font-medium text-foreground/90">Unlinked close fills detected</div>
                                <div>
                                    {noPos} close fill{noPos !== 1 ? 's' : ''} were missing a position ID, so they are shown as standalone completed records
                                    (<span className="font-mono">fill_&lt;id&gt;</span>). This keeps realized PnL complete while preserving audit visibility.
                                </div>
                            </div>
                        </div>
                    );
                })()}
            </div>

            <PositionDrawer position={selectedPosition} onClose={() => setSelectedPosition(null)} />
        </div>
    );
}
