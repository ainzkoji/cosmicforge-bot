import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
    AlertTriangle,
    Ban,
    BarChart3,
    CheckCircle2,
    DatabaseZap,
    Eye,
    FileSearch,
    Loader2,
    RefreshCw,
    Search,
    Settings2,
    ShieldCheck,
    ShieldQuestion,
    SlidersHorizontal,
    Star,
    X,
} from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import {
    blacklistAdminSignalPair,
    disableAdminSignalPair,
    enableAdminSignalPair,
    getAdminSignalPairMetrics,
    getAdminSignalPairs,
    getAdminSignalScanRunDetail,
    getAdminSignalScanRuns,
    refreshAdminSignalPairs,
    whitelistAdminSignalPair,
    type SignalPair,
    type SignalPairMetrics,
    type SignalScanRun,
    type SignalScanResult,
} from "@/api/adminSignalPairs";

type TabId = "universe" | "metrics" | "lists" | "scanRuns" | "performance" | "settings";

function truthy(value: unknown): boolean {
    return value === true || value === 1 || value === "1";
}

function formatValue(value: unknown): string {
    if (value === null || value === undefined || value === "") return "n/a";
    if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
    if (typeof value === "boolean") return value ? "Yes" : "No";
    return String(value);
}

function formatMoney(value?: number | null): string {
    if (value === null || value === undefined) return "n/a";
    return `$${value.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function errorMessage(error: unknown): string {
    const anyError = error as any;
    const detail = anyError?.response?.data?.detail;
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) return detail.map((item) => item.msg || JSON.stringify(item)).join(", ");
    return anyError?.message || "Action failed.";
}

function Badge({ children, variant = "info" }: { children: React.ReactNode; variant?: "info" | "success" | "warning" | "danger" }) {
    const className = {
        info: "admin-badge-info",
        success: "admin-badge-success",
        warning: "admin-badge-warning",
        danger: "admin-badge-danger",
    }[variant];
    return <span className={`admin-badge ${className}`}>{children}</span>;
}

function SafeBadge({ value }: { value: unknown }) {
    return truthy(value) ? <Badge variant="success">Safe</Badge> : <Badge variant="danger">Unsafe</Badge>;
}

function TierBadge({ value }: { value?: string | null }) {
    if (!value) return <Badge>Unknown</Badge>;
    if (value === "TIER_1") return <Badge variant="success">Tier 1</Badge>;
    if (value === "TIER_2") return <Badge variant="info">Tier 2</Badge>;
    if (value === "TIER_3") return <Badge variant="warning">Tier 3</Badge>;
    return <Badge>{value}</Badge>;
}

function EmptyState({ title, body }: { title: string; body: string }) {
    return (
        <div className="admin-card py-12 text-center">
            <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full" style={{ background: "rgba(59,130,246,0.12)", color: "var(--admin-blue)" }}>
                <FileSearch className="h-6 w-6" />
            </div>
            <h3 className="text-lg font-semibold" style={{ color: "var(--admin-text-primary)" }}>{title}</h3>
            <p className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>{body}</p>
        </div>
    );
}

function LoadingBlock() {
    return (
        <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin" style={{ color: "var(--admin-blue)" }} />
        </div>
    );
}

function DetailModal({
    title,
    rows,
    onClose,
}: {
    title: string;
    rows: [string, unknown][];
    onClose: () => void;
}) {
    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm">
            <div className="max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-xl border" style={{ background: "var(--admin-bg-card)", borderColor: "var(--admin-border-color)" }}>
                <div className="sticky top-0 z-10 flex items-start justify-between border-b p-5" style={{ background: "var(--admin-bg-card)", borderColor: "var(--admin-border-color)" }}>
                    <div>
                        <div className="text-xs uppercase tracking-widest" style={{ color: "var(--admin-blue)" }}>Pair Universe Details</div>
                        <h2 className="mt-1 text-2xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{title}</h2>
                    </div>
                    <button className="admin-btn admin-btn-secondary" type="button" onClick={onClose} aria-label="Close details">
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="grid grid-cols-1 gap-3 p-5 md:grid-cols-2 xl:grid-cols-3">
                    {rows.map(([label, value]) => (
                        <div key={label} className="rounded-lg border p-3" style={{ borderColor: "var(--admin-border-color)", background: "var(--admin-bg-primary)" }}>
                            <div className="text-xs uppercase tracking-wide" style={{ color: "var(--admin-text-muted)" }}>{label}</div>
                            <div className="mt-1 break-words text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>{formatValue(value)}</div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}

export default function SignalPairs() {
    const [activeTab, setActiveTab] = useState<TabId>("universe");
    const [search, setSearch] = useState("");
    const [tierFilter, setTierFilter] = useState("all");
    const [safeFilter, setSafeFilter] = useState("all");
    const [enabledFilter, setEnabledFilter] = useState("all");
    const [blacklistedFilter, setBlacklistedFilter] = useState("all");
    const [detailRows, setDetailRows] = useState<{ title: string; rows: [string, unknown][] } | null>(null);
    const [selectedScanRunId, setSelectedScanRunId] = useState<string | null>(null);
    const [notice, setNotice] = useState<{ type: "success" | "error"; text: string } | null>(null);
    const queryClient = useQueryClient();

    const pairParams = {
        asset_class: "crypto",
        quote_asset: "USDT",
        search: search.trim() || undefined,
        tier: tierFilter === "all" ? undefined : tierFilter,
        enabled: enabledFilter === "all" ? undefined : enabledFilter,
        blacklisted: blacklistedFilter === "all" ? undefined : blacklistedFilter,
        limit: 200,
    };
    const metricsParams = {
        exchange: "binance_futures",
        search: search.trim() || undefined,
        is_safe: safeFilter === "all" ? undefined : safeFilter,
        limit: 200,
    };

    const pairsQuery = useQuery({
        queryKey: ["adminSignalPairs", pairParams],
        queryFn: () => getAdminSignalPairs(pairParams),
    });
    const metricsQuery = useQuery({
        queryKey: ["adminSignalPairMetrics", metricsParams],
        queryFn: () => getAdminSignalPairMetrics(metricsParams),
    });
    const scanRunsQuery = useQuery({
        queryKey: ["adminSignalScanRuns"],
        queryFn: () => getAdminSignalScanRuns({ limit: 50 }),
    });
    const scanRunDetailQuery = useQuery({
        queryKey: ["adminSignalScanRunDetail", selectedScanRunId],
        queryFn: () => getAdminSignalScanRunDetail(selectedScanRunId || ""),
        enabled: Boolean(selectedScanRunId),
    });

    const refreshAll = () => {
        queryClient.invalidateQueries({ queryKey: ["adminSignalPairs"] });
        queryClient.invalidateQueries({ queryKey: ["adminSignalPairMetrics"] });
        queryClient.invalidateQueries({ queryKey: ["adminSignalScanRuns"] });
        if (selectedScanRunId) queryClient.invalidateQueries({ queryKey: ["adminSignalScanRunDetail", selectedScanRunId] });
    };

    const actionOptions = {
        onSuccess: (pair?: SignalPair) => {
            setNotice({ type: "success", text: pair?.warning || "Pair universe action completed successfully." });
            refreshAll();
        },
        onError: (error: unknown) => setNotice({ type: "error", text: errorMessage(error) }),
    };

    const enableMutation = useMutation({ mutationFn: enableAdminSignalPair, ...actionOptions });
    const disableMutation = useMutation({ mutationFn: disableAdminSignalPair, ...actionOptions });
    const whitelistMutation = useMutation({ mutationFn: whitelistAdminSignalPair, ...actionOptions });
    const blacklistMutation = useMutation({ mutationFn: blacklistAdminSignalPair, ...actionOptions });
    const refreshDiscoveryMutation = useMutation({
        mutationFn: () =>
            refreshAdminSignalPairs({
                min_quote_volume_24h: 50_000_000,
                max_spread_percent: 0.2,
                quote_asset: "USDT",
                contract_type: "PERPETUAL",
                validate_candles: true,
                candle_timeframe: "1h",
                min_candles: 200,
            }),
        onSuccess: (summary) => {
            setNotice({
                type: "success",
                text: `Discovery refreshed: ${summary.symbols_eligible} eligible, ${summary.symbols_skipped} skipped.`,
            });
            refreshAll();
        },
        onError: (error: unknown) => setNotice({ type: "error", text: errorMessage(error) }),
    });

    const pairs = pairsQuery.data?.items || [];
    const metrics = metricsQuery.data?.items || [];
    const scanRuns = scanRunsQuery.data?.items || [];
    const actionPending = enableMutation.isPending || disableMutation.isPending || whitelistMutation.isPending || blacklistMutation.isPending;

    const universeSummary = useMemo(() => {
        const total = pairs.length;
        const enabled = pairs.filter((pair) => truthy(pair.enabled)).length;
        const blacklisted = pairs.filter((pair) => truthy(pair.blacklisted)).length;
        const safe = metrics.filter((row) => truthy(row.is_safe)).length;
        return { total, enabled, blacklisted, safe };
    }, [pairs, metrics]);

    const showPairDetails = (pair: SignalPair) => {
        setDetailRows({
            title: pair.symbol,
            rows: Object.entries(pair) as [string, unknown][],
        });
    };

    const showMetricDetails = (row: SignalPairMetrics) => {
        setDetailRows({
            title: `${row.symbol} Metrics`,
            rows: Object.entries(row) as [string, unknown][],
        });
    };

    const handleBlacklist = (symbol: string) => {
        const reason = window.prompt(`Why should ${symbol} be blacklisted?`, "Admin risk control");
        if (reason === null) return;
        blacklistMutation.mutate({ symbol, reason });
    };

    const tabs: { id: TabId; label: string }[] = [
        { id: "universe", label: "Pair Universe" },
        { id: "metrics", label: "Pair Metrics" },
        { id: "lists", label: "Whitelist / Blacklist" },
        { id: "scanRuns", label: "Scan Runs" },
        { id: "performance", label: "Pair Performance" },
        { id: "settings", label: "Settings" },
    ];

    return (
        <AdminLayout>
            <div className="space-y-8">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>Signal Pair Universe</h1>
                        <p className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            Manage eligible crypto pairs, review liquidity/spread health, refresh discovery, and inspect scan history.
                        </p>
                    </div>
                    <button
                        className="admin-btn admin-btn-primary"
                        type="button"
                        disabled={refreshDiscoveryMutation.isPending}
                        onClick={() => refreshDiscoveryMutation.mutate()}
                    >
                        {refreshDiscoveryMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                        Refresh Discovery
                    </button>
                </div>

                {notice && (
                    <div className="admin-card" style={{ borderColor: notice.type === "success" ? "rgba(16,185,129,0.35)" : "rgba(239,68,68,0.35)" }}>
                        <div className="flex items-center gap-3" style={{ color: notice.type === "success" ? "var(--admin-green)" : "var(--admin-red)" }}>
                            {notice.type === "success" ? <CheckCircle2 className="h-5 w-5" /> : <AlertTriangle className="h-5 w-5" />}
                            <span>{notice.text}</span>
                        </div>
                    </div>
                )}

                <div className="admin-card" style={{ borderColor: "rgba(59,130,246,0.35)", background: "linear-gradient(135deg, rgba(59,130,246,0.10), rgba(16,185,129,0.06))" }}>
                    <div className="flex items-start gap-4">
                        <DatabaseZap className="h-7 w-7 flex-shrink-0" style={{ color: "var(--admin-blue)" }} />
                        <div>
                            <h2 className="text-lg font-bold" style={{ color: "var(--admin-text-primary)" }}>Discovery and eligibility control only.</h2>
                            <p className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                                Refresh Discovery updates pair universe and metrics only. It does not generate signals, publish signals, trade, copy trade, place orders, or touch broker/TradingView execution.
                            </p>
                        </div>
                    </div>
                </div>

                <div className="grid grid-cols-1 gap-5 md:grid-cols-2 xl:grid-cols-4">
                    <div className="admin-card">
                        <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>Known Pairs</div>
                        <div className="mt-3 text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{universeSummary.total}</div>
                    </div>
                    <div className="admin-card">
                        <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>Enabled</div>
                        <div className="mt-3 text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{universeSummary.enabled}</div>
                    </div>
                    <div className="admin-card">
                        <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>Safe Metrics</div>
                        <div className="mt-3 text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{universeSummary.safe}</div>
                    </div>
                    <div className="admin-card">
                        <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>Blacklisted</div>
                        <div className="mt-3 text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{universeSummary.blacklisted}</div>
                    </div>
                </div>

                <div className="admin-card">
                    <div className="mb-4 flex items-center gap-2 text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                        <SlidersHorizontal className="h-4 w-4" />
                        Filters
                    </div>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-5">
                        <div className="relative md:col-span-2">
                            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2" style={{ color: "var(--admin-text-muted)" }} />
                            <input className="admin-input pl-9" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search symbol, e.g. BTCUSDT" />
                        </div>
                        <select className="admin-input" value={tierFilter} onChange={(event) => setTierFilter(event.target.value)}>
                            <option value="all">All tiers</option>
                            <option value="TIER_1">Tier 1</option>
                            <option value="TIER_2">Tier 2</option>
                            <option value="TIER_3">Tier 3</option>
                            <option value="DISCOVERED">Discovered</option>
                        </select>
                        <select className="admin-input" value={safeFilter} onChange={(event) => setSafeFilter(event.target.value)}>
                            <option value="all">Safe + unsafe</option>
                            <option value="1">Safe only</option>
                            <option value="0">Unsafe only</option>
                        </select>
                        <select className="admin-input" value={blacklistedFilter} onChange={(event) => setBlacklistedFilter(event.target.value)}>
                            <option value="all">All blacklist states</option>
                            <option value="0">Not blacklisted</option>
                            <option value="1">Blacklisted</option>
                        </select>
                    </div>
                </div>

                <div className="flex flex-wrap gap-2">
                    {tabs.map((tab) => (
                        <button
                            key={tab.id}
                            className={`admin-btn ${activeTab === tab.id ? "admin-btn-primary" : "admin-btn-secondary"}`}
                            type="button"
                            onClick={() => setActiveTab(tab.id)}
                        >
                            {tab.label}
                        </button>
                    ))}
                </div>

                {activeTab === "universe" && (
                    pairsQuery.isLoading ? <LoadingBlock /> : pairs.length === 0 ? <EmptyState title="No pairs discovered yet." body="Run Refresh Discovery to seed and evaluate the pair universe." /> : (
                        <div className="admin-card overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>Symbol</th>
                                        <th>Tier</th>
                                        <th>Exchange</th>
                                        <th>Quote</th>
                                        <th>Contract</th>
                                        <th>Enabled</th>
                                        <th>Whitelist</th>
                                        <th>Blacklist</th>
                                        <th>Last Seen</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {pairs.map((pair) => (
                                        <tr key={pair.symbol}>
                                            <td className="font-semibold">{pair.symbol}</td>
                                            <td><TierBadge value={pair.tier} /></td>
                                            <td>{pair.exchange}</td>
                                            <td>{pair.quote_asset}</td>
                                            <td>{pair.contract_type}</td>
                                            <td>{truthy(pair.enabled) ? <Badge variant="success">Enabled</Badge> : <Badge variant="warning">Disabled</Badge>}</td>
                                            <td>{truthy(pair.whitelisted) ? <Badge variant="success">Whitelisted</Badge> : <span>n/a</span>}</td>
                                            <td>{truthy(pair.blacklisted) ? <Badge variant="danger">Blacklisted</Badge> : <span>n/a</span>}</td>
                                            <td>{formatValue(pair.last_seen_at)}</td>
                                            <td>
                                                <div className="flex flex-wrap gap-2">
                                                    <button className="admin-btn admin-btn-secondary" type="button" onClick={() => showPairDetails(pair)}><Eye className="h-4 w-4" />Details</button>
                                                    {truthy(pair.enabled) ? (
                                                        <button className="admin-btn admin-btn-secondary" type="button" disabled={actionPending} onClick={() => disableMutation.mutate(pair.symbol)}>Disable</button>
                                                    ) : (
                                                        <button className="admin-btn admin-btn-secondary" type="button" disabled={actionPending} onClick={() => enableMutation.mutate(pair.symbol)}>Enable</button>
                                                    )}
                                                    <button className="admin-btn admin-btn-secondary" type="button" disabled={actionPending} onClick={() => whitelistMutation.mutate(pair.symbol)}><Star className="h-4 w-4" />Whitelist</button>
                                                    <button className="admin-btn admin-btn-danger" type="button" disabled={actionPending} onClick={() => handleBlacklist(pair.symbol)}><Ban className="h-4 w-4" />Blacklist</button>
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )
                )}

                {activeTab === "metrics" && (
                    metricsQuery.isLoading ? <LoadingBlock /> : metrics.length === 0 ? <EmptyState title="No metrics available yet." body="Refresh discovery to collect liquidity, spread, and candle health metrics." /> : (
                        <div className="admin-card overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>Symbol</th>
                                        <th>Tier</th>
                                        <th>Safety</th>
                                        <th>24h Volume</th>
                                        <th>Spread %</th>
                                        <th>Bid</th>
                                        <th>Ask</th>
                                        <th>Candles</th>
                                        <th>ATR %</th>
                                        <th>Liquidity</th>
                                        <th>Spread Score</th>
                                        <th>Reliability</th>
                                        <th>Unsafe Reason</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {metrics.map((row) => (
                                        <tr key={row.symbol}>
                                            <td className="font-semibold">{row.symbol}</td>
                                            <td><TierBadge value={row.tier} /></td>
                                            <td><SafeBadge value={row.is_safe} /></td>
                                            <td>{formatMoney(row.quote_volume_24h)}</td>
                                            <td>{formatValue(row.spread_percent)}</td>
                                            <td>{formatValue(row.bid_price)}</td>
                                            <td>{formatValue(row.ask_price)}</td>
                                            <td>{formatValue(row.candle_count)}</td>
                                            <td>{formatValue(row.atr_percent)}</td>
                                            <td>{formatValue(row.liquidity_score)}</td>
                                            <td>{formatValue(row.spread_score)}</td>
                                            <td>{formatValue(row.reliability_score)}</td>
                                            <td>{formatValue(row.unsafe_reason)}</td>
                                            <td>
                                                <div className="flex flex-wrap gap-2">
                                                    <button className="admin-btn admin-btn-secondary" type="button" onClick={() => showMetricDetails(row)}><Eye className="h-4 w-4" />View</button>
                                                    <button className="admin-btn admin-btn-secondary" type="button" disabled={actionPending || truthy(row.blacklisted)} onClick={() => whitelistMutation.mutate(row.symbol)}>Whitelist</button>
                                                    <button className="admin-btn admin-btn-danger" type="button" disabled={actionPending} onClick={() => handleBlacklist(row.symbol)}>Blacklist</button>
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )
                )}

                {activeTab === "lists" && (
                    <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
                        <div className="admin-card">
                            <h2 className="text-lg font-bold" style={{ color: "var(--admin-text-primary)" }}>Whitelisted Pairs</h2>
                            <div className="mt-4 space-y-3">
                                {pairs.filter((pair) => truthy(pair.whitelisted)).length === 0 ? (
                                    <p className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>No whitelisted pairs yet.</p>
                                ) : pairs.filter((pair) => truthy(pair.whitelisted)).map((pair) => (
                                    <div key={pair.symbol} className="flex items-center justify-between rounded-lg border p-3" style={{ borderColor: "var(--admin-border-color)" }}>
                                        <div>
                                            <div className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{pair.symbol}</div>
                                            <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>Whitelist still requires healthy safety metrics.</div>
                                        </div>
                                        {truthy(pair.blacklisted) ? <Badge variant="danger">Blacklist Overrides</Badge> : <Badge variant="success">Whitelisted</Badge>}
                                    </div>
                                ))}
                            </div>
                        </div>
                        <div className="admin-card">
                            <h2 className="text-lg font-bold" style={{ color: "var(--admin-text-primary)" }}>Blacklisted Pairs</h2>
                            <div className="mt-4 space-y-3">
                                {pairs.filter((pair) => truthy(pair.blacklisted)).length === 0 ? (
                                    <p className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>No blacklisted pairs yet.</p>
                                ) : pairs.filter((pair) => truthy(pair.blacklisted)).map((pair) => (
                                    <div key={pair.symbol} className="rounded-lg border p-3" style={{ borderColor: "rgba(239,68,68,0.35)" }}>
                                        <div className="flex items-center justify-between">
                                            <div className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{pair.symbol}</div>
                                            <Badge variant="danger">Blacklisted</Badge>
                                        </div>
                                        <div className="mt-1 text-xs" style={{ color: "var(--admin-text-muted)" }}>{pair.blacklist_reason || "No reason recorded."}</div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                )}

                {activeTab === "scanRuns" && (
                    <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
                        <div>
                            {scanRunsQuery.isLoading ? <LoadingBlock /> : scanRuns.length === 0 ? <EmptyState title="No scan runs yet." body="Discovery and generation runs will appear here once logged." /> : (
                                <div className="admin-card overflow-x-auto">
                                    <table className="admin-table">
                                        <thead>
                                            <tr>
                                                <th>Type</th>
                                                <th>Status</th>
                                                <th>Started</th>
                                                <th>Eligible</th>
                                                <th>Scanned</th>
                                                <th>Published</th>
                                                <th>Actions</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {scanRuns.map((run: SignalScanRun) => (
                                                <tr key={run.id}>
                                                    <td>{run.scan_type}</td>
                                                    <td><Badge variant={run.status === "FAILED" ? "danger" : run.status === "PARTIAL" ? "warning" : "success"}>{run.status}</Badge></td>
                                                    <td>{formatValue(run.started_at)}</td>
                                                    <td>{formatValue(run.symbols_eligible)}</td>
                                                    <td>{formatValue(run.symbols_scanned)}</td>
                                                    <td>{formatValue(run.signals_published)}</td>
                                                    <td>
                                                        <button className="admin-btn admin-btn-secondary" type="button" onClick={() => setSelectedScanRunId(run.id)}>
                                                            <Eye className="h-4 w-4" />
                                                            View Details
                                                        </button>
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </div>
                        <div className="admin-card">
                            <h2 className="text-lg font-bold" style={{ color: "var(--admin-text-primary)" }}>Scan Run Details</h2>
                            {!selectedScanRunId ? (
                                <p className="mt-3 text-sm" style={{ color: "var(--admin-text-secondary)" }}>Select a scan run to inspect per-symbol scan results and skip reasons.</p>
                            ) : scanRunDetailQuery.isLoading ? (
                                <LoadingBlock />
                            ) : scanRunDetailQuery.data ? (
                                <div className="mt-4 space-y-4">
                                    <div className="rounded-lg border p-3" style={{ borderColor: "var(--admin-border-color)" }}>
                                        <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>{scanRunDetailQuery.data.scan_run.id}</div>
                                        <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>{scanRunDetailQuery.data.scan_run.scan_type} • {scanRunDetailQuery.data.scan_run.status}</div>
                                    </div>
                                    <div className="max-h-[420px] overflow-auto">
                                        <table className="admin-table">
                                            <thead>
                                                <tr>
                                                    <th>Symbol</th>
                                                    <th>Result</th>
                                                    <th>Reason</th>
                                                    <th>Candidates</th>
                                                    <th>Published</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {scanRunDetailQuery.data.results.map((result: SignalScanResult) => (
                                                    <tr key={result.id}>
                                                        <td>{result.symbol}</td>
                                                        <td>{truthy(result.was_skipped) ? <Badge variant="warning">Skipped</Badge> : <Badge variant="success">Scanned</Badge>}</td>
                                                        <td>{formatValue(result.skip_reason || result.error)}</td>
                                                        <td>{formatValue(result.candidate_count)}</td>
                                                        <td>{formatValue(result.published_count)}</td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            ) : (
                                <p className="mt-3 text-sm" style={{ color: "var(--admin-text-secondary)" }}>Unable to load scan run details.</p>
                            )}
                        </div>
                    </div>
                )}

                {activeTab === "performance" && (
                    <div className="admin-card py-12 text-center">
                        <BarChart3 className="mx-auto mb-3 h-10 w-10" style={{ color: "var(--admin-blue)" }} />
                        <h2 className="text-xl font-bold" style={{ color: "var(--admin-text-primary)" }}>Pair performance analytics are coming later.</h2>
                        <p className="mt-2 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            Pair performance analytics will appear after symbol-level performance stats are implemented.
                        </p>
                    </div>
                )}

                {activeTab === "settings" && (
                    <div className="admin-card">
                        <div className="mb-4 flex items-center gap-2">
                            <Settings2 className="h-5 w-5" style={{ color: "var(--admin-blue)" }} />
                            <h2 className="text-xl font-bold" style={{ color: "var(--admin-text-primary)" }}>Current Safety Defaults</h2>
                        </div>
                        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                            {[
                                ["Minimum volume", "$50,000,000"],
                                ["Maximum spread", "0.20%"],
                                ["Minimum candles", "200"],
                                ["Quote asset", "USDT"],
                                ["Contract type", "PERPETUAL"],
                                ["Generation caps", "Controlled by generation script"],
                            ].map(([label, value]) => (
                                <div key={label} className="rounded-lg border p-4" style={{ borderColor: "var(--admin-border-color)", background: "var(--admin-bg-primary)" }}>
                                    <div className="text-xs uppercase tracking-wide" style={{ color: "var(--admin-text-muted)" }}>{label}</div>
                                    <div className="mt-1 font-semibold" style={{ color: "var(--admin-text-primary)" }}>{value}</div>
                                </div>
                            ))}
                        </div>
                        <p className="mt-4 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            These settings are informational in this phase. Persisted admin-editable scan settings are intentionally deferred.
                        </p>
                    </div>
                )}

                <div className="admin-card">
                    <div className="flex items-start gap-3">
                        <ShieldCheck className="h-5 w-5 flex-shrink-0" style={{ color: "var(--admin-green)" }} />
                        <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            Blacklist always overrides whitelist and enabled state. Whitelisted pairs still require healthy market data before generation eligibility.
                        </div>
                    </div>
                    <div className="mt-3 flex items-start gap-3">
                        <ShieldQuestion className="h-5 w-5 flex-shrink-0" style={{ color: "var(--admin-blue)" }} />
                        <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            This page has no trading controls. It only manages discovery, pair eligibility, and scan history for manual signal display.
                        </div>
                    </div>
                </div>

                {detailRows && <DetailModal title={detailRows.title} rows={detailRows.rows} onClose={() => setDetailRows(null)} />}
            </div>
        </AdminLayout>
    );
}
