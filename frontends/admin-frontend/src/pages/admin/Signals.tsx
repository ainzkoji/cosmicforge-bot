import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
    AlertTriangle,
    BarChart3,
    CheckCircle2,
    Clock3,
    Eye,
    FileSearch,
    Loader2,
    RadioTower,
    RefreshCw,
    Send,
    ShieldX,
    SlidersHorizontal,
    Trophy,
    X,
    XCircle,
} from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import {
    cancelAdminSignal,
    getAdminSignalCandidates,
    getAdminSignals,
    publishAdminSignal,
    unpublishAdminSignal,
    type AdminSignalCandidate,
    type AdminTradingSignal,
} from "@/api/adminSignals";

type TabId = "generated" | "unpublished" | "published" | "rejected" | "performance";
type DetailItem = AdminTradingSignal | AdminSignalCandidate;

const terminalStatuses = new Set(["EXPIRED", "SL_HIT", "CANCELLED", "INVALIDATED", "TP3_HIT"]);
const pendingStatuses = new Set(["PENDING_ENTRY", "ACTIVE"]);

function truthy(value: unknown): boolean {
    return value === true || value === 1 || value === "1";
}

function formatValue(value: unknown): string {
    if (value === null || value === undefined || value === "") return "n/a";
    if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 8 });
    if (typeof value === "boolean") return value ? "Yes" : "No";
    return String(value);
}

function formatStatus(status: string): string {
    const labels: Record<string, string> = {
        PENDING_ENTRY: "Pending Entry",
        ACTIVE: "Active",
        EXPIRED: "Expired",
        TP1_HIT: "TP1 Hit / Partial",
        TP2_HIT: "TP2 Hit / Win",
        TP3_HIT: "TP3 Hit / Strong Win",
        SL_HIT: "Stop Loss Hit",
        CANCELLED: "Cancelled",
        INVALIDATED: "Invalidated",
    };
    return labels[status] || status.replace(/_/g, " ");
}

function statusBadgeClass(status: string): string {
    if (status === "TP2_HIT" || status === "TP3_HIT" || status === "ACTIVE") return "admin-badge-success";
    if (status === "PENDING_ENTRY" || status === "TP1_HIT") return "admin-badge-warning";
    if (status === "SL_HIT" || status === "CANCELLED" || status === "INVALIDATED") return "admin-badge-danger";
    return "admin-badge-info";
}

function publishErrorMessage(error: unknown): string {
    const anyError = error as any;
    const detail = anyError?.response?.data?.detail;
    if (typeof detail === "string") return detail;
    if (detail?.reason) return detail.reason;
    if (Array.isArray(detail)) return detail.map((item) => item.msg || JSON.stringify(item)).join(", ");
    return anyError?.message || "Action failed.";
}

function MetricCard({ label, value, helper, icon }: { label: string; value: string | number; helper: string; icon: React.ReactNode }) {
    return (
        <div className="admin-card">
            <div className="flex items-center justify-between">
                <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>{label}</div>
                <div className="rounded-lg p-2" style={{ background: "rgba(59,130,246,0.12)", color: "var(--admin-blue)" }}>{icon}</div>
            </div>
            <div className="mt-3 text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{value}</div>
            <div className="mt-1 text-xs" style={{ color: "var(--admin-text-muted)" }}>{helper}</div>
        </div>
    );
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

function ErrorState({ onRetry }: { onRetry: () => void }) {
    return (
        <div className="admin-card">
            <div className="flex items-center justify-between gap-4">
                <div className="flex items-center gap-3" style={{ color: "var(--admin-red)" }}>
                    <AlertTriangle className="h-5 w-5" />
                    <span>Unable to load admin signals right now. Please try again.</span>
                </div>
                <button className="admin-btn admin-btn-secondary" type="button" onClick={onRetry}>
                    <RefreshCw className="h-4 w-4" />
                    Retry
                </button>
            </div>
        </div>
    );
}

function SignalStatusBadge({ status }: { status: string }) {
    return <span className={`admin-badge ${statusBadgeClass(status)}`}>{formatStatus(status)}</span>;
}

function PublishedBadge({ value }: { value: unknown }) {
    return (
        <span className={`admin-badge ${truthy(value) ? "admin-badge-success" : "admin-badge-warning"}`}>
            {truthy(value) ? "Published" : "Unpublished"}
        </span>
    );
}

function DevBadge({ value }: { value: unknown }) {
    if (!truthy(value)) return <span>No</span>;
    return <span className="admin-badge admin-badge-warning">DEV/TEST</span>;
}

function DetailModal({ item, onClose }: { item: DetailItem | null; onClose: () => void }) {
    if (!item) return null;
    const rows: [string, unknown][] = [
        ["ID", item.id],
        ["Candidate ID", "candidate_id" in item ? item.candidate_id : undefined],
        ["Asset Class", item.asset_class],
        ["Symbol", item.symbol],
        ["Side", item.side],
        ["Timeframe", item.timeframe],
        ["Strategy", item.strategy_name],
        ["Entry Price", item.entry_price],
        ["Entry Zone Low", item.entry_zone_low],
        ["Entry Zone High", item.entry_zone_high],
        ["Stop Loss", item.stop_loss],
        ["Take Profit 1", item.take_profit_1],
        ["Take Profit 2", item.take_profit_2],
        ["Take Profit 3", item.take_profit_3],
        ["Risk / Reward", item.risk_reward],
        ["Confidence", item.confidence_score],
        ["Signal Reason", item.signal_reason],
        ["Rejection Reason", "rejection_reason" in item ? item.rejection_reason : undefined],
        ["Status", item.status],
        ["Published", "is_published" in item ? truthy(item.is_published) : undefined],
        ["Source", item.source],
        ["Dev Mode", item.dev_mode],
        ["Created", item.created_at],
        ["Published At", "published_at" in item ? item.published_at : undefined],
        ["Expires At", "expires_at" in item ? item.expires_at : undefined],
        ["Invalidated At", "invalidated_at" in item ? item.invalidated_at : undefined],
        ["TP1 Hit At", "tp1_hit_at" in item ? item.tp1_hit_at : undefined],
        ["TP2 Hit At", "tp2_hit_at" in item ? item.tp2_hit_at : undefined],
        ["TP3 Hit At", "tp3_hit_at" in item ? item.tp3_hit_at : undefined],
        ["SL Hit At", "sl_hit_at" in item ? item.sl_hit_at : undefined],
        ["Cancelled At", "cancelled_at" in item ? item.cancelled_at : undefined],
        ["Updated", item.updated_at],
    ];

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm">
            <div className="max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-xl border" style={{ background: "var(--admin-bg-card)", borderColor: "var(--admin-border-color)" }}>
                <div className="sticky top-0 z-10 flex items-start justify-between border-b p-5" style={{ background: "var(--admin-bg-card)", borderColor: "var(--admin-border-color)" }}>
                    <div>
                        <div className="text-xs uppercase tracking-widest" style={{ color: "var(--admin-blue)" }}>Signal Review Details</div>
                        <div className="mt-1 flex flex-wrap items-center gap-3">
                            <h2 className="text-2xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{item.symbol} {item.side}</h2>
                            <DevBadge value={item.dev_mode} />
                        </div>
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

function SignalsTable({
    items,
    emptyTitle,
    emptyBody,
    onView,
    onPublish,
    onUnpublish,
    onCancel,
    actionPending,
}: {
    items: AdminTradingSignal[];
    emptyTitle: string;
    emptyBody: string;
    onView: (item: AdminTradingSignal) => void;
    onPublish: (id: string) => void;
    onUnpublish: (id: string) => void;
    onCancel: (id: string) => void;
    actionPending: boolean;
}) {
    if (items.length === 0) return <EmptyState title={emptyTitle} body={emptyBody} />;

    return (
        <div className="admin-card overflow-x-auto">
            <table className="admin-table">
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Status</th>
                        <th>Published</th>
                        <th>Confidence</th>
                        <th>R/R</th>
                        <th>Entry</th>
                        <th>SL</th>
                        <th>TP1</th>
                        <th>TP2</th>
                        <th>TP3</th>
                        <th>Expires</th>
                        <th>Created</th>
                        <th>Dev</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {items.map((item) => {
                        const published = truthy(item.is_published);
                        const terminal = terminalStatuses.has(item.status);
                        const productionDevSignal = import.meta.env.PROD && truthy(item.dev_mode);
                        const canPublish = !published && !terminal && !productionDevSignal;
                        const canUnpublish = published;
                        const canCancel = item.status !== "CANCELLED" && !terminalStatuses.has(item.status);
                        return (
                            <tr key={item.id}>
                                <td className="font-semibold">{item.symbol}</td>
                                <td>
                                    <span className={`admin-badge ${item.side === "BUY" ? "admin-badge-success" : "admin-badge-danger"}`}>{item.side}</span>
                                </td>
                                <td><SignalStatusBadge status={item.status} /></td>
                                <td><PublishedBadge value={item.is_published} /></td>
                                <td>{formatValue(item.confidence_score)}%</td>
                                <td>{formatValue(item.risk_reward)}</td>
                                <td>{formatValue(item.entry_price)}</td>
                                <td>{formatValue(item.stop_loss)}</td>
                                <td>
                                    {formatValue(item.take_profit_1)}
                                    <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>Partial</div>
                                </td>
                                <td>{formatValue(item.take_profit_2)}</td>
                                <td>{formatValue(item.take_profit_3)}</td>
                                <td>{formatValue(item.expires_at)}</td>
                                <td>{formatValue(item.created_at)}</td>
                                <td><DevBadge value={item.dev_mode} /></td>
                                <td>
                                    <div className="flex flex-wrap gap-2">
                                        <button className="admin-btn admin-btn-secondary" type="button" onClick={() => onView(item)}>
                                            <Eye className="h-4 w-4" />
                                            View
                                        </button>
                                        {canPublish && (
                                            <button className="admin-btn admin-btn-primary" type="button" disabled={actionPending} onClick={() => onPublish(item.id)}>
                                                <Send className="h-4 w-4" />
                                                Publish
                                            </button>
                                        )}
                                        {canUnpublish && (
                                            <button className="admin-btn admin-btn-secondary" type="button" disabled={actionPending} onClick={() => onUnpublish(item.id)}>
                                                <XCircle className="h-4 w-4" />
                                                Unpublish
                                            </button>
                                        )}
                                        {canCancel && (
                                            <button className="admin-btn admin-btn-danger" type="button" disabled={actionPending} onClick={() => onCancel(item.id)}>
                                                <ShieldX className="h-4 w-4" />
                                                Cancel
                                            </button>
                                        )}
                                    </div>
                                </td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
}

function CandidatesTable({ items, onView }: { items: AdminSignalCandidate[]; onView: (item: AdminSignalCandidate) => void }) {
    if (items.length === 0) return <EmptyState title="No rejected candidates yet." body="Rejected setups will appear here after the engine stores candidate diagnostics." />;

    return (
        <div className="admin-card overflow-x-auto">
            <table className="admin-table">
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Confidence</th>
                        <th>R/R</th>
                        <th>Rejection Reason</th>
                        <th>Diagnostic Note</th>
                        <th>Created</th>
                        <th>Dev</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {items.map((item) => (
                        <tr key={item.id}>
                            <td className="font-semibold">{item.symbol}</td>
                            <td><span className={`admin-badge ${item.side === "BUY" ? "admin-badge-success" : "admin-badge-danger"}`}>{item.side}</span></td>
                            <td>{formatValue(item.confidence_score)}%</td>
                            <td>{formatValue(item.risk_reward)}</td>
                            <td><span className="admin-badge admin-badge-danger">{formatValue(item.rejection_reason)}</span></td>
                            <td className="max-w-md">{formatValue(item.signal_reason)}</td>
                            <td>{formatValue(item.created_at)}</td>
                            <td><DevBadge value={item.dev_mode} /></td>
                            <td>
                                <button className="admin-btn admin-btn-secondary" type="button" onClick={() => onView(item)}>
                                    <Eye className="h-4 w-4" />
                                    View
                                </button>
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

function LoadingState() {
    return (
        <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin" style={{ color: "var(--admin-blue)" }} />
        </div>
    );
}

export default function Signals() {
    const [activeTab, setActiveTab] = useState<TabId>("generated");
    const [detailItem, setDetailItem] = useState<DetailItem | null>(null);
    const [symbolFilter, setSymbolFilter] = useState("");
    const [statusFilter, setStatusFilter] = useState("all");
    const [sideFilter, setSideFilter] = useState("all");
    const [publishedFilter, setPublishedFilter] = useState("all");
    const [devModeFilter, setDevModeFilter] = useState("all");
    const [notice, setNotice] = useState<{ type: "success" | "error"; text: string } | null>(null);
    const queryClient = useQueryClient();

    const queryParams = {
        asset_class: "crypto",
        symbol: symbolFilter.trim() || undefined,
        status: statusFilter === "all" ? undefined : statusFilter,
        side: sideFilter === "all" ? undefined : sideFilter,
        is_published: publishedFilter === "all" ? undefined : publishedFilter,
        dev_mode: devModeFilter === "all" ? undefined : devModeFilter,
        limit: 100,
    };

    const signalsQuery = useQuery({
        queryKey: ["adminSignals", queryParams],
        queryFn: () => getAdminSignals(queryParams),
    });

    const candidatesQuery = useQuery({
        queryKey: ["adminSignalCandidates", symbolFilter, sideFilter, devModeFilter],
        queryFn: () =>
            getAdminSignalCandidates({
                asset_class: "crypto",
                status: "REJECTED",
                symbol: symbolFilter.trim() || undefined,
                side: sideFilter === "all" ? undefined : sideFilter,
                dev_mode: devModeFilter === "all" ? undefined : devModeFilter,
                limit: 100,
            }),
    });

    const refresh = () => {
        queryClient.invalidateQueries({ queryKey: ["adminSignals"] });
        queryClient.invalidateQueries({ queryKey: ["adminSignalCandidates"] });
    };

    const mutationOptions = {
        onSuccess: () => {
            setNotice({ type: "success", text: "Signal action completed successfully." });
            refresh();
        },
        onError: (error: unknown) => setNotice({ type: "error", text: publishErrorMessage(error) }),
    };

    const publishMutation = useMutation({ mutationFn: publishAdminSignal, ...mutationOptions });
    const unpublishMutation = useMutation({ mutationFn: unpublishAdminSignal, ...mutationOptions });
    const cancelMutation = useMutation({ mutationFn: cancelAdminSignal, ...mutationOptions });
    const actionPending = publishMutation.isPending || unpublishMutation.isPending || cancelMutation.isPending;

    const signals = signalsQuery.data?.items || [];
    const candidates = candidatesQuery.data?.items || [];
    const unpublishedSignals = signals.filter((item) => !truthy(item.is_published) && pendingStatuses.has(item.status));
    const publishedSignals = signals.filter((item) => truthy(item.is_published));

    const performance = useMemo(() => {
        const total = signals.length;
        const pending = unpublishedSignals.length;
        const published = publishedSignals.length;
        const expired = signals.filter((item) => item.status === "EXPIRED").length;
        const tp1 = signals.filter((item) => item.status === "TP1_HIT" || item.tp1_hit_at).length;
        const tp2 = signals.filter((item) => item.status === "TP2_HIT" || item.tp2_hit_at).length;
        const tp3 = signals.filter((item) => item.status === "TP3_HIT" || item.tp3_hit_at).length;
        const sl = signals.filter((item) => item.status === "SL_HIT" || item.sl_hit_at).length;
        const cancelled = signals.filter((item) => item.status === "CANCELLED").length;
        const invalidated = signals.filter((item) => item.status === "INVALIDATED").length;
        const completedForWinRate = signals.filter((item) => ["TP2_HIT", "TP3_HIT", "SL_HIT", "EXPIRED", "CANCELLED", "INVALIDATED"].includes(item.status)).length;
        const wins = tp2 + tp3;
        return {
            total,
            pending,
            published,
            rejected: candidates.length,
            active: signals.filter((item) => item.status === "ACTIVE" || item.status === "PENDING_ENTRY").length,
            expired,
            tp1,
            tp2,
            tp3,
            sl,
            cancelled,
            invalidated,
            winRate: completedForWinRate > 0 ? `${Math.round((wins / completedForWinRate) * 1000) / 10}%` : "n/a",
        };
    }, [signals, unpublishedSignals.length, publishedSignals.length, candidates.length]);

    const tabs: { id: TabId; label: string; count?: number }[] = [
        { id: "generated", label: "Generated Signals", count: signals.length },
        { id: "unpublished", label: "Unpublished / Manual Review", count: unpublishedSignals.length },
        { id: "published", label: "Published Signals", count: publishedSignals.length },
        { id: "rejected", label: "Rejected Candidates", count: candidates.length },
        { id: "performance", label: "Performance" },
    ];

    const handleCancel = (id: string) => {
        if (!window.confirm("Cancel this signal and remove it from user visibility?")) return;
        cancelMutation.mutate(id);
    };

    const isLoading = signalsQuery.isLoading || candidatesQuery.isLoading;
    const isError = signalsQuery.isError || candidatesQuery.isError;

    return (
        <AdminLayout>
            <div className="space-y-8">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>Admin Signals</h1>
                        <p className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            Monitor generated crypto signals, manage rejected candidates, unpublish or cancel unsafe setups, and review performance.
                        </p>
                    </div>
                    <button className="admin-btn admin-btn-secondary" type="button" onClick={refresh}>
                        <RefreshCw className="h-4 w-4" />
                        Refresh
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

                <div className="admin-card" style={{ borderColor: "rgba(16,185,129,0.35)", background: "linear-gradient(135deg, rgba(16,185,129,0.10), rgba(59,130,246,0.06))" }}>
                    <div className="flex items-start gap-4">
                        <RadioTower className="h-7 w-7 flex-shrink-0" style={{ color: "var(--admin-green)" }} />
                        <div>
                            <h2 className="text-lg font-bold" style={{ color: "var(--admin-text-primary)" }}>Publishing control only. No execution controls are available here.</h2>
                            <p className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                                This page can publish, unpublish, or cancel manual Crypto Signal Center records. It cannot trade, copy trade, place orders, send orders to brokers, or touch TradingView execution.
                            </p>
                        </div>
                    </div>
                </div>

                <div className="grid grid-cols-1 gap-5 md:grid-cols-2 xl:grid-cols-5">
                    <MetricCard label="Unpublished / Manual Review" value={performance.pending} helper="Dev, manual, or recovery records" icon={<Clock3 className="h-5 w-5" />} />
                    <MetricCard label="Published Signals" value={performance.published} helper="Visible to users" icon={<Send className="h-5 w-5" />} />
                    <MetricCard label="Rejected Candidates" value={performance.rejected} helper="Stored engine diagnostics" icon={<ShieldX className="h-5 w-5" />} />
                    <MetricCard label="Active Signals" value={performance.active} helper="Pending entry or active" icon={<RadioTower className="h-5 w-5" />} />
                    <MetricCard label="TP2/TP3 Win Rate" value={performance.winRate} helper="TP1 is partial only" icon={<Trophy className="h-5 w-5" />} />
                </div>

                <div className="admin-card">
                    <div className="mb-4 flex items-center gap-2 text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                        <SlidersHorizontal className="h-4 w-4" />
                        Filters
                    </div>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-5">
                        <input className="admin-input" value={symbolFilter} onChange={(event) => setSymbolFilter(event.target.value)} placeholder="Symbol, e.g. BTCUSDT" />
                        <select className="admin-input" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                            <option value="all">All statuses</option>
                            <option value="PENDING_ENTRY">Pending Entry</option>
                            <option value="ACTIVE">Active</option>
                            <option value="EXPIRED">Expired</option>
                            <option value="TP1_HIT">TP1 Partial</option>
                            <option value="TP2_HIT">TP2 Win</option>
                            <option value="TP3_HIT">TP3 Strong Win</option>
                            <option value="SL_HIT">Stop Loss Hit</option>
                            <option value="CANCELLED">Cancelled</option>
                            <option value="INVALIDATED">Invalidated</option>
                        </select>
                        <select className="admin-input" value={sideFilter} onChange={(event) => setSideFilter(event.target.value)}>
                            <option value="all">All sides</option>
                            <option value="BUY">BUY</option>
                            <option value="SELL">SELL</option>
                        </select>
                        <select className="admin-input" value={publishedFilter} onChange={(event) => setPublishedFilter(event.target.value)}>
                            <option value="all">Published + unpublished</option>
                            <option value="1">Published only</option>
                            <option value="0">Unpublished only</option>
                        </select>
                        <select className="admin-input" value={devModeFilter} onChange={(event) => setDevModeFilter(event.target.value)}>
                            <option value="all">Dev + prod</option>
                            <option value="0">Production records</option>
                            <option value="1">Dev records</option>
                        </select>
                    </div>
                </div>

                <div className="admin-card">
                    <div className="flex flex-wrap gap-2">
                        {tabs.map((tab) => (
                            <button
                                key={tab.id}
                                className={`admin-btn ${activeTab === tab.id ? "admin-btn-primary" : "admin-btn-secondary"}`}
                                type="button"
                                onClick={() => setActiveTab(tab.id)}
                            >
                                {tab.label}
                                {tab.count !== undefined && <span className="ml-1 opacity-80">({tab.count})</span>}
                            </button>
                        ))}
                    </div>
                </div>

                {isLoading && <LoadingState />}
                {isError && <ErrorState onRetry={refresh} />}

                {!isLoading && !isError && activeTab === "generated" && (
                    <SignalsTable
                        items={signals}
                        emptyTitle="No generated signals yet."
                        emptyBody="Generated trading signals will appear after the daily crypto signal script creates valid unpublished records."
                        onView={setDetailItem}
                        onPublish={(id) => publishMutation.mutate(id)}
                        onUnpublish={(id) => unpublishMutation.mutate(id)}
                        onCancel={handleCancel}
                        actionPending={actionPending}
                    />
                )}

                {!isLoading && !isError && activeTab === "unpublished" && (
                    <SignalsTable
                        items={unpublishedSignals}
                        emptyTitle="No unpublished signals need manual review."
                        emptyBody="Normal real signals auto-publish after passing quality gates. Dev/manual recovery records may appear here."
                        onView={setDetailItem}
                        onPublish={(id) => publishMutation.mutate(id)}
                        onUnpublish={(id) => unpublishMutation.mutate(id)}
                        onCancel={handleCancel}
                        actionPending={actionPending}
                    />
                )}

                {!isLoading && !isError && activeTab === "published" && (
                    <SignalsTable
                        items={publishedSignals}
                        emptyTitle="No published signals yet."
                        emptyBody="Published signals become visible to authenticated users in the Crypto Signals dashboard."
                        onView={setDetailItem}
                        onPublish={(id) => publishMutation.mutate(id)}
                        onUnpublish={(id) => unpublishMutation.mutate(id)}
                        onCancel={handleCancel}
                        actionPending={actionPending}
                    />
                )}

                {!isLoading && !isError && activeTab === "rejected" && (
                    <CandidatesTable items={candidates} onView={setDetailItem} />
                )}

                {!isLoading && !isError && activeTab === "performance" && (
                    <div className="grid grid-cols-1 gap-5 md:grid-cols-2 xl:grid-cols-3">
                        <MetricCard label="Total Generated Signals" value={performance.total} helper="Trading signal records only" icon={<BarChart3 className="h-5 w-5" />} />
                        <MetricCard label="Published Signals" value={performance.published} helper="Currently user-visible" icon={<Send className="h-5 w-5" />} />
                        <MetricCard label="Unpublished / Manual Review" value={performance.pending} helper="Not the normal real-signal path" icon={<Clock3 className="h-5 w-5" />} />
                        <MetricCard label="Rejected Candidates" value={performance.rejected} helper="Candidate diagnostics" icon={<ShieldX className="h-5 w-5" />} />
                        <MetricCard label="Expired Signals" value={performance.expired} helper="Expired lifecycle status" icon={<XCircle className="h-5 w-5" />} />
                        <MetricCard label="TP1 Partial Count" value={performance.tp1} helper="Partial progress, not a win" icon={<RadioTower className="h-5 w-5" />} />
                        <MetricCard label="TP2 Win Count" value={performance.tp2} helper="Counts as win" icon={<Trophy className="h-5 w-5" />} />
                        <MetricCard label="TP3 Strong Win Count" value={performance.tp3} helper="Counts as win" icon={<Trophy className="h-5 w-5" />} />
                        <MetricCard label="SL Hit Count" value={performance.sl} helper="Stop-loss outcomes" icon={<ShieldX className="h-5 w-5" />} />
                        <MetricCard label="Cancelled Count" value={performance.cancelled} helper="Admin-cancelled records" icon={<XCircle className="h-5 w-5" />} />
                        <MetricCard label="Invalidated Count" value={performance.invalidated} helper="Invalidated lifecycle records" icon={<AlertTriangle className="h-5 w-5" />} />
                    </div>
                )}

                {!isLoading && !isError && activeTab === "performance" && signals.length === 0 && candidates.length === 0 && (
                    <EmptyState title="Performance data will appear after enough completed signals." body="No performance numbers are invented or backfilled from incomplete data." />
                )}

                <DetailModal item={detailItem} onClose={() => setDetailItem(null)} />
            </div>
        </AdminLayout>
    );
}
