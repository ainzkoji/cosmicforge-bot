import { useState } from "react";
import {
    Activity,
    BarChart3,
    CheckCircle2,
    Cpu,
    Database,
    GitBranch,
    Loader2,
    Radar,
    Shield,
    Sparkles,
    Target,
    TriangleAlert,
    XCircle,
} from "lucide-react";
import {
    Bar,
    BarChart,
    CartesianGrid,
    Cell,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from "recharts";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { MetricCard } from "@/components/admin/cards/MetricCard";
import { ActionConfirmModal } from "@/components/admin/ml/ActionConfirmModal";
import { MLAlertPanel } from "@/components/admin/ml/MLAlertPanel";
import { MLKpiCard } from "@/components/admin/ml/MLKpiCard";
import { MLSectionTabs, type MLSectionTabItem } from "@/components/admin/ml/MLSectionTabs";
import { ProgressBar } from "@/components/admin/ml/ProgressBar";
import { SectionCard } from "@/components/admin/ml/SectionCard";
import { StatusBadge } from "@/components/admin/ml/StatusBadge";
import {
    useMLActivityQuery,
    useMLAlertsQuery,
    useMLControlPanelQuery,
    useMLDashboardQuery,
    useMLDriftMonitoringQuery,
    useMLFeatureCompletenessQuery,
    useMLRuntimeStatusQuery,
    useMLShadowPerformanceQuery,
    useMLValidationHistoryQuery,
    useTriggerMLActionMutation,
} from "@/hooks/useAdminML";
import type { MLActionDefinition, MLPhaseIChecklistItem } from "@/api/admin";
import {
    buildDocument1ChecklistSummary,
    buildSafeStateStatus,
    formatCompactPath as _formatCompactPath,
} from "@/utils/mlMonitoringUtils";

export type MLSectionKey = "overview" | "readiness" | "data-quality" | "activity" | "controls" | "history";

export interface MLMonitoringProps {
    section?: MLSectionKey;
    embedded?: boolean;
}

function formatPercent(value: number | null | undefined, digits: number = 1) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "--";
    }
    return `${value.toFixed(digits)}%`;
}

function formatNumber(value: number | null | undefined, digits: number = 0) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "--";
    }
    return new Intl.NumberFormat("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    }).format(value);
}

function formatDecimal(value: number | null | undefined, digits: number = 3) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "--";
    }
    return value.toFixed(digits);
}

function formatDate(value: string | null | undefined) {
    if (!value) {
        return "--";
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return value;
    }
    return parsed.toLocaleString();
}

function formatCompactPath(value: string | null | undefined) {
    if (!value) {
        return "--";
    }
    if (value.length <= 64) {
        return value;
    }
    return `${value.slice(0, 30)}...${value.slice(-26)}`;
}

function formatRange(value: Record<string, unknown> | null | undefined) {
    if (!value) {
        return "--";
    }
    const start = value.start || value.from || value.min || value.begin || value.earliest;
    const end = value.end || value.to || value.max || value.finish || value.latest;
    return `${formatDate(typeof start === "string" ? start : null)} - ${formatDate(typeof end === "string" ? end : null)}`;
}

function toneForProgress(percent: number) {
    if (percent >= 90) {
        return "success" as const;
    }
    if (percent >= 70) {
        return "warning" as const;
    }
    return "danger" as const;
}

function scoreBarColor(index: number) {
    const colors = [
        "#1d4ed8",
        "#2563eb",
        "#3b82f6",
        "#60a5fa",
        "#f59e0b",
        "#fbbf24",
        "#fb7185",
        "#f43f5e",
        "#ef4444",
        "#dc2626",
    ];
    return colors[index % colors.length];
}

function formatModeLabel(mode: string) {
    if (mode === "shadow") {
        return "Shadow Only";
    }
    return mode.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function actionMixToDistribution(
    rows: Array<{ key: string; allow_count: number; shadow_count: number; block_count: number; skip_count: number }>,
) {
    const total = rows.reduce((sum, row) => sum + row.allow_count + row.shadow_count + row.block_count + row.skip_count, 0);
    return rows.map((row) => {
        const count = row.allow_count + row.shadow_count + row.block_count + row.skip_count;
        return {
            key: row.key,
            count,
            pct: total > 0 ? (count / total) * 100 : 0,
            average_pnl: null,
        };
    });
}

function buildReadinessIssues({
    totalLinkedCompletedTrades,
    requiredTrades,
    wins,
    requiredWins,
    featureCoveragePct,
    linkageHealthy,
    labelDistributionSingleClass,
    brokenFeatureCount,
    tradesMissingCriticalFeatures,
}: {
    totalLinkedCompletedTrades: number;
    requiredTrades: number;
    wins: number;
    requiredWins: number;
    featureCoveragePct: number;
    linkageHealthy: boolean;
    labelDistributionSingleClass: boolean;
    brokenFeatureCount: number;
    tradesMissingCriticalFeatures: number;
}) {
    const issues: string[] = [];

    if (totalLinkedCompletedTrades < requiredTrades) {
        issues.push(`Linked completed trades are below the gate (${formatNumber(totalLinkedCompletedTrades)} / ${formatNumber(requiredTrades)}).`);
    }
    if (wins < requiredWins) {
        issues.push(`Wins are below the gate (${formatNumber(wins)} / ${formatNumber(requiredWins)}).`);
    }
    if (featureCoveragePct < 90) {
        issues.push(`Feature coverage is below the 90% readiness floor (${formatPercent(featureCoveragePct)}).`);
    }
    if (!linkageHealthy) {
        issues.push("Linkage health is failing the readiness gate.");
    }
    if (brokenFeatureCount > 0) {
        issues.push(`${formatNumber(brokenFeatureCount)} tracked features are currently broken.`);
    }
    if (tradesMissingCriticalFeatures > 0) {
        issues.push(`${formatNumber(tradesMissingCriticalFeatures)} linked trades are still missing critical features.`);
    }
    if (labelDistributionSingleClass) {
        issues.push("Label distribution is single-class, so training must remain blocked.");
    }

    return issues;
}

function SectionError({ message }: { message: string }) {
    return (
        <div className="admin-empty-state">
            <TriangleAlert className="h-4 w-4" style={{ color: "var(--admin-yellow)" }} />
            <span>{message}</span>
        </div>
    );
}

// ── FIX-F: Safe-state banner ──────────────────────────────────────────────────

function SafeStateBanner({
    mlEnabled,
    safeStateConfirmed,
    snapshotMissing,
    modelLoadError,
    safeStateNote,
}: {
    mlEnabled: boolean;
    safeStateConfirmed: boolean;
    snapshotMissing: boolean;
    modelLoadError: string | null;
    safeStateNote: string;
}) {
    const status = buildSafeStateStatus(mlEnabled, safeStateConfirmed, snapshotMissing);

    const borderColor =
        status.level === "safe"
            ? "var(--admin-green)"
            : status.level === "danger"
              ? "var(--admin-red)"
              : "var(--admin-yellow)";

    const iconColor = borderColor;

    return (
        <div
            className="rounded-xl p-4 flex flex-col gap-3"
            style={{
                background: "var(--admin-bg-secondary)",
                border: `1.5px solid ${borderColor}`,
            }}
        >
            <div className="flex items-start gap-3">
                {status.level === "safe" ? (
                    <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0" style={{ color: iconColor }} />
                ) : status.level === "danger" ? (
                    <XCircle className="mt-0.5 h-5 w-5 shrink-0" style={{ color: iconColor }} />
                ) : (
                    <TriangleAlert className="mt-0.5 h-5 w-5 shrink-0" style={{ color: iconColor }} />
                )}
                <div className="flex-1 min-w-0">
                    <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                        {status.title}
                    </div>
                    <div className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                        {status.message}
                    </div>
                    {snapshotMissing ? null : (
                        <div className="mt-2 text-xs" style={{ color: "var(--admin-text-muted)" }}>
                            {safeStateNote}
                        </div>
                    )}
                </div>
                <div className="shrink-0">
                    <StatusBadge
                        status={status.level === "safe" ? "healthy" : status.level === "danger" ? "blocked" : "warning"}
                        label={status.level === "safe" ? "Safe" : status.level === "danger" ? "Active" : "Unknown"}
                    />
                </div>
            </div>

            {/* Model load error alert */}
            {modelLoadError ? (
                <div
                    className="rounded-lg px-3 py-2 flex items-start gap-2 text-sm"
                    style={{ background: "var(--admin-bg-primary)", border: "1px solid var(--admin-red)" }}
                >
                    <XCircle className="mt-0.5 h-4 w-4 shrink-0" style={{ color: "var(--admin-red)" }} />
                    <div>
                        <div className="font-semibold" style={{ color: "var(--admin-red)" }}>Model Load Error</div>
                        <div className="mt-0.5 font-mono break-all" style={{ color: "var(--admin-text-secondary)" }}>
                            {modelLoadError}
                        </div>
                    </div>
                </div>
            ) : null}
        </div>
    );
}

// ── FIX-F: Document 1 readiness checklist ────────────────────────────────────

function ChecklistItemRow({ item }: { item: MLPhaseIChecklistItem }) {
    return (
        <div
            className="flex items-start gap-3 rounded-lg px-3 py-2.5"
            style={{ background: "var(--admin-bg-primary)" }}
        >
            {item.met ? (
                <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" style={{ color: "var(--admin-green)" }} />
            ) : (
                <XCircle className="mt-0.5 h-4 w-4 shrink-0" style={{ color: "var(--admin-red)" }} />
            )}
            <div className="flex-1 min-w-0">
                <div className="text-sm font-medium" style={{ color: "var(--admin-text-primary)" }}>
                    {item.label}
                </div>
                <div className="text-xs mt-0.5" style={{ color: "var(--admin-text-muted)" }}>
                    {item.detail}
                </div>
            </div>
            <StatusBadge
                status={item.met ? "healthy" : "not_ready"}
                label={item.met ? "Met" : "Pending"}
            />
        </div>
    );
}

function Document1ChecklistCard({
    metCount,
    totalCount,
    overallReady,
    overallNote,
    items,
}: {
    metCount: number;
    totalCount: number;
    overallReady: boolean;
    overallNote: string;
    items: MLPhaseIChecklistItem[];
}) {
    const summaries = buildDocument1ChecklistSummary({ checklist: items, met_count: metCount, total_count: totalCount, overall_ready: overallReady, overall_note: overallNote });

    return (
        <SectionCard
            title="Document 1 — Phase I Readiness Checklist"
            subtitle="All gates must be met before ML training can be considered. This checklist is read-only — it reflects live snapshot data and static deployment state."
        >
            {/* Header progress bar */}
            <div className="mb-4">
                <div className="flex items-center justify-between mb-2">
                    <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                        {metCount} / {totalCount} gates satisfied
                    </div>
                    <StatusBadge
                        status={overallReady ? "ready_for_training" : "not_ready"}
                        label={overallReady ? "All Gates Met" : "Gates Pending"}
                    />
                </div>
                <div className="h-2 rounded-full overflow-hidden" style={{ background: "var(--admin-bg-primary)" }}>
                    <div
                        className="h-full rounded-full transition-all"
                        style={{
                            width: `${totalCount > 0 ? (metCount / totalCount) * 100 : 0}%`,
                            background: overallReady ? "var(--admin-green)" : "var(--admin-yellow)",
                        }}
                    />
                </div>
                <div className="mt-2 text-xs" style={{ color: "var(--admin-text-muted)" }}>
                    {overallNote}
                </div>
            </div>

            {/* Category groups */}
            <div className="space-y-4">
                {summaries.map((cat) => (
                    <div key={cat.category}>
                        <div className="flex items-center gap-2 mb-2">
                            <div
                                className="text-xs font-semibold uppercase tracking-wider px-2 py-0.5 rounded"
                                style={{
                                    background: cat.allMet ? "rgba(34,197,94,0.12)" : "rgba(148,163,184,0.10)",
                                    color: cat.allMet ? "var(--admin-green)" : "var(--admin-text-muted)",
                                }}
                            >
                                {cat.category}
                            </div>
                            <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>
                                {cat.metCount}/{cat.totalCount}
                            </div>
                        </div>
                        <div className="space-y-2">
                            {cat.items.map((item) => (
                                <ChecklistItemRow key={item.id} item={item} />
                            ))}
                        </div>
                    </div>
                ))}
            </div>
        </SectionCard>
    );
}

function KeyValue({
    label,
    value,
    monospace = false,
}: {
    label: string;
    value: string;
    monospace?: boolean;
}) {
    return (
        <div className="admin-ml-detail-card">
            <div className="admin-ml-detail-card-title">{label}</div>
            <div
                className={monospace ? "break-all text-sm font-medium" : "text-sm font-medium"}
                style={{ color: "var(--admin-text-primary)" }}
            >
                {value}
            </div>
        </div>
    );
}

function ActionCard({
    action,
    busy,
    onOpen,
}: {
    action: MLActionDefinition;
    busy: boolean;
    onOpen: (action: MLActionDefinition) => void;
}) {
    const disabled = !action.allowed || !action.supported || busy;
    const buttonLabel = action.supported ? (disabled ? "Blocked" : "Confirm") : "Restricted";

    return (
        <div className="admin-card" style={{ background: "var(--admin-bg-primary)" }}>
            <div className="flex items-start justify-between gap-3">
                <div className="space-y-2">
                    <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                        {action.label}
                    </div>
                    <div className="flex flex-wrap gap-2">
                        <StatusBadge status={action.supported ? "healthy" : "unsupported"} label={action.supported ? "Supported" : "Restricted"} />
                        <StatusBadge status={action.allowed ? "healthy" : "blocked"} label={action.allowed ? "Allowed" : "Blocked"} />
                        {action.dangerous ? <StatusBadge status="warning" label="Dangerous" /> : <StatusBadge status="info" label="Supported" />}
                    </div>
                </div>
                <button
                    type="button"
                    className={`admin-btn ${action.dangerous ? "admin-btn-danger" : "admin-btn-primary"}`}
                    onClick={() => onOpen(action)}
                    disabled={disabled}
                >
                    {buttonLabel}
                </button>
            </div>

            <div className="mt-4 space-y-2 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                <div>
                    Confirmation phrase: <span className="font-semibold">{action.confirmation_phrase}</span>
                </div>
                <div>
                    Dataset: <span className="font-medium">{formatCompactPath(action.dataset_path)}</span>
                </div>
                <div>
                    Target model: <span className="font-medium">{action.target_model_version || "--"}</span>
                </div>
                {action.blocked_reason ? <div style={{ color: "var(--admin-yellow)" }}>{action.blocked_reason}</div> : null}
            </div>
        </div>
    );
}

function DistributionList({
    title,
    rows,
}: {
    title: string;
    rows: Array<{ key: string; count: number; pct: number; average_pnl?: number | null }>;
}) {
    return (
        <div className="space-y-3">
            <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                {title}
            </div>
            {rows.length ? (
                <div className="space-y-2">
                    {rows.slice(0, 6).map((row) => (
                        <div
                            key={`${title}-${row.key}`}
                            className="flex items-center justify-between rounded-lg px-3 py-3"
                            style={{ background: "var(--admin-bg-primary)" }}
                        >
                            <div>
                                <div className="font-medium capitalize" style={{ color: "var(--admin-text-primary)" }}>
                                    {row.key.toLowerCase()}
                                </div>
                                <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>
                                    {formatPercent(row.pct)} share
                                </div>
                            </div>
                            <div className="text-right text-sm">
                                <div style={{ color: "var(--admin-text-primary)" }}>{formatNumber(row.count)}</div>
                                {row.average_pnl !== undefined ? (
                                    <div style={{ color: "var(--admin-text-muted)" }}>
                                        Avg PnL {formatNumber(row.average_pnl, 2)}
                                    </div>
                                ) : null}
                            </div>
                        </div>
                    ))}
                </div>
            ) : (
                <div className="admin-empty-state">No distribution data available</div>
            )}
        </div>
    );
}

function DecisionGroupCard({
    title,
    stats,
}: {
    title: string;
    stats?: {
        count: number;
        wins: number;
        losses: number;
        breakevens: number;
        total_pnl: number;
        average_pnl: number;
    };
}) {
    return (
        <div className="admin-card" style={{ background: "var(--admin-bg-primary)" }}>
            <div className="mb-4 flex items-center justify-between">
                <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                    {title}
                </div>
                <StatusBadge
                    status={stats && stats.count > 0 ? "healthy" : "not_ready"}
                    label={stats && stats.count > 0 ? "Healthy" : "No Data"}
                />
            </div>
            <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                    <div style={{ color: "var(--admin-text-muted)" }}>Count</div>
                    <div className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{formatNumber(stats?.count)}</div>
                </div>
                <div>
                    <div style={{ color: "var(--admin-text-muted)" }}>Wins</div>
                    <div className="font-semibold" style={{ color: "var(--admin-green)" }}>{formatNumber(stats?.wins)}</div>
                </div>
                <div>
                    <div style={{ color: "var(--admin-text-muted)" }}>Losses</div>
                    <div className="font-semibold" style={{ color: "var(--admin-red)" }}>{formatNumber(stats?.losses)}</div>
                </div>
                <div>
                    <div style={{ color: "var(--admin-text-muted)" }}>Breakevens</div>
                    <div className="font-semibold" style={{ color: "var(--admin-yellow)" }}>{formatNumber(stats?.breakevens)}</div>
                </div>
                <div>
                    <div style={{ color: "var(--admin-text-muted)" }}>Total PnL</div>
                    <div className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{formatNumber(stats?.total_pnl, 2)}</div>
                </div>
                <div>
                    <div style={{ color: "var(--admin-text-muted)" }}>Avg PnL</div>
                    <div className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{formatNumber(stats?.average_pnl, 2)}</div>
                </div>
            </div>
        </div>
    );
}

const tabItems: Array<MLSectionTabItem & { key: MLSectionKey }> = [
    { key: "overview", label: "Overview", icon: <Cpu className="h-4 w-4" /> },
    { key: "readiness", label: "Readiness", icon: <Target className="h-4 w-4" /> },
    { key: "data-quality", label: "Data Quality", icon: <Database className="h-4 w-4" /> },
    { key: "activity", label: "Activity", icon: <Activity className="h-4 w-4" /> },
    { key: "controls", label: "Controls", icon: <Shield className="h-4 w-4" /> },
    { key: "history", label: "History", icon: <GitBranch className="h-4 w-4" /> },
];

export default function MLMonitoring({ section, embedded = false }: MLMonitoringProps = {}) {
    const [activeTab, setActiveTab] = useState<MLSectionKey>(section ?? "overview");
    const [activityPage, setActivityPage] = useState(1);
    const [activityWindow, setActivityWindow] = useState(30);
    const [driftWindow, setDriftWindow] = useState(30);
    const [selectedAction, setSelectedAction] = useState<MLActionDefinition | null>(null);
    const [confirmationValue, setConfirmationValue] = useState("");
    const [actionNote, setActionNote] = useState("");

    const dashboardQuery = useMLDashboardQuery();
    const featureQuery = useMLFeatureCompletenessQuery(500);
    const activityQuery = useMLActivityQuery({ days: activityWindow, page: activityPage, pageSize: 12 });
    const shadowQuery = useMLShadowPerformanceQuery(90);
    const validationQuery = useMLValidationHistoryQuery(10);
    const controlPanelQuery = useMLControlPanelQuery();
    const alertsQuery = useMLAlertsQuery();
    const driftQuery = useMLDriftMonitoringQuery(driftWindow);
    // FIX-F: runtime status (safety-critical, polls every 30 s)
    const runtimeStatusQuery = useMLRuntimeStatusQuery();
    const actionMutation = useTriggerMLActionMutation();

    const dashboard = dashboardQuery.data;
    const feature = featureQuery.data;
    const activityData = activityQuery.data;
    const shadow = shadowQuery.data;
    const validation = validationQuery.data;
    const controlPanel = controlPanelQuery.data ?? dashboard?.control_panel;
    const alerts = alertsQuery.data ?? dashboard?.alerts;
    const drift = driftQuery.data;
    // FIX-F: runtime status — always display (shows safe defaults if no snapshot)
    const runtimeStatus = runtimeStatusQuery.data;

    const featureCoveragePct = feature?.recent_completeness_pct ?? dashboard?.feature_completeness.recent_completeness_pct ?? 0;
    const brokenFeatureCount = feature?.broken_feature_count ?? dashboard?.feature_completeness.broken_feature_count ?? 0;
    const totalPages = activityData ? Math.max(Math.ceil(activityData.total_recent_rows / activityData.page_size), 1) : 1;
    const activityError = (activityQuery.error as Error | undefined)?.message;
    const busyAction = actionMutation.isPending;
    const safeActions = controlPanel?.actions.filter((action) => !action.dangerous) ?? [];
    const restrictedActions = controlPanel?.actions.filter((action) => action.dangerous) ?? [];
    const showModuleChrome = !embedded;
    const showOverviewSummary = !embedded || section === "overview";

    const openActionModal = (action: MLActionDefinition) => {
        setSelectedAction(action);
        setConfirmationValue("");
        setActionNote("");
    };

    const closeActionModal = () => {
        setSelectedAction(null);
        setConfirmationValue("");
        setActionNote("");
    };

    const handleConfirmAction = async () => {
        if (!selectedAction) {
            return;
        }
        try {
            await actionMutation.mutateAsync({
                actionKey: selectedAction.action_key,
                payload: {
                    confirmation_phrase: confirmationValue,
                    note: actionNote || undefined,
                },
            });
            closeActionModal();
        } catch {
            // Error is surfaced from the mutation object.
        }
    };

    if (dashboardQuery.isLoading && !dashboard) {
        const loadingState = (
            <div className="flex min-h-[50vh] items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin" style={{ color: "var(--admin-blue)" }} />
            </div>
        );

        return embedded ? loadingState : <AdminLayout>{loadingState}</AdminLayout>;
    }

    if (dashboardQuery.isError || !dashboard) {
        const errorState = (
            <div className="space-y-6">
                <div>
                    <h1 className="text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>
                        ML Monitoring
                    </h1>
                    <p className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                        Monitoring, readiness, and protected ML admin controls.
                    </p>
                </div>
                <div className="admin-card">
                    <div className="flex items-start gap-3">
                        <XCircle className="h-6 w-6" style={{ color: "var(--admin-red)" }} />
                        <div>
                            <div className="text-lg font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                Unable to load ML dashboard
                            </div>
                            <p className="mt-2 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                                {(dashboardQuery.error as Error | undefined)?.message || "The backend returned an error while loading the aggregated ML dashboard."}
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        );

        return embedded ? errorState : <AdminLayout>{errorState}</AdminLayout>;
    }

    const readinessIssues = buildReadinessIssues({
        totalLinkedCompletedTrades: dashboard.training_gate.total_linked_completed_trades,
        requiredTrades: dashboard.training_gate.required_trades,
        wins: dashboard.training_gate.wins,
        requiredWins: dashboard.training_gate.required_wins,
        featureCoveragePct: dashboard.training_gate.feature_coverage_pct,
        linkageHealthy: dashboard.training_gate.linkage_healthy,
        labelDistributionSingleClass: dashboard.training_gate.label_distribution_single_class,
        brokenFeatureCount,
        tradesMissingCriticalFeatures: dashboard.training_gate.trades_missing_critical_features,
    });

    const pageContent = (
        <div className="admin-ml-shell">
                {/* FIX-F: Safe-state banner — always rendered at the top so the admin
                    can immediately see whether ML is disabled.  Uses safe defaults
                    (ml_enabled=false, safe_state_confirmed=true) if the runtime-status
                    snapshot is not yet available. */}
                <div className="px-0 pb-2">
                    <SafeStateBanner
                        mlEnabled={runtimeStatus?.ml_enabled ?? false}
                        safeStateConfirmed={runtimeStatus?.safe_state_confirmed ?? true}
                        snapshotMissing={runtimeStatus?.snapshot_missing ?? true}
                        modelLoadError={runtimeStatus?.model_load_error ?? null}
                        safeStateNote={runtimeStatus?.safe_state_note ?? ""}
                    />
                </div>

                {showModuleChrome ? (
                    <>
                        <header className="admin-ml-header">
                            <div className="admin-ml-header-row">
                                <div className="admin-ml-header-copy">
                                    <div className="admin-ml-header-eyebrow">Runtime ML / Shadow ML / Offline Experiments</div>
                                    <h1 className="text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>
                                        ML Monitoring
                                    </h1>
                                    <p>
                                        Executive control surface for readiness, live scoring, linkage health, and protected ML actions.
                                    </p>
                                </div>
                                <div className="admin-ml-header-badges">
                                    <div className="admin-ml-header-badge">
                                        <span className="admin-ml-header-badge-label">ML Mode</span>
                                        <StatusBadge status={dashboard.overview.ml_mode} />
                                    </div>
                                    <div className="admin-ml-header-badge">
                                        <span className="admin-ml-header-badge-label">Training Ready</span>
                                        <StatusBadge
                                            status={dashboard.training_gate.training_ready ? "healthy" : "not_ready"}
                                            label={dashboard.training_gate.training_ready ? "Ready" : "Not Ready"}
                                        />
                                    </div>
                                    <div className="admin-ml-header-badge">
                                        <span className="admin-ml-header-badge-label">Linkage Health</span>
                                        <StatusBadge
                                            status={dashboard.linkage_health.linkage_healthy ? "healthy" : "broken"}
                                            label={dashboard.linkage_health.linkage_healthy ? "Healthy" : "Unhealthy"}
                                        />
                                    </div>
                                </div>
                            </div>
                            <div className="admin-ml-header-meta">
                                <span>Runtime ML: {formatModeLabel(dashboard.overview.ml_mode)}</span>
                                <span>Model {dashboard.overview.current_model_version || "Unassigned"}</span>
                                <span>Restart {formatDate(dashboard.overview.last_bot_restart_time)}</span>
                                <span>{formatNumber(dashboard.activity_summary.total_ml_scored_entries)} scored entries</span>
                            </div>
                        </header>

                        <div className="admin-ml-tab-shell">
                            <MLSectionTabs items={tabItems} activeKey={activeTab} onChange={(key) => setActiveTab(key as MLSectionKey)} />
                        </div>
                    </>
                ) : null}

                {showOverviewSummary ? (
                    <>
                        <section className="admin-ml-kpi-grid">
                            <MLKpiCard
                                title="ML Mode"
                                value={formatModeLabel(dashboard.overview.ml_mode)}
                                helper={dashboard.overview.ml_enabled ? "Scoring active" : "Scoring paused"}
                                icon={<Cpu className="h-5 w-5" style={{ color: "var(--admin-blue)" }} />}
                                status={dashboard.overview.ml_mode}
                            />
                            <MLKpiCard
                                title="Model Version"
                                value={dashboard.overview.current_model_version || "Unassigned"}
                                helper="Deployed reference"
                                icon={<GitBranch className="h-5 w-5" style={{ color: "var(--admin-cyan)" }} />}
                                status={dashboard.overview.current_model_version ? "healthy" : "not_ready"}
                            />
                            <MLKpiCard
                                title="Linked Trades"
                                value={formatNumber(dashboard.training_gate.total_linked_completed_trades)}
                                helper={`Gate ${formatNumber(dashboard.training_gate.required_trades)}`}
                                icon={<BarChart3 className="h-5 w-5" style={{ color: "var(--admin-green)" }} />}
                                status={dashboard.training_gate.total_linked_completed_trades >= dashboard.training_gate.required_trades ? "healthy" : "warning"}
                            />
                            <MLKpiCard
                                title="Wins"
                                value={formatNumber(dashboard.training_gate.wins)}
                                helper={`Gate ${formatNumber(dashboard.training_gate.required_wins)}`}
                                icon={<CheckCircle2 className="h-5 w-5" style={{ color: "var(--admin-green)" }} />}
                                status={dashboard.training_gate.wins >= dashboard.training_gate.required_wins ? "healthy" : "warning"}
                            />
                            <MLKpiCard
                                title="Feature Coverage"
                                value={formatPercent(featureCoveragePct)}
                                helper={`${formatNumber(brokenFeatureCount)} broken`}
                                icon={<Target className="h-5 w-5" style={{ color: "var(--admin-yellow)" }} />}
                                status={brokenFeatureCount > 0 ? "broken" : featureCoveragePct >= 90 ? "healthy" : "warning"}
                            />
                            <MLKpiCard
                                title="Linkage Health"
                                value={dashboard.linkage_health.linkage_healthy ? "Healthy" : "Warning"}
                                helper={`${formatPercent(dashboard.linkage_health.fully_linked_completed_trades_pct)} linked`}
                                icon={<Shield className="h-5 w-5" style={{ color: dashboard.linkage_health.linkage_healthy ? "var(--admin-green)" : "var(--admin-red)" }} />}
                                status={dashboard.linkage_health.linkage_healthy ? "healthy" : "broken"}
                            />
                            <MLKpiCard
                                title="Training Ready"
                                value={dashboard.training_gate.training_ready ? "Ready" : "Blocked"}
                                helper={readinessIssues.length ? readinessIssues[0] : "All gates clear"}
                                icon={<Database className="h-5 w-5" style={{ color: dashboard.training_gate.training_ready ? "var(--admin-green)" : "var(--admin-red)" }} />}
                                status={dashboard.training_gate.training_ready ? "ready_for_training" : dashboard.training_gate.status}
                            />
                        </section>

                        <MLAlertPanel alerts={alerts?.items ?? []} />
                    </>
                ) : null}
                <div className="admin-ml-workspace">
                    {activeTab === "overview" ? (
                        <div className="admin-ml-tab-panel">
                            <SectionCard
                                title="Runtime ML Scorer/Gate"
                                subtitle="Compact operational state for the active ML stack, thresholds, and deployment readiness."
                            >
                                <div className="admin-ml-split-panel">
                                    <div className="admin-ml-subgrid two-up">
                                        <div className="admin-ml-detail-card">
                                            <div className="admin-ml-detail-card-title">ML Enabled</div>
                                            <div className="flex flex-wrap items-center gap-2">
                                                <StatusBadge status={dashboard.overview.ml_enabled} label={dashboard.overview.ml_enabled ? "Healthy" : "Disabled"} />
                                                <span className="admin-ml-muted">
                                                    {dashboard.overview.ml_enabled ? "Scoring active." : "Scoring paused."}
                                                </span>
                                            </div>
                                        </div>
                                        <div className="admin-ml-detail-card">
                                            <div className="admin-ml-detail-card-title">Current ML Status</div>
                                            <div className="flex flex-wrap items-center gap-2">
                                                <StatusBadge status={dashboard.overview.current_ml_status} />
                                                <span className="admin-ml-muted">Mode {formatModeLabel(dashboard.overview.ml_mode)}</span>
                                            </div>
                                        </div>
                                        <div className="admin-ml-detail-card">
                                            <div className="admin-ml-detail-card-title">Threshold</div>
                                            <div className="text-2xl font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                {formatDecimal(dashboard.overview.current_threshold, 3)}
                                            </div>
                                            <div className="admin-ml-muted">Live threshold</div>
                                        </div>
                                        <div className="admin-ml-detail-card">
                                            <div className="admin-ml-detail-card-title">Hard Block Floor</div>
                                            <div className="text-2xl font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                {formatDecimal(dashboard.overview.current_hard_block_floor, 3)}
                                            </div>
                                            <div className="admin-ml-muted">Hard block floor</div>
                                        </div>
                                        <KeyValue label="Current Model Version" value={dashboard.overview.current_model_version || "Unassigned"} />
                                        <KeyValue label="Last Bot Restart" value={formatDate(dashboard.overview.last_bot_restart_time)} />
                                    </div>

                                    <div className="admin-ml-subgrid">
                                        <KeyValue label="Model Artifact Path" value={formatCompactPath(dashboard.overview.model_artifact_path)} monospace />
                                        <KeyValue label="Encoder Path" value={formatCompactPath(dashboard.overview.encoder_path)} monospace />
                                        <KeyValue label="Metadata Path" value={formatCompactPath(dashboard.overview.metadata_path)} monospace />
                                        <KeyValue label="Last Model Load Time" value={formatDate(dashboard.overview.last_model_load_time)} />
                                    </div>
                                </div>
                            </SectionCard>

                            <SectionCard
                                title="Shadow ML and Offline Experiment Signals"
                                subtitle="Shadow activity, offline validation history, and dataset readiness are shown separately from runtime execution."
                            >
                                <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                                    <MetricCard
                                        title="Current Status"
                                        value={dashboard.overview.current_ml_status.replace(/_/g, " ")}
                                        icon={<Cpu className="h-5 w-5" style={{ color: "var(--admin-blue)" }} />}
                                    />
                                    <MetricCard
                                        title="Training Gate"
                                        value={dashboard.training_gate.training_ready ? "Ready" : "Blocked"}
                                        icon={<Target className="h-5 w-5" style={{ color: dashboard.training_gate.training_ready ? "var(--admin-green)" : "var(--admin-red)" }} />}
                                    />
                                    <MetricCard
                                        title="Latest Validation"
                                        value={dashboard.validation_history.latest_model?.verdict || "No History"}
                                        icon={<GitBranch className="h-5 w-5" style={{ color: "var(--admin-cyan)" }} />}
                                    />
                                    <MetricCard
                                        title="Offline Experiments"
                                        value={formatNumber(dashboard.validation_history.total_models)}
                                        icon={<BarChart3 className="h-5 w-5" style={{ color: "var(--admin-yellow)" }} />}
                                    />
                                </div>
                            </SectionCard>
                        </div>
                    ) : null}

                    {activeTab === "readiness" ? (
                        <div className="admin-ml-tab-panel">
                            <SectionCard
                                title="Training Readiness Gate"
                                subtitle="This panel remains authoritative for whether training can proceed."
                            >
                                <div className="admin-ml-split-panel">
                                    <div className="space-y-4">
                                        <ProgressBar
                                            label="Trades Progress"
                                            value={dashboard.training_gate.total_linked_completed_trades}
                                            max={dashboard.training_gate.required_trades}
                                            tone={toneForProgress((dashboard.training_gate.total_linked_completed_trades / Math.max(dashboard.training_gate.required_trades, 1)) * 100)}
                                            helper={`${formatNumber(dashboard.training_gate.total_linked_completed_trades)} / ${formatNumber(dashboard.training_gate.required_trades)} linked completed trades`}
                                        />
                                        <ProgressBar
                                            label="Wins Progress"
                                            value={dashboard.training_gate.wins}
                                            max={dashboard.training_gate.required_wins}
                                            tone={toneForProgress((dashboard.training_gate.wins / Math.max(dashboard.training_gate.required_wins, 1)) * 100)}
                                            helper={`${formatNumber(dashboard.training_gate.wins)} / ${formatNumber(dashboard.training_gate.required_wins)} wins`}
                                        />
                                        <ProgressBar
                                            label="Feature Coverage"
                                            value={dashboard.training_gate.feature_coverage_pct}
                                            max={100}
                                            tone={toneForProgress(dashboard.training_gate.feature_coverage_pct)}
                                            helper={`${formatPercent(dashboard.training_gate.feature_coverage_pct)} across linked completed trades`}
                                        />
                                    </div>

                                    <div className="space-y-4">
                                        <div className="admin-ml-subgrid two-up">
                                            <div className="admin-ml-detail-card">
                                                <div className="admin-ml-detail-card-title">Gate Status</div>
                                                <div className="flex flex-wrap gap-2">
                                                    <StatusBadge status={dashboard.training_gate.status} />
                                                    <StatusBadge
                                                        status={dashboard.training_gate.training_ready ? "healthy" : "blocked"}
                                                        label={dashboard.training_gate.training_ready ? "Training Ready" : "Blocked"}
                                                    />
                                                </div>
                                            </div>
                                            <div className="admin-ml-detail-card">
                                                <div className="admin-ml-detail-card-title">Current Win Rate</div>
                                                <div className="text-2xl font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                    {formatPercent(dashboard.training_gate.current_win_rate)}
                                                </div>
                                            </div>
                                            <KeyValue label="Losses / Breakevens" value={`${dashboard.training_gate.losses} / ${dashboard.training_gate.breakeven_trades}`} />
                                            <KeyValue label="Excluded Open Positions" value={formatNumber(dashboard.training_gate.excluded_open_positions)} />
                                            <KeyValue label="Full Feature Coverage" value={formatNumber(dashboard.training_gate.trades_with_full_feature_coverage)} />
                                            <KeyValue label="Missing Critical Features" value={formatNumber(dashboard.training_gate.trades_missing_critical_features)} />
                                            <div className="admin-ml-detail-card">
                                                <div className="admin-ml-detail-card-title">Linkage</div>
                                                <StatusBadge
                                                    status={dashboard.training_gate.linkage_healthy ? "healthy" : "blocked"}
                                                    label={dashboard.training_gate.linkage_healthy ? "Healthy" : "Unhealthy"}
                                                />
                                            </div>
                                            <div className="admin-ml-detail-card">
                                                <div className="admin-ml-detail-card-title">Labels</div>
                                                <StatusBadge
                                                    status={dashboard.training_gate.label_distribution_single_class ? "rejected" : "healthy"}
                                                    label={dashboard.training_gate.label_distribution_single_class ? "Single-Class" : "Healthy"}
                                                />
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </SectionCard>

                            <SectionCard
                                title="Blocking Reasons"
                                subtitle="Explicit gate failures are listed here so operators can see why training is blocked."
                            >
                                {readinessIssues.length ? (
                                    <div className="admin-ml-list">
                                        {readinessIssues.map((issue) => (
                                            <div key={issue} className="admin-ml-list-item">
                                                <div className="flex items-start gap-3">
                                                    <TriangleAlert className="mt-0.5 h-4 w-4" style={{ color: "var(--admin-yellow)" }} />
                                                    <span className="admin-ml-list-item-label">{issue}</span>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="admin-empty-state">
                                        <CheckCircle2 className="h-4 w-4" style={{ color: "var(--admin-green)" }} />
                                        <span>All current readiness gates are satisfied.</span>
                                    </div>
                                )}
                            </SectionCard>

                            {/* FIX-F: Document 1 Phase I readiness checklist */}
                            {runtimeStatus?.phase_i_readiness ? (
                                <Document1ChecklistCard
                                    metCount={runtimeStatus.phase_i_readiness.met_count}
                                    totalCount={runtimeStatus.phase_i_readiness.total_count}
                                    overallReady={runtimeStatus.phase_i_readiness.overall_ready}
                                    overallNote={runtimeStatus.phase_i_readiness.overall_note}
                                    items={runtimeStatus.phase_i_readiness.checklist}
                                />
                            ) : null}
                        </div>
                    ) : null}

                    {activeTab === "data-quality" ? (
                        <div className="admin-ml-tab-panel">
                            <SectionCard
                                title="Data Quality Summary"
                                subtitle="High-level data pipeline health before drilling into feature and linkage detail."
                            >
                                <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                                    <MetricCard
                                        title="Recent Completeness"
                                        value={formatPercent(featureCoveragePct)}
                                        icon={<Target className="h-5 w-5" style={{ color: "var(--admin-yellow)" }} />}
                                    />
                                    <MetricCard
                                        title="Broken Features"
                                        value={formatNumber(brokenFeatureCount)}
                                        icon={<TriangleAlert className="h-5 w-5" style={{ color: brokenFeatureCount > 0 ? "var(--admin-red)" : "var(--admin-green)" }} />}
                                    />
                                    <MetricCard
                                        title="Linked Trades"
                                        value={formatPercent(dashboard.linkage_health.fully_linked_completed_trades_pct)}
                                        icon={<Shield className="h-5 w-5" style={{ color: dashboard.linkage_health.linkage_healthy ? "var(--admin-green)" : "var(--admin-red)" }} />}
                                    />
                                    <MetricCard
                                        title="Usable Dataset Rows"
                                        value={formatNumber(dashboard.dataset_builder_status.fully_usable_rows)}
                                        icon={<Database className="h-5 w-5" style={{ color: "var(--admin-blue)" }} />}
                                    />
                                </div>
                            </SectionCard>

                            <SectionCard
                                title="Feature Completeness Monitor"
                                subtitle="Recent and lifetime completeness across the tracked v2 ML feature schema."
                            >
                            {featureQuery.isLoading && !feature ? (
                                <div className="admin-empty-state">
                                    <Loader2 className="h-4 w-4 animate-spin" style={{ color: "var(--admin-blue)" }} />
                                    <span>Loading feature completeness...</span>
                                </div>
                            ) : featureQuery.isError || !feature ? (
                                <SectionError message={(featureQuery.error as Error | undefined)?.message || "Feature completeness could not be loaded."} />
                            ) : (
                                <>
                                    <div className="grid gap-4 md:grid-cols-3">
                                        <KeyValue label="Recent Window" value={`${feature.recent_window_size} linked trades`} />
                                        <KeyValue label="Recent Completeness" value={formatPercent(feature.recent_completeness_pct)} />
                                        <KeyValue label="Lifetime Completeness" value={formatPercent(feature.lifetime_completeness_pct)} />
                                    </div>
                                    <div className="overflow-x-auto">
                                        <table className="admin-table">
                                            <thead>
                                                <tr>
                                                    <th>Feature</th>
                                                    <th>Recent Completeness</th>
                                                    <th>Lifetime Completeness</th>
                                                    <th>Last Seen Populated</th>
                                                    <th>Status</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {feature.features.map((item) => (
                                                    <tr key={item.feature_name}>
                                                        <td className="font-medium">{item.feature_name}</td>
                                                        <td>{formatPercent(100 - item.null_pct_recent)}</td>
                                                        <td>{formatPercent(100 - item.null_pct_lifetime)}</td>
                                                        <td>{formatDate(item.last_seen_populated_at)}</td>
                                                        <td><StatusBadge status={item.status} /></td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </>
                            )}
                        </SectionCard>

                        <div className="admin-ml-split-panel">
                            <SectionCard
                                title="Linkage Health"
                                subtitle="Coverage across run_id, cycle_id, and position_id plus linked completion quality."
                            >
                                <div className="admin-ml-subgrid three-up">
                                    <KeyValue
                                        label="run_id Coverage"
                                        value={formatPercent((dashboard.linkage_health.fills_with_non_null_run_id / Math.max(dashboard.linkage_health.total_post_fix_fills, 1)) * 100)}
                                    />
                                    <KeyValue
                                        label="cycle_id Coverage"
                                        value={formatPercent((dashboard.linkage_health.fills_with_non_null_cycle_id / Math.max(dashboard.linkage_health.total_post_fix_fills, 1)) * 100)}
                                    />
                                    <KeyValue
                                        label="position_id Coverage"
                                        value={formatPercent((dashboard.linkage_health.fills_with_non_null_position_id / Math.max(dashboard.linkage_health.total_post_fix_fills, 1)) * 100)}
                                    />
                                    <KeyValue label="Fully Linked Trades" value={formatNumber(dashboard.linkage_health.fully_linked_completed_trades)} />
                                    <KeyValue label="Orphan OPEN Fills" value={formatNumber(dashboard.linkage_health.orphan_open_fills)} />
                                    <KeyValue label="Unmatched CLOSE Fills" value={formatNumber(dashboard.linkage_health.unmatched_close_fills)} />
                                </div>
                                <div className="mt-4 flex items-center gap-2">
                                    <StatusBadge status={dashboard.linkage_health.linkage_healthy ? "healthy" : "broken"} />
                                    <span className="admin-ml-muted">
                                        Post-fix fill count {formatNumber(dashboard.linkage_health.total_post_fix_fills)}
                                    </span>
                                </div>
                            </SectionCard>

                            <SectionCard
                                title="Dataset Readiness"
                                subtitle="Current dataset readiness, label balance, and dropped-row signal."
                            >
                                <div className="admin-ml-subgrid two-up">
                                    <KeyValue label="Source Date Range" value={formatRange(dashboard.dataset_builder_status.dataset_source_date_range)} />
                                    <KeyValue label="Last Dataset Build" value={formatDate(dashboard.dataset_builder_status.last_dataset_build_time)} />
                                    <KeyValue label="Last Dataset Path" value={formatCompactPath(dashboard.dataset_builder_status.last_dataset_path)} monospace />
                                    <KeyValue label="Linked Trade Count" value={formatNumber(dashboard.dataset_builder_status.linked_trade_count)} />
                                </div>

                                <div className="mt-4 grid gap-4 md:grid-cols-2">
                                    <div className="admin-ml-detail-card">
                                        <div className="mb-3 flex items-center justify-between">
                                            <div className="admin-ml-detail-card-title" style={{ marginBottom: 0 }}>
                                                Label Distribution
                                            </div>
                                            <StatusBadge
                                                status={dashboard.dataset_builder_status.label_distribution.single_class ? "rejected" : "healthy"}
                                                label={dashboard.dataset_builder_status.label_distribution.single_class ? "Single-Class" : "Healthy"}
                                            />
                                        </div>
                                        <div className="grid grid-cols-3 gap-3 text-sm">
                                            <div>
                                                <div style={{ color: "var(--admin-text-muted)" }}>Wins</div>
                                                <div className="font-semibold" style={{ color: "var(--admin-green)" }}>
                                                    {dashboard.dataset_builder_status.label_distribution.wins}
                                                </div>
                                            </div>
                                            <div>
                                                <div style={{ color: "var(--admin-text-muted)" }}>Losses</div>
                                                <div className="font-semibold" style={{ color: "var(--admin-red)" }}>
                                                    {dashboard.dataset_builder_status.label_distribution.losses}
                                                </div>
                                            </div>
                                            <div>
                                                <div style={{ color: "var(--admin-text-muted)" }}>Breakevens</div>
                                                <div className="font-semibold" style={{ color: "var(--admin-yellow)" }}>
                                                    {dashboard.dataset_builder_status.label_distribution.breakevens}
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="admin-ml-detail-card">
                                        <div className="mb-3 flex items-center justify-between">
                                            <div className="admin-ml-detail-card-title" style={{ marginBottom: 0 }}>
                                                Dropped Row Reasons
                                            </div>
                                            <StatusBadge status={dashboard.dataset_builder_status.feature_completeness_status} />
                                        </div>
                                        {dashboard.dataset_builder_status.dropped_row_reasons.length ? (
                                            <div className="space-y-2">
                                                {dashboard.dataset_builder_status.dropped_row_reasons.slice(0, 8).map((reason) => (
                                                    <div key={reason.reason} className="flex items-center justify-between text-sm">
                                                        <span style={{ color: "var(--admin-text-secondary)" }}>{reason.reason}</span>
                                                        <span className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{reason.count}</span>
                                                    </div>
                                                ))}
                                            </div>
                                        ) : (
                                            <div className="admin-empty-state">No dropped-row reasons recorded.</div>
                                        )}
                                    </div>
                                </div>

                                <div className="mt-4 admin-ml-table-note">{dashboard.dataset_builder_status.source_note}</div>
                            </SectionCard>
                        </div>
                    </div>
                ) : null}

                    {activeTab === "activity" ? (
                        <div className="admin-ml-tab-panel">
                        <SectionCard
                            title="Runtime / Shadow ML Activity"
                            subtitle="Recent scoring behavior, action mix, and paginated decision-trace rows."
                            action={
                                <div className="admin-ml-window-switch">
                                    {[7, 30, 90].map((window) => (
                                        <button
                                            key={window}
                                            type="button"
                                            className={`admin-btn ${activityWindow === window ? "admin-btn-primary" : "admin-btn-secondary"}`}
                                            onClick={() => {
                                                setActivityPage(1);
                                                setActivityWindow(window);
                                            }}
                                        >
                                            {window}d
                                        </button>
                                    ))}
                                </div>
                            }
                        >
                            {activityQuery.isLoading && !activityData ? (
                                <div className="admin-empty-state">
                                    <Loader2 className="h-4 w-4 animate-spin" style={{ color: "var(--admin-blue)" }} />
                                    <span>Loading activity...</span>
                                </div>
                            ) : activityQuery.isError || !activityData ? (
                                <SectionError message={activityError || "ML activity could not be loaded."} />
                            ) : (
                                <>
                                    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
                                        <MetricCard
                                            title="Scored Entries"
                                            value={formatNumber(activityData.total_ml_scored_entries)}
                                            icon={<Activity className="h-5 w-5" style={{ color: "var(--admin-blue)" }} />}
                                        />
                                        <MetricCard
                                            title="Allow / Shadow"
                                            value={`${formatNumber(activityData.allow_count)} / ${formatNumber(activityData.shadow_count)}`}
                                            icon={<Shield className="h-5 w-5" style={{ color: "var(--admin-green)" }} />}
                                        />
                                        <MetricCard
                                            title="Block / Skip"
                                            value={`${formatNumber(activityData.block_count)} / ${formatNumber(activityData.skip_count)}`}
                                            icon={<TriangleAlert className="h-5 w-5" style={{ color: "var(--admin-yellow)" }} />}
                                        />
                                        <MetricCard
                                            title="Average Score"
                                            value={formatDecimal(activityData.average_ml_score, 3)}
                                            icon={<Sparkles className="h-5 w-5" style={{ color: "var(--admin-cyan)" }} />}
                                        />
                                        <MetricCard
                                            title="Threshold / Floor"
                                            value={`${formatDecimal(activityData.current_threshold, 3)} / ${formatDecimal(activityData.current_hard_floor, 3)}`}
                                            icon={<Target className="h-5 w-5" style={{ color: "var(--admin-yellow)" }} />}
                                        />
                                    </div>

                                    <div className="admin-ml-split-panel">
                                        <div className="admin-card" style={{ background: "var(--admin-bg-primary)" }}>
                                            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                                                <div>
                                                    <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                        Score Distribution
                                                    </div>
                                                    <div className="admin-ml-muted">
                                                        Balanced view of current ML scoring and action volume.
                                                    </div>
                                                </div>
                                                <div className="flex gap-2 text-xs">
                                                    <span className="admin-badge admin-badge-success">Allow {activityData.allow_count}</span>
                                                    <span className="admin-badge admin-badge-info">Shadow {activityData.shadow_count}</span>
                                                    <span className="admin-badge admin-badge-danger">Block {activityData.block_count}</span>
                                                </div>
                                            </div>
                                            <div className="h-72">
                                                <ResponsiveContainer width="100%" height="100%">
                                                    <BarChart data={activityData.score_distribution}>
                                                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.16)" />
                                                        <XAxis dataKey="bucket" stroke="var(--admin-text-muted)" fontSize={12} />
                                                        <YAxis stroke="var(--admin-text-muted)" fontSize={12} />
                                                        <Tooltip />
                                                        <Bar dataKey="count" radius={[8, 8, 0, 0]}>
                                                            {activityData.score_distribution.map((item, index) => (
                                                                <Cell key={`${item.bucket}-${item.count}`} fill={scoreBarColor(index)} />
                                                            ))}
                                                        </Bar>
                                                    </BarChart>
                                                </ResponsiveContainer>
                                            </div>
                                        </div>

                                        <div className="space-y-4">
                                            <DistributionList title="Per Symbol Actions" rows={actionMixToDistribution(activityData.per_symbol_actions)} />
                                            <DistributionList title="Per Regime Actions" rows={actionMixToDistribution(activityData.per_regime_actions)} />
                                            <DistributionList title="Per Session Actions" rows={actionMixToDistribution(activityData.per_session_actions)} />
                                        </div>
                                    </div>

                                    <div className="overflow-x-auto">
                                        <table className="admin-table">
                                            <thead>
                                                <tr>
                                                    <th>Timestamp</th>
                                                    <th>Symbol</th>
                                                    <th>Side</th>
                                                    <th>Score</th>
                                                    <th>Action</th>
                                                    <th>Model</th>
                                                    <th>Threshold</th>
                                                    <th>Regime</th>
                                                    <th>Session</th>
                                                    <th>Linkage</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {activityData.recent_activity_rows.length ? (
                                                    activityData.recent_activity_rows.map((row, index) => (
                                                        <tr key={`${row.timestamp}-${row.symbol}-${index}`}>
                                                            <td>{formatDate(row.timestamp)}</td>
                                                            <td>{row.symbol || "--"}</td>
                                                            <td>{row.side || "--"}</td>
                                                            <td>{formatDecimal(row.ml_score, 3)}</td>
                                                            <td><StatusBadge status={row.ml_action?.toLowerCase() || "not_ready"} label={row.ml_action || "N/A"} /></td>
                                                            <td>{row.ml_model_version || "--"}</td>
                                                            <td>{formatDecimal(row.threshold, 3)}</td>
                                                            <td>{row.regime || "--"}</td>
                                                            <td className="capitalize">{row.session || "--"}</td>
                                                            <td><StatusBadge status={/fully_linked/i.test(row.linkage_status) ? "healthy" : "warning"} label={row.linkage_status} /></td>
                                                        </tr>
                                                    ))
                                                ) : (
                                                    <tr>
                                                        <td colSpan={10} className="py-8 text-center" style={{ color: "var(--admin-text-muted)" }}>
                                                            No recent ML activity rows found for this window.
                                                        </td>
                                                    </tr>
                                                )}
                                            </tbody>
                                        </table>
                                    </div>

                                    <div className="flex flex-wrap items-center justify-between gap-3">
                                        <div className="admin-ml-table-note">
                                            Page {activityData.page} of {totalPages} / {formatNumber(activityData.total_recent_rows)} recent rows
                                        </div>
                                        <div className="flex gap-3">
                                            <button
                                                type="button"
                                                className="admin-btn admin-btn-secondary"
                                                onClick={() => setActivityPage((page) => Math.max(page - 1, 1))}
                                                disabled={activityData.page <= 1}
                                            >
                                                Previous
                                            </button>
                                            <button
                                                type="button"
                                                className="admin-btn admin-btn-secondary"
                                                onClick={() => setActivityPage((page) => Math.min(page + 1, totalPages))}
                                                disabled={activityData.page >= totalPages}
                                            >
                                                Next
                                            </button>
                                        </div>
                                    </div>
                                </>
                            )}
                        </SectionCard>

                        <SectionCard
                            title="Drift and Regime Monitoring"
                            subtitle="Compare recent live conditions against the retained historical ML baseline."
                            action={
                                <div className="admin-ml-window-switch">
                                    {[30, 60, 90].map((window) => (
                                        <button
                                            key={window}
                                            type="button"
                                            className={`admin-btn ${driftWindow === window ? "admin-btn-primary" : "admin-btn-secondary"}`}
                                            onClick={() => setDriftWindow(window)}
                                        >
                                            {window}d
                                        </button>
                                    ))}
                                </div>
                            }
                        >
                            {driftQuery.isLoading && !drift ? (
                                <div className="admin-empty-state">
                                    <Loader2 className="h-4 w-4 animate-spin" style={{ color: "var(--admin-blue)" }} />
                                    <span>Loading drift metrics...</span>
                                </div>
                            ) : driftQuery.isError || !drift ? (
                                <SectionError message={(driftQuery.error as Error | undefined)?.message || "Drift monitoring could not be loaded."} />
                            ) : (
                                <>
                                    <div className="grid gap-4 md:grid-cols-3">
                                        <MetricCard
                                            title="Live Win Rate"
                                            value={formatPercent(drift.live_win_rate)}
                                            icon={<Activity className="h-5 w-5" style={{ color: "var(--admin-blue)" }} />}
                                        />
                                        <MetricCard
                                            title="Historical Win Rate"
                                            value={formatPercent(drift.historical_win_rate)}
                                            icon={<Radar className="h-5 w-5" style={{ color: "var(--admin-cyan)" }} />}
                                        />
                                        <MetricCard
                                            title="Win Rate Delta"
                                            value={formatPercent(drift.win_rate_delta)}
                                            icon={<Sparkles className="h-5 w-5" style={{ color: drift.win_rate_delta !== null && drift.win_rate_delta < 0 ? "var(--admin-red)" : "var(--admin-green)" }} />}
                                        />
                                    </div>

                                    <div className="admin-ml-split-panel">
                                        <div className="admin-card" style={{ background: "var(--admin-bg-primary)" }}>
                                            <div className="mb-4 text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                Live vs Historical Score Distribution
                                            </div>
                                            <div className="h-72">
                                                <ResponsiveContainer width="100%" height="100%">
                                                    <BarChart
                                                        data={drift.live_score_distribution.map((bucket, index) => ({
                                                            bucket: bucket.bucket,
                                                            live: bucket.count,
                                                            historical: drift.training_score_distribution[index]?.count ?? 0,
                                                        }))}
                                                    >
                                                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.16)" />
                                                        <XAxis dataKey="bucket" stroke="var(--admin-text-muted)" fontSize={12} />
                                                        <YAxis stroke="var(--admin-text-muted)" fontSize={12} />
                                                        <Tooltip />
                                                        <Bar dataKey="live" fill="#2563eb" radius={[6, 6, 0, 0]} />
                                                        <Bar dataKey="historical" fill="#f59e0b" radius={[6, 6, 0, 0]} />
                                                    </BarChart>
                                                </ResponsiveContainer>
                                            </div>
                                            <div className="mt-4 admin-ml-table-note">{drift.source_note}</div>
                                        </div>

                                        <div className="space-y-4">
                                            <DistributionList title="Symbol Distribution" rows={drift.symbol_distribution} />
                                            <DistributionList title="Regime Distribution" rows={drift.regime_distribution} />
                                            <DistributionList title="Session Distribution" rows={drift.session_distribution} />
                                        </div>
                                    </div>

                                    <div className="grid gap-6 xl:grid-cols-3">
                                        <div className="admin-card" style={{ background: "var(--admin-bg-primary)" }}>
                                            <div className="mb-3 text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                Average PnL by Regime
                                            </div>
                                            <div className="space-y-2">
                                                {drift.average_pnl_by_regime.length ? drift.average_pnl_by_regime.map((item) => (
                                                    <div key={`regime-${item.key}`} className="flex items-center justify-between text-sm">
                                                        <span style={{ color: "var(--admin-text-secondary)" }}>{item.key}</span>
                                                        <span className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{formatNumber(item.average_pnl, 2)}</span>
                                                    </div>
                                                )) : <div className="admin-empty-state">No regime PnL data</div>}
                                            </div>
                                        </div>
                                        <div className="admin-card" style={{ background: "var(--admin-bg-primary)" }}>
                                            <div className="mb-3 text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                Average PnL by Symbol
                                            </div>
                                            <div className="space-y-2">
                                                {drift.average_pnl_by_symbol.length ? drift.average_pnl_by_symbol.map((item) => (
                                                    <div key={`symbol-${item.key}`} className="flex items-center justify-between text-sm">
                                                        <span style={{ color: "var(--admin-text-secondary)" }}>{item.key}</span>
                                                        <span className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>{formatNumber(item.average_pnl, 2)}</span>
                                                    </div>
                                                )) : <div className="admin-empty-state">No symbol PnL data</div>}
                                            </div>
                                        </div>
                                        <div className="admin-card" style={{ background: "var(--admin-bg-primary)" }}>
                                            <div className="mb-3 text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                Average PnL by Score Band
                                            </div>
                                            <div className="space-y-2">
                                                {drift.average_pnl_by_score_band.map((item) => (
                                                    <div key={item.bucket} className="flex items-center justify-between text-sm">
                                                        <span style={{ color: "var(--admin-text-secondary)" }}>{item.bucket}</span>
                                                        <span className="font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                            {formatNumber(item.average_pnl, 2)} ({formatNumber(item.count)})
                                                        </span>
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    </div>
                                </>
                            )}
                        </SectionCard>

                        <SectionCard
                            title="Shadow Evaluation"
                            subtitle="Outcome comparison across ALLOW, SHADOW, and BLOCK groups using linked completed trades with ML attribution."
                        >
                            {shadowQuery.isLoading && !shadow ? (
                                <div className="admin-empty-state">
                                    <Loader2 className="h-4 w-4 animate-spin" style={{ color: "var(--admin-blue)" }} />
                                    <span>Loading shadow evaluation...</span>
                                </div>
                            ) : shadowQuery.isError || !shadow ? (
                                <SectionError message={(shadowQuery.error as Error | undefined)?.message || "Shadow performance could not be loaded."} />
                            ) : (
                                <>
                                    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                                        <KeyValue label="Attributed Trades" value={formatNumber(shadow.total_linked_completed_trades_with_ml_attribution)} />
                                        <KeyValue label="Good Allows / Bad Allows" value={`${shadow.good_allows} / ${shadow.bad_allows}`} />
                                        <KeyValue label="Good Blocks / Bad Blocks" value={`${shadow.good_blocks} / ${shadow.bad_blocks}`} />
                                        <KeyValue label="Classification Logic" value={shadow.classification_logic} />
                                    </div>
                                    <div className="grid gap-4 xl:grid-cols-3">
                                        <DecisionGroupCard title="ALLOW" stats={shadow.decision_groups.ALLOW} />
                                        <DecisionGroupCard title="SHADOW" stats={shadow.decision_groups.SHADOW} />
                                        <DecisionGroupCard title="BLOCK" stats={shadow.decision_groups.BLOCK} />
                                    </div>
                                </>
                            )}
                        </SectionCard>
                    </div>
                ) : null}

                    {activeTab === "controls" ? (
                        <div className="admin-ml-tab-panel">
                        <SectionCard
                            title="Training Control Panel"
                            subtitle="Guarded operational actions stay confirmation-gated and fail closed when readiness or support conditions are not satisfied."
                        >
                            {!controlPanel ? (
                                <div className="admin-empty-state">
                                    <Loader2 className="h-4 w-4 animate-spin" style={{ color: "var(--admin-blue)" }} />
                                    <span>Loading control panel...</span>
                                </div>
                            ) : (
                                <>
                                    <div className="grid gap-4 lg:grid-cols-4">
                                        <div className="admin-ml-detail-card">
                                            <div className="admin-ml-detail-card-title">Readiness</div>
                                            <div className="flex flex-wrap gap-2">
                                                <StatusBadge status={controlPanel.readiness_status} />
                                                <StatusBadge
                                                    status={controlPanel.training_allowed_right_now ? "healthy" : "blocked"}
                                                    label={controlPanel.training_allowed_right_now ? "Training Allowed" : "Training Blocked"}
                                                />
                                            </div>
                                        </div>
                                        <KeyValue label="Current Dataset Path" value={formatCompactPath(controlPanel.current_dataset_path)} monospace />
                                        <KeyValue label="Target Output Model" value={controlPanel.target_output_model_version} />
                                        <div className="admin-ml-detail-card">
                                            <div className="admin-ml-detail-card-title">Last Training Run</div>
                                            <div className="flex flex-wrap gap-2">
                                                <StatusBadge status={controlPanel.last_training_run_status || "not_ready"} />
                                                <span className="admin-ml-muted">
                                                    Dataset {controlPanel.last_dataset_rebuild_status || "none"} / Validation {controlPanel.last_validation_run_status || "none"}
                                                </span>
                                            </div>
                                        </div>
                                    </div>

                                    <div>
                                        <div className="mb-3 flex items-center justify-between gap-3">
                                            <div>
                                                <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                    Safe and Supported Actions
                                                </div>
                                                <div className="admin-ml-muted">Use this group for dataset, training, and validation actions that the backend currently supports.</div>
                                            </div>
                                        </div>
                                        {safeActions.length ? (
                                            <div className="grid gap-4 xl:grid-cols-2">
                                                {safeActions.map((action) => (
                                                    <ActionCard key={action.action_key} action={action} busy={busyAction} onOpen={openActionModal} />
                                                ))}
                                            </div>
                                        ) : (
                                            <div className="admin-empty-state">No supported actions are currently available.</div>
                                        )}
                                    </div>

                                    <div className="admin-ml-restricted-zone">
                                        <div className="admin-ml-restricted-zone-header">
                                            <div className="admin-ml-restricted-zone-copy">
                                                <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                    Restricted / Dangerous Actions
                                                </div>
                                                <p>
                                                    Deployment, rollback, and disable actions are intentionally isolated here so they never compete visually with normal operational workflows.
                                                </p>
                                            </div>
                                            <StatusBadge status="warning" label="Restricted" />
                                        </div>

                                        {restrictedActions.length ? (
                                            <div className="grid gap-4 xl:grid-cols-2">
                                                {restrictedActions.map((action) => (
                                                    <ActionCard key={action.action_key} action={action} busy={busyAction} onOpen={openActionModal} />
                                                ))}
                                            </div>
                                        ) : (
                                            <div className="admin-empty-state">No restricted actions are defined.</div>
                                        )}
                                    </div>

                                    <div className="admin-ml-split-panel">
                                        <div className="space-y-3">
                                            <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                Last training run logs
                                            </div>
                                            {controlPanel.last_training_run_logs.length ? (
                                                <pre
                                                    className="overflow-x-auto rounded-xl p-4 text-xs"
                                                    style={{
                                                        background: "var(--admin-bg-primary)",
                                                        color: "var(--admin-text-primary)",
                                                        border: "1px solid var(--admin-border-color)",
                                                    }}
                                                >
                                                    {controlPanel.last_training_run_logs.join("\n")}
                                                </pre>
                                            ) : (
                                                <div className="admin-empty-state">No training logs have been retained yet.</div>
                                            )}
                                        </div>

                                        <div className="space-y-3">
                                            <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                                                Recent action history
                                            </div>
                                            <div className="overflow-x-auto">
                                                <table className="admin-table">
                                                    <thead>
                                                        <tr>
                                                            <th>Action</th>
                                                            <th>Status</th>
                                                            <th>Requested</th>
                                                            <th>Operator</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {controlPanel.recent_action_runs.length ? (
                                                            controlPanel.recent_action_runs.map((run) => (
                                                                <tr key={run.id}>
                                                                    <td>
                                                                        <div className="font-medium">{run.action_key.replace(/_/g, " ")}</div>
                                                                        <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>
                                                                            {run.reason || run.note || "Protected admin action"}
                                                                        </div>
                                                                    </td>
                                                                    <td><StatusBadge status={run.status} /></td>
                                                                    <td>{formatDate(run.created_at)}</td>
                                                                    <td>{run.requested_by_email || "--"}</td>
                                                                </tr>
                                                            ))
                                                        ) : (
                                                            <tr>
                                                                <td colSpan={4} className="py-8 text-center" style={{ color: "var(--admin-text-muted)" }}>
                                                                    No action history recorded yet.
                                                                </td>
                                                            </tr>
                                                        )}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>
                                </>
                            )}
                        </SectionCard>
                    </div>
                ) : null}

                    {activeTab === "history" ? (
                        <div className="admin-ml-tab-panel">
                        <SectionCard
                            title="Offline Experiments and Validation History"
                            subtitle="Persisted model-version evidence, verdicts, and deployment disposition."
                        >
                            {validationQuery.isLoading && !validation ? (
                                <div className="admin-empty-state">
                                    <Loader2 className="h-4 w-4 animate-spin" style={{ color: "var(--admin-blue)" }} />
                                    <span>Loading validation history...</span>
                                </div>
                            ) : validationQuery.isError || !validation ? (
                                <SectionError message={(validationQuery.error as Error | undefined)?.message || "Validation history could not be loaded."} />
                            ) : validation.items.length === 0 ? (
                                <div className="admin-empty-state">No validation history has been synchronized yet.</div>
                            ) : (
                                <>
                                    <div className="grid gap-4 md:grid-cols-3">
                                        <KeyValue label="Total Models" value={formatNumber(dashboard.validation_history.total_models)} />
                                        <KeyValue label="Latest Verdict" value={dashboard.validation_history.latest_model?.verdict || "Unknown"} />
                                        <KeyValue label="Latest Deployed Mode" value={dashboard.validation_history.latest_model?.deployed_mode || "not_deployed"} />
                                    </div>
                                    <div className="admin-ml-table-note">{validation.source_note}</div>
                                    <div className="overflow-x-auto">
                                        <table className="admin-table">
                                            <thead>
                                                <tr>
                                                    <th>Model Version</th>
                                                    <th>Training Date</th>
                                                    <th>Dataset</th>
                                                    <th>Train Rows</th>
                                                    <th>Test Rows</th>
                                                    <th>Train AUC</th>
                                                    <th>Test AUC</th>
                                                    <th>Validation Method</th>
                                                    <th>Verdict</th>
                                                    <th>Mode</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {validation.items.map((item) => (
                                                    <tr key={`${item.model_version}-${item.training_date || "unknown"}`}>
                                                        <td className="font-medium">{item.model_version}</td>
                                                        <td>{formatDate(item.training_date)}</td>
                                                        <td title={item.dataset_used || undefined}>{formatCompactPath(item.dataset_used)}</td>
                                                        <td>{formatNumber(item.train_rows)}</td>
                                                        <td>{formatNumber(item.test_rows)}</td>
                                                        <td>{formatDecimal(item.train_auc, 3)}</td>
                                                        <td>{formatDecimal(item.test_auc, 3)}</td>
                                                        <td>{item.validation_method || "--"}</td>
                                                        <td><StatusBadge status={/(overfit|reject)/i.test(item.verdict) ? "rejected" : item.verdict === "shadow_only" ? "shadow" : "healthy"} label={item.verdict} /></td>
                                                        <td><StatusBadge status={item.deployed_mode} /></td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </>
                            )}
                        </SectionCard>

                        <SectionCard
                            title="Dataset Snapshot"
                            subtitle="Current dataset state is shown here. Historical dataset snapshots are not yet persisted separately."
                        >
                            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                                <MetricCard
                                    title="Linked Trade Count"
                                    value={formatNumber(dashboard.dataset_builder_status.linked_trade_count)}
                                    icon={<Database className="h-5 w-5" style={{ color: "var(--admin-blue)" }} />}
                                />
                                <MetricCard
                                    title="Usable Rows"
                                    value={formatNumber(dashboard.dataset_builder_status.fully_usable_rows)}
                                    icon={<CheckCircle2 className="h-5 w-5" style={{ color: "var(--admin-green)" }} />}
                                />
                                <MetricCard
                                    title="Dropped Rows"
                                    value={formatNumber(dashboard.dataset_builder_status.dropped_rows)}
                                    icon={<TriangleAlert className="h-5 w-5" style={{ color: "var(--admin-yellow)" }} />}
                                />
                                <MetricCard
                                    title="Rebuild Ready"
                                    value={dashboard.dataset_builder_status.rebuild_dataset_allowed ? "Yes" : "Blocked"}
                                    icon={<Sparkles className="h-5 w-5" style={{ color: dashboard.dataset_builder_status.rebuild_dataset_allowed ? "var(--admin-green)" : "var(--admin-red)" }} />}
                                />
                            </div>

                            <div className="admin-ml-subgrid two-up">
                                <KeyValue label="Dataset Source Date Range" value={formatRange(dashboard.dataset_builder_status.dataset_source_date_range)} />
                                <KeyValue label="Last Dataset Build Time" value={formatDate(dashboard.dataset_builder_status.last_dataset_build_time)} />
                                <KeyValue label="Last Dataset Path" value={formatCompactPath(dashboard.dataset_builder_status.last_dataset_path)} monospace />
                                <KeyValue label="Feature Completeness Status" value={dashboard.dataset_builder_status.feature_completeness_status} />
                            </div>

                            <div className="admin-empty-state">
                                Historical dataset-build snapshots are not yet available from the backend, so this tab keeps the current dataset state visible without inventing history.
                            </div>
                        </SectionCard>
                    </div>
                ) : null}
                </div>

            <ActionConfirmModal
                action={selectedAction}
                open={Boolean(selectedAction)}
                confirmationValue={confirmationValue}
                note={actionNote}
                submitting={actionMutation.isPending}
                error={(actionMutation.error as Error | undefined)?.message || null}
                onChangeConfirmation={setConfirmationValue}
                onChangeNote={setActionNote}
                onClose={closeActionModal}
                onConfirm={handleConfirmAction}
            />
        </div>
    );

    return embedded ? pageContent : <AdminLayout>{pageContent}</AdminLayout>;
}
