import { Outlet, useLocation } from "react-router-dom";
import { Activity, Cpu, Database, GitBranch, Shield, Target } from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { MLSectionTabs } from "@/components/admin/ml/MLSectionTabs";
import { StatusBadge } from "@/components/admin/ml/StatusBadge";
import { useMLDashboardQuery } from "@/hooks/useAdminML";

const tabItems = [
    { key: "overview", label: "Overview", icon: <Cpu className="h-4 w-4" />, to: "overview" },
    { key: "readiness", label: "Readiness", icon: <Target className="h-4 w-4" />, to: "readiness" },
    { key: "data-quality", label: "Data Quality", icon: <Database className="h-4 w-4" />, to: "data-quality" },
    { key: "activity", label: "Activity", icon: <Activity className="h-4 w-4" />, to: "activity" },
    { key: "controls", label: "Controls", icon: <Shield className="h-4 w-4" />, to: "controls" },
    { key: "history", label: "History", icon: <GitBranch className="h-4 w-4" />, to: "history" },
];

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

function formatNumber(value: number | null | undefined) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "--";
    }
    return new Intl.NumberFormat("en-US").format(value);
}

function routeKey(pathname: string) {
    const parts = pathname.split("/").filter(Boolean);
    const last = parts[parts.length - 1] || "overview";
    return tabItems.some((item) => item.key === last) ? last : "overview";
}

export function MLLayout() {
    const location = useLocation();
    const dashboardQuery = useMLDashboardQuery();
    const dashboard = dashboardQuery.data;
    const activeKey = routeKey(location.pathname);

    return (
        <AdminLayout>
            <div className="admin-ml-shell">
                <header className="admin-ml-header">
                    <div className="admin-ml-header-row">
                        <div className="admin-ml-header-copy">
                            <div className="admin-ml-header-eyebrow">Enterprise ML Operations</div>
                            <h1 className="text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>
                                ML Monitoring
                            </h1>
                            <p>
                                Route-based operational control surface for readiness, scoring, data quality, and protected ML actions.
                            </p>
                        </div>
                        <div className="admin-ml-header-badges">
                            <div className="admin-ml-header-badge">
                                <span className="admin-ml-header-badge-label">ML Mode</span>
                                {dashboard ? <StatusBadge status={dashboard.overview.ml_mode} /> : <StatusBadge status="queued" label="Loading" />}
                            </div>
                            <div className="admin-ml-header-badge">
                                <span className="admin-ml-header-badge-label">Training Ready</span>
                                {dashboard ? (
                                    <StatusBadge
                                        status={dashboard.training_gate.training_ready ? "healthy" : "not_ready"}
                                        label={dashboard.training_gate.training_ready ? "Ready" : "Not Ready"}
                                    />
                                ) : (
                                    <StatusBadge status="queued" label="Loading" />
                                )}
                            </div>
                            <div className="admin-ml-header-badge">
                                <span className="admin-ml-header-badge-label">Linkage Health</span>
                                {dashboard ? (
                                    <StatusBadge
                                        status={dashboard.linkage_health.linkage_healthy ? "healthy" : "broken"}
                                        label={dashboard.linkage_health.linkage_healthy ? "Healthy" : "Unhealthy"}
                                    />
                                ) : (
                                    <StatusBadge status="queued" label="Loading" />
                                )}
                            </div>
                        </div>
                    </div>
                    <div className="admin-ml-header-meta">
                        <span>Model {dashboard?.overview.current_model_version || "Unassigned"}</span>
                        <span>Restart {formatDate(dashboard?.overview.last_bot_restart_time)}</span>
                        <span>{formatNumber(dashboard?.activity_summary.total_ml_scored_entries)} scored entries</span>
                    </div>
                </header>

                <div className="admin-ml-tab-shell">
                    <MLSectionTabs items={tabItems} activeKey={activeKey} onChange={() => undefined} />
                </div>

                {dashboardQuery.isLoading && !dashboard ? (
                    <div className="flex min-h-[48vh] items-center justify-center">
                        <div className="admin-empty-state">
                            <span>Loading ML module...</span>
                        </div>
                    </div>
                ) : dashboardQuery.isError || !dashboard ? (
                    <div className="admin-card">
                        <div className="flex items-start gap-3">
                            <div className="h-6 w-6 rounded-full" style={{ background: "var(--admin-red)" }} />
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
                ) : (
                    <Outlet />
                )}
            </div>
        </AdminLayout>
    );
}
