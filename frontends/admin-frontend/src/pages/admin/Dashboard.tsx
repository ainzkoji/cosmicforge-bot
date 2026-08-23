import { useState } from "react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { MetricCard } from "@/components/admin/cards/MetricCard";
import {
    Activity,
    AlertTriangle,
    DollarSign,
    Loader2,
    TrendingUp,
    Users,
} from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import {
    getAdminDashboardStats,
    listUsers,
    getRevenueOverview,
    getTopTradingPairs,
} from "@/api/admin";
import { RevenueOverviewChart } from "@/components/admin/charts/RevenueOverviewChart";
import { TopTradingPairsChart } from "@/components/admin/charts/TopTradingPairsChart";

// ─── Formatters ───────────────────────────────────────────────────────────────

function formatCurrency(value: number) {
    return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: 0,
    }).format(value);
}

function formatNumber(value: number) {
    return new Intl.NumberFormat("en-US").format(value);
}

// Badge formatting helper

// ─── User role / status badge ─────────────────────────────────────────────────

function UserBadge({ value, type }: { value: string; type: "role" | "status" }) {
    const v = (value || "").toLowerCase();
    let bg = "rgba(255,255,255,0.04)";
    let color = "var(--admin-text-secondary)";
    let border = "var(--admin-border-color)";

    if (type === "role") {
        if (v === "admin") { bg = "rgba(139,92,246,0.12)"; color = "var(--admin-purple)"; border = "rgba(139,92,246,0.28)"; }
        else               { bg = "rgba(59,130,246,0.10)";  color = "var(--admin-blue)";   border = "rgba(59,130,246,0.28)";  }
    } else {
        if (v === "active")    { bg = "rgba(16,185,129,0.10)";  color = "var(--admin-green)";  border = "rgba(16,185,129,0.28)";  }
        if (v === "suspended") { bg = "rgba(239,68,68,0.10)";   color = "var(--admin-red)";    border = "rgba(239,68,68,0.28)";   }
        if (v === "pending")   { bg = "rgba(245,158,11,0.10)";  color = "var(--admin-yellow)"; border = "rgba(245,158,11,0.28)";  }
    }

    return (
        <span style={{
            display: "inline-flex", alignItems: "center",
            padding: "2px 8px", borderRadius: 999,
            fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em",
            border: `1px solid ${border}`, background: bg, color,
            whiteSpace: "nowrap",
        }}>
            {value}
        </span>
    );
}

// ─── Section card header ──────────────────────────────────────────────────────

function CardHeader({ title, subtitle, badge }: { title: string; subtitle?: string; badge?: string }) {
    return (
        <div style={{ marginBottom: 16 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <h2 style={{ margin: 0, fontSize: "0.95rem", fontWeight: 700, color: "var(--admin-text-primary)" }}>
                    {title}
                </h2>
                {badge && (
                    <span style={{
                        fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 999,
                        background: "rgba(245,158,11,0.12)", color: "var(--admin-yellow)",
                        border: "1px solid rgba(245,158,11,0.25)",
                        textTransform: "uppercase", letterSpacing: "0.06em",
                    }}>
                        {badge}
                    </span>
                )}
            </div>
            {subtitle && (
                <p style={{ margin: "3px 0 0", fontSize: 11, color: "var(--admin-text-muted)" }}>{subtitle}</p>
            )}
        </div>
    );
}

function SectionError({ message, onRetry }: { message: string; onRetry: () => void }) {
    return (
        <div style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            padding: "18px 0",
            color: "var(--admin-text-secondary)",
            fontSize: 12,
        }}>
            <AlertTriangle className="w-4 h-4 flex-shrink-0" style={{ color: "var(--admin-red)" }} />
            <span>{message}</span>
            <button
                type="button"
                className="admin-btn admin-btn-secondary"
                style={{ marginLeft: "auto", padding: "6px 10px", fontSize: 11 }}
                onClick={onRetry}
            >
                Retry
            </button>
        </div>
    );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function AdminDashboard() {
    const [revenueTimeframe, setRevenueTimeframe] = useState<"30d" | "6m" | "12m">("12m");

    const { data: stats, isLoading: statsLoading, isError: statsError, refetch: refetchStats } = useQuery({
        queryKey: ["adminDashboardStats"],
        queryFn: getAdminDashboardStats,
        staleTime: 60_000,
    });

    const { data: revenueOverview, isLoading: revenueOverviewLoading, isError: revenueOverviewError, refetch: refetchRevenueOverview } = useQuery({
        queryKey: ["revenueOverview", revenueTimeframe],
        queryFn: () => getRevenueOverview(revenueTimeframe),
        staleTime: 60_000,
    });

    const { data: topTradingPairs, isLoading: topTradingPairsLoading, isError: topTradingPairsError, refetch: refetchTopTradingPairs } = useQuery({
        queryKey: ["topTradingPairs"],
        queryFn: getTopTradingPairs,
        staleTime: 60_000,
    });

    const { data: usersData, isLoading: usersLoading, isError: usersError, refetch: refetchUsers } = useQuery({
        queryKey: ["recentUsers"],
        queryFn: () => listUsers(undefined, 5),
        staleTime: 60_000,
    });

    return (
        <AdminLayout>
            <div style={{ display: "flex", flexDirection: "column", gap: 20, maxWidth: 1600, margin: "0 auto" }}>

                {/* ── Page header ── */}
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "flex-start", justifyContent: "space-between", gap: 10 }}>
                    <div>
                        <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 8, marginBottom: 5 }}>
                            <h1 style={{ margin: 0, fontSize: "1.55rem", fontWeight: 800, letterSpacing: "-0.025em", color: "var(--admin-text-primary)", lineHeight: 1.2 }}>
                                Dashboard
                            </h1>
                            <span style={{
                                fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 999,
                                background: "rgba(59,130,246,0.10)", color: "var(--admin-blue)",
                                border: "1px solid rgba(59,130,246,0.22)",
                                textTransform: "uppercase", letterSpacing: "0.07em",
                            }}>Admin Portal</span>
                            <span style={{
                                fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 999,
                                background: "rgba(16,185,129,0.10)", color: "var(--admin-green)",
                                border: "1px solid rgba(16,185,129,0.22)",
                                textTransform: "uppercase", letterSpacing: "0.07em",
                            }}>Live</span>
                        </div>
                        <p style={{ margin: 0, fontSize: 13, color: "var(--admin-text-secondary)", lineHeight: 1.45 }}>
                            Platform performance, system activity, revenue, users, and trading overview.
                        </p>
                    </div>
                </div>

                {/* ── Metric cards ── */}
                {statsLoading ? (
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", padding: "32px 0" }}>
                        <Loader2 className="w-7 h-7 animate-spin" style={{ color: "var(--admin-blue)" }} />
                    </div>
                ) : statsError ? (
                    <div className="admin-card" style={{ padding: "16px 20px" }}>
                        <SectionError message="Dashboard stats could not be loaded." onRetry={() => refetchStats()} />
                    </div>
                ) : (
                    <div style={{ display: "grid", gap: 14, gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))" }}>
                        <MetricCard
                            title="Total Users"
                            value={formatNumber(stats?.total_users || 0)}
                            change={{ value: "+12.5%", positive: true }}
                            icon={<Users className="w-5 h-5" style={{ color: "#3B82F6" }} />}
                            accent="#3B82F6"
                        />
                        <MetricCard
                            title="Active Subscriptions"
                            value={formatNumber(stats?.active_subscriptions || 0)}
                            change={{ value: "+8.3%", positive: true }}
                            icon={<TrendingUp className="w-5 h-5" style={{ color: "#10B981" }} />}
                            accent="#10B981"
                        />
                        <MetricCard
                            title="Total Revenue"
                            value={formatCurrency(stats?.total_revenue || 0)}
                            change={{ value: "+15.2%", positive: true }}
                            icon={<DollarSign className="w-5 h-5" style={{ color: "#8B5CF6" }} />}
                            accent="#8B5CF6"
                        />
                        <MetricCard
                            title="Platform Trades"
                            value={formatNumber(stats?.platform_trades || 0)}
                            change={{ value: "+6.7%", positive: true }}
                            icon={<Activity className="w-5 h-5" style={{ color: "#06B6D4" }} />}
                            accent="#06B6D4"
                        />
                    </div>
                )}

                {/* ── Revenue chart ── */}
                <div className="admin-card" style={{ padding: "20px 24px" }}>
                    <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, marginBottom: 14 }}>
                        <CardHeader
                            title="Revenue Overview"
                            subtitle="Historical and current platform financial analytics."
                            badge="Live Data"
                        />
                    </div>
                    {revenueOverviewError ? (
                        <SectionError message="Revenue overview could not be loaded." onRetry={() => refetchRevenueOverview()} />
                    ) : revenueOverview?.data?.length === 0 && !revenueOverviewLoading ? (
                        <div style={{ padding: "24px 0", color: "var(--admin-text-muted)", fontSize: 12 }}>No revenue data available yet.</div>
                    ) : (
                        <RevenueOverviewChart
                            data={revenueOverview?.data}
                            isLoading={revenueOverviewLoading}
                            timeframe={revenueTimeframe}
                            setTimeframe={setRevenueTimeframe}
                        />
                    )}
                </div>

                {/* ── Bottom grid ── */}
                <div style={{ display: "grid", gap: 16, gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))" }}>

                    {/* Recent User Signups */}
                    <div className="admin-card" style={{ padding: "20px 24px", minWidth: 0 }}>
                        <CardHeader
                            title="Recent User Signups"
                            subtitle="Last 5 registered accounts"
                        />
                        {usersLoading ? (
                            <div style={{ display: "flex", alignItems: "center", justifyContent: "center", padding: "24px 0" }}>
                                <Loader2 className="w-6 h-6 animate-spin" style={{ color: "var(--admin-blue)" }} />
                            </div>
                        ) : usersError ? (
                            <SectionError message="Recent users could not be loaded." onRetry={() => refetchUsers()} />
                        ) : (
                            <div style={{ overflowX: "auto" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                                    <thead>
                                        <tr style={{ borderBottom: "1px solid var(--admin-border-color)" }}>
                                            {["Email", "Role", "Status", "Trades"].map((h) => (
                                                <th key={h} style={{
                                                    padding: "7px 10px", textAlign: "left",
                                                    fontSize: 10, fontWeight: 700, textTransform: "uppercase",
                                                    letterSpacing: "0.07em", color: "var(--admin-text-muted)",
                                                    whiteSpace: "nowrap",
                                                }}>{h}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {usersData?.users && usersData.users.length > 0 ? (
                                            usersData.users.map((user: any) => (
                                                <tr key={user.id}
                                                    style={{ borderBottom: "1px solid var(--admin-border-color)", transition: "background 0.1s" }}
                                                    onMouseEnter={(e) => { (e.currentTarget as HTMLTableRowElement).style.background = "var(--admin-bg-hover)"; }}
                                                    onMouseLeave={(e) => { (e.currentTarget as HTMLTableRowElement).style.background = ""; }}
                                                >
                                                    <td style={{ padding: "8px 10px", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                                        <span title={user.email} style={{ color: "var(--admin-text-primary)" }}>{user.email}</span>
                                                    </td>
                                                    <td style={{ padding: "8px 10px", whiteSpace: "nowrap" }}>
                                                        <UserBadge value={user.role || "user"} type="role" />
                                                    </td>
                                                    <td style={{ padding: "8px 10px", whiteSpace: "nowrap" }}>
                                                        <UserBadge value={user.status || "active"} type="status" />
                                                    </td>
                                                    <td style={{ padding: "8px 10px", color: "var(--admin-text-secondary)", textAlign: "right" }}>
                                                        {user.total_trades ?? 0}
                                                    </td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={4} style={{ padding: "24px 10px", textAlign: "center", color: "var(--admin-text-muted)", fontSize: 12 }}>
                                                    No recent signups yet
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        )}
                    </div>

                    {/* Top Trading Pairs */}
                    <div className="admin-card" style={{ padding: "20px 24px", minWidth: 0 }}>
                        <CardHeader
                            title="Top Trading Pairs"
                            subtitle="Actual execution volumes aggregated by symbol."
                            badge="Live Data"
                        />
                        {topTradingPairsError ? (
                            <SectionError message="Top trading pairs could not be loaded." onRetry={() => refetchTopTradingPairs()} />
                        ) : topTradingPairs?.data?.length === 0 && !topTradingPairsLoading ? (
                            <div style={{ padding: "24px 0", color: "var(--admin-text-muted)", fontSize: 12 }}>No trading pair activity available yet.</div>
                        ) : (
                            <TopTradingPairsChart
                                data={topTradingPairs?.data}
                                isLoading={topTradingPairsLoading}
                            />
                        )}
                    </div>

                </div>
            </div>
        </AdminLayout>
    );
}
