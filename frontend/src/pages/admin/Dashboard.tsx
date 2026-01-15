import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { MetricCard } from "@/components/admin/cards/MetricCard";
import { Users, DollarSign, Activity, TrendingUp, Loader2 } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { getAdminDashboardStats, listUsers } from "@/api/admin";

export default function AdminDashboard() {
    // Fetch dashboard stats
    const { data: stats, isLoading: statsLoading } = useQuery({
        queryKey: ["adminDashboardStats"],
        queryFn: getAdminDashboardStats,
    });

    // Fetch recent users
    const { data: usersData, isLoading: usersLoading } = useQuery({
        queryKey: ["recentUsers"],
        queryFn: () => listUsers(undefined, 5),
    });

    const formatCurrency = (value: number) => {
        return new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: "USD",
            minimumFractionDigits: 0,
        }).format(value);
    };

    const formatNumber = (value: number) => {
        return new Intl.NumberFormat("en-US").format(value);
    };

    return (
        <AdminLayout>
            <div className="space-y-8">
                {/* Page Header */}
                <div>
                    <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                        Dashboard
                    </h1>
                    <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                        Overview of your platform performance
                    </p>
                </div>

                {/* Metrics Grid */}
                {statsLoading ? (
                    <div className="flex items-center justify-center py-12">
                        <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                    </div>
                ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                        <MetricCard
                            title="Total Users"
                            value={formatNumber(stats?.total_users || 0)}
                            change={{ value: "+12.5%", positive: true }}
                            icon={<Users className="w-5 h-5" style={{ color: 'var(--admin-blue)' }} />}
                        />
                        <MetricCard
                            title="Active Subscriptions"
                            value={formatNumber(stats?.active_subscriptions || 0)}
                            change={{ value: "+8.3%", positive: true }}
                            icon={<TrendingUp className="w-5 h-5" style={{ color: 'var(--admin-green)' }} />}
                        />
                        <MetricCard
                            title="Total Revenue"
                            value={formatCurrency(stats?.total_revenue || 0)}
                            change={{ value: "+15.2%", positive: true }}
                            icon={<DollarSign className="w-5 h-5" style={{ color: 'var(--admin-green)' }} />}
                        />
                        <MetricCard
                            title="Platform Trades"
                            value={formatNumber(stats?.platform_trades || 0)}
                            change={{ value: "+6.7%", positive: true }}
                            icon={<Activity className="w-5 h-5" style={{ color: 'var(--admin-cyan)' }} />}
                        />
                    </div>
                )}

                {/* Revenue Chart Placeholder */}
                <div className="admin-card">
                    <h2 className="text-xl font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Revenue Overview
                    </h2>
                    <div className="h-80 flex items-center justify-center" style={{ background: 'var(--admin-bg-primary)', borderRadius: 'var(--admin-radius-md)' }}>
                        <p style={{ color: 'var(--admin-text-muted)' }}>Chart will be rendered here</p>
                    </div>
                </div>

                {/* Bottom Grid */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Recent Users */}
                    <div className="admin-card">
                        <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            Recent User Signups
                        </h3>
                        {usersLoading ? (
                            <div className="flex items-center justify-center py-8">
                                <Loader2 className="w-6 h-6 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                            </div>
                        ) : (
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>Email</th>
                                        <th>Role</th>
                                        <th>Status</th>
                                        <th>Trades</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {usersData?.users?.map((user: any) => (
                                        <tr key={user.id}>
                                            <td>{user.email}</td>
                                            <td><span className="admin-badge admin-badge-info">{user.role}</span></td>
                                            <td>
                                                <span className={`admin-badge ${user.status === 'active' ? 'admin-badge-success' :
                                                        user.status === 'suspended' ? 'admin-badge-danger' :
                                                            'admin-badge-warning'
                                                    }`}>
                                                    {user.status}
                                                </span>
                                            </td>
                                            <td>{user.total_trades || 0}</td>
                                        </tr>
                                    ))}
                                    {(!usersData?.users || usersData.users.length === 0) && (
                                        <tr>
                                            <td colSpan={4} className="text-center" style={{ color: 'var(--admin-text-muted)' }}>
                                                No users found
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        )}
                    </div>

                    {/* Top Trading Pairs */}
                    <div className="admin-card">
                        <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            Top Trading Pairs
                        </h3>
                        <div className="h-64 flex items-center justify-center" style={{ background: 'var(--admin-bg-primary)', borderRadius: 'var(--admin-radius-md)' }}>
                            <p style={{ color: 'var(--admin-text-muted)' }}>Donut chart will be rendered here</p>
                        </div>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
