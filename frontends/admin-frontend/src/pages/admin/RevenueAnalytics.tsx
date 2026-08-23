import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { MetricCard } from "@/components/admin/cards/MetricCard";
import { DollarSign, TrendingUp, Percent, Users, Loader2 } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { getRevenueAnalytics, getAdminDashboardStats } from "@/api/admin";
import { ExportButton } from "@/components/admin/common/ExportButton";

export default function RevenueAnalytics() {
    // Fetch revenue analytics data
    const { data: revenueData, isLoading } = useQuery({
        queryKey: ["adminRevenueAnalytics"],
        queryFn: getRevenueAnalytics,
    });

    // Fetch dashboard stats for additional metrics
    const { data: stats } = useQuery({
        queryKey: ["adminDashboardStats"],
        queryFn: getAdminDashboardStats,
    });

    const formatCurrency = (value: number) => {
        return new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: "USD",
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
        }).format(value);
    };

    // Calculate metrics from data
    const totalRevenue = stats?.total_revenue || 0;
    const subscriptionRevenue = revenueData?.subscription_revenue || 0;
    const commissionRevenue = revenueData?.commission_revenue || 0;
    const avgRevenuePerUser = stats?.total_users ? (totalRevenue / stats.total_users) : 0;

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            Revenue Analytics
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Detailed insights into your revenue streams and performance
                        </p>
                    </div>
                    <div className="flex gap-3">
                        <select className="admin-input" style={{ width: '200px' }}>
                            <option>Last 12 Months</option>
                            <option>Last 6 Months</option>
                            <option>Last 30 Days</option>
                            <option>Custom Range</option>
                        </select>
                        <ExportButton
                            data={revenueData?.by_plan ? Object.entries(revenueData.by_plan).map(([plan, amount]) => ({ plan, amount })) : []}
                            filename="revenue_analytics"
                            label="Export"
                        />
                    </div>
                </div>

                {isLoading ? (
                    <div className="flex items-center justify-center py-12">
                        <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                    </div>
                ) : (
                    <>
                        {/* KPI Cards */}
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                            <MetricCard
                                title="Total Revenue"
                                value={formatCurrency(totalRevenue)}
                                change={{ value: "+18.5%", positive: true }}
                                icon={<DollarSign className="w-5 h-5" style={{ color: 'var(--admin-green)' }} />}
                            />
                            <MetricCard
                                title="Subscription Revenue"
                                value={formatCurrency(subscriptionRevenue)}
                                change={{ value: "+12.3%", positive: true }}
                                icon={<TrendingUp className="w-5 h-5" style={{ color: 'var(--admin-blue)' }} />}
                            />
                            <MetricCard
                                title="Commission Revenue"
                                value={formatCurrency(commissionRevenue)}
                                change={{ value: "+24.7%", positive: true }}
                                icon={<Percent className="w-5 h-5" style={{ color: 'var(--admin-purple)' }} />}
                            />
                            <MetricCard
                                title="Avg. Revenue Per User"
                                value={formatCurrency(avgRevenuePerUser)}
                                change={{ value: "+5.4%", positive: true }}
                                icon={<Users className="w-5 h-5" style={{ color: 'var(--admin-cyan)' }} />}
                            />
                        </div>

                        {/* Revenue Breakdown Chart */}
                        <div className="admin-card">
                            <h2 className="text-xl font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                                Revenue Breakdown
                            </h2>
                            <div className="h-96 flex items-center justify-center" style={{ background: 'var(--admin-bg-primary)', borderRadius: 'var(--admin-radius-md)' }}>
                                <div className="text-center">
                                    <p style={{ color: 'var(--admin-text-muted)' }} className="mb-2">Revenue Visualization</p>
                                    <div className="space-y-2 text-sm">
                                        <div style={{ color: 'var(--admin-text-secondary)' }}>
                                            Total: {formatCurrency(totalRevenue)}
                                        </div>
                                        <div style={{ color: 'var(--admin-blue)' }}>
                                            Subscriptions: {formatCurrency(subscriptionRevenue)}
                                        </div>
                                        <div style={{ color: 'var(--admin-green)' }}>
                                            Commissions: {formatCurrency(commissionRevenue)}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Bottom Grid */}
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            {/* Revenue by Plan */}
                            <div className="admin-card">
                                <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                                    Revenue by Plan
                                </h3>
                                <div className="space-y-4">
                                    {revenueData?.by_plan && Object.entries(revenueData.by_plan).map(([plan, amount]: [string, any]) => {
                                        const percentage = totalRevenue > 0 ? ((amount / totalRevenue) * 100) : 0;
                                        return (
                                            <div key={plan}>
                                                <div className="flex justify-between mb-2">
                                                    <span style={{ color: 'var(--admin-text-secondary)' }} className="capitalize">{plan}</span>
                                                    <span className="font-semibold" style={{ color: 'var(--admin-text-primary)' }}>
                                                        {formatCurrency(amount)} ({percentage.toFixed(0)}%)
                                                    </span>
                                                </div>
                                                <div className="h-3 rounded-full overflow-hidden" style={{ background: 'var(--admin-bg-primary)' }}>
                                                    <div
                                                        className="h-full rounded-full"
                                                        style={{
                                                            width: `${percentage}%`,
                                                            background: plan === 'enterprise' ? 'var(--admin-gold)' :
                                                                plan === 'pro' ? 'var(--admin-purple)' : 'var(--admin-blue)'
                                                        }}
                                                    />
                                                </div>
                                            </div>
                                        );
                                    })}
                                    {(!revenueData?.by_plan || Object.keys(revenueData.by_plan).length === 0) && (
                                        <div className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                            No plan revenue data available
                                        </div>
                                    )}
                                </div>
                            </div>

                            {/* Revenue Summary */}
                            <div className="admin-card">
                                <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                                    Revenue Summary
                                </h3>
                                <table className="admin-table">
                                    <thead>
                                        <tr>
                                            <th>Source</th>
                                            <th>Amount</th>
                                            <th>% of Total</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td>Subscription Revenue</td>
                                            <td style={{ color: 'var(--admin-green)' }}>{formatCurrency(subscriptionRevenue)}</td>
                                            <td style={{ color: 'var(--admin-text-secondary)' }}>
                                                {totalRevenue > 0 ? ((subscriptionRevenue / totalRevenue) * 100).toFixed(1) : 0}%
                                            </td>
                                        </tr>
                                        <tr>
                                            <td>Commission Revenue</td>
                                            <td style={{ color: 'var(--admin-green)' }}>{formatCurrency(commissionRevenue)}</td>
                                            <td style={{ color: 'var(--admin-text-secondary)' }}>
                                                {totalRevenue > 0 ? ((commissionRevenue / totalRevenue) * 100).toFixed(1) : 0}%
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="font-semibold">Total Revenue</td>
                                            <td className="font-semibold" style={{ color: 'var(--admin-green)' }}>{formatCurrency(totalRevenue)}</td>
                                            <td style={{ color: 'var(--admin-text-secondary)' }}>100%</td>
                                        </tr>
                                        {revenueData?.by_plan && (
                                            <>
                                                <tr>
                                                    <td colSpan={3} className="pt-4 pb-2">
                                                        <div className="text-sm font-semibold" style={{ color: 'var(--admin-text-secondary)' }}>
                                                            By Plan Type
                                                        </div>
                                                    </td>
                                                </tr>
                                                {Object.entries(revenueData.by_plan).map(([plan, amount]: [string, any]) => (
                                                    <tr key={plan}>
                                                        <td className="capitalize pl-4">{plan}</td>
                                                        <td style={{ color: 'var(--admin-green)' }}>{formatCurrency(amount)}</td>
                                                        <td style={{ color: 'var(--admin-text-secondary)' }}>
                                                            {totalRevenue > 0 ? ((amount / totalRevenue) * 100).toFixed(1) : 0}%
                                                        </td>
                                                    </tr>
                                                ))}
                                            </>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </AdminLayout>
    );
}
