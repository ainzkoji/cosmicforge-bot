import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { Edit2, Loader2 } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { getCommissionTiers } from "@/api/admin";

export default function CommissionSettings() {
    // Fetch commission tiers
    const { data: tiersData, isLoading } = useQuery({
        queryKey: ["adminCommissionTiers"],
        queryFn: getCommissionTiers,
    });

    const tiers = tiersData?.tiers || [];

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            Commission Settings
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Configure commission tiers and affiliate program settings
                        </p>
                    </div>
                    <button className="admin-btn admin-btn-primary">
                        Save Changes
                    </button>
                </div>

                {/* Commission Tiers */}
                <div className="admin-card">
                    <h2 className="text-xl font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Trading Commission Tiers
                    </h2>
                    {isLoading ? (
                        <div className="flex items-center justify-center py-12">
                            <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                        </div>
                    ) : (
                        <table className="admin-table">
                            <thead>
                                <tr>
                                    <th>Tier</th>
                                    <th>Monthly Volume</th>
                                    <th>Commission Rate</th>
                                    <th>Status</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {tiers.map((tier: any) => (
                                    <tr key={tier.id}>
                                        <td className="font-medium">{tier.name || `Tier ${tier.tier_level}`}</td>
                                        <td>
                                            {tier.min_volume ? `$${tier.min_volume.toLocaleString()}` : '$0'} -
                                            {tier.max_volume ? ` $${tier.max_volume.toLocaleString()}` : '+'}
                                        </td>
                                        <td style={{ color: 'var(--admin-text-primary)' }}>
                                            {(tier.commission_rate * 100).toFixed(2)}%
                                        </td>
                                        <td>
                                            <label className="relative inline-flex items-center cursor-pointer">
                                                <input
                                                    type="checkbox"
                                                    className="sr-only peer"
                                                    checked={tier.is_active}
                                                    readOnly
                                                />
                                                <div className="w-11 h-6 rounded-full peer peer-checked:after:translate-x-full after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-green-600" style={{ background: 'var(--admin-bg-hover)' }}></div>
                                            </label>
                                        </td>
                                        <td>
                                            <button className="admin-btn admin-btn-secondary px-3 py-1 text-xs">
                                                <Edit2 className="w-3 h-3" />
                                                Edit
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                                {tiers.length === 0 && (
                                    <tr>
                                        <td colSpan={5} className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                            No commission tiers configured
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    )}
                </div>

                {/* Grid Section */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Platform Fee Breakdown */}
                    <div className="admin-card">
                        <h2 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            Platform Fee Breakdown
                        </h2>
                        <div className="h-64 flex items-center justify-center" style={{ background: 'var(--admin-bg-primary)', borderRadius: 'var(--admin-radius-md)' }}>
                            <div className="text-center">
                                <p style={{ color: 'var(--admin-text-muted)' }} className="mb-3">Fee Distribution</p>
                                <div className="space-y-2 text-sm">
                                    <div style={{ color: 'var(--admin-text-secondary)' }}>Platform: 70%</div>
                                    <div style={{ color: 'var(--admin-blue)' }}>Affiliate: 20%</div>
                                    <div style={{ color: 'var(--admin-green)' }}>Referral: 10%</div>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Affiliate Program Settings */}
                    <div className="admin-card">
                        <h2 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            Affiliate Program Settings
                        </h2>
                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-medium mb-2" style={{ color: 'var(--admin-text-secondary)' }}>
                                    Referral Commission Rate
                                </label>
                                <input
                                    type="text"
                                    defaultValue="15%"
                                    className="admin-input"
                                />
                            </div>
                            <div>
                                <label className="block text-sm font-medium mb-2" style={{ color: 'var(--admin-text-secondary)' }}>
                                    Cookie Duration
                                </label>
                                <select className="admin-input">
                                    <option>30 days</option>
                                    <option>60 days</option>
                                    <option>90 days</option>
                                </select>
                            </div>
                            <div>
                                <label className="block text-sm font-medium mb-2" style={{ color: 'var(--admin-text-secondary)' }}>
                                    Minimum Payout
                                </label>
                                <input
                                    type="text"
                                    defaultValue="$100"
                                    className="admin-input"
                                />
                            </div>
                            <div className="flex items-center gap-3">
                                <input type="checkbox" id="tiered" defaultChecked className="w-4 h-4" />
                                <label htmlFor="tiered" style={{ color: 'var(--admin-text-secondary)' }}>
                                    Enable Tiered Referrals
                                </label>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Recent Changes */}
                <div className="admin-card">
                    <h2 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Recent Commission Changes
                    </h2>
                    <div className="space-y-3">
                        {tiers.slice(0, 3).map((tier: any, idx: number) => (
                            <div key={idx} className="flex items-start gap-3 p-3 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                                <div className="w-2 h-2 mt-2 rounded-full" style={{ background: 'var(--admin-blue)' }} />
                                <div className="flex-1">
                                    <div className="text-sm font-medium" style={{ color: 'var(--admin-text-primary)' }}>
                                        {tier.name || `Tier ${tier.tier_level}`} - {(tier.commission_rate * 100).toFixed(2)}% commission rate
                                    </div>
                                    <div className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                        Status: {tier.is_active ? 'Active' : 'Inactive'}
                                    </div>
                                </div>
                            </div>
                        ))}
                        {tiers.length === 0 && (
                            <div className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                No recent changes
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
