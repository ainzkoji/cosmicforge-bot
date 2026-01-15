import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useState } from "react";
import { Search, Download, UserPlus, MoreVertical, User, Loader2, Ban, CheckCircle } from "lucide-react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { listUsers, suspendUser, activateUser, getAdminDashboardStats } from "@/api/admin";
import { ExportButton } from "@/components/admin/common/ExportButton";

export default function UserManagement() {
    const [searchQuery, setSearchQuery] = useState("");
    const [statusFilter, setStatusFilter] = useState<string | undefined>(undefined);
    const queryClient = useQueryClient();

    // Fetch users with filters
    const { data: usersData, isLoading: usersLoading } = useQuery({
        queryKey: ["adminUsers", statusFilter],
        queryFn: () => listUsers(statusFilter, 100),
    });

    // Fetch stats for the metrics
    const { data: stats } = useQuery({
        queryKey: ["adminDashboardStats"],
        queryFn: getAdminDashboardStats,
    });

    // Suspend user mutation
    const suspendMutation = useMutation({
        mutationFn: suspendUser,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["adminUsers"] });
            queryClient.invalidateQueries({ queryKey: ["adminDashboardStats"] });
        },
    });

    // Activate user mutation
    const activateMutation = useMutation({
        mutationFn: activateUser,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["adminUsers"] });
            queryClient.invalidateQueries({ queryKey: ["adminDashboardStats"] });
        },
    });

    const users = usersData?.users || [];
    console.log("Users Data:", usersData);
    console.log("Stats Data:", stats);

    // Client-side search filtering
    const filteredUsers = users.filter((user: any) => {
        const matchesSearch = user.email.toLowerCase().includes(searchQuery.toLowerCase());
        return matchesSearch;
    });

    const getStatusBadgeClass = (status: string) => {
        switch (status) {
            case "active": return "admin-badge-success";
            case "pending": return "admin-badge-warning";
            case "suspended": return "admin-badge-danger";
            default: return "";
        }
    };

    const getRoleBadgeClass = (role: string) => {
        switch (role) {
            case "enterprise": return "admin-badge-info";
            case "pro": return "admin-badge-info";
            default: return "";
        }
    };

    const handleToggleStatus = (userId: string, currentStatus: string) => {
        if (currentStatus === "suspended") {
            activateMutation.mutate(userId);
        } else {
            suspendMutation.mutate(userId);
        }
    };

    const formatCurrency = (value: number) => {
        return new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: "USD",
            minimumFractionDigits: 0,
        }).format(value);
    };

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            User Management
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Manage platform users and their subscriptions
                        </p>
                    </div>
                    <div className="flex gap-3">
                        <button className="admin-btn admin-btn-secondary">
                            <Download className="w-4 h-4" />
                            Export Users
                        </button>
                        <button className="admin-btn admin-btn-primary">
                            <UserPlus className="w-4 h-4" />
                            Add User
                        </button>
                    </div>
                </div>

                {/* Filters */}
                <div className="flex items-center gap-4">
                    {/* Search */}
                    <div className="relative flex-1 max-w-md">
                        <Search
                            className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4"
                            style={{ color: 'var(--admin-text-muted)' }}
                        />
                        <input
                            type="text"
                            placeholder="Search by email..."
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            className="admin-input pl-10"
                        />
                    </div>

                    {/* Status Filter Chips */}
                    <div className="flex gap-2">
                        {[
                            { value: undefined, label: "All" },
                            { value: "active", label: "Active" },
                            { value: "pending", label: "Pending" },
                            { value: "suspended", label: "Suspended" }
                        ].map(({ value, label }) => (
                            <button
                                key={label}
                                onClick={() => setStatusFilter(value)}
                                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors`}
                                style={{
                                    background: statusFilter === value ? 'var(--admin-blue)' : 'var(--admin-bg-hover)',
                                    color: statusFilter === value ? 'white' : 'var(--admin-text-secondary)'
                                }}
                            >
                                {label}
                            </button>
                        ))}
                    </div>
                </div>

                {/* Stats Row */}
                <div className="grid grid-cols-4 gap-4">
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Total Users</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {stats?.total_users?.toLocaleString() || '0'}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Active Today</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-green)' }}>
                            {/* Placeholder - can add active users count */}
                            {Math.floor((stats?.total_users || 0) * 0.14).toLocaleString()}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">New This Month</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-blue)' }}>
                            {/* Placeholder - can add new users count */}
                            {Math.floor((stats?.total_users || 0) * 0.05).toLocaleString()}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Suspended</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-red)' }}>
                            {users.filter((u: any) => u.status === 'suspended').length}
                        </div>
                    </div>
                </div>

                {/* Users Table */}
                <div className="admin-card">
                    {usersLoading ? (
                        <div className="flex items-center justify-center py-12">
                            <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                        </div>
                    ) : (
                        <table className="admin-table">
                            <thead>
                                <tr>
                                    <th>User</th>
                                    <th>Account Created</th>
                                    <th>Role</th>
                                    <th>Total Trades</th>
                                    <th>Commission Earned</th>
                                    <th>Status</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filteredUsers.map((user: any) => (
                                    <tr key={user.id}>
                                        <td>
                                            <div className="flex items-center gap-3">
                                                <div
                                                    className="w-10 h-10 rounded-full flex items-center justify-center"
                                                    style={{ background: 'var(--admin-blue)' }}
                                                >
                                                    <User className="w-5 h-5 text-white" />
                                                </div>
                                                <div>
                                                    <div className="font-medium" style={{ color: 'var(--admin-text-primary)' }}>
                                                        {user.email}
                                                    </div>
                                                    <div className="text-sm" style={{ color: 'var(--admin-text-muted)' }}>
                                                        {user.id}
                                                    </div>
                                                </div>
                                            </div>
                                        </td>
                                        <td>{new Date(user.created_at).toLocaleDateString()}</td>
                                        <td>
                                            <span className={`admin-badge ${getRoleBadgeClass(user.role)}`}>
                                                {user.role}
                                            </span>
                                        </td>
                                        <td>{(user.total_trades || 0).toLocaleString()}</td>
                                        <td style={{ color: 'var(--admin-green)' }}>
                                            {formatCurrency(user.total_commission || 0)}
                                        </td>
                                        <td>
                                            <span className={`admin-badge ${getStatusBadgeClass(user.status)}`}>
                                                {user.status}
                                            </span>
                                        </td>
                                        <td>
                                            <div className="flex gap-2">
                                                <button
                                                    onClick={() => handleToggleStatus(user.id, user.status)}
                                                    disabled={suspendMutation.isPending || activateMutation.isPending}
                                                    className="p-2 hover:bg-gray-700 rounded transition-colors disabled:opacity-50"
                                                    title={user.status === 'suspended' ? 'Activate User' : 'Suspend User'}
                                                >
                                                    {user.status === 'suspended' ? (
                                                        <CheckCircle className="w-4 h-4" style={{ color: 'var(--admin-green)' }} />
                                                    ) : (
                                                        <Ban className="w-4 h-4" style={{ color: 'var(--admin-red)' }} />
                                                    )}
                                                </button>
                                                <button className="p-2 hover:bg-gray-700 rounded transition-colors">
                                                    <MoreVertical className="w-4 h-4" style={{ color: 'var(--admin-text-secondary)' }} />
                                                </button>
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                                {filteredUsers.length === 0 && (
                                    <tr>
                                        <td colSpan={7} className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                            No users found
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    )}
                </div>
            </div>
        </AdminLayout>
    );
}
