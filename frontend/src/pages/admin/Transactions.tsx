import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useState, useEffect } from "react";
import { Download, CheckCircle, XCircle, Clock, DollarSign, TrendingUp } from "lucide-react";

interface Transaction {
    id: string;
    user_email: string;
    type: string;
    amount: number;
    currency: string;
    status: string;
    payment_method: string;
    created_at: string;
    completed_at?: string;
}

export default function Transactions() {
    const [transactions, setTransactions] = useState<Transaction[]>([]);
    const [statusFilter, setStatusFilter] = useState<string>("all");
    const [typeFilter, setTypeFilter] = useState<string>("all");
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetchTransactions();
    }, [statusFilter, typeFilter]);

    const fetchTransactions = async () => {
        try {
            // Mock data - will be replaced with actual API call
            setTransactions([
                {
                    id: "1",
                    user_email: "john@example.com",
                    type: "deposit",
                    amount: 5000,
                    currency: "USD",
                    status: "completed",
                    payment_method: "credit_card",
                    created_at: new Date(Date.now() - 2 * 3600000).toISOString(),
                    completed_at: new Date(Date.now() - 1 * 3600000).toISOString()
                },
                {
                    id: "2",
                    user_email: "sarah@example.com",
                    type: "subscription",
                    amount: 99,
                    currency: "USD",
                    status: "pending",
                    payment_method: "paypal",
                    created_at: new Date(Date.now() - 10 * 60000).toISOString()
                },
                {
                    id: "3",
                    user_email: "mike@example.com",
                    type: "withdrawal",
                    amount: 2500,
                    currency: "USD",
                    status: "pending",
                    payment_method: "bank_transfer",
                    created_at: new Date(Date.now() - 30 * 60000).toISOString()
                },
                {
                    id: "4",
                    user_email: "emily@example.com",
                    type: "commission",
                    amount: 125.50,
                    currency: "USD",
                    status: "completed",
                    payment_method: "wallet",
                    created_at: new Date(Date.now() - 5 * 3600000).toISOString(),
                    completed_at: new Date(Date.now() - 4 * 3600000).toISOString()
                }
            ]);
            setLoading(false);
        } catch (error) {
            console.error("Failed to fetch transactions:", error);
            setLoading(false);
        }
    };

    const handleApprove = async (transactionId: string) => {
        if (!confirm("Approve this transaction?")) return;
        // API call to approve
        alert(`Transaction ${transactionId} approved`);
        fetchTransactions();
    };

    const handleReject = async (transactionId: string) => {
        if (!confirm("Reject this transaction?")) return;
        // API call to reject
        alert(`Transaction ${transactionId} rejected`);
        fetchTransactions();
    };

    const getStatusBadge = (status: string) => {
        switch (status) {
            case "completed": return "admin-badge-success";
            case "pending": return "admin-badge-warning";
            case "failed": return "admin-badge-danger";
            default: return "";
        }
    };

    const getStatusIcon = (status: string) => {
        switch (status) {
            case "completed": return <CheckCircle className="w-4 h-4" style={{ color: 'var(--admin-green)' }} />;
            case "pending": return <Clock className="w-4 h-4" style={{ color: 'var(--admin-yellow)' }} />;
            case "failed": return <XCircle className="w-4 h-4" style={{ color: 'var(--admin-red)' }} />;
            default: return null;
        }
    };

    const getTypeColor = (type: string) => {
        switch (type) {
            case "deposit": return "var(--admin-green)";
            case "withdrawal": return "var(--admin-red)";
            case "subscription": return "var(--admin-blue)";
            case "commission": return "var(--admin-purple)";
            default: return "var(--admin-text-primary)";
        }
    };

    const filteredTransactions = transactions.filter(t => {
        const matchesStatus = statusFilter === "all" || t.status === statusFilter;
        const matchesType = typeFilter === "all" || t.type === typeFilter;
        return matchesStatus && matchesType;
    });

    const totalVolume = transactions.reduce((sum, t) => sum + t.amount, 0);
    const pendingCount = transactions.filter(t => t.status === "pending").length;
    const completedToday = transactions.filter(t =>
        t.status === "completed" &&
        new Date(t.completed_at!).toDateString() === new Date().toDateString()
    ).length;

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            Transaction Monitor
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Track and manage all financial transactions
                        </p>
                    </div>
                    <button className="admin-btn admin-btn-primary">
                        <Download className="w-4 h-4" />
                        Export Transactions
                    </button>
                </div>

                {/* Stats Cards */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">Total Volume</div>
                                <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                                    ${totalVolume.toLocaleString()}
                                </div>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(16, 185, 129, 0.1)' }}>
                                <DollarSign className="w-5 h-5" style={{ color: 'var(--admin-green)' }} />
                            </div>
                        </div>
                    </div>

                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Pending Approval</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-yellow)' }}>
                            {pendingCount}
                        </div>
                    </div>

                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Completed Today</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-green)' }}>
                            {completedToday}
                        </div>
                    </div>

                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Success Rate</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-green)' }}>
                            94.2%
                        </div>
                    </div>
                </div>

                {/* Filters */}
                <div className="flex items-center gap-4">
                    <select
                        className="admin-input"
                        value={statusFilter}
                        onChange={(e) => setStatusFilter(e.target.value)}
                        style={{ width: '200px' }}
                    >
                        <option value="all">All Statuses</option>
                        <option value="pending">Pending</option>
                        <option value="completed">Completed</option>
                        <option value="failed">Failed</option>
                    </select>

                    <select
                        className="admin-input"
                        value={typeFilter}
                        onChange={(e) => setTypeFilter(e.target.value)}
                        style={{ width: '200px' }}
                    >
                        <option value="all">All Types</option>
                        <option value="deposit">Deposits</option>
                        <option value="withdrawal">Withdrawals</option>
                        <option value="subscription">Subscriptions</option>
                        <option value="commission">Commissions</option>
                    </select>
                </div>

                {/* Transactions Table */}
                <div className="admin-card">
                    <h2 className="text-xl font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        All Transactions
                    </h2>
                    <table className="admin-table">
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>User</th>
                                <th>Type</th>
                                <th>Amount</th>
                                <th>Payment Method</th>
                                <th>Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {filteredTransactions.map((txn) => (
                                <tr key={txn.id}>
                                    <td style={{ fontSize: '0.85rem' }}>
                                        {new Date(txn.created_at).toLocaleString()}
                                    </td>
                                    <td className="font-medium">{txn.user_email}</td>
                                    <td>
                                        <span className="capitalize font-medium" style={{ color: getTypeColor(txn.type) }}>
                                            {txn.type}
                                        </span>
                                    </td>
                                    <td className="font-mono font-semibold">
                                        ${txn.amount.toLocaleString()}
                                    </td>
                                    <td className="capitalize">{txn.payment_method.replace('_', ' ')}</td>
                                    <td>
                                        <div className="flex items-center gap-2">
                                            {getStatusIcon(txn.status)}
                                            <span className={`admin-badge ${getStatusBadge(txn.status)}`}>
                                                {txn.status}
                                            </span>
                                        </div>
                                    </td>
                                    <td>
                                        {txn.status === "pending" && (
                                            <div className="flex gap-2">
                                                <button
                                                    onClick={() => handleApprove(txn.id)}
                                                    className="admin-btn admin-btn-primary px-3 py-1 text-xs"
                                                >
                                                    Approve
                                                </button>
                                                <button
                                                    onClick={() => handleReject(txn.id)}
                                                    className="admin-btn admin-btn-danger px-3 py-1 text-xs"
                                                >
                                                    Reject
                                                </button>
                                            </div>
                                        )}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </AdminLayout>
    );
}
