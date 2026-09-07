import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { AlertTriangle, Shield, FileText, Download, CheckCircle, Loader2, X, Check } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { getPendingKYC, getAMLFlags } from "@/api/admin";
import { ExportButton } from "@/components/admin/common/ExportButton";

export default function Compliance() {
    // Fetch pending KYC submissions
    const { data: kycData, isLoading: kycLoading } = useQuery({
        queryKey: ["adminPendingKYC"],
        queryFn: getPendingKYC,
    });

    // Fetch AML flags
    const { data: amlData, isLoading: amlLoading } = useQuery({
        queryKey: ["adminAMLFlags"],
        queryFn: getAMLFlags,
    });

    const kycSubmissions = kycData?.submissions || [];
    const amlFlags = amlData?.flags || [];

    const getRiskBadgeClass = (risk: string) => {
        const lowerRisk = risk?.toLowerCase();
        if (lowerRisk === "low") return "admin-badge-success";
        if (lowerRisk === "medium") return "admin-badge-warning";
        if (lowerRisk === "high") return "admin-badge-danger";
        return "admin-badge-info";
    };

    const getStatusBadgeClass = (status: string) => {
        const lowerStatus = status?.toLowerCase();
        if (lowerStatus === "pending") return "admin-badge-warning";
        if (lowerStatus === "under_review") return "admin-badge-info";
        if (lowerStatus === "approved") return "admin-badge-success";
        if (lowerStatus === "rejected") return "admin-badge-danger";
        return "admin-badge-info";
    };

    const formatDate = (dateStr: string) => {
        try {
            return new Date(dateStr).toLocaleDateString();
        } catch {
            return dateStr;
        }
    };

    // Calculate compliance score (simplified)
    const totalSubmissions = kycSubmissions.length + 100; // Adding baseline
    const pendingCount = kycSubmissions.length;
    const complianceScore = ((totalSubmissions - pendingCount) / totalSubmissions * 100).toFixed(1);

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                        Compliance & Regulatory Reporting
                    </h1>
                    <div className="flex gap-3">
                        <ExportButton
                            data={[...kycSubmissions, ...amlFlags]}
                            filename="compliance_data"
                            label="Export"
                        />
                    </div>
                </div>
                <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                    Manage KYC verifications, AML monitoring, and compliance reports
                </p>

                {/* Status Cards */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">KYC Pending</div>
                                <div className="text-3xl font-bold" style={{ color: 'var(--admin-yellow)' }}>
                                    {kycLoading ? '...' : kycSubmissions.length}
                                </div>
                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                    Review Required
                                </p>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(245, 158, 11, 0.1)' }}>
                                <AlertTriangle className="w-6 h-6" style={{ color: 'var(--admin-yellow)' }} />
                            </div>
                        </div>
                    </div>

                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">AML Flags</div>
                                <div className="text-3xl font-bold" style={{ color: 'var(--admin-red)' }}>
                                    {amlLoading ? '...' : amlFlags.length}
                                </div>
                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                    Urgent Action
                                </p>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(239, 68, 68, 0.1)' }}>
                                <Shield className="w-6 h-6" style={{ color: 'var(--admin-red)' }} />
                            </div>
                        </div>
                    </div>

                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">Compliance Score</div>
                                <div className="text-3xl font-bold" style={{ color: 'var(--admin-green)' }}>
                                    {complianceScore}%
                                </div>
                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                    {parseFloat(complianceScore) >= 95 ? 'Excellent' : 'Good'}
                                </p>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(16, 185, 129, 0.1)' }}>
                                <CheckCircle className="w-6 h-6" style={{ color: 'var(--admin-green)' }} />
                            </div>
                        </div>
                    </div>
                </div>

                {/* KYC Verification Queue */}
                <div className="admin-card">
                    <h2 className="text-xl font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Pending Verifications
                    </h2>
                    {kycLoading ? (
                        <div className="flex items-center justify-center py-12">
                            <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                        </div>
                    ) : (
                        <div className="overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>User</th>
                                        <th>Submission Date</th>
                                        <th>Risk Level</th>
                                        <th>Status</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {kycSubmissions.map((submission: any) => (
                                        <tr key={submission.id}>
                                            <td className="font-medium">
                                                <div>
                                                    <div>{submission.email || submission.user_id}</div>
                                                    {submission.full_name && (
                                                        <div className="text-xs" style={{ color: 'var(--admin-text-muted)' }}>
                                                            {submission.full_name}
                                                        </div>
                                                    )}
                                                </div>
                                            </td>
                                            <td>{formatDate(submission.submitted_at)}</td>
                                            <td>
                                                <span className={`admin-badge ${getRiskBadgeClass(submission.risk_level || 'low')}`}>
                                                    {submission.risk_level || 'Low'}
                                                </span>
                                            </td>
                                            <td>
                                                <span className={`admin-badge ${getStatusBadgeClass(submission.status)}`}>
                                                    {submission.status?.replace('_', ' ')}
                                                </span>
                                            </td>
                                            <td>
                                                <div className="flex gap-2">
                                                    <button
                                                        className="admin-btn admin-btn-primary px-3 py-1 text-xs"
                                                        title="Approve KYC"
                                                    >
                                                        <Check className="w-3 h-3" />
                                                    </button>
                                                    <button
                                                        className="admin-btn admin-btn-danger px-3 py-1 text-xs"
                                                        title="Reject KYC"
                                                    >
                                                        <X className="w-3 h-3" />
                                                    </button>
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                    {kycSubmissions.length === 0 && (
                                        <tr>
                                            <td colSpan={5} className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                                No pending KYC submissions
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>

                {/* Bottom Grid */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* AML Monitoring */}
                    <div className="admin-card">
                        <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            AML Monitoring - Suspicious Activity Alerts
                        </h3>
                        {amlLoading ? (
                            <div className="flex items-center justify-center py-8">
                                <Loader2 className="w-6 h-6 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                            </div>
                        ) : (
                            <div className="space-y-3">
                                {amlFlags.slice(0, 3).map((flag: any) => (
                                    <div key={flag.id} className="flex items-start gap-3 p-3 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                                        <div
                                            className="w-2 h-2 mt-2 rounded-full"
                                            style={{ background: flag.risk_score > 70 ? 'var(--admin-red)' : 'var(--admin-yellow)' }}
                                        />
                                        <div className="flex-1">
                                            <p className="text-sm font-medium" style={{ color: 'var(--admin-text-primary)' }}>
                                                {flag.alert_type?.replace('_', ' ').toUpperCase()}
                                            </p>
                                            <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                                User: {flag.email || flag.user_id}
                                            </p>
                                            {flag.details && (
                                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                                                    {flag.details}
                                                </p>
                                            )}
                                        </div>
                                        <span className="text-xs font-medium" style={{ color: 'var(--admin-red)' }}>
                                            Risk: {flag.risk_score || 'N/A'}
                                        </span>
                                    </div>
                                ))}
                                {amlFlags.length === 0 && (
                                    <div className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                        No AML alerts
                                    </div>
                                )}
                                {amlFlags.length > 0 && (
                                    <button className="admin-btn admin-btn-secondary w-full mt-3">
                                        View All Alerts ({amlFlags.length})
                                    </button>
                                )}
                            </div>
                        )}
                    </div>

                    {/* Regulatory Reports */}
                    <div className="admin-card">
                        <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            Regulatory Reports
                        </h3>
                        <div className="space-y-3">
                            {[
                                { name: "Monthly Transaction Report", type: "PDF", date: "Dec 2024" },
                                { name: "KYC Compliance Summary", type: "Excel", date: "Q4 2024" },
                                { name: "AML Activity Log", type: "PDF", date: "Dec 2024" },
                                { name: "User Verification Stats", type: "Excel", date: "2024" }
                            ].map((report, idx) => (
                                <div key={idx} className="flex items-center justify-between p-3 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                                    <div className="flex items-center gap-3">
                                        <FileText className="w-5 h-5" style={{ color: 'var(--admin-blue)' }} />
                                        <div>
                                            <p className="text-sm font-medium" style={{ color: 'var(--admin-text-primary)' }}>
                                                {report.name}
                                            </p>
                                            <p className="text-xs" style={{ color: 'var(--admin-text-muted)' }}>
                                                {report.type} • {report.date}
                                            </p>
                                        </div>
                                    </div>
                                    <button className="admin-btn admin-btn-secondary px-3 py-1 text-xs">
                                        <Download className="w-3 h-3" />
                                    </button>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>

                {/* Audit Preparation Banner */}
                <div className="admin-card" style={{ background: 'linear-gradient(to right, rgba(59, 130, 246, 0.1), rgba(59, 130, 246, 0.05))' }}>
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4">
                            <div className="p-3 rounded-lg" style={{ background: 'var(--admin-blue)' }}>
                                <FileText className="w-6 h-6 text-white" />
                            </div>
                            <div>
                                <p className="font-semibold" style={{ color: 'var(--admin-text-primary)' }}>
                                    Next Regulatory Audit: February 2024
                                </p>
                                <p className="text-sm" style={{ color: 'var(--admin-text-secondary)' }}>
                                    Prepare compliance documentation and reports
                                </p>
                            </div>
                        </div>
                        <button className="admin-btn admin-btn-primary">
                            Prepare Report
                        </button>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
