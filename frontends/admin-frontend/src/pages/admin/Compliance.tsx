import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertCircle, AlertTriangle, Eye, Loader2, RefreshCw, Shield, X } from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { ExportButton } from "@/components/admin/common/ExportButton";
import { KycCaseDialog } from "@/components/admin/compliance/KycCaseDialog";
import { getAMLFlags } from "@/api/admin";
import { KYC_QUEUE_QUERY_KEY, getKycQueue, kycStatusBadgeClass } from "@/api/kycReview";
import type { KycDecisionResponse } from "@/api/kycReview";

/** Row of GET /api/admin/compliance/aml-flags (open rows of the `aml_alerts` table). */
interface AmlAlert {
    id: string;
    user_id: string;
    email: string | null;
    alert_type: string | null;
    severity: string | null;
    description: string | null;
    status: string | null;
    created_at: string | null;
}

const DECISION_LABELS: Record<string, string> = {
    approved: "approved",
    rejected: "rejected",
    needs_resubmission: "sent back for resubmission",
};

function errorText(error: unknown): string {
    return error instanceof Error && error.message ? error.message : "Request failed";
}

function formatDateTime(value: string | null | undefined): string {
    if (!value) return "—";
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function humanize(value: string | null | undefined): string {
    return value ? value.replace(/_/g, " ") : "—";
}

export default function Compliance() {
    const [reviewCaseId, setReviewCaseId] = useState<string | null>(null);
    const [notice, setNotice] = useState<string | null>(null);

    const kycQuery = useQuery({
        queryKey: KYC_QUEUE_QUERY_KEY,
        queryFn: ({ signal }) => getKycQueue(signal),
    });

    const amlQuery = useQuery({
        queryKey: ["adminAMLFlags"],
        queryFn: getAMLFlags,
    });

    const kycSubmissions = kycQuery.data?.submissions ?? [];
    const amlAlerts: AmlAlert[] = amlQuery.data?.flags ?? [];

    const openReview = (caseId: string) => {
        setNotice(null);
        setReviewCaseId(caseId);
    };

    const handleDecided = (response: KycDecisionResponse, applicant: string) => {
        setReviewCaseId(null);
        setNotice(`KYC case for ${applicant} was ${DECISION_LABELS[response.status] ?? response.status} (confirmed by the server).`);
    };

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                        Compliance
                    </h1>
                    <div className="flex gap-3">
                        <ExportButton
                            data={[...kycSubmissions, ...amlAlerts]}
                            filename="compliance_data"
                            label="Export"
                        />
                    </div>
                </div>
                <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                    Review identity verification (KYC) cases. A submitted case is never approved automatically: it stays
                    in this queue until an admin approves it, rejects it or sends it back.
                </p>

                {notice && (
                    <div role="status" className="rounded-lg border border-green-500/30 bg-green-500/10 px-4 py-3 text-sm text-green-200 flex items-start justify-between gap-3">
                        <span>{notice}</span>
                        <button type="button" onClick={() => setNotice(null)} aria-label="Dismiss">
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                )}

                {/* Status Cards */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">KYC awaiting review</div>
                                <div className="text-3xl font-bold" style={{ color: 'var(--admin-yellow)' }}>
                                    {kycQuery.isPending ? '...' : kycQuery.isError ? 'Unavailable' : kycSubmissions.length}
                                </div>
                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                    Submitted cases with no decision yet
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
                                <div className="admin-metric-label mb-2">Open AML alerts</div>
                                <div className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                                    {amlQuery.isPending ? '...' : amlQuery.isError ? 'Unavailable' : amlAlerts.length}
                                </div>
                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                    Automated AML monitoring is not implemented
                                </p>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(239, 68, 68, 0.1)' }}>
                                <Shield className="w-6 h-6" style={{ color: 'var(--admin-red)' }} />
                            </div>
                        </div>
                    </div>
                </div>

                {/* KYC Verification Queue */}
                <div className="admin-card">
                    <div className="flex items-center justify-between gap-4 mb-4">
                        <h2 className="text-xl font-semibold" style={{ color: 'var(--admin-text-primary)' }}>
                            KYC review queue
                        </h2>
                        <button
                            type="button"
                            className="admin-btn admin-btn-secondary text-sm flex items-center gap-2"
                            onClick={() => kycQuery.refetch()}
                            disabled={kycQuery.isFetching}
                        >
                            <RefreshCw className={`w-4 h-4 ${kycQuery.isFetching ? "animate-spin" : ""}`} />
                            Refresh
                        </button>
                    </div>

                    {kycQuery.isError && (
                        <div role="alert" className="mb-4 rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                            <div className="flex items-start gap-2">
                                <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
                                <div>
                                    <div className="font-semibold">The KYC review queue could not be loaded.</div>
                                    <div className="mt-1">{errorText(kycQuery.error)}</div>
                                </div>
                            </div>
                        </div>
                    )}

                    {kycQuery.isPending ? (
                        <div className="flex items-center justify-center py-12">
                            <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                        </div>
                    ) : kycQuery.isError ? null : (
                        <div className="overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>Applicant</th>
                                        <th>Document</th>
                                        <th>Submitted</th>
                                        <th>Status</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {kycSubmissions.map((submission) => (
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
                                            <td className="capitalize">{humanize(submission.document_type)}</td>
                                            <td>{formatDateTime(submission.submitted_at)}</td>
                                            <td>
                                                <span className={`admin-badge ${kycStatusBadgeClass(submission.status)}`}>
                                                    {humanize(submission.status)}
                                                </span>
                                            </td>
                                            <td>
                                                <button
                                                    type="button"
                                                    className="admin-btn admin-btn-primary px-3 py-1 text-xs flex items-center gap-2"
                                                    onClick={() => openReview(submission.id)}
                                                >
                                                    <Eye className="w-3 h-3" />
                                                    Review
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                    {kycSubmissions.length === 0 && (
                                        <tr>
                                            <td colSpan={5} className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                                No KYC cases are awaiting review
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
                    {/* AML alerts */}
                    <div className="admin-card">
                        <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            AML alerts
                        </h3>
                        {amlQuery.isPending ? (
                            <div className="flex items-center justify-center py-8">
                                <Loader2 className="w-6 h-6 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                            </div>
                        ) : amlQuery.isError ? (
                            <div role="alert" className="rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                                AML alerts could not be loaded. {errorText(amlQuery.error)}
                            </div>
                        ) : (
                            <div className="space-y-3">
                                {amlAlerts.map((alert) => (
                                    <div key={alert.id} className="flex items-start gap-3 p-3 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                                        <div className="flex-1">
                                            <p className="text-sm font-medium uppercase" style={{ color: 'var(--admin-text-primary)' }}>
                                                {humanize(alert.alert_type)}
                                            </p>
                                            <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                                User: {alert.email || alert.user_id} · {formatDateTime(alert.created_at)}
                                            </p>
                                            {alert.description && (
                                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                                                    {alert.description}
                                                </p>
                                            )}
                                        </div>
                                        <span className="text-xs font-medium" style={{ color: 'var(--admin-text-secondary)' }}>
                                            Severity: {alert.severity || 'n/a'}
                                        </span>
                                    </div>
                                ))}
                                {amlAlerts.length === 0 && (
                                    <div className="text-center py-8 text-sm" style={{ color: 'var(--admin-text-muted)' }}>
                                        No AML alerts on record. The platform does not run automated transaction
                                        monitoring yet, so nothing creates alerts; an empty list here is not evidence
                                        that activity has been screened.
                                    </div>
                                )}
                            </div>
                        )}
                    </div>

                    {/* Regulatory reports */}
                    <div className="admin-card">
                        <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                            Regulatory reports
                        </h3>
                        <div className="text-center py-8 text-sm" style={{ color: 'var(--admin-text-muted)' }}>
                            No reports available. Regulatory report generation is not implemented. Use Export above
                            to download the current review queue.
                        </div>
                    </div>
                </div>
            </div>

            <KycCaseDialog
                caseId={reviewCaseId}
                onClose={() => setReviewCaseId(null)}
                onDecided={handleDecided}
            />
        </AdminLayout>
    );
}
