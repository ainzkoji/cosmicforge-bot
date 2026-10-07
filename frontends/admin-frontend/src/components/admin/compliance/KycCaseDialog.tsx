import { useEffect, useState } from "react";
import type { ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, Check, ExternalLink, FileText, Loader2, X } from "lucide-react";
import Modal from "@/components/UI/Modal";
import { KYC_QUEUE_QUERY_KEY, decideKycCase, fetchKycFile, getKycCase, kycStatusBadgeClass } from "@/api/kycReview";
import type { KycCaseDetail, KycDecision, KycDecisionResponse, KycDocument } from "@/api/kycReview";

/** File types that are safe to open in their own tab from an object URL. */
const IMAGE_TYPES = ["image/jpeg", "image/png"];
const PDF_TYPE = "application/pdf";

const DECISION_COPY: Record<KycDecision, { title: string; confirm: string; pending: string; help: string }> = {
    "approve": {
        title: "Approve this KYC case?",
        confirm: "Approve",
        pending: "Approving…",
        help: "The applicant will be marked as verified. Approve only after checking the details, every document image and the selfie above.",
    },
    "reject": {
        title: "Reject this KYC case",
        confirm: "Reject",
        pending: "Rejecting…",
        help: "The case will be marked as rejected. A reason is required and is recorded with the decision.",
    },
    "request-resubmission": {
        title: "Request resubmission",
        confirm: "Request resubmission",
        pending: "Sending…",
        help: "The case is sent back to the applicant for corrections. A reason is required and is recorded with the decision.",
    },
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

type FileState =
    | { status: "loading" }
    | { status: "ready"; objectUrl: string; contentType: string }
    | { status: "error"; message: string };

/**
 * One stored file (document side or selfie). The file is fetched with the
 * admin token, shown from an object URL, and the URL is revoked on unmount.
 * Mount with `key={path}` so a different file starts from the loading state.
 */
function KycFileView({ path, label }: { path: string; label: string }) {
    const [state, setState] = useState<FileState>({ status: "loading" });

    useEffect(() => {
        const controller = new AbortController();
        let cancelled = false;
        let objectUrl: string | null = null;

        fetchKycFile(path, controller.signal).then(
            (file) => {
                if (cancelled) return;
                objectUrl = URL.createObjectURL(file.blob);
                setState({ status: "ready", objectUrl, contentType: file.contentType });
            },
            (error: unknown) => {
                if (cancelled) return;
                setState({ status: "error", message: errorText(error) });
            },
        );

        return () => {
            cancelled = true;
            controller.abort();
            if (objectUrl) URL.revokeObjectURL(objectUrl);
        };
    }, [path]);

    let content: ReactNode;
    if (state.status === "loading") {
        content = (
            <div className="flex h-40 items-center justify-center text-muted-foreground">
                <Loader2 className="h-5 w-5 animate-spin" />
            </div>
        );
    } else if (state.status === "error") {
        content = (
            <div role="alert" className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-100">
                {state.message}
            </div>
        );
    } else if (IMAGE_TYPES.includes(state.contentType)) {
        content = (
            <div className="space-y-2">
                <img
                    src={state.objectUrl}
                    alt={label}
                    className="max-h-96 w-full rounded-lg bg-black/40 object-contain"
                />
                <a
                    href={state.objectUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
                >
                    <ExternalLink className="h-3 w-3" />
                    Open full size in a new tab
                </a>
            </div>
        );
    } else if (state.contentType === PDF_TYPE) {
        content = (
            <a
                href={state.objectUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 rounded-lg border border-border px-3 py-2 text-sm text-primary hover:underline"
            >
                <FileText className="h-4 w-4" />
                Open PDF in a new tab
                <ExternalLink className="h-3 w-3" />
            </a>
        );
    } else {
        content = (
            <div className="space-y-2 text-sm text-muted-foreground">
                <div>This file type ({state.contentType || "unknown"}) cannot be previewed here.</div>
                <a
                    href={state.objectUrl}
                    download={label.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}
                    className="inline-flex items-center gap-1 text-primary hover:underline"
                >
                    <FileText className="h-4 w-4" />
                    Download file
                </a>
            </div>
        );
    }

    return (
        <div className="space-y-2">
            <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{label}</div>
            {content}
        </div>
    );
}

function MissingFile({ label }: { label: string }) {
    return (
        <div className="space-y-2">
            <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{label}</div>
            <div className="rounded-lg border border-dashed border-border px-3 py-6 text-center text-sm text-muted-foreground">
                No file on record
            </div>
        </div>
    );
}

function Field({ label, value }: { label: string; value: string | null | undefined }) {
    return (
        <div>
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd className="break-words text-sm text-foreground">{value || "—"}</dd>
        </div>
    );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
    return (
        <section className="space-y-3">
            <h3 className="text-sm font-semibold text-foreground">{title}</h3>
            {children}
        </section>
    );
}

function DocumentCard({ doc }: { doc: KycDocument }) {
    const name = humanize(doc.doc_type);
    return (
        <div className="space-y-3 rounded-lg border border-border p-4">
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
                <span className="font-medium capitalize text-foreground">{name}</span>
                <span className="text-muted-foreground">Issuing country: {doc.issuing_country || "—"}</span>
                <span className="text-muted-foreground">Uploaded: {formatDateTime(doc.uploaded_at)}</span>
                <span className="text-muted-foreground">Document status: {humanize(doc.status)}</span>
            </div>
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                {doc.front_url
                    ? <KycFileView key={doc.front_url} path={doc.front_url} label={`${name} front`} />
                    : <MissingFile label={`${name} front`} />}
                {doc.back_url
                    ? <KycFileView key={doc.back_url} path={doc.back_url} label={`${name} back`} />
                    : <MissingFile label={`${name} back`} />}
            </div>
        </div>
    );
}

function CaseBody({ detail }: { detail: KycCaseDetail }) {
    const profile = detail.profile;
    return (
        <>
            {!detail.awaiting_review && (
                <div className="rounded-lg border border-border bg-muted/30 px-4 py-3 text-sm text-foreground">
                    This case is “{humanize(detail.status)}” and is not awaiting review, so no decision can be recorded.
                    {detail.rejection_reason ? ` Recorded reason: ${detail.rejection_reason}` : ""}
                </div>
            )}

            {detail.awaiting_review && detail.evidence_problems.length > 0 && (
                <div role="alert" className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-foreground">
                    <div className="font-semibold">This case cannot be approved yet:</div>
                    <ul className="mt-1 list-disc pl-5">
                        {detail.evidence_problems.map((problem) => (
                            <li key={problem}>{problem}</li>
                        ))}
                    </ul>
                </div>
            )}

            <Section title="Applicant">
                <dl className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
                    <Field label="Account email" value={detail.email} />
                    <Field label="User ID" value={detail.user_id} />
                    <Field label="Submitted" value={formatDateTime(detail.submitted_at)} />
                    {profile && (
                        <>
                            <Field label="Full legal name" value={profile.full_legal_name} />
                            <Field label="Date of birth" value={profile.date_of_birth} />
                            <Field label="Phone" value={profile.phone} />
                            <Field label="Nationality" value={profile.nationality} />
                            <Field label="Country of residence" value={profile.country_of_residence} />
                            <Field label="Address" value={profile.address_line1} />
                            <Field label="City" value={profile.address_city} />
                            <Field label="State / region" value={profile.address_state} />
                            <Field label="Postal code" value={profile.address_postal_code} />
                        </>
                    )}
                </dl>
                {!profile && (
                    <div className="text-sm text-muted-foreground">The applicant has not submitted personal details.</div>
                )}
            </Section>

            <Section title="Identity documents">
                {detail.documents.length === 0 ? (
                    <div className="text-sm text-muted-foreground">No identity document has been uploaded.</div>
                ) : (
                    <div className="space-y-4">
                        {detail.documents.map((doc) => <DocumentCard key={doc.id} doc={doc} />)}
                    </div>
                )}
            </Section>

            <Section title="Selfie">
                {detail.selfie ? (
                    <div className="space-y-3 rounded-lg border border-border p-4">
                        <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm text-muted-foreground">
                            <span>Selfie status: {humanize(detail.selfie.status)}</span>
                            <span>Completed: {formatDateTime(detail.selfie.completed_at)}</span>
                        </div>
                        <div className="text-xs text-muted-foreground">
                            No automated face match is performed. Compare the selfie with the document photo yourself.
                        </div>
                        <div className="md:max-w-md">
                            {detail.selfie.url
                                ? <KycFileView key={detail.selfie.url} path={detail.selfie.url} label="Selfie" />
                                : <MissingFile label="Selfie" />}
                        </div>
                    </div>
                ) : (
                    <div className="text-sm text-muted-foreground">No selfie has been submitted.</div>
                )}
            </Section>

            <Section title="Review history">
                {detail.reviews.length === 0 ? (
                    <div className="text-sm text-muted-foreground">No review decision has been recorded for this case.</div>
                ) : (
                    <ul className="space-y-2">
                        {detail.reviews.map((review) => (
                            <li key={review.id} className="rounded-lg bg-muted/30 px-4 py-3 text-sm text-foreground">
                                <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                                    <span className={`admin-badge ${kycStatusBadgeClass(review.decision)}`}>{humanize(review.decision)}</span>
                                    <span className="text-xs text-muted-foreground">
                                        {formatDateTime(review.created_at)} · {review.reviewer_type || "reviewer"} {review.reviewer_id || ""}
                                    </span>
                                </div>
                                {review.reason && <div className="mt-1 whitespace-pre-wrap">{review.reason}</div>}
                                {review.reason_codes.length > 0 && (
                                    <div className="mt-1 text-xs text-muted-foreground">Codes: {review.reason_codes.join(", ")}</div>
                                )}
                            </li>
                        ))}
                    </ul>
                )}
            </Section>
        </>
    );
}

interface KycCaseContentProps {
    caseId: string;
    onClose: () => void;
    onDecided: (response: KycDecisionResponse, applicant: string) => void;
}

function KycCaseContent({ caseId, onClose, onDecided }: KycCaseContentProps) {
    const queryClient = useQueryClient();
    const [decision, setDecision] = useState<KycDecision | null>(null);
    const [reason, setReason] = useState("");

    const caseQueryKey = ["adminKYCCase", caseId] as const;
    const caseQuery = useQuery({
        queryKey: caseQueryKey,
        queryFn: ({ signal }) => getKycCase(caseId, signal),
        // Every load is written to the KYC audit log; do not reload in the background.
        staleTime: Infinity,
        gcTime: 0,
        retry: false,
    });
    const detail = caseQuery.data;

    const decisionMutation = useMutation({
        mutationFn: (vars: { decision: KycDecision; reason: string }) =>
            decideKycCase(caseId, vars.decision, vars.reason),
        onSuccess: (response: KycDecisionResponse) => {
            queryClient.invalidateQueries({ queryKey: KYC_QUEUE_QUERY_KEY });
            onDecided(response, detail?.email || detail?.user_id || caseId);
        },
        onError: () => {
            // The case may have been decided elsewhere; show its current state.
            queryClient.invalidateQueries({ queryKey: KYC_QUEUE_QUERY_KEY });
            queryClient.invalidateQueries({ queryKey: caseQueryKey });
        },
    });

    const pending = decisionMutation.isPending;
    const needsReason = decision !== null && decision !== "approve";
    const reasonReady = !needsReason || reason.trim().length > 0;
    const canDecide = detail !== undefined && detail.awaiting_review;

    const startDecision = (next: KycDecision) => {
        decisionMutation.reset();
        setReason("");
        setDecision(next);
    };

    const cancelDecision = () => {
        decisionMutation.reset();
        setDecision(null);
    };

    const confirmDecision = () => {
        if (decision === null || !reasonReady || pending) return;
        decisionMutation.mutate({ decision, reason: reason.trim() });
    };

    const copy = decision ? DECISION_COPY[decision] : null;

    return (
        <>
            <div className="flex items-center justify-between gap-4 border-b border-border px-6 py-4">
                <div className="min-w-0">
                    <div className="text-lg font-semibold text-foreground">KYC review</div>
                    <div className="truncate font-mono text-xs text-muted-foreground">
                        {detail?.email || detail?.user_id || caseId}
                    </div>
                </div>
                <div className="flex items-center gap-3">
                    {detail && (
                        <span className={`admin-badge ${kycStatusBadgeClass(detail.status)}`}>{humanize(detail.status)}</span>
                    )}
                    <button
                        type="button"
                        className="text-muted-foreground transition hover:text-foreground"
                        onClick={onClose}
                        disabled={pending}
                        aria-label="Close"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>
            </div>

            <div className="min-h-0 flex-1 space-y-6 overflow-y-auto px-6 py-6">
                {caseQuery.isPending && (
                    <div className="flex items-center justify-center py-16 text-muted-foreground">
                        <Loader2 className="h-8 w-8 animate-spin" />
                    </div>
                )}
                {caseQuery.isError && (
                    <div role="alert" className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                        <div className="flex items-start gap-2">
                            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                            <div>
                                <div className="font-semibold">The KYC case could not be loaded.</div>
                                <div className="mt-1">{errorText(caseQuery.error)}</div>
                            </div>
                        </div>
                        <button
                            type="button"
                            className="admin-btn admin-btn-secondary mt-3 text-xs"
                            onClick={() => caseQuery.refetch()}
                            disabled={caseQuery.isFetching}
                        >
                            Try again
                        </button>
                    </div>
                )}
                {detail && <CaseBody detail={detail} />}
            </div>

            <div className="space-y-4 border-t border-border px-6 py-4">
                {copy && decision && (
                    <div className="space-y-3">
                        <div className="text-sm font-semibold text-foreground">{copy.title}</div>
                        <div className="text-sm text-muted-foreground">{copy.help}</div>
                        {needsReason && (
                            <div className="space-y-2">
                                <label htmlFor="kyc-decision-reason" className="block text-sm font-medium text-foreground">
                                    Reason (required)
                                </label>
                                <textarea
                                    id="kyc-decision-reason"
                                    value={reason}
                                    onChange={(event) => setReason(event.target.value)}
                                    disabled={pending}
                                    className="min-h-[80px] w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                                    placeholder={decision === "reject"
                                        ? "Why is this case being rejected?"
                                        : "What does the applicant need to correct?"}
                                />
                            </div>
                        )}
                    </div>
                )}

                {decisionMutation.isError && (
                    <div role="alert" className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                        {errorText(decisionMutation.error)}
                    </div>
                )}

                {decision && copy ? (
                    <div className="flex items-center justify-end gap-3">
                        <button type="button" className="admin-btn admin-btn-secondary" onClick={cancelDecision} disabled={pending}>
                            Back
                        </button>
                        <button
                            type="button"
                            className={`admin-btn ${decision === "reject" ? "admin-btn-danger" : "admin-btn-primary"} flex items-center gap-2 disabled:cursor-not-allowed disabled:opacity-50`}
                            onClick={confirmDecision}
                            disabled={!canDecide || !reasonReady || pending}
                        >
                            {pending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                            {pending ? copy.pending : copy.confirm}
                        </button>
                    </div>
                ) : (
                    <div className="flex flex-wrap items-center justify-end gap-3">
                        <button type="button" className="admin-btn admin-btn-secondary" onClick={onClose}>
                            Close
                        </button>
                        <button
                            type="button"
                            className="admin-btn admin-btn-secondary disabled:cursor-not-allowed disabled:opacity-50"
                            onClick={() => startDecision("request-resubmission")}
                            disabled={!canDecide}
                        >
                            Request resubmission
                        </button>
                        <button
                            type="button"
                            className="admin-btn admin-btn-danger flex items-center gap-2 disabled:cursor-not-allowed disabled:opacity-50"
                            onClick={() => startDecision("reject")}
                            disabled={!canDecide}
                        >
                            <X className="h-4 w-4" />
                            Reject
                        </button>
                        <button
                            type="button"
                            className="admin-btn admin-btn-primary flex items-center gap-2 disabled:cursor-not-allowed disabled:opacity-50"
                            onClick={() => startDecision("approve")}
                            disabled={!canDecide || !detail?.can_approve}
                            title={canDecide && !detail?.can_approve ? "Required evidence is missing" : undefined}
                        >
                            <Check className="h-4 w-4" />
                            Approve
                        </button>
                    </div>
                )}
            </div>
        </>
    );
}

interface KycCaseDialogProps {
    /** The `kyc_cases.id` to review, or null when the dialog is closed. */
    caseId: string | null;
    onClose: () => void;
    onDecided: (response: KycDecisionResponse, applicant: string) => void;
}

/** Review dialog for one KYC case: applicant details, documents, selfie and the decision. */
export function KycCaseDialog({ caseId, onClose, onDecided }: KycCaseDialogProps) {
    return (
        <Modal isOpen={caseId !== null} onClose={onClose}>
            {caseId !== null && (
                <KycCaseContent key={caseId} caseId={caseId} onClose={onClose} onDecided={onDecided} />
            )}
        </Modal>
    );
}
