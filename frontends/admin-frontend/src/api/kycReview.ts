/**
 * Admin KYC review API (main backend, /api/admin/compliance/*).
 *
 * A submitted KYC case is never approved automatically: the approve endpoint
 * below is the only way a case becomes "approved". Every call goes through
 * `apiFetch`, and a failed call throws an Error carrying the backend's own
 * message (FastAPI `detail`).
 */
import { apiFetch, responseErrorMessage } from "./http";

const API_BASE: string = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const COMPLIANCE_PATH = "/api/admin/compliance";

/** React Query key of the review queue (shared by the page and the review dialog). */
export const KYC_QUEUE_QUERY_KEY = ["adminPendingKYC"] as const;

export function kycStatusBadgeClass(status: string | null | undefined): string {
    switch ((status || "").toLowerCase()) {
        case "approved":
            return "admin-badge-success";
        case "rejected":
            return "admin-badge-danger";
        case "submitted":
        case "needs_resubmission":
            return "admin-badge-warning";
        default:
            return "admin-badge-info";
    }
}

export interface KycQueueItem {
    id: string;
    user_id: string;
    email: string | null;
    full_name: string | null;
    document_type: string | null;
    status: string;
    submitted_at: string | null;
    created_at: string | null;
    updated_at: string | null;
}

export interface KycQueueResponse {
    submissions: KycQueueItem[];
    count: number;
}

export interface KycProfile {
    full_legal_name: string | null;
    date_of_birth: string | null;
    nationality: string | null;
    country_of_residence: string | null;
    address_line1: string | null;
    address_city: string | null;
    address_state: string | null;
    address_postal_code: string | null;
    phone: string | null;
}

export interface KycDocument {
    id: string;
    doc_type: string | null;
    issuing_country: string | null;
    status: string | null;
    uploaded_at: string | null;
    has_front: boolean;
    has_back: boolean;
    front_url: string | null;
    back_url: string | null;
}

export interface KycSelfie {
    id: string;
    status: string | null;
    completed_at: string | null;
    has_file: boolean;
    url: string | null;
}

export interface KycReview {
    id: string;
    reviewer_id: string | null;
    reviewer_type: string | null;
    decision: string | null;
    reason_codes: string[];
    reason: string | null;
    created_at: string | null;
}

export interface KycCaseDetail {
    id: string;
    user_id: string;
    email: string | null;
    status: string;
    submitted_at: string | null;
    approved_at: string | null;
    rejected_at: string | null;
    rejection_reason: string | null;
    created_at: string | null;
    updated_at: string | null;
    profile: KycProfile | null;
    documents: KycDocument[];
    selfie: KycSelfie | null;
    reviews: KycReview[];
    awaiting_review: boolean;
    evidence_problems: string[];
    can_approve: boolean;
}

export type KycDecision = "approve" | "reject" | "request-resubmission";

export interface KycDecisionResponse {
    message: string;
    case_id: string;
    status: string;
}

export interface KycFile {
    blob: Blob;
    /** Media type without parameters, lower-cased (for example "image/jpeg"). */
    contentType: string;
}

async function getJson<T>(path: string, fallback: string, signal?: AbortSignal): Promise<T> {
    const response = await apiFetch(`${API_BASE}${path}`, { signal });
    if (!response.ok) {
        throw new Error(await responseErrorMessage(response, fallback));
    }
    return (await response.json()) as T;
}

/** Cases awaiting review (status "submitted" or "under_review"). */
export function getKycQueue(signal?: AbortSignal): Promise<KycQueueResponse> {
    return getJson<KycQueueResponse>(`${COMPLIANCE_PATH}/kyc-pending`, "Failed to load the KYC review queue", signal);
}

/** Full case for review. The backend records each call in the KYC audit log. */
export function getKycCase(caseId: string, signal?: AbortSignal): Promise<KycCaseDetail> {
    return getJson<KycCaseDetail>(
        `${COMPLIANCE_PATH}/kyc/${encodeURIComponent(caseId)}`,
        "Failed to load the KYC case",
        signal,
    );
}

/**
 * Download a document image or selfie. `path` is the server-relative URL the
 * case detail returns (`front_url`, `back_url`, `selfie.url`). Anything that is
 * not a path under the compliance API is refused, so the admin token is never
 * sent anywhere else.
 */
export async function fetchKycFile(path: string, signal?: AbortSignal): Promise<KycFile> {
    if (!path.startsWith(`${COMPLIANCE_PATH}/kyc/`)) {
        throw new Error("Unexpected file location returned by the server");
    }
    const response = await apiFetch(`${API_BASE}${path}`, { signal });
    if (!response.ok) {
        throw new Error(await responseErrorMessage(response, "Failed to load the file"));
    }
    const blob = await response.blob();
    const header = response.headers.get("Content-Type") || blob.type || "";
    const contentType = header.split(";", 1)[0].trim().toLowerCase();
    return { blob, contentType };
}

/**
 * Record a review decision. Rejecting and requesting a resubmission need a
 * reason (the backend answers 400 without one); approving takes none.
 */
export async function decideKycCase(caseId: string, decision: KycDecision, reason?: string): Promise<KycDecisionResponse> {
    const url = `${API_BASE}${COMPLIANCE_PATH}/kyc/${encodeURIComponent(caseId)}/${decision}`;
    const init: RequestInit = decision === "approve"
        ? { method: "POST" }
        : {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ reason: (reason ?? "").trim() }),
        };
    const response = await apiFetch(url, init);
    if (!response.ok) {
        throw new Error(await responseErrorMessage(response, "The KYC decision was not saved"));
    }
    return (await response.json()) as KycDecisionResponse;
}
