/** Legal documents and consent (/api/legal), Section H Step 8.2. */
import { API_BASE, apiFetch, responseErrorMessage } from "./http";
import type { LegalDocument, LegalStatus } from "@/utils/legalDocuments";

export interface CurrentDocumentsResponse {
    documents: LegalDocument[];
    purpose: string | null;
    unpublished: string[];
}

export interface WithdrawResult {
    category: string;
    withdrawn: number;
    effects: Array<{ code: string; description: string }>;
    has_current_acceptance: boolean;
}

const LEGAL = `${API_BASE}/api/legal`;

/** Public: current published versions (optionally only those a purpose requires). */
export async function getCurrentDocuments(params: { purpose?: string; categories?: string[]; locale?: string } = {}):
    Promise<CurrentDocumentsResponse> {
    const query = new URLSearchParams();
    if (params.purpose) query.set("purpose", params.purpose);
    if (params.locale) query.set("locale", params.locale);
    for (const category of params.categories ?? []) query.append("category", category);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    const res = await fetch(`${LEGAL}/documents${suffix}`);
    if (!res.ok) throw new Error(await responseErrorMessage(res, "Could not load legal documents"));
    return res.json();
}

export async function getLegalStatus(): Promise<LegalStatus> {
    const res = await apiFetch(`${LEGAL}/status`);
    if (!res.ok) throw new Error(await responseErrorMessage(res, "Could not load consent status"));
    return res.json();
}

export async function acceptDocuments(documents: Array<{ doc_id: string; content_sha256: string }>): Promise<void> {
    const res = await apiFetch(`${LEGAL}/accept`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ documents }),
    });
    if (!res.ok) throw new Error(await responseErrorMessage(res, "Could not record your acceptance"));
}

export async function withdrawConsent(category: string, reason?: string): Promise<WithdrawResult> {
    const res = await apiFetch(`${LEGAL}/withdraw`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category, reason }),
    });
    if (!res.ok) throw new Error(await responseErrorMessage(res, "Could not withdraw consent"));
    return res.json();
}
