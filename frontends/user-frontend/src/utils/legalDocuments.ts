/**
 * Legal document helpers (Section H Step 8.2). Pure: no React, no network.
 *
 * A document is a DRAFT unless the server reports COUNSEL_APPROVED; the
 * client never upgrades a document's status on its own.
 */

export interface LegalDocument {
    doc_id: string;
    category: string;
    version: number;
    locale: string;
    title: string;
    body: string;
    content_sha256: string;
    published_at: string;
    material_change: boolean;
    review_status: string;
    is_draft?: boolean;
    change_summary?: string | null;
}

export interface PendingItem {
    reason: string;
    accepted_version: number | null;
    document: LegalDocument;
}

export interface LegalStatus {
    pending: PendingItem[];
    current: string[];
    unpublished: string[];
    reconsent_required: boolean;
}

export type LegalBlock =
    | { kind: "heading"; text: string }
    | { kind: "paragraph"; text: string }
    | { kind: "list"; items: string[] };

export const COUNSEL_APPROVED = "COUNSEL_APPROVED";

export const CATEGORY_LABELS: Record<string, string> = {
    TERMS_OF_SERVICE: "Terms of Service",
    PRIVACY_POLICY: "Privacy Policy",
    TRADING_RISK_DISCLOSURE: "Risk Disclosure",
    ACCOUNT_CONNECTION_AUTHORIZATION: "Account Connection Authorization",
    AUTOMATED_EXECUTION_CONSENT: "Automated Execution Consent",
    DATA_PROCESSING_NOTICE: "Data Processing Notice",
    SUBSCRIPTION_TERMS: "Subscription Terms",
    COUNTRY_ELIGIBILITY: "Country Eligibility",
    REGISTRATION_CONSENT: "Registration Consent",
};

export function categoryLabel(category: string): string {
    return CATEGORY_LABELS[category] ?? category;
}

/** Draft unless the server explicitly reports a recorded counsel approval. */
export function isDraft(doc: Pick<LegalDocument, "review_status"> | null | undefined): boolean {
    return !doc || doc.review_status !== COUNSEL_APPROVED;
}

export function draftBannerText(doc: LegalDocument): string {
    return `Draft — version ${doc.version}. This document has not been reviewed by legal counsel and may change.`;
}

/** Split a stored body ("## " headings, "- " list items, blank-line paragraphs) into render blocks. */
export function parseLegalBody(body: string): LegalBlock[] {
    const blocks: LegalBlock[] = [];
    let paragraph: string[] = [];
    let list: string[] = [];
    const flushParagraph = () => {
        if (paragraph.length) blocks.push({ kind: "paragraph", text: paragraph.join(" ") });
        paragraph = [];
    };
    const flushList = () => {
        if (list.length) blocks.push({ kind: "list", items: list });
        list = [];
    };
    for (const raw of String(body ?? "").replace(/\r\n?/g, "\n").split("\n")) {
        const line = raw.trim();
        if (!line) {
            flushParagraph();
            flushList();
        } else if (line.startsWith("#")) {
            flushParagraph();
            flushList();
            const text = line.replace(/^#+\s*/, "");
            if (text) blocks.push({ kind: "heading", text });
        } else if (/^[-*]\s+/.test(line)) {
            flushParagraph();
            list.push(line.replace(/^[-*]\s+/, ""));
        } else {
            flushList();
            paragraph.push(line);
        }
    }
    flushParagraph();
    flushList();
    return blocks;
}

/** What POST /api/legal/accept (and registration) send: the exact versions shown. */
export function acceptancePayload(docs: LegalDocument[]): Array<{ doc_id: string; content_sha256: string }> {
    return docs.map((doc) => ({ doc_id: doc.doc_id, content_sha256: doc.content_sha256 }));
}

export function allAccepted(docs: LegalDocument[], checked: Record<string, boolean>): boolean {
    return docs.every((doc) => checked[doc.doc_id] === true);
}

export function reconsentReasonLabel(reason: string): string {
    switch (reason) {
        case "MATERIAL_CHANGE":
            return "Updated — please review the changes and accept again.";
        case "NOT_ACCEPTED":
            return "Not yet accepted.";
        case "WITHDRAWN":
            return "You withdrew your acceptance.";
        default:
            return "Acceptance required.";
    }
}

/** Documents the prompt should show (server order, one per category). */
export function pendingDocuments(status: LegalStatus | null | undefined): PendingItem[] {
    if (!status || !Array.isArray(status.pending)) return [];
    const seen = new Set<string>();
    return status.pending.filter((item) => {
        const category = item?.document?.category;
        if (!category || seen.has(category)) return false;
        seen.add(category);
        return true;
    });
}
