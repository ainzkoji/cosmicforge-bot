/**
 * Section H Step 8.5 -- support desk: the pure logic behind /dashboard/support.
 *
 * The backend (POST /api/support/tickets) is the authority: it refuses any message that looks like an API
 * key, password or other secret (422 SECRET_DETECTED) and never stores it. `secretWarning` mirrors the
 * same patterns so the user is warned BEFORE anything is sent; a miss here is still caught server-side.
 */

export const TICKET_CATEGORIES = [
    { value: "ACCOUNT", label: "Account & login" },
    { value: "BILLING", label: "Billing & subscription" },
    { value: "BROKER_CONNECTION", label: "Exchange / broker connection" },
    { value: "TRADING", label: "Trading & orders" },
    { value: "BOTS", label: "Bots & Auto Pilot" },
    { value: "KYC", label: "Identity verification" },
    { value: "SECURITY", label: "Security" },
    { value: "TECHNICAL", label: "Technical problem" },
    { value: "OTHER", label: "Something else" },
] as const;

export type TicketCategory = (typeof TICKET_CATEGORIES)[number]["value"];
export type TicketStatus = "OPEN" | "IN_PROGRESS" | "WAITING_ON_CUSTOMER" | "CLOSED";

export const STATUS_LABELS: Record<TicketStatus, string> = {
    OPEN: "Open",
    IN_PROGRESS: "In progress",
    WAITING_ON_CUSTOMER: "Waiting for your reply",
    CLOSED: "Closed",
};

export const MAX_SUBJECT = 200;
export const MAX_MESSAGE = 10_000;

export interface TicketMessage {
    message_id: string;
    from: "YOU" | "SUPPORT";
    body: string;
    created_at: string;
}

export interface Ticket {
    ticket_id: string;
    category: TicketCategory;
    subject: string;
    status: TicketStatus;
    created_at: string;
    updated_at: string;
    closed_at: string | null;
    assigned: boolean;
    messages?: TicketMessage[];
}

export interface TicketForm {
    category: string;
    subject: string;
    message: string;
}

export const SECRET_WARNING =
    "This looks like an API key, secret, password or token. Never share secrets with support -- we will never " +
    "ask for them. Remove it before sending. If you pasted a real key anywhere, revoke it at your exchange now.";

const SECRET_NAME =
    "(?:api[\\s_\\-]?key|api[\\s_\\-]?secret|secret[\\s_\\-]?key|client[\\s_\\-]?secret|private[\\s_\\-]?key|" +
    "pass(?:word|phrase)|passwd|access[\\s_\\-]?token|refresh[\\s_\\-]?token|auth[\\s_\\-]?token|bearer|token|" +
    "secret|seed[\\s_\\-]?phrase|mnemonic|2fa[\\s_\\-]?code|totp|otp)";

const PATTERNS: RegExp[] = [
    new RegExp(`\\b${SECRET_NAME}\\b["']?\\s*(?:[:=]|\\bis\\b)\\s*["']?[^\\s"',;]{8,}`, "i"),
    /-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----/,
    /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/,
    /\b(?:sk|rk|pk)_(?:live|test)_[0-9A-Za-z]{16,}\b|\bwhsec_[0-9A-Za-z]{16,}\b/,
    /\b(?:ghp|gho|ghs|github_pat|glpat|xox[abprs])[_-][0-9A-Za-z_-]{16,}\b/,
    /\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/,
    /\b\d{6,}:[A-Za-z0-9_-]{30,}\b/,
    /\b[a-z][a-z0-9+.-]*:\/\/[^\s/@:]+:[^\s/@]+@/i,
];

function highEntropyToken(text: string): boolean {
    for (const token of text.match(/[A-Za-z0-9_-]{32,}/g) ?? []) {
        if (/[A-Z]/.test(token) && /[a-z]/.test(token) && /[0-9]/.test(token)) return true;
    }
    return false;
}

/** True when the text looks like it contains a credential (same rules as the backend). */
export function looksLikeSecret(text: string): boolean {
    if (!text) return false;
    return PATTERNS.some((p) => p.test(text)) || highEntropyToken(text);
}

export function secretWarning(...texts: string[]): string | null {
    return texts.some(looksLikeSecret) ? SECRET_WARNING : null;
}

export function isCategory(value: string): value is TicketCategory {
    return TICKET_CATEGORIES.some((c) => c.value === value);
}

/** Problems that block sending (empty: ok to send). */
export function validateTicketForm(form: TicketForm): string[] {
    const problems: string[] = [];
    if (!isCategory(form.category)) problems.push("Choose a category.");
    const subject = form.subject.trim();
    const message = form.message.trim();
    if (!subject) problems.push("Enter a subject.");
    if (subject.length > MAX_SUBJECT) problems.push(`The subject is longer than ${MAX_SUBJECT} characters.`);
    if (!message) problems.push("Describe the problem.");
    if (message.length > MAX_MESSAGE) problems.push(`The message is longer than ${MAX_MESSAGE} characters.`);
    const warning = secretWarning(subject, message);
    if (warning) problems.push(warning);
    return problems;
}

export function validateReply(message: string): string[] {
    const text = message.trim();
    if (!text) return ["Write a reply."];
    if (text.length > MAX_MESSAGE) return [`The reply is longer than ${MAX_MESSAGE} characters.`];
    const warning = secretWarning(text);
    return warning ? [warning] : [];
}

/** The user-facing message for a failed support request (axios error or anything else). */
export function supportErrorMessage(error: unknown): string {
    const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
    if (detail && typeof detail === "object") {
        const d = detail as { error_code?: string; message?: string };
        if (d.error_code === "SECRET_DETECTED") return SECRET_WARNING;
        if (d.message) return d.message;
        if (d.error_code) return d.error_code;
    }
    if (typeof detail === "string") return detail;
    const message = (error as { message?: string })?.message;
    return message || "The request failed. Please try again.";
}

function str(v: unknown, field: string): string {
    if (typeof v !== "string") throw new Error(`UNEXPECTED_TICKET_FIELD:${field}`);
    return v;
}

export function parseTicket(raw: unknown): Ticket {
    const r = raw as Record<string, unknown>;
    if (!r || typeof r !== "object") throw new Error("UNEXPECTED_TICKET");
    const status = str(r.status, "status") as TicketStatus;
    if (!(status in STATUS_LABELS)) throw new Error("UNEXPECTED_TICKET_STATUS");
    const category = str(r.category, "category");
    if (!isCategory(category)) throw new Error("UNEXPECTED_TICKET_CATEGORY");
    const ticket: Ticket = {
        ticket_id: str(r.ticket_id, "ticket_id"),
        category,
        subject: str(r.subject, "subject"),
        status,
        created_at: str(r.created_at, "created_at"),
        updated_at: str(r.updated_at, "updated_at"),
        closed_at: typeof r.closed_at === "string" ? r.closed_at : null,
        assigned: Boolean(r.assigned),
    };
    if (Array.isArray(r.messages)) {
        ticket.messages = r.messages.map((m) => {
            const mm = m as Record<string, unknown>;
            const from = mm.from === "SUPPORT" ? "SUPPORT" : "YOU";
            return { message_id: str(mm.message_id, "message_id"), from, body: str(mm.body, "body"),
                     created_at: str(mm.created_at, "created_at") };
        });
    }
    return ticket;
}

export function canReply(ticket: Ticket): boolean {
    return ticket.status !== "CLOSED";
}

export function categoryLabel(value: string): string {
    return TICKET_CATEGORIES.find((c) => c.value === value)?.label ?? value;
}
