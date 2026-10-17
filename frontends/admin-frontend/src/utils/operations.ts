/**
 * Section H Step 8.5: the logic of the admin Operations and Support desk pages (no React, no network --
 * src/tests/operations.test.ts runs this file itself).
 *
 * Backend: user-backend /api/admin/operations/* (alerts, on-call, backup drill, status-page incidents) and
 * /api/admin/support/* (tickets). Nothing here invents a person: an empty on-call configuration is shown as
 * OPERATIONS OWNER REQUIRED, exactly as the backend reports it.
 */

export const OPERATIONS_OWNER_REQUIRED = "OPERATIONS_OWNER_REQUIRED";
export const OWNER_REQUIRED_TEXT = "OPERATIONS OWNER REQUIRED";

export type Severity = "CRITICAL" | "HIGH" | "MEDIUM" | "LOW";
export type AlertStatus = "OPEN" | "ACKNOWLEDGED" | "RESOLVED";

export interface Alert {
    alert_id: string;
    category: string;
    severity: Severity;
    status: AlertStatus;
    title: string;
    rotation_key: string;
    escalation_level: number;
    owner_required: boolean;
    occurrences: number;
    first_seen_at: number;
    last_seen_at: number;
    acknowledged_by: string | null;
    resolved_by: string | null;
    scope: string;
}

export interface OncallMember {
    member_id: string;
    rotation_key: string;
    person_name: string;
    level: number;
    level_name: string;
    contact_channel: string;
}

export interface Rotation {
    rotation_key: string;
    description: string;
    status: string;
    members: OncallMember[];
}

export interface Finding {
    code: string;
    severity: string;
    message: string;
    rotation_key?: string;
}

export interface Oncall {
    rotations: Rotation[];
    findings: Finding[];
    owner_assigned: boolean;
    status: string;
}

export const SEVERITY_ORDER: Record<Severity, number> = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 };
export const SEVERITY_TONE: Record<Severity, string> = {
    CRITICAL: "bg-red-500/10 text-red-400 border-red-500/30",
    HIGH: "bg-orange-500/10 text-orange-400 border-orange-500/30",
    MEDIUM: "bg-yellow-500/10 text-yellow-500 border-yellow-500/30",
    LOW: "bg-muted text-muted-foreground border-border",
};
export const LEVEL_NAMES = ["Primary", "Secondary", "Escalation owner"];
export const CONTACT_CHANNELS = ["EMAIL", "TELEGRAM", "WEBHOOK", "PHONE", "OTHER"] as const;
export const COMPONENT_KEYS = ["PORTAL", "TRADING_ENGINE", "MARKET_DATA", "EXCHANGE_CONNECTIVITY", "NOTIFICATIONS"] as const;
export const INCIDENT_STATUSES = ["INVESTIGATING", "IDENTIFIED", "MONITORING", "RESOLVED"] as const;
export const INCIDENT_IMPACTS = ["DEGRADED", "OUTAGE", "MAINTENANCE"] as const;

export function sortAlerts(alerts: Alert[]): Alert[] {
    const rank = (a: Alert) => (a.status === "OPEN" ? 0 : a.status === "ACKNOWLEDGED" ? 1 : 2);
    return [...alerts].sort((a, b) => rank(a) - rank(b) || SEVERITY_ORDER[a.severity] - SEVERITY_ORDER[b.severity]
        || b.last_seen_at - a.last_seen_at);
}

/** Which buttons an alert row shows. */
export function alertActions(alert: Alert): { acknowledge: boolean; resolve: boolean } {
    return { acknowledge: alert.status === "OPEN", resolve: alert.status !== "RESOLVED" };
}

/** The owner line shown on the page: a named owner when the backend has one, otherwise the requirement. */
export function ownerLine(oncall: Oncall | null): string {
    if (!oncall) return OWNER_REQUIRED_TEXT;
    const owner = oncall.rotations.find((r) => r.rotation_key === "operations-owner");
    const primary = owner?.members.find((m) => m.level === 0);
    return primary ? `Operations owner: ${primary.person_name}` : OWNER_REQUIRED_TEXT;
}

export function rotationOwnerText(rotation: Rotation): string {
    const primary = rotation.members.filter((m) => m.level === 0);
    return primary.length ? primary.map((m) => m.person_name).join(", ") : OWNER_REQUIRED_TEXT;
}

export function unassignedRotations(oncall: Oncall | null): string[] {
    if (!oncall) return [];
    return oncall.rotations.filter((r) => r.status === OPERATIONS_OWNER_REQUIRED).map((r) => r.rotation_key);
}

export interface MemberForm {
    rotation_key: string;
    level: number;
    person_name: string;
    contact_channel: string;
    contact_address: string;
}

export function validateMember(form: MemberForm, rotations: string[]): string[] {
    const problems: string[] = [];
    if (!rotations.includes(form.rotation_key)) problems.push("Choose a rotation.");
    if (![0, 1, 2].includes(form.level)) problems.push("Choose a level.");
    if (!form.person_name.trim()) problems.push("A named person is required.");
    if (!(CONTACT_CHANNELS as readonly string[]).includes(form.contact_channel)) problems.push("Choose a contact channel.");
    if (!form.contact_address.trim()) problems.push("A contact address is required.");
    return problems;
}

const IPV4 = /\b(?:\d{1,3}\.){3}\d{1,3}\b/;
const INTERNAL_HOST = /\b(?:localhost|[a-z0-9-]+\.(?:internal|local|lan|intra|corp|localdomain))\b/i;
const EMAIL = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/;
const ACCOUNTISH = /\b(?:account|tenant|user|worker)[\s_-]?(?:id)?\s*[:=#]\s*\S+/i;
const SECRETISH = /\b(?:api[\s_-]?key|secret|password|token)\b\s*[:=]\s*\S{8,}/i;

/** Client-side mirror of the backend's public-text check (the backend is authoritative). */
export function publicTextProblems(text: string): string[] {
    const problems: string[] = [];
    if (IPV4.test(text)) problems.push("IP_ADDRESS");
    if (INTERNAL_HOST.test(text)) problems.push("INTERNAL_HOSTNAME");
    if (EMAIL.test(text)) problems.push("EMAIL_ADDRESS");
    if (ACCOUNTISH.test(text)) problems.push("ACCOUNT_IDENTIFIER");
    if (SECRETISH.test(text)) problems.push("SECRET");
    return problems;
}

export interface IncidentForm {
    title: string;
    components: string[];
    impact: string;
    message: string;
}

export function validateIncident(form: IncidentForm): string[] {
    const problems: string[] = [];
    if (form.title.trim().length < 3) problems.push("A title is required.");
    if (!form.components.length || form.components.some((c) => !(COMPONENT_KEYS as readonly string[]).includes(c)))
        problems.push("Choose at least one component.");
    if (!(INCIDENT_IMPACTS as readonly string[]).includes(form.impact)) problems.push("Choose an impact.");
    if (form.message.trim().length < 3) problems.push("A public message is required.");
    const leaks = publicTextProblems(`${form.title}\n${form.message}`);
    if (leaks.length) problems.push(`The notice is public and must not contain: ${leaks.join(", ")}.`);
    return problems;
}

export interface DrillSummary {
    status: string;
    finished_at?: string | null;
    age_hours?: number | null;
    rpo_ms?: number | null;
    rto_ms?: number | null;
    checks_passed?: number;
    checks_failed?: string[];
    encryption?: string | null;
    postgres_leg?: string;
    rpo_target_minutes: string;
    rto_target_minutes: string;
}

export function drillHeadline(d: DrillSummary | null): string {
    if (!d) return "Drill status unavailable";
    if (d.status === "NO_DRILL_RECORDED") return "No restore drill has been recorded";
    if (d.status === "PASSED") return `Last restore drill PASSED${d.age_hours != null ? ` (${d.age_hours} h ago)` : ""}`;
    if (d.status === "FAILED") return `Last restore drill FAILED: ${(d.checks_failed ?? []).join(", ") || "see evidence"}`;
    return d.status;
}

export function formatMs(ms: number | null | undefined): string {
    if (ms == null) return "n/a";
    if (ms < 1000) return `${ms} ms`;
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
    return `${(ms / 60_000).toFixed(1)} min`;
}

export function formatEpochMs(ms: number | null | undefined): string {
    if (ms == null) return "";
    const d = new Date(ms);
    return Number.isNaN(d.getTime()) ? "" : d.toLocaleString();
}

/** The error text an admin sees: the backend's error code first (never a guess). */
export function operationsError(error: unknown): string {
    const detail = (error as { response?: { status?: number; data?: { detail?: unknown } } })?.response?.data?.detail;
    if (detail && typeof detail === "object") {
        const d = detail as { error_code?: string; message?: string; problems?: string[] };
        const extra = d.problems?.length ? ` (${d.problems.join(", ")})` : "";
        return `${d.error_code ?? "ERROR"}${d.message ? `: ${d.message}` : ""}${extra}`;
    }
    if (typeof detail === "string") return detail;
    return (error as { message?: string })?.message || "REQUEST_FAILED";
}

// ── support desk ────────────────────────────────────────────────────────────────────────────────────────
export const TICKET_STATUSES = ["OPEN", "IN_PROGRESS", "WAITING_ON_CUSTOMER", "CLOSED"] as const;
export type TicketStatus = (typeof TICKET_STATUSES)[number];

export interface AdminTicket {
    ticket_id: string;
    user_id: string;
    user_email: string | null;
    category: string;
    subject: string;
    status: TicketStatus;
    assigned_admin_id: string | null;
    created_at: string;
    updated_at: string;
    message_count?: number;
}

export function ticketActions(t: AdminTicket): { reply: boolean; assign: boolean; close: boolean } {
    const open = t.status !== "CLOSED";
    return { reply: open, assign: open, close: open };
}

/** Admin replies are checked like user messages: support never asks for or sends a secret. */
export function replyProblems(message: string): string[] {
    const text = message.trim();
    if (!text) return ["Write a reply."];
    const highEntropy = (text.match(/[A-Za-z0-9_-]{32,}/g) ?? [])
        .some((t) => /[A-Z]/.test(t) && /[a-z]/.test(t) && /\d/.test(t));
    if (SECRETISH.test(text) || highEntropy)
        return ["The reply looks like it contains a secret. Support never asks for or sends API keys or passwords."];
    return [];
}
