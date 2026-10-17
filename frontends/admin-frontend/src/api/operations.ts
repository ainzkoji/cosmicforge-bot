/**
 * Section H Step 8.5: admin operations API (user-backend /api/admin/operations/*) and support desk
 * (/api/admin/support/*). Interpretation lives in src/utils/operations.ts; this file only sends and receives.
 */
import { apiClient } from "./client";
import type { AdminTicket, Alert, DrillSummary, IncidentForm, MemberForm, Oncall } from "@/utils/operations";

const OPS = "/api/admin/operations";
const SUPPORT = "/api/admin/support";

export const OPERATIONS_QUERY_KEY = ["admin-operations"] as const;

export async function getOverview(): Promise<Record<string, unknown>> {
    return (await apiClient.get(`${OPS}/overview`)).data;
}

export async function listAlerts(status = "OPEN,ACKNOWLEDGED"): Promise<Alert[]> {
    return (await apiClient.get(`${OPS}/alerts`, { params: { status } })).data.alerts;
}

export async function acknowledgeAlert(alertId: string, note?: string): Promise<Alert> {
    return (await apiClient.post(`${OPS}/alerts/${encodeURIComponent(alertId)}/acknowledge`, { note: note || null })).data;
}

export async function resolveAlert(alertId: string, note: string): Promise<Alert> {
    return (await apiClient.post(`${OPS}/alerts/${encodeURIComponent(alertId)}/resolve`, { note })).data;
}

export async function getOncall(): Promise<Oncall> {
    return (await apiClient.get(`${OPS}/oncall`)).data;
}

export async function addOncallMember(form: MemberForm): Promise<Oncall> {
    return (await apiClient.post(`${OPS}/oncall/members`, form)).data;
}

export async function removeOncallMember(memberId: string): Promise<Oncall> {
    return (await apiClient.post(`${OPS}/oncall/members/${encodeURIComponent(memberId)}/remove`)).data;
}

export async function getBackupDrill(): Promise<DrillSummary> {
    return (await apiClient.get(`${OPS}/backup-drill`)).data;
}

export async function listIncidents(): Promise<{ incidents: Array<Record<string, any>> }> {
    return (await apiClient.get(`${OPS}/status/incidents`)).data;
}

export async function createIncident(form: IncidentForm): Promise<Record<string, unknown>> {
    return (await apiClient.post(`${OPS}/status/incidents`, form)).data;
}

export async function updateIncident(id: string, body: { status: string; message: string; impact?: string }) {
    return (await apiClient.post(`${OPS}/status/incidents/${encodeURIComponent(id)}/updates`, body)).data;
}

export async function resolveIncident(id: string, message: string) {
    return (await apiClient.post(`${OPS}/status/incidents/${encodeURIComponent(id)}/resolve`, { message })).data;
}

// ── support desk ────────────────────────────────────────────────────────────────────────────────────────
export async function listTickets(status?: string): Promise<AdminTicket[]> {
    return (await apiClient.get(`${SUPPORT}/tickets`, { params: status ? { status } : {} })).data.tickets;
}

export async function getTicket(id: string): Promise<Record<string, any>> {
    return (await apiClient.get(`${SUPPORT}/tickets/${encodeURIComponent(id)}`)).data;
}

export async function listAssignees(): Promise<Array<{ id: string; email: string; display_name: string }>> {
    return (await apiClient.get(`${SUPPORT}/assignees`)).data.assignees;
}

export async function assignTicket(id: string, assigneeId: string | null) {
    return (await apiClient.post(`${SUPPORT}/tickets/${encodeURIComponent(id)}/assign`, { assignee_id: assigneeId })).data;
}

export async function replyTicket(id: string, message: string) {
    return (await apiClient.post(`${SUPPORT}/tickets/${encodeURIComponent(id)}/replies`, { message })).data;
}

export async function closeTicket(id: string, reason: string) {
    return (await apiClient.post(`${SUPPORT}/tickets/${encodeURIComponent(id)}/close`, { reason })).data;
}
