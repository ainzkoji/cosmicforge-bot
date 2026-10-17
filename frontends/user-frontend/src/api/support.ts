/** Support desk (/api/support), Section H Step 8.5. Logic and validation live in src/lib/support.ts. */
import { apiClient } from "./client";
import { parseTicket } from "../lib/support";
import type { Ticket, TicketForm } from "../lib/support";

export async function listTickets(): Promise<Ticket[]> {
    const { data } = await apiClient.get("/api/support/tickets");
    return (Array.isArray(data?.tickets) ? data.tickets : []).map(parseTicket);
}

export async function getTicket(ticketId: string): Promise<Ticket> {
    const { data } = await apiClient.get(`/api/support/tickets/${encodeURIComponent(ticketId)}`);
    return parseTicket(data);
}

export async function createTicket(form: TicketForm): Promise<Ticket> {
    const { data } = await apiClient.post("/api/support/tickets", {
        category: form.category, subject: form.subject.trim(), message: form.message.trim(),
    });
    return parseTicket(data);
}

export async function replyToTicket(ticketId: string, message: string): Promise<Ticket> {
    const { data } = await apiClient.post(`/api/support/tickets/${encodeURIComponent(ticketId)}/replies`,
        { message: message.trim() });
    return parseTicket(data);
}
