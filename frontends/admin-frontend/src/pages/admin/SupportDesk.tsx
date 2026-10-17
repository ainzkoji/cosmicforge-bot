import { useCallback, useEffect, useState } from "react";
import { LifeBuoy, RefreshCw } from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { assignTicket, closeTicket, getTicket, listAssignees, listTickets, replyTicket } from "@/api/operations";
import { TICKET_STATUSES, operationsError, replyProblems, ticketActions } from "@/utils/operations";
import type { AdminTicket } from "@/utils/operations";

/**
 * Support desk (Section H Step 8.5): list, assign, reply to and close customer tickets. Replies are checked
 * for secrets on both sides -- support never asks for or sends API keys or passwords.
 */
const input = "bg-background border border-border rounded p-1.5 text-sm";
const button = "px-3 py-1.5 rounded border border-border text-sm hover:bg-muted/40 disabled:opacity-50";

function TicketPanel({ ticketId, assignees, onChanged }: {
    ticketId: string; assignees: Array<{ id: string; display_name: string }>; onChanged: () => void;
}) {
    const [ticket, setTicket] = useState<Record<string, any> | null>(null);
    const [reply, setReply] = useState("");
    const [reason, setReason] = useState("");
    const [error, setError] = useState<string | null>(null);
    const load = useCallback(() => {
        getTicket(ticketId).then(setTicket).catch((e) => setError(operationsError(e)));
    }, [ticketId]);
    useEffect(() => { load(); }, [load]);
    const act = async (fn: () => Promise<unknown>) => {
        try { setTicket(await fn() as Record<string, any>); setError(null); onChanged(); } catch (e) { setError(operationsError(e)); }
    };
    if (!ticket) return <div className="text-sm text-muted-foreground">{error ?? "Loading..."}</div>;
    const actions = ticketActions(ticket as AdminTicket);
    return (
        <div className="border border-border/50 rounded-xl p-4 space-y-3">
            <div className="font-bold">{ticket.subject} <code className="text-xs">{ticket.status}</code></div>
            <div className="text-xs text-muted-foreground">{ticket.category} | {ticket.user_email ?? ticket.user_id} | assigned: {ticket.assigned_admin_id ?? "nobody"}</div>
            <div className="space-y-2">
                {(ticket.messages ?? []).map((m: any) => (
                    <div key={m.message_id} className={`rounded p-2 text-sm whitespace-pre-wrap ${m.author_kind === "ADMIN" ? "bg-primary/5 border border-primary/20" : "bg-muted/40"}`}>
                        <div className="text-xs text-muted-foreground">{m.author_kind} {m.author_id} - {m.created_at}</div>{m.body}
                    </div>))}
            </div>
            {actions.assign && (
                <div className="flex gap-2 items-center text-sm">
                    <select className={input} value={ticket.assigned_admin_id ?? ""} onChange={(e) => act(() => assignTicket(ticketId, e.target.value || null))}>
                        <option value="">unassigned</option>{assignees.map((a) => <option key={a.id} value={a.id}>{a.display_name}</option>)}
                    </select>
                </div>)}
            {actions.reply && (
                <div className="space-y-2">
                    <textarea className={`${input} w-full min-h-[90px]`} value={reply} onChange={(e) => setReply(e.target.value)}
                        placeholder="Reply to the customer (never ask for keys or passwords)" />
                    <button className={button} disabled={!reply.trim()} onClick={() => {
                        const problems = replyProblems(reply);
                        if (problems.length) { setError(problems[0]); return; }
                        act(() => replyTicket(ticketId, reply)).then(() => setReply(""));
                    }}>Send reply</button>
                </div>)}
            {actions.close && (
                <div className="flex gap-2">
                    <input className={input} placeholder="close reason" value={reason} onChange={(e) => setReason(e.target.value)} />
                    <button className={button} disabled={!reason.trim()} onClick={() => act(() => closeTicket(ticketId, reason))}>Close ticket</button>
                </div>)}
            {ticket.status === "CLOSED" && <div className="text-xs text-muted-foreground">Closed by {ticket.closed_by}: {ticket.close_reason}</div>}
            <details className="text-xs"><summary>History</summary>
                {(ticket.events ?? []).map((e: any) => <div key={e.id}>{e.at} {e.event} {e.actor_kind}:{e.actor_id}</div>)}</details>
            {error && <div className="text-sm text-red-400">{error}</div>}
        </div>
    );
}

export default function SupportDesk() {
    const [status, setStatus] = useState<string>("");
    const [tickets, setTickets] = useState<AdminTicket[]>([]);
    const [assignees, setAssignees] = useState<Array<{ id: string; email: string; display_name: string }>>([]);
    const [selected, setSelected] = useState<string | null>(null);
    const [error, setError] = useState<string | null>(null);
    const load = useCallback(() => {
        Promise.all([listTickets(status || undefined), listAssignees()])
            .then(([t, a]) => { setTickets(t); setAssignees(a); setError(null); })
            .catch((e) => setError(operationsError(e)));
    }, [status]);
    useEffect(() => { load(); }, [load]);

    return (
        <AdminLayout>
            <div className="p-6 space-y-6">
                <div className="flex items-center justify-between">
                    <h1 className="text-2xl font-bold flex items-center gap-2"><LifeBuoy className="w-6 h-6" /> Support desk</h1>
                    <div className="flex gap-2">
                        <select className={input} value={status} onChange={(e) => setStatus(e.target.value)}>
                            <option value="">all statuses</option>{TICKET_STATUSES.map((s) => <option key={s}>{s}</option>)}</select>
                        <button className={button} onClick={load}><RefreshCw className="w-4 h-4 inline" /> Refresh</button>
                    </div>
                </div>
                {error && <div className="text-sm text-red-400">Unavailable: <code>{error}</code></div>}
                <div className="grid md:grid-cols-2 gap-4">
                    <div className="border border-border/50 rounded-xl divide-y divide-border/40">
                        {tickets.length === 0 && <div className="p-4 text-sm text-muted-foreground">No tickets.</div>}
                        {tickets.map((t) => (
                            <button key={t.ticket_id} onClick={() => setSelected(t.ticket_id)}
                                className={`w-full text-left p-3 hover:bg-muted/30 ${selected === t.ticket_id ? "bg-muted/40" : ""}`}>
                                <div className="font-medium">{t.subject}</div>
                                <div className="text-xs text-muted-foreground">{t.status} | {t.category} | {t.user_email ?? t.user_id} | {t.updated_at}</div>
                            </button>))}
                    </div>
                    {selected && <TicketPanel key={selected} ticketId={selected} assignees={assignees} onChanged={load} />}
                </div>
            </div>
        </AdminLayout>
    );
}
