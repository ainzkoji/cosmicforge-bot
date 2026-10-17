import { useCallback, useEffect, useState } from "react";
import { Mail, ChevronDown, ChevronUp, LifeBuoy, ShieldAlert, Send, ArrowLeft } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { createTicket, getTicket, listTickets, replyToTicket } from "@/api/support";
import {
    MAX_MESSAGE, MAX_SUBJECT, STATUS_LABELS, TICKET_CATEGORIES, canReply, categoryLabel, secretWarning,
    supportErrorMessage, validateReply, validateTicketForm,
} from "@/lib/support";
import type { Ticket, TicketForm } from "@/lib/support";

/**
 * Help desk (Section H Step 8.5): open a ticket, follow your tickets and reply. Tickets are visible only to
 * you and to support staff. Never paste API keys, secrets or passwords -- the form warns, and the server
 * refuses such messages without storing them. Attachments are not supported.
 */
const EMPTY_FORM: TicketForm = { category: "", subject: "", message: "" };

function when(iso: string | null): string {
    if (!iso) return "";
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function StatusPill({ status }: { status: Ticket["status"] }) {
    const tone = status === "CLOSED" ? "bg-muted text-muted-foreground border-border"
        : status === "WAITING_ON_CUSTOMER" ? "bg-yellow-500/10 text-yellow-500 border-yellow-500/20"
            : "bg-primary/10 text-primary border-primary/20";
    return <span className={`px-2 py-0.5 rounded border text-xs font-semibold ${tone}`}>{STATUS_LABELS[status]}</span>;
}

function NewTicket({ onCreated }: { onCreated: (t: Ticket) => void }) {
    const [form, setForm] = useState<TicketForm>(EMPTY_FORM);
    const [problems, setProblems] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [sending, setSending] = useState(false);
    const warning = secretWarning(form.subject, form.message);

    const submit = async (e: React.FormEvent) => {
        e.preventDefault();
        const found = validateTicketForm(form);
        setProblems(found);
        setError(null);
        if (found.length) return;
        setSending(true);
        try {
            const created = await createTicket(form);
            setForm(EMPTY_FORM);
            onCreated(created);
        } catch (err) {
            setError(supportErrorMessage(err));
        } finally {
            setSending(false);
        }
    };

    return (
        <form onSubmit={submit} className="bg-card border border-border rounded-xl p-6 space-y-4">
            <h2 className="text-xl font-bold flex items-center gap-2"><LifeBuoy className="w-5 h-5" /> Open a support ticket</h2>
            <div className="flex items-start gap-2 text-sm text-muted-foreground bg-muted/30 rounded-lg p-3">
                <ShieldAlert className="w-4 h-4 mt-0.5 shrink-0 text-yellow-500" />
                <span>Never include API keys, secrets, passwords or 2FA codes. Support will never ask for them.
                    Describe the problem and which bot or exchange account it concerns.</span>
            </div>
            <label className="block text-sm font-medium">Category
                <select className="mt-1 w-full bg-background border border-border rounded-lg p-2"
                    value={form.category} onChange={(e) => setForm({ ...form, category: e.target.value })}>
                    <option value="">Choose a category</option>
                    {TICKET_CATEGORIES.map((c) => <option key={c.value} value={c.value}>{c.label}</option>)}
                </select>
            </label>
            <label className="block text-sm font-medium">Subject
                <input className="mt-1 w-full bg-background border border-border rounded-lg p-2" maxLength={MAX_SUBJECT}
                    value={form.subject} onChange={(e) => setForm({ ...form, subject: e.target.value })} />
            </label>
            <label className="block text-sm font-medium">Message
                <textarea className="mt-1 w-full bg-background border border-border rounded-lg p-2 min-h-[140px]"
                    maxLength={MAX_MESSAGE} value={form.message}
                    onChange={(e) => setForm({ ...form, message: e.target.value })} />
            </label>
            {warning && <div className="text-sm text-red-400 border border-red-500/30 rounded-lg p-3">{warning}</div>}
            {problems.filter((p) => p !== warning).map((p) => <div key={p} className="text-sm text-red-400">{p}</div>)}
            {error && <div className="text-sm text-red-400">{error}</div>}
            <button type="submit" disabled={sending || Boolean(warning)}
                className="px-4 py-2 rounded-lg bg-primary text-primary-foreground font-semibold disabled:opacity-50 flex items-center gap-2">
                <Send className="w-4 h-4" /> {sending ? "Sending..." : "Send ticket"}
            </button>
        </form>
    );
}

function TicketDetail({ ticketId, onBack }: { ticketId: string; onBack: () => void }) {
    const [ticket, setTicket] = useState<Ticket | null>(null);
    const [reply, setReply] = useState("");
    const [error, setError] = useState<string | null>(null);
    const [sending, setSending] = useState(false);
    const warning = secretWarning(reply);

    useEffect(() => {
        getTicket(ticketId).then(setTicket).catch((err) => setError(supportErrorMessage(err)));
    }, [ticketId]);

    const send = async () => {
        const problems = validateReply(reply);
        if (problems.length) { setError(problems[0]); return; }
        setSending(true);
        setError(null);
        try {
            setTicket(await replyToTicket(ticketId, reply));
            setReply("");
        } catch (err) {
            setError(supportErrorMessage(err));
        } finally {
            setSending(false);
        }
    };

    return (
        <div className="bg-card border border-border rounded-xl p-6 space-y-4">
            <button onClick={onBack} className="text-sm text-muted-foreground flex items-center gap-1 hover:text-foreground">
                <ArrowLeft className="w-4 h-4" /> All tickets</button>
            {!ticket && !error && <div className="text-sm text-muted-foreground">Loading...</div>}
            {ticket && (
                <>
                    <div className="flex flex-wrap items-center gap-3">
                        <h2 className="text-xl font-bold">{ticket.subject}</h2>
                        <StatusPill status={ticket.status} />
                        <span className="text-xs text-muted-foreground">{categoryLabel(ticket.category)}</span>
                    </div>
                    <div className="space-y-3">
                        {(ticket.messages ?? []).map((m) => (
                            <div key={m.message_id}
                                className={`rounded-lg p-3 text-sm whitespace-pre-wrap ${m.from === "YOU" ? "bg-primary/5 border border-primary/20" : "bg-muted/40 border border-border"}`}>
                                <div className="text-xs text-muted-foreground mb-1">{m.from === "YOU" ? "You" : "Support"} - {when(m.created_at)}</div>
                                {m.body}
                            </div>
                        ))}
                    </div>
                    {canReply(ticket) ? (
                        <div className="space-y-2">
                            <textarea className="w-full bg-background border border-border rounded-lg p-2 min-h-[100px]"
                                maxLength={MAX_MESSAGE} value={reply} onChange={(e) => setReply(e.target.value)}
                                placeholder="Write a reply (never include keys or passwords)" />
                            {warning && <div className="text-sm text-red-400">{warning}</div>}
                            <button onClick={send} disabled={sending || Boolean(warning) || !reply.trim()}
                                className="px-4 py-2 rounded-lg bg-primary text-primary-foreground font-semibold disabled:opacity-50">
                                {sending ? "Sending..." : "Send reply"}</button>
                        </div>
                    ) : <div className="text-sm text-muted-foreground">This ticket is closed. Open a new ticket if you still need help.</div>}
                </>
            )}
            {error && <div className="text-sm text-red-400">{error}</div>}
        </div>
    );
}

export default function Support() {
    const [faqOpen, setFaqOpen] = useState<number | null>(null);
    const [tickets, setTickets] = useState<Ticket[] | null>(null);
    const [listError, setListError] = useState<string | null>(null);
    const [selected, setSelected] = useState<string | null>(null);

    const load = useCallback(() => {
        listTickets().then((t) => { setTickets(t); setListError(null); })
            .catch((err) => setListError(supportErrorMessage(err)));
    }, []);
    useEffect(() => { load(); }, [load]);

    const faqs = [
        { q: "How do I connect my exchange API keys?", a: "Go to the Broker Connection page, select your exchange, and paste your API Key and Secret there -- never in a support ticket. Ensure you have enabled 'Trading' permissions but disabled 'Withdrawal' permissions for security." },
        { q: "What happens if my bot hits a stop loss?", a: "The bot will automatically close the position to prevent further losses. You will receive a notification via your configured channels (Email, Push, Telegram)." },
        { q: "Can I run multiple bots simultaneously?", a: "Yes! Depending on your subscription plan, you can run multiple bots on different pairs or strategies at the same time." },
        { q: "How is the 'Profit Factor' calculated?", a: "Profit Factor is the ratio of gross profit to gross loss. A value greater than 1.5 is generally considered good." },
        { q: "Is the platform having problems right now?", a: "Check the public status page at /status for the current state of the portal, trading engine, market data, exchange connectivity and notifications." },
    ];

    return (
        <div className="max-w-4xl mx-auto space-y-12 animate-in fade-in">
            <div className="text-center space-y-4">
                <h1 className="text-4xl font-bold">How can we help you?</h1>
                <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
                    Open a support ticket below, read the FAQs, or email our support team.
                </p>
            </div>

            {selected ? (
                <TicketDetail ticketId={selected} onBack={() => { setSelected(null); load(); }} />
            ) : (
                <div className="space-y-6">
                    <NewTicket onCreated={(t) => { load(); setSelected(t.ticket_id); }} />
                    <div className="bg-card border border-border rounded-xl p-6">
                        <h2 className="text-xl font-bold mb-4">Your tickets</h2>
                        {listError && <div className="text-sm text-red-400">{listError}</div>}
                        {tickets && tickets.length === 0 && <div className="text-sm text-muted-foreground">You have no tickets yet.</div>}
                        <div className="divide-y divide-border">
                            {(tickets ?? []).map((t) => (
                                <button key={t.ticket_id} onClick={() => setSelected(t.ticket_id)}
                                    className="w-full text-left py-3 flex items-center justify-between gap-3 hover:bg-muted/30 px-2 rounded">
                                    <div>
                                        <div className="font-medium">{t.subject}</div>
                                        <div className="text-xs text-muted-foreground">{categoryLabel(t.category)} - updated {when(t.updated_at)}</div>
                                    </div>
                                    <StatusPill status={t.status} />
                                </button>
                            ))}
                        </div>
                    </div>
                </div>
            )}

            <div className="max-w-md mx-auto">
                <div className="bg-card border border-border rounded-xl p-6 text-center hover:border-primary/50 transition-colors group">
                    <div className="w-12 h-12 bg-primary/10 rounded-full flex items-center justify-center mx-auto mb-4 text-primary group-hover:scale-110 transition-transform">
                        <Mail className="w-6 h-6" />
                    </div>
                    <h3 className="font-bold text-lg mb-2">Email Support</h3>
                    <p className="text-sm text-muted-foreground mb-4">
                        You can also email us. Never include API keys, secrets or passwords in an email either.
                    </p>
                    <a href="mailto:support@cosmicforge.com" className="text-primary font-bold text-sm hover:underline">support@cosmicforge.com</a>
                </div>
            </div>

            <div>
                <h2 className="text-2xl font-bold mb-6">Frequently Asked Questions</h2>
                <div className="space-y-4">
                    {faqs.map((faq, i) => (
                        <div key={i} className="bg-card border border-border rounded-xl overflow-hidden">
                            <button
                                onClick={() => setFaqOpen(faqOpen === i ? null : i)}
                                className="w-full text-left p-4 flex justify-between items-center hover:bg-muted/50 transition-colors"
                            >
                                <span className="font-medium">{faq.q}</span>
                                {faqOpen === i ? <ChevronUp className="w-4 h-4 text-muted-foreground" /> : <ChevronDown className="w-4 h-4 text-muted-foreground" />}
                            </button>
                            <AnimatePresence>
                                {faqOpen === i && (
                                    <motion.div initial={{ height: 0 }} animate={{ height: "auto" }} exit={{ height: 0 }} className="overflow-hidden">
                                        <div className="p-4 pt-0 text-muted-foreground text-sm border-t border-border/50 bg-muted/20">{faq.a}</div>
                                    </motion.div>
                                )}
                            </AnimatePresence>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
