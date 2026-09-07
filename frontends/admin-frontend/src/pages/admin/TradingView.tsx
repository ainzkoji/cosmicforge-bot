import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
    AlertTriangle,
    CheckCircle2,
    Database,
    Lock,
    RefreshCw,
    ShieldCheck,
    SlidersHorizontal,
    Webhook,
    XCircle,
} from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import {
    getTradingViewAlerts,
    getTradingViewDecisions,
    getTradingViewExternalSignals,
    getTradingViewWebhooks,
    type ExternalSignalQueueRow,
    type TradingViewAlert,
    type TradingViewDecision,
    type TradingViewWebhook,
} from "@/api/admin";

type TabId = "webhooks" | "alerts" | "decisions" | "queue";

// retry:1 — fail fast so error state appears within ~2 s of a real failure.
// staleTime:30_000 — don't refetch on every tab switch within 30 s.
const TV_QUERY_OPTIONS = { retry: 1, staleTime: 30_000 } as const;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fv(v: string | number | null | undefined): string {
    return v === null || v === undefined || v === "" ? "—" : String(v);
}

function formatDate(v: string | null | undefined): string {
    if (!v) return "—";
    try {
        const d = new Date(v);
        if (isNaN(d.getTime())) return v;
        return d.toLocaleString("en-GB", {
            month: "short", day: "numeric",
            hour: "2-digit", minute: "2-digit", hour12: false,
        });
    } catch {
        return v;
    }
}

function truncate(s: string | null | undefined, max = 60): string {
    if (!s) return "—";
    return s.length > max ? s.slice(0, max) + "…" : s;
}

// ─── Badge ────────────────────────────────────────────────────────────────────

type Tone = "green" | "yellow" | "red" | "blue" | "muted";

function toneColor(t: Tone): string {
    switch (t) {
        case "green":  return "var(--admin-green)";
        case "yellow": return "var(--admin-yellow)";
        case "red":    return "var(--admin-red)";
        case "blue":   return "var(--admin-blue)";
        default:       return "var(--admin-text-secondary)";
    }
}

function toneBg(t: Tone): string {
    switch (t) {
        case "green":  return "rgba(16,185,129,0.10)";
        case "yellow": return "rgba(245,158,11,0.10)";
        case "red":    return "rgba(239,68,68,0.10)";
        case "blue":   return "rgba(59,130,246,0.10)";
        default:       return "rgba(255,255,255,0.04)";
    }
}

function toneBorder(t: Tone): string {
    switch (t) {
        case "green":  return "rgba(16,185,129,0.28)";
        case "yellow": return "rgba(245,158,11,0.28)";
        case "red":    return "rgba(239,68,68,0.28)";
        case "blue":   return "rgba(59,130,246,0.28)";
        default:       return "var(--admin-border-color)";
    }
}

function statusTone(v: string | null | undefined): Tone {
    const n = (v || "").toUpperCase();
    if (n === "ACCEPTED_ADVISORY" || n === "ADVISORY_ONLY" || n === "ACCEPTED" || n === "ENABLED") return "green";
    if (n === "PENDING") return "blue";
    if (n === "PROCESSED" || n.includes("EXTERNAL") || n.includes("CONFIRMATION")) return "blue";
    if (n === "DUPLICATE" || n === "STALE" || n.includes("RATE") || n === "DISABLED") return "yellow";
    if (n.includes("INVALID") || n.includes("UNSUPPORTED") || n.includes("REJECTED") || n === "FAILED" || n === "EXPIRED" || n === "FORBIDDEN") return "red";
    return "muted";
}

function Badge({ value }: { value: string | null | undefined }) {
    const t = statusTone(value);
    return (
        <span style={{
            display: "inline-flex", alignItems: "center",
            borderRadius: 999, border: "1px solid", borderColor: toneBorder(t),
            background: toneBg(t), color: toneColor(t),
            padding: "2px 8px", fontSize: 11, fontWeight: 700,
            whiteSpace: "nowrap", letterSpacing: "0.04em", textTransform: "uppercase",
        }}>
            {fv(value)}
        </span>
    );
}

// ─── Symbols list ─────────────────────────────────────────────────────────────

function SymbolsList({ symbols }: { symbols: string[] | null | undefined }) {
    if (!symbols || symbols.length === 0) {
        return <span style={{ color: "var(--admin-text-muted)", fontSize: 12 }}>All symbols</span>;
    }
    const MAX = 3;
    const visible = symbols.slice(0, MAX);
    const rest = symbols.length - MAX;
    return (
        <span title={symbols.join(", ")} style={{ cursor: "default", fontSize: 12 }}>
            <span style={{ color: "var(--admin-text-primary)" }}>{visible.join(", ")}</span>
            {rest > 0 && (
                <span style={{ color: "var(--admin-blue)", marginLeft: 5, fontWeight: 600 }}>
                    +{rest} more
                </span>
            )}
        </span>
    );
}

// ─── Loading / empty / error states ──────────────────────────────────────────

function TabSpinner() {
    return (
        <div style={{
            display: "flex", flexDirection: "column", alignItems: "center",
            justifyContent: "center", padding: "52px 24px", gap: 12,
            background: "var(--admin-bg-card)", borderRadius: 12,
            border: "1px solid var(--admin-border-color)",
        }}>
            <RefreshCw className="h-6 w-6 animate-spin" style={{ color: "var(--admin-blue)" }} />
            <span style={{ fontSize: 12, color: "var(--admin-text-muted)" }}>Loading…</span>
        </div>
    );
}

function EmptyState({ icon: Icon = Webhook, title, body }: {
    icon?: React.ElementType; title: string; body: string;
}) {
    return (
        <div style={{
            display: "flex", flexDirection: "column", alignItems: "center",
            justifyContent: "center", padding: "52px 24px", gap: 12,
            background: "var(--admin-bg-card)", borderRadius: 12,
            border: "1px solid var(--admin-border-color)", textAlign: "center",
        }}>
            <div style={{
                width: 44, height: 44, borderRadius: 10,
                background: "rgba(59,130,246,0.10)", color: "var(--admin-blue)",
                display: "flex", alignItems: "center", justifyContent: "center",
            }}>
                <Icon className="h-5 w-5" />
            </div>
            <div>
                <div style={{ fontSize: 14, fontWeight: 600, color: "var(--admin-text-primary)" }}>{title}</div>
                <div style={{ fontSize: 12, color: "var(--admin-text-muted)", marginTop: 4, maxWidth: 380 }}>{body}</div>
            </div>
        </div>
    );
}

function ErrorState({ message, onRetry }: { message?: string; onRetry: () => void }) {
    return (
        <div style={{
            display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16,
            padding: "14px 18px", borderRadius: 12,
            border: "1px solid rgba(239,68,68,0.28)", background: "rgba(239,68,68,0.07)",
        }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, color: "var(--admin-red)", fontSize: 13 }}>
                <AlertTriangle className="h-4 w-4 flex-shrink-0" />
                <span>{message || "Unable to load data. Check the backend service is running."}</span>
            </div>
            <button
                className="admin-btn admin-btn-secondary"
                style={{ fontSize: 12, padding: "5px 12px", flexShrink: 0 }}
                onClick={onRetry}
            >
                <RefreshCw className="h-3 w-3" />
                Retry
            </button>
        </div>
    );
}

// ─── Safety banner ────────────────────────────────────────────────────────────

function SafetyStatusPanel() {
    const chips: Array<{ label: string; value: string; tone: Tone }> = [
        { label: "Mode",       value: "ADVISORY_ONLY",   tone: "blue"   },
        { label: "Impact",     value: "None",             tone: "green"  },
        { label: "Storage",    value: "Candidate only",   tone: "muted"  },
        { label: "Processing", value: "Disabled",         tone: "yellow" },
        { label: "Runner",     value: "Disabled",         tone: "yellow" },
        { label: "Execution",  value: "Forbidden",        tone: "red"    },
    ];

    return (
        <div style={{
            display: "flex", flexWrap: "wrap", alignItems: "center",
            justifyContent: "space-between", gap: 14,
            padding: "13px 18px", borderRadius: 12,
            border: "1px solid rgba(16,185,129,0.26)",
            background: "linear-gradient(90deg, rgba(16,185,129,0.07) 0%, rgba(16,185,129,0.03) 100%)",
        }}>
            {/* Left: identity */}
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <div style={{
                    width: 36, height: 36, borderRadius: 9, flexShrink: 0,
                    background: "rgba(16,185,129,0.14)", color: "var(--admin-green)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                }}>
                    <ShieldCheck className="h-5 w-5" />
                </div>
                <div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-primary)" }}>
                        TradingView Safety Mode
                    </div>
                    <div style={{ fontSize: 11, color: "var(--admin-text-muted)", marginTop: 2 }}>
                        Alerts are advisory/candidate signals only — cannot place trades
                    </div>
                </div>
            </div>
            {/* Right: status chips */}
            <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {chips.map(({ label, value, tone }) => (
                    <div key={label} style={{
                        padding: "5px 10px", borderRadius: 8, minWidth: 74,
                        border: "1px solid var(--admin-border-color)",
                        background: "rgba(15,17,23,0.55)",
                    }}>
                        <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--admin-text-muted)", fontWeight: 700, marginBottom: 2 }}>{label}</div>
                        <div style={{ fontSize: 11, fontWeight: 700, color: toneColor(tone) }}>{value}</div>
                    </div>
                ))}
            </div>
        </div>
    );
}

// ─── Coming-later locked controls ─────────────────────────────────────────────

function ComingLaterControls() {
    return (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {["Create Webhook", "Enable / Disable", "Rotate Token"].map((label) => (
                <button
                    key={label}
                    disabled
                    title="Backend write support is not implemented yet."
                    style={{
                        display: "inline-flex", alignItems: "center", gap: 5,
                        padding: "5px 10px", fontSize: 11, fontWeight: 600, borderRadius: 6,
                        border: "1px solid var(--admin-border-color)",
                        background: "transparent", color: "var(--admin-text-muted)",
                        opacity: 0.6, cursor: "not-allowed",
                    }}
                >
                    <Lock className="h-3 w-3" />
                    {label}
                </button>
            ))}
        </div>
    );
}

// ─── Table shared styles ───────────────────────────────────────────────────────

const TH: React.CSSProperties = {
    padding: "9px 12px", textAlign: "left",
    fontSize: 10, fontWeight: 700, color: "var(--admin-text-muted)",
    textTransform: "uppercase", letterSpacing: "0.07em", whiteSpace: "nowrap",
};

const TD: React.CSSProperties = {
    padding: "8px 12px",
    color: "var(--admin-text-primary)",
    verticalAlign: "middle",
};

const TD_MUTED: React.CSSProperties = { ...TD, color: "var(--admin-text-muted)", fontSize: 12 };
const TD_SEC: React.CSSProperties   = { ...TD, color: "var(--admin-text-secondary)" };

function TableRow({
    children, last,
}: { children: React.ReactNode; last?: boolean }) {
    return (
        <tr
            style={{ borderBottom: last ? undefined : "1px solid var(--admin-border-color)", transition: "background 0.1s" }}
            onMouseEnter={(e) => { (e.currentTarget as HTMLTableRowElement).style.background = "var(--admin-bg-hover)"; }}
            onMouseLeave={(e) => { (e.currentTarget as HTMLTableRowElement).style.background = ""; }}
        >
            {children}
        </tr>
    );
}

function DataCard({ children }: { children: React.ReactNode }) {
    return (
        <div className="admin-card" style={{ padding: 0, overflow: "hidden" }}>
            <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                    {children}
                </table>
            </div>
        </div>
    );
}

function TableHead({ cols }: { cols: string[] }) {
    return (
        <thead>
            <tr style={{ background: "var(--admin-bg-sidebar)", borderBottom: "1px solid var(--admin-border-color)" }}>
                {cols.map((h) => <th key={h} style={TH}>{h}</th>)}
            </tr>
        </thead>
    );
}

// ─── Webhooks tab ─────────────────────────────────────────────────────────────

function WebhooksTab({ items }: { items: TradingViewWebhook[] }) {
    if (items.length === 0) {
        return <EmptyState icon={Webhook} title="No webhooks configured yet." body="Create and rotation controls are read-only until backend write support is added." />;
    }
    return (
        <DataCard>
            <TableHead cols={["Name", "Bot", "Mode", "Status", "Symbols", "Actions", "Rate", "Last Used", "Updated"]} />
            <tbody>
                {items.map((item, i) => (
                    <TableRow key={item.id} last={i === items.length - 1}>
                        <td style={{ ...TD, fontWeight: 600, whiteSpace: "nowrap" }}>{item.name}</td>
                        <td style={TD_SEC}>{fv(item.bot_id)}</td>
                        <td style={TD}><Badge value={item.mode} /></td>
                        <td style={TD}><Badge value={item.is_enabled ? "Enabled" : "Disabled"} /></td>
                        <td style={TD}><SymbolsList symbols={item.allowed_symbols} /></td>
                        <td style={TD}><SymbolsList symbols={item.allowed_actions} /></td>
                        <td style={{ ...TD_SEC, whiteSpace: "nowrap" }}>{fv(item.rate_limit_per_minute)}/min</td>
                        <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.last_used_at)}</td>
                        <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.updated_at)}</td>
                    </TableRow>
                ))}
            </tbody>
        </DataCard>
    );
}

// ─── Alerts tab ───────────────────────────────────────────────────────────────

function AlertsTab({ items }: { items: TradingViewAlert[] }) {
    const [status,    setStatus]    = useState("all");
    const [symbol,    setSymbol]    = useState("");
    const [action,    setAction]    = useState("all");
    const [signature, setSignature] = useState("all");

    const filtered = useMemo(() => items.filter((item) => {
        const okStatus =
            status === "all" ||
            item.status === status ||
            (status === "accepted" && item.status === "ACCEPTED_ADVISORY") ||
            (status === "rejected" && item.status !== "ACCEPTED_ADVISORY");
        const okSymbol = !symbol.trim() ||
            (item.symbol_normalized || item.symbol_raw || "").toUpperCase().includes(symbol.trim().toUpperCase());
        const okAction = action === "all" || item.action === action;
        const okSig =
            signature === "all" ||
            (signature === "valid"       && item.signature_valid === 1) ||
            (signature === "invalid"     && item.signature_valid === 0) ||
            (signature === "not-present" && item.signature_valid === null);
        return okStatus && okSymbol && okAction && okSig;
    }), [items, status, symbol, action, signature]);

    const acceptedCount = items.filter((i) => i.status === "ACCEPTED_ADVISORY").length;
    const rejectedCount = items.length - acceptedCount;

    const sel: React.CSSProperties = { ...filterInput, width: "auto" };

    return (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {/* Compact filter bar */}
            <div className="admin-card" style={{ padding: "10px 14px" }}>
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 8 }}>
                    <span style={{ fontSize: 11, fontWeight: 700, color: "var(--admin-text-muted)", display: "flex", alignItems: "center", gap: 5, flexShrink: 0 }}>
                        <SlidersHorizontal className="h-3 w-3" />
                        Filters
                    </span>
                    <select className="admin-input" style={sel} value={status} onChange={(e) => setStatus(e.target.value)}>
                        <option value="all">All statuses</option>
                        <option value="accepted">Accepted</option>
                        <option value="rejected">Rejected</option>
                        <option value="DUPLICATE">Duplicate</option>
                        <option value="STALE">Stale</option>
                        <option value="INVALID_SIGNATURE">Invalid signature</option>
                        <option value="UNSUPPORTED_ACTION">Unsupported action</option>
                        <option value="UNSUPPORTED_SYMBOL">Unsupported symbol</option>
                    </select>
                    <input className="admin-input" style={{ ...filterInput, width: 130 }} value={symbol} onChange={(e) => setSymbol(e.target.value)} placeholder="Symbol…" />
                    <select className="admin-input" style={sel} value={action} onChange={(e) => setAction(e.target.value)}>
                        <option value="all">All actions</option>
                        <option value="BUY">BUY</option>
                        <option value="SELL">SELL</option>
                        <option value="CLOSE">CLOSE</option>
                        <option value="REVERSE">REVERSE</option>
                    </select>
                    <select className="admin-input" style={sel} value={signature} onChange={(e) => setSignature(e.target.value)}>
                        <option value="all">All signatures</option>
                        <option value="valid">Valid</option>
                        <option value="invalid">Invalid</option>
                        <option value="not-present">Not present</option>
                    </select>
                    {/* Summary stats */}
                    <div style={{ marginLeft: "auto", display: "flex", gap: 12, alignItems: "center" }}>
                        <span style={{ fontSize: 11, fontWeight: 700, color: "var(--admin-green)" }}>{acceptedCount} accepted</span>
                        <span style={{ fontSize: 11, fontWeight: 700, color: "var(--admin-yellow)" }}>{rejectedCount} rejected</span>
                        <span style={{ fontSize: 11, color: "var(--admin-text-muted)" }}>{filtered.length} shown</span>
                    </div>
                </div>
            </div>

            {items.length === 0 ? (
                <EmptyState title="No TradingView alerts received yet." body="Accepted and rejected alerts will appear here after webhook activity." />
            ) : filtered.length === 0 ? (
                <EmptyState title="No alerts match these filters." body="Adjust the status, symbol, action, or signature filters above." />
            ) : (
                <DataCard>
                    <TableHead cols={["Symbol", "Action", "Status", "Reject Reason", "Received", "Alert Time", "Strategy", "TF", "Signature"]} />
                    <tbody>
                        {filtered.map((item, i) => (
                            <TableRow key={item.id} last={i === filtered.length - 1}>
                                <td style={{ ...TD, fontWeight: 600, whiteSpace: "nowrap" }}>
                                    {fv(item.symbol_normalized || item.symbol_raw)}
                                </td>
                                <td style={TD}><Badge value={item.action} /></td>
                                <td style={TD}><Badge value={item.status} /></td>
                                <td style={{ ...TD, maxWidth: 200 }}>
                                    {item.reject_reason ? (
                                        <span title={item.reject_reason} style={{ color: "var(--admin-yellow)", cursor: "default" }}>
                                            {truncate(item.reject_reason, 42)}
                                        </span>
                                    ) : <span style={{ color: "var(--admin-text-muted)" }}>—</span>}
                                </td>
                                <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.received_at)}</td>
                                <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.alert_timestamp)}</td>
                                <td style={TD_SEC}>{fv(item.strategy_name)}</td>
                                <td style={TD_SEC}>{fv(item.timeframe)}</td>
                                <td style={TD}>
                                    {item.signature_valid === 1 ? (
                                        <span style={{ display: "inline-flex", alignItems: "center", gap: 4, color: "var(--admin-green)", fontSize: 11, fontWeight: 700 }}>
                                            <CheckCircle2 className="h-3 w-3" /> Valid
                                        </span>
                                    ) : item.signature_valid === 0 ? (
                                        <span style={{ display: "inline-flex", alignItems: "center", gap: 4, color: "var(--admin-red)", fontSize: 11, fontWeight: 700 }}>
                                            <XCircle className="h-3 w-3" /> Invalid
                                        </span>
                                    ) : (
                                        <span style={{ color: "var(--admin-text-muted)", fontSize: 11 }}>None</span>
                                    )}
                                </td>
                            </TableRow>
                        ))}
                    </tbody>
                </DataCard>
            )}
        </div>
    );
}

// ─── Decisions tab ────────────────────────────────────────────────────────────

function DecisionsTab({ items }: { items: TradingViewDecision[] }) {
    if (items.length === 0) {
        return <EmptyState title="No signal decisions recorded yet." body="Advisory decisions appear here after accepted alerts are processed." />;
    }
    return (
        <DataCard>
            <TableHead cols={["Symbol", "Action", "Mode", "Final Status", "Final Reason", "Event Filter", "Policy", "Execution", "Trace ID", "Created"]} />
            <tbody>
                {items.map((item, i) => (
                    <TableRow key={item.id} last={i === items.length - 1}>
                        <td style={{ ...TD, fontWeight: 600, whiteSpace: "nowrap" }}>{fv(item.symbol)}</td>
                        <td style={TD}><Badge value={item.action} /></td>
                        <td style={TD}><Badge value={item.mode} /></td>
                        <td style={TD}><Badge value={item.final_status} /></td>
                        <td style={{ ...TD_SEC, maxWidth: 260 }}>
                            <span title={item.final_reason ?? undefined} style={{ cursor: "default" }}>
                                {truncate(item.final_reason || "Alert accepted and stored, execution disabled", 52)}
                            </span>
                        </td>
                        <td style={TD_SEC}>{fv(item.event_filter_result)}</td>
                        <td style={TD_SEC}>{fv(item.policy_result)}</td>
                        <td style={TD_MUTED}>{item.execution_result || "NOT_APPLICABLE"}</td>
                        <td style={{ ...TD_MUTED, fontFamily: "monospace" }}>
                            <span title={fv(item.decision_trace_id)}>
                                {truncate(fv(item.decision_trace_id), 14)}
                            </span>
                        </td>
                        <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.created_at)}</td>
                    </TableRow>
                ))}
            </tbody>
        </DataCard>
    );
}

// ─── Queue tab ────────────────────────────────────────────────────────────────

function QueueTab({ items }: { items: ExternalSignalQueueRow[] }) {
    if (items.length === 0) {
        return (
            <EmptyState
                icon={Database}
                title="No external signal queue rows yet."
                body="ADVISORY_ONLY alerts do not enqueue. EXTERNAL_SIGNAL_CANDIDATE webhooks create PENDING rows for future runner processing."
            />
        );
    }
    return (
        <DataCard>
            <TableHead cols={["ID", "Symbol", "Action", "Side", "Confidence", "Status", "Available", "Expires", "Result", "Created"]} />
            <tbody>
                {items.map((item, i) => {
                    const resultStr = item.result_json ? JSON.stringify(item.result_json) : null;
                    return (
                        <TableRow key={item.id} last={i === items.length - 1}>
                            <td style={{ ...TD_MUTED, fontFamily: "monospace" }}>{item.id}</td>
                            <td style={{ ...TD, fontWeight: 600, whiteSpace: "nowrap" }}>{item.symbol}</td>
                            <td style={TD}><Badge value={item.action} /></td>
                            <td style={TD_SEC}>{fv(item.side)}</td>
                            <td style={TD_SEC}>
                                {item.confidence == null ? "—" : item.confidence.toFixed(2)}
                            </td>
                            <td style={TD}><Badge value={item.status} /></td>
                            <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.available_at)}</td>
                            <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.expires_at)}</td>
                            <td style={{ ...TD_MUTED, maxWidth: 180 }}>
                                {resultStr ? (
                                    <span title={resultStr} style={{ cursor: "default", fontFamily: "monospace", fontSize: 11 }}>
                                        {truncate(resultStr, 32)}
                                    </span>
                                ) : "—"}
                            </td>
                            <td style={{ ...TD_MUTED, whiteSpace: "nowrap" }}>{formatDate(item.created_at)}</td>
                        </TableRow>
                    );
                })}
            </tbody>
        </DataCard>
    );
}

// ─── Active tab router ────────────────────────────────────────────────────────

// Each tab independently manages its own loading/error/data state.
// Other tabs loading or failing does not block the active tab.
function ActiveTabContent({
    activeTab, webhooks, alerts, decisions, queue,
}: {
    activeTab: TabId;
    webhooks:  ReturnType<typeof useQuery<{ items: TradingViewWebhook[] }>>;
    alerts:    ReturnType<typeof useQuery<{ items: TradingViewAlert[] }>>;
    decisions: ReturnType<typeof useQuery<{ items: TradingViewDecision[] }>>;
    queue:     ReturnType<typeof useQuery<{ items: ExternalSignalQueueRow[] }>>;
}) {
    if (activeTab === "webhooks") {
        if (webhooks.isLoading) return <TabSpinner />;
        if (webhooks.isError)   return <ErrorState onRetry={() => webhooks.refetch()} />;
        return <WebhooksTab items={webhooks.data?.items ?? []} />;
    }
    if (activeTab === "alerts") {
        if (alerts.isLoading) return <TabSpinner />;
        if (alerts.isError)   return <ErrorState onRetry={() => alerts.refetch()} />;
        return <AlertsTab items={alerts.data?.items ?? []} />;
    }
    if (activeTab === "decisions") {
        if (decisions.isLoading) return <TabSpinner />;
        if (decisions.isError)   return <ErrorState onRetry={() => decisions.refetch()} />;
        return <DecisionsTab items={decisions.data?.items ?? []} />;
    }
    // queue tab
    if (queue.isLoading) return <TabSpinner />;
    if (queue.isError)   return <ErrorState onRetry={() => queue.refetch()} />;
    return <QueueTab items={queue.data?.items ?? []} />;
}

// ─── Shared filter input style ────────────────────────────────────────────────

const filterInput: React.CSSProperties = { fontSize: 12, padding: "5px 10px" };

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function TradingView() {
    const [activeTab, setActiveTab] = useState<TabId>("webhooks");

    const webhooks = useQuery({
        queryKey: ["adminTradingViewWebhooks"],
        queryFn:  getTradingViewWebhooks,
        refetchInterval: 60_000,
        ...TV_QUERY_OPTIONS,
    });
    const alerts = useQuery({
        queryKey: ["adminTradingViewAlerts"],
        queryFn:  getTradingViewAlerts,
        refetchInterval: 30_000,
        ...TV_QUERY_OPTIONS,
    });
    const decisions = useQuery({
        queryKey: ["adminTradingViewDecisions"],
        queryFn:  getTradingViewDecisions,
        refetchInterval: 30_000,
        ...TV_QUERY_OPTIONS,
    });
    const queue = useQuery({
        queryKey: ["adminTradingViewExternalSignals"],
        queryFn:  getTradingViewExternalSignals,
        refetchInterval: 30_000,
        ...TV_QUERY_OPTIONS,
    });

    const isFetching = webhooks.isFetching || alerts.isFetching || decisions.isFetching || queue.isFetching;

    const refresh = () => {
        webhooks.refetch();
        alerts.refetch();
        decisions.refetch();
        queue.refetch();
    };

    const webhookItems  = webhooks.data?.items  ?? [];
    const alertItems    = alerts.data?.items    ?? [];
    const queueItems    = queue.data?.items     ?? [];
    const acceptedCount = alertItems.filter((a) => a.status === "ACCEPTED_ADVISORY").length;
    const rejectedCount = alertItems.length - acceptedCount;
    const processedCount = queueItems.filter((q) => q.status === "PROCESSED").length;

    const TABS: Array<{ id: TabId; label: string; count?: number }> = [
        { id: "webhooks",  label: "Webhooks",             count: webhooks.isLoading ? undefined : webhookItems.length },
        { id: "alerts",    label: "Alerts",               count: alerts.isLoading   ? undefined : alertItems.length },
        { id: "decisions", label: "Signal Decisions" },
        { id: "queue",     label: "External Signal Queue", count: queue.isLoading   ? undefined : queueItems.length },
    ];

    return (
        <AdminLayout>
            <div style={{ display: "flex", flexDirection: "column", gap: 18, maxWidth: 1600, margin: "0 auto" }}>

                {/* Header */}
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "flex-start", justifyContent: "space-between", gap: 10 }}>
                    <div>
                        <h1 style={{ fontSize: "1.55rem", fontWeight: 800, letterSpacing: "-0.025em", color: "var(--admin-text-primary)", margin: 0, lineHeight: 1.2 }}>
                            TradingView
                        </h1>
                        <p style={{ margin: "4px 0 0", fontSize: 13, color: "var(--admin-text-secondary)", lineHeight: 1.4 }}>
                            External alert intake, advisory validation, queue visibility, and decision audit trail.
                        </p>
                    </div>
                    <button
                        className="admin-btn admin-btn-secondary"
                        onClick={refresh}
                        disabled={isFetching}
                        title="Refresh all TradingView data"
                        style={{ flexShrink: 0, fontSize: 13 }}
                    >
                        <RefreshCw className={`h-4 w-4 ${isFetching ? "animate-spin" : ""}`} />
                        {isFetching ? "Refreshing…" : "Refresh"}
                    </button>
                </div>

                {/* Safety banner */}
                <SafetyStatusPanel />

                {/* Summary cards — 4-up grid */}
                <div style={{ display: "grid", gap: 12, gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))" }}>

                    {/* Webhooks */}
                    <div className="admin-card" style={{ padding: "14px 18px" }}>
                        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 8 }}>
                            <div>
                                <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)", fontWeight: 700 }}>Webhooks</div>
                                <div style={{ fontSize: "1.75rem", fontWeight: 800, color: "var(--admin-blue)", lineHeight: 1.1, margin: "4px 0" }}>
                                    {webhooks.isLoading ? "—" : webhookItems.length}
                                </div>
                                <div style={{ fontSize: 11, color: "var(--admin-text-muted)" }}>Active intake endpoints</div>
                            </div>
                            <div style={{ width: 34, height: 34, borderRadius: 9, background: "rgba(59,130,246,0.12)", color: "var(--admin-blue)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                                <Webhook className="h-4 w-4" />
                            </div>
                        </div>
                    </div>

                    {/* Accepted */}
                    <div className="admin-card" style={{ padding: "14px 18px" }}>
                        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 8 }}>
                            <div>
                                <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)", fontWeight: 700 }}>Accepted Advisory</div>
                                <div style={{ fontSize: "1.75rem", fontWeight: 800, color: "var(--admin-green)", lineHeight: 1.1, margin: "4px 0" }}>
                                    {alerts.isLoading ? "—" : acceptedCount}
                                </div>
                                <div style={{ fontSize: 11, color: "var(--admin-text-muted)" }}>Stored for review</div>
                            </div>
                            <div style={{ width: 34, height: 34, borderRadius: 9, background: "rgba(16,185,129,0.12)", color: "var(--admin-green)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                                <CheckCircle2 className="h-4 w-4" />
                            </div>
                        </div>
                    </div>

                    {/* Rejected */}
                    <div className="admin-card" style={{ padding: "14px 18px" }}>
                        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 8 }}>
                            <div>
                                <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)", fontWeight: 700 }}>Rejected / Filtered</div>
                                <div style={{
                                    fontSize: "1.75rem", fontWeight: 800,
                                    color: rejectedCount > 0 ? "var(--admin-yellow)" : "var(--admin-text-primary)",
                                    lineHeight: 1.1, margin: "4px 0",
                                }}>
                                    {alerts.isLoading ? "—" : rejectedCount}
                                </div>
                                <div style={{ fontSize: 11, color: "var(--admin-text-muted)" }}>Duplicate, stale, invalid</div>
                            </div>
                            <div style={{ width: 34, height: 34, borderRadius: 9, background: "rgba(245,158,11,0.12)", color: "var(--admin-yellow)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                                <XCircle className="h-4 w-4" />
                            </div>
                        </div>
                    </div>

                    {/* Signal Queue */}
                    <div className="admin-card" style={{ padding: "14px 18px" }}>
                        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 8 }}>
                            <div>
                                <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)", fontWeight: 700 }}>Signal Queue</div>
                                {queue.isLoading ? (
                                    <div style={{ fontSize: "1.75rem", fontWeight: 800, color: "var(--admin-blue)", lineHeight: 1.1, margin: "4px 0" }}>—</div>
                                ) : (
                                    <div style={{ display: "flex", alignItems: "baseline", gap: 6, margin: "4px 0" }}>
                                        <span style={{ fontSize: "1.75rem", fontWeight: 800, color: "var(--admin-blue)", lineHeight: 1.1 }}>{queueItems.length}</span>
                                        <span style={{ fontSize: 11, color: "var(--admin-text-muted)" }}>stored / {processedCount} processed</span>
                                    </div>
                                )}
                                <div style={{ fontSize: 11, color: "var(--admin-text-muted)" }}>Runner claiming disabled</div>
                            </div>
                            <div style={{ width: 34, height: 34, borderRadius: 9, background: "rgba(59,130,246,0.12)", color: "var(--admin-blue)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                                <Database className="h-4 w-4" />
                            </div>
                        </div>
                    </div>

                </div>

                {/* Tab bar + locked controls in one row */}
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: 10 }}>
                    <div className="admin-ml-tabs" style={{ position: "static", flex: "1 1 auto", minWidth: 0 }}>
                        {TABS.map(({ id, label, count }) => (
                            <button
                                key={id}
                                className={`admin-ml-tab ${activeTab === id ? "active" : ""}`}
                                onClick={() => setActiveTab(id)}
                            >
                                {label}
                                {count !== undefined && (
                                    <span style={{
                                        marginLeft: 5, padding: "1px 6px", borderRadius: 999, fontSize: 10, fontWeight: 700,
                                        background: activeTab === id ? "rgba(59,130,246,0.22)" : "rgba(255,255,255,0.07)",
                                        color: activeTab === id ? "var(--admin-blue)" : "var(--admin-text-muted)",
                                    }}>
                                        {count}
                                    </span>
                                )}
                            </button>
                        ))}
                    </div>
                    <ComingLaterControls />
                </div>

                {/* Tab content */}
                <ActiveTabContent
                    activeTab={activeTab}
                    webhooks={webhooks}
                    alerts={alerts}
                    decisions={decisions}
                    queue={queue}
                />

            </div>
        </AdminLayout>
    );
}
