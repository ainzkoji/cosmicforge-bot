import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, BellRing, DatabaseBackup, Radio, RefreshCw, Users } from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import {
    acknowledgeAlert, addOncallMember, createIncident, getBackupDrill, getOncall, getOverview, listAlerts,
    listIncidents, removeOncallMember, resolveAlert, resolveIncident, updateIncident,
} from "@/api/operations";
import {
    COMPONENT_KEYS, CONTACT_CHANNELS, INCIDENT_IMPACTS, INCIDENT_STATUSES, LEVEL_NAMES, OWNER_REQUIRED_TEXT,
    SEVERITY_TONE, alertActions, drillHeadline, formatEpochMs, formatMs, operationsError, ownerLine,
    rotationOwnerText, sortAlerts, validateIncident, validateMember,
} from "@/utils/operations";
import type { Alert, DrillSummary, Finding, IncidentForm, MemberForm, Oncall } from "@/utils/operations";

/**
 * Production operations (Section H Step 8.5): alerts (acknowledge / resolve), on-call ownership, backup and
 * restore-drill status, and public status-page incidents. On-call people exist only when an administrator
 * adds them here; until then every rotation shows OPERATIONS OWNER REQUIRED.
 */
const card = "border border-border/50 rounded-xl p-4 space-y-3";
const input = "bg-background border border-border rounded p-1.5 text-sm";
const button = "px-3 py-1.5 rounded border border-border text-sm hover:bg-muted/40 disabled:opacity-50";

function Findings({ findings }: { findings: Finding[] }) {
    if (!findings.length) return <div className="text-sm text-green-500">No configuration findings.</div>;
    return (
        <ul className="space-y-1 text-sm">
            {findings.map((f, i) => (
                <li key={i} className={f.severity === "CRITICAL" ? "text-red-400" : "text-yellow-500"}>
                    <code>{f.severity}</code> {f.message}
                </li>
            ))}
        </ul>
    );
}

function AlertRow({ alert, onDone }: { alert: Alert; onDone: (msg: string | null) => void }) {
    const [note, setNote] = useState("");
    const [busy, setBusy] = useState(false);
    const actions = alertActions(alert);
    const run = async (fn: () => Promise<unknown>) => {
        setBusy(true);
        try { await fn(); setNote(""); onDone(null); } catch (e) { onDone(operationsError(e)); } finally { setBusy(false); }
    };
    return (
        <tr className="border-t border-border/40 align-top">
            <td className="p-2"><span className={`px-2 py-0.5 rounded border text-xs font-semibold ${SEVERITY_TONE[alert.severity]}`}>{alert.severity}</span></td>
            <td className="p-2"><code className="text-xs">{alert.category}</code><div className="text-sm">{alert.title}</div>
                {alert.owner_required && <div className="text-xs text-red-400 font-semibold">{OWNER_REQUIRED_TEXT}</div>}</td>
            <td className="p-2 text-xs">{alert.status}{alert.acknowledged_by ? ` by ${alert.acknowledged_by}` : ""}
                <div>level {alert.escalation_level} - {alert.rotation_key}</div></td>
            <td className="p-2 text-xs">{alert.occurrences}x<div>{formatEpochMs(alert.last_seen_at)}</div></td>
            <td className="p-2 space-y-1">
                <input className={input} placeholder="note (required to resolve)" value={note} onChange={(e) => setNote(e.target.value)} />
                <div className="flex gap-2">
                    {actions.acknowledge && <button className={button} disabled={busy}
                        onClick={() => run(() => acknowledgeAlert(alert.alert_id, note))}>Acknowledge</button>}
                    {actions.resolve && <button className={button} disabled={busy || !note.trim()}
                        onClick={() => run(() => resolveAlert(alert.alert_id, note))}>Resolve</button>}
                </div>
            </td>
        </tr>
    );
}

const EMPTY_MEMBER: MemberForm = { rotation_key: "", level: 0, person_name: "", contact_channel: "EMAIL", contact_address: "" };
const EMPTY_INCIDENT: IncidentForm = { title: "", components: [], impact: "DEGRADED", message: "" };

export default function Operations() {
    const [overview, setOverview] = useState<Record<string, any> | null>(null);
    const [alerts, setAlerts] = useState<Alert[]>([]);
    const [oncall, setOncall] = useState<Oncall | null>(null);
    const [drill, setDrill] = useState<DrillSummary | null>(null);
    const [incidents, setIncidents] = useState<Array<Record<string, any>>>([]);
    const [error, setError] = useState<string | null>(null);
    const [member, setMember] = useState<MemberForm>(EMPTY_MEMBER);
    const [incident, setIncident] = useState<IncidentForm>(EMPTY_INCIDENT);
    const [formError, setFormError] = useState<string | null>(null);
    const [updateText, setUpdateText] = useState<Record<string, { status: string; message: string }>>({});

    const load = useCallback(() => {
        Promise.all([getOverview(), listAlerts(), getOncall(), getBackupDrill(), listIncidents()])
            .then(([o, a, c, d, i]) => {
                setOverview(o); setAlerts(sortAlerts(a)); setOncall(c); setDrill(d); setIncidents(i.incidents); setError(null);
            })
            .catch((e) => setError(operationsError(e)));
    }, []);
    useEffect(() => { load(); }, [load]);

    const rotations = oncall?.rotations.map((r) => r.rotation_key) ?? [];
    const alerting = overview?.alerting as { status: string; findings: Finding[]; recent_deliveries: any[] } | undefined;

    const addMember = async () => {
        const problems = validateMember(member, rotations);
        if (problems.length) { setFormError(problems.join(" ")); return; }
        try { await addOncallMember(member); setMember(EMPTY_MEMBER); setFormError(null); load(); }
        catch (e) { setFormError(operationsError(e)); }
    };
    const postIncident = async () => {
        const problems = validateIncident(incident);
        if (problems.length) { setFormError(problems.join(" ")); return; }
        try { await createIncident(incident); setIncident(EMPTY_INCIDENT); setFormError(null); load(); }
        catch (e) { setFormError(operationsError(e)); }
    };

    return (
        <AdminLayout>
            <div className="p-6 space-y-6">
                <div className="flex items-center justify-between">
                    <h1 className="text-2xl font-bold flex items-center gap-2"><BellRing className="w-6 h-6" /> Operations</h1>
                    <button className={button} onClick={load}><RefreshCw className="w-4 h-4 inline" /> Refresh</button>
                </div>
                <div className={`rounded-xl border p-4 font-semibold flex items-center gap-2 ${ownerLine(oncall) === OWNER_REQUIRED_TEXT
                    ? "border-red-500/40 bg-red-500/10 text-red-400" : "border-green-500/30 bg-green-500/10 text-green-500"}`}>
                    <AlertTriangle className="w-5 h-5" /> {ownerLine(oncall)}
                </div>
                {error && <div className="text-sm text-red-400">Unavailable: <code>{error}</code></div>}
                {formError && <div className="text-sm text-red-400">{formError}</div>}

                <section className={card}>
                    <h2 className="font-bold text-lg">Alerting system: <code>{alerting?.status ?? "?"}</code></h2>
                    <Findings findings={alerting?.findings ?? []} />
                    <div className="text-xs text-muted-foreground">Deliveries (24 h): {(alerting?.recent_deliveries ?? [])
                        .map((d: any) => `${d.sink} ${d.status} ${d.count}`).join(" | ") || "none recorded"}</div>
                </section>

                <section className={card}>
                    <h2 className="font-bold text-lg">Open alerts ({alerts.length})</h2>
                    {alerts.length === 0 ? <div className="text-sm text-muted-foreground">No open or acknowledged alerts.</div> : (
                        <table className="w-full text-left">
                            <thead className="text-xs text-muted-foreground"><tr><th className="p-2">Severity</th><th className="p-2">Alert</th>
                                <th className="p-2">State</th><th className="p-2">Seen</th><th className="p-2">Action</th></tr></thead>
                            <tbody>{alerts.map((a) => <AlertRow key={a.alert_id} alert={a} onDone={(m) => { setFormError(m); load(); }} />)}</tbody>
                        </table>
                    )}
                </section>

                <section className={card}>
                    <h2 className="font-bold text-lg flex items-center gap-2"><Users className="w-5 h-5" /> On-call ownership</h2>
                    <div className="grid md:grid-cols-2 gap-3">
                        {(oncall?.rotations ?? []).map((r) => (
                            <div key={r.rotation_key} className="border border-border/40 rounded-lg p-3 text-sm space-y-1">
                                <div className="font-semibold"><code>{r.rotation_key}</code></div>
                                <div className="text-xs text-muted-foreground">{r.description}</div>
                                <div className={r.members.some((m) => m.level === 0) ? "" : "text-red-400 font-semibold"}>
                                    Primary: {rotationOwnerText(r)}</div>
                                {r.members.map((m) => (
                                    <div key={m.member_id} className="flex items-center justify-between text-xs">
                                        <span>{LEVEL_NAMES[m.level]}: {m.person_name} ({m.contact_channel})</span>
                                        <button className={button} onClick={() => removeOncallMember(m.member_id).then(load)
                                            .catch((e) => setFormError(operationsError(e)))}>Remove</button>
                                    </div>
                                ))}
                            </div>
                        ))}
                    </div>
                    <div className="flex flex-wrap gap-2 items-end text-sm">
                        <select className={input} value={member.rotation_key} onChange={(e) => setMember({ ...member, rotation_key: e.target.value })}>
                            <option value="">rotation</option>{rotations.map((r) => <option key={r}>{r}</option>)}</select>
                        <select className={input} value={member.level} onChange={(e) => setMember({ ...member, level: Number(e.target.value) })}>
                            {LEVEL_NAMES.map((n, i) => <option key={n} value={i}>{n}</option>)}</select>
                        <input className={input} placeholder="person name" value={member.person_name}
                            onChange={(e) => setMember({ ...member, person_name: e.target.value })} />
                        <select className={input} value={member.contact_channel} onChange={(e) => setMember({ ...member, contact_channel: e.target.value })}>
                            {CONTACT_CHANNELS.map((c) => <option key={c}>{c}</option>)}</select>
                        <input className={input} placeholder="contact address" value={member.contact_address}
                            onChange={(e) => setMember({ ...member, contact_address: e.target.value })} />
                        <button className={button} onClick={addMember}>Assign person</button>
                    </div>
                </section>

                <section className={card}>
                    <h2 className="font-bold text-lg flex items-center gap-2"><DatabaseBackup className="w-5 h-5" /> Backups and restore drill</h2>
                    <div className={drill?.status === "PASSED" ? "text-green-500" : "text-yellow-500"}>{drillHeadline(drill)}</div>
                    {drill && drill.status !== "NO_DRILL_RECORDED" && (
                        <div className="text-sm grid md:grid-cols-3 gap-2">
                            <div>RPO measured: {formatMs(drill.rpo_ms)} (target {drill.rpo_target_minutes})</div>
                            <div>RTO measured: {formatMs(drill.rto_ms)} (target {drill.rto_target_minutes})</div>
                            <div>Encryption: <code>{drill.encryption}</code> | PostgreSQL leg: <code>{drill.postgres_leg}</code></div>
                        </div>)}
                    <div className="text-xs text-muted-foreground">RPO/RTO targets stay UNSET until the operations owner decides them.</div>
                </section>

                <section className={card}>
                    <h2 className="font-bold text-lg flex items-center gap-2"><Radio className="w-5 h-5" /> Public status page incidents</h2>
                    <div className="text-xs text-muted-foreground">Everything written here is PUBLIC: no hostnames, IPs, account ids, e-mail addresses or secrets.</div>
                    {incidents.map((i) => (
                        <div key={i.incident_id} className="border border-border/40 rounded-lg p-3 text-sm space-y-2">
                            <div className="font-semibold">{i.title} - <code>{i.status}</code> / <code>{i.impact}</code> ({(i.components ?? []).join(", ")})</div>
                            {(i.updates ?? []).map((u: any) => <div key={u.id} className="text-xs">{formatEpochMs(u.at)} <code>{u.status}</code> {u.message} <span className="text-muted-foreground">({u.actor})</span></div>)}
                            {i.status !== "RESOLVED" && (
                                <div className="flex flex-wrap gap-2">
                                    <select className={input} value={updateText[i.incident_id]?.status ?? "MONITORING"}
                                        onChange={(e) => setUpdateText({ ...updateText, [i.incident_id]: { message: updateText[i.incident_id]?.message ?? "", status: e.target.value } })}>
                                        {INCIDENT_STATUSES.filter((s) => s !== "RESOLVED").map((s) => <option key={s}>{s}</option>)}</select>
                                    <input className={input} placeholder="public update" value={updateText[i.incident_id]?.message ?? ""}
                                        onChange={(e) => setUpdateText({ ...updateText, [i.incident_id]: { status: updateText[i.incident_id]?.status ?? "MONITORING", message: e.target.value } })} />
                                    <button className={button} onClick={() => updateIncident(i.incident_id, { status: updateText[i.incident_id]?.status ?? "MONITORING", message: updateText[i.incident_id]?.message ?? "" })
                                        .then(load).catch((e) => setFormError(operationsError(e)))}>Post update</button>
                                    <button className={button} onClick={() => resolveIncident(i.incident_id, updateText[i.incident_id]?.message || "Resolved.")
                                        .then(load).catch((e) => setFormError(operationsError(e)))}>Resolve</button>
                                </div>)}
                        </div>
                    ))}
                    <div className="flex flex-wrap gap-2 items-end text-sm border-t border-border/40 pt-3">
                        <input className={input} placeholder="title" value={incident.title} onChange={(e) => setIncident({ ...incident, title: e.target.value })} />
                        <select className={input} value={incident.impact} onChange={(e) => setIncident({ ...incident, impact: e.target.value })}>
                            {INCIDENT_IMPACTS.map((s) => <option key={s}>{s}</option>)}</select>
                        {COMPONENT_KEYS.map((c) => (
                            <label key={c} className="text-xs flex items-center gap-1">
                                <input type="checkbox" checked={incident.components.includes(c)} onChange={(e) => setIncident({
                                    ...incident, components: e.target.checked ? [...incident.components, c] : incident.components.filter((x) => x !== c) })} />{c}
                            </label>))}
                        <input className={`${input} flex-1`} placeholder="public message" value={incident.message}
                            onChange={(e) => setIncident({ ...incident, message: e.target.value })} />
                        <button className={button} onClick={postIncident}>Publish incident</button>
                    </div>
                </section>
            </div>
        </AdminLayout>
    );
}
