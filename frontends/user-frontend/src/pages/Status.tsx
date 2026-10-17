import { useEffect, useState } from "react";
import { Activity, RefreshCw } from "lucide-react";
import { getPublicStatus } from "@/api/status";
import {
    INCIDENT_STATUS_LABELS, STATE_LABELS, STATE_TONE, activeIncidents, headline, resolvedIncidents,
} from "@/lib/statusPage";
import type { PublicStatus, StatusIncident } from "@/lib/statusPage";

/**
 * Public status page (Section H Step 8.5). Shows only what GET /api/public/status publishes: the state of
 * each platform component and incident notices written by the operations team. Refreshes every 60 seconds.
 */
const REFRESH_MS = 60_000;

function when(iso: string | null): string {
    if (!iso) return "";
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? "" : d.toLocaleString();
}

function Incident({ incident }: { incident: StatusIncident }) {
    return (
        <div className="bg-card border border-border rounded-xl p-5 space-y-3">
            <div className="flex flex-wrap items-center gap-3">
                <h3 className="font-bold">{incident.title}</h3>
                <span className={`px-2 py-0.5 rounded border text-xs font-semibold ${STATE_TONE[incident.impact]}`}>
                    {STATE_LABELS[incident.impact]}</span>
                <span className="text-xs text-muted-foreground">{INCIDENT_STATUS_LABELS[incident.status] ?? incident.status}</span>
            </div>
            {incident.components.length > 0 && (
                <div className="text-xs text-muted-foreground">Affects: {incident.components.join(", ")}</div>)}
            <ol className="space-y-2">
                {incident.updates.map((u, i) => (
                    <li key={i} className="text-sm">
                        <span className="font-semibold">{INCIDENT_STATUS_LABELS[u.status] ?? u.status}</span>
                        <span className="text-muted-foreground"> - {when(u.at)}</span>
                        <div className="whitespace-pre-wrap">{u.message}</div>
                    </li>
                ))}
            </ol>
        </div>
    );
}

export default function Status() {
    const [status, setStatus] = useState<PublicStatus | null>(null);

    useEffect(() => {
        let alive = true;
        const load = () => getPublicStatus().then((s) => { if (alive) setStatus(s); });
        load();
        const timer = window.setInterval(load, REFRESH_MS);
        return () => { alive = false; window.clearInterval(timer); };
    }, []);

    return (
        <div className="max-w-3xl mx-auto px-4 py-16 space-y-10">
            <div className="text-center space-y-3">
                <h1 className="text-4xl font-bold flex items-center justify-center gap-3"><Activity className="w-8 h-8" /> System status</h1>
                {status ? (
                    <div className={`inline-block px-4 py-2 rounded-lg border font-semibold ${STATE_TONE[status.overall]}`}>
                        {headline(status)}</div>
                ) : <div className="text-muted-foreground flex items-center justify-center gap-2"><RefreshCw className="w-4 h-4 animate-spin" /> Loading...</div>}
                {status?.generated_at && <div className="text-xs text-muted-foreground">Updated {when(status.generated_at)}</div>}
            </div>

            {status && status.components.length > 0 && (
                <div className="bg-card border border-border rounded-xl divide-y divide-border">
                    {status.components.map((c) => (
                        <div key={c.key} className="flex items-center justify-between p-4">
                            <span className="font-medium">{c.name}</span>
                            <span className={`px-2 py-0.5 rounded border text-xs font-semibold ${STATE_TONE[c.status]}`}>{STATE_LABELS[c.status]}</span>
                        </div>
                    ))}
                </div>
            )}

            {status && activeIncidents(status).length > 0 && (
                <section className="space-y-4">
                    <h2 className="text-2xl font-bold">Current incidents</h2>
                    {activeIncidents(status).map((i) => <Incident key={i.id} incident={i} />)}
                </section>
            )}
            {status && resolvedIncidents(status).length > 0 && (
                <section className="space-y-4">
                    <h2 className="text-2xl font-bold">Recently resolved</h2>
                    {resolvedIncidents(status).map((i) => <Incident key={i.id} incident={i} />)}
                </section>
            )}
        </div>
    );
}
