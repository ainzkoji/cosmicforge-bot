import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useEffect, useState } from "react";
import { Database, ShieldAlert } from "lucide-react";
import { apiClient } from "@/api/client";

/**
 * Operator view of CATI research datasets and certification state (Section 21.9).
 *
 * READ-ONLY. Every state is shown separately — market availability, data readiness, manifest/freeze,
 * pre-holdout certification, holdout, governance, execution authority, external venue validation — and never
 * collapsed into one "ready" flag. No holdout observation or performance is requested or shown; there is no
 * control here that opens a holdout, changes certification or governance, or enables execution.
 */
function Pill({ ok, children }: { ok: boolean | null; children: React.ReactNode }) {
    const tone = ok === true ? "bg-green-500/10 text-green-500 border-green-500/20"
        : ok === false ? "bg-red-500/10 text-red-400 border-red-500/20" : "bg-yellow-500/10 text-yellow-500 border-yellow-500/20";
    return <span className={`px-2 py-0.5 rounded border text-xs font-semibold ${tone}`}>{children}</span>;
}

export default function CatiStatus() {
    const [status, setStatus] = useState<any>(null);
    const [datasets, setDatasets] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        Promise.all([apiClient.get("/api/admin/cati/multi-asset"), apiClient.get("/api/admin/cati/datasets")])
            .then(([s, d]) => { setStatus(s.data); setDatasets(d.data); })
            .catch((e) => setError(e?.response?.data?.detail?.reason_code || e?.message || "REQUEST_FAILED"));
    }, []);

    return (
        <AdminLayout>
            <div className="p-6 space-y-6">
                <h1 className="text-2xl font-bold flex items-center gap-2"><Database className="w-6 h-6" /> CATI research &amp; certification</h1>
                <p className="text-sm text-muted-foreground flex items-center gap-2"><ShieldAlert className="w-4 h-4" />
                    Read-only. Holdouts stay closed; nothing on this page can open one or grant authority.</p>
                {error && <div className="text-sm text-red-400">Unavailable: <code>{error}</code></div>}

                {status && (
                    <div className="grid md:grid-cols-2 gap-4">
                        {Object.entries(status.families).map(([family, f]: [string, any]) => (
                            <div key={family} className="border border-border/50 rounded-xl p-4 space-y-2 text-sm">
                                <h2 className="font-bold text-lg">{family}</h2>
                                <div>Market available: <Pill ok={f.market_available.state}>{f.market_available.state ? "yes" : "no"}</Pill></div>
                                <div>Data: {Object.entries(f.data_readiness).map(([k, v]: any) => (
                                    <span key={k} className="mr-2"><Pill ok={v === "COMPLETE" ? true : v === "ACQUIRING" ? null : false}>{k}: {v}</Pill></span>))}
                                </div>
                                <div>Certification: <Pill ok={null}>{f.certification.state}</Pill>{" "}
                                    <code className="text-xs">{(f.certification.reason_codes || []).join(", ")}</code></div>
                                <div>Holdout: <Pill ok={f.holdout.state === "CLOSED" ? null : false}>{f.holdout.state}</Pill></div>
                                <div>Governance phase: <code>{f.governance.phase ?? f.governance.reason}</code></div>
                                <div>Execution authority: demo <Pill ok={f.execution_authority.demo}>{String(f.execution_authority.demo)}</Pill>{" "}
                                    production <Pill ok={f.execution_authority.production}>{String(f.execution_authority.production)}</Pill>{" "}
                                    <code className="text-xs">{f.execution_authority.reason}</code></div>
                                <div>External venue validation: {Object.entries(f.external_venue_validation).map(([v, s]: any) => (
                                    <div key={v} className="text-xs"><code>{v}</code>: {s}</div>))}</div>
                            </div>
                        ))}
                    </div>
                )}

                {datasets && (
                    <table className="w-full text-sm border border-border/50 rounded-xl">
                        <thead><tr className="text-left text-muted-foreground">
                            <th className="p-2">Dataset</th><th>Manifest</th><th>Instruments</th><th>Acquisition</th><th>Holdout</th><th>Certification</th><th>Blockers</th>
                        </tr></thead>
                        <tbody>{datasets.datasets.map((d: any) => (
                            <tr key={d.dataset} className="border-t border-border/40 align-top">
                                <td className="p-2 font-semibold">{d.dataset}<div className="text-xs text-muted-foreground">{d.provider} {d.asset_class}</div></td>
                                <td><Pill ok={d.status === "FROZEN"}>{d.status}</Pill><div className="font-mono text-[10px] break-all">{d.universe_hash}</div></td>
                                <td>{d.instrument_count}</td>
                                <td><Pill ok={d.acquisition.state === "COMPLETE" ? true : d.acquisition.state === "ACQUIRING" ? null : false}>{d.acquisition.state}</Pill>
                                    {d.acquisition["1m"] && <div className="text-xs">{d.acquisition["1m"].done_periods}/{d.acquisition["1m"].expected_periods} periods
                                        · failed {d.acquisition["1m"].failed_retryable_periods}</div>}
                                    {d.acquisition.rows && <div className="text-xs">{d.acquisition.rows.toLocaleString()} rows</div>}</td>
                                <td><Pill ok={d.holdout === "CLOSED" ? null : false}>{d.holdout}</Pill></td>
                                <td><Pill ok={null}>{d.certification}</Pill></td>
                                <td><code className="text-xs">{d.blockers.join(", ") || "-"}</code></td>
                            </tr>))}
                        </tbody>
                    </table>
                )}
            </div>
        </AdminLayout>
    );
}
