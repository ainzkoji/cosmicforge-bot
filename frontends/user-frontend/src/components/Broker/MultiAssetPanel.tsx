import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { RefreshCw, ShieldCheck, Layers, Route, ListChecks } from "lucide-react";
import { api, ReasonedApiError } from "../../api/client";
import {
    autoRoutingEnabled, automationSummary, familyRows, instrumentBadges, permissionRows, reasonView,
    routeRequirement, stripSecrets, topologyLabel, transferStage, validateRoutingPolicy, TRANSFER_FLOW,
    type ReasonView, type Tone,
} from "../../utils/multiAssetView.ts";

const TONE: Record<Tone, string> = {
    ok: "text-green-500 bg-green-500/10 border-green-500/20",
    warn: "text-yellow-500 bg-yellow-500/10 border-yellow-500/20",
    blocked: "text-red-400 bg-red-500/10 border-red-500/20",
    neutral: "text-muted-foreground bg-muted/40 border-border/50",
};

function Badge({ tone, children }: { tone: Tone; children: React.ReactNode }) {
    return <span className={`px-2 py-0.5 rounded border text-[11px] font-semibold ${TONE[tone]}`}>{children}</span>;
}

/** Human copy plus the machine reason code (never a generic "something went wrong"). */
function Reason({ reason }: { reason: ReasonView }) {
    if (!reason.code) return null;
    return (
        <span className="text-xs text-muted-foreground">
            {reason.text} <code className="ml-1 text-[10px] opacity-70">{reason.code}</code>
        </span>
    );
}

function errorReason(err: unknown): ReasonView {
    if (err instanceof ReasonedApiError) return reasonView(err.reasonCode || err.message);
    return reasonView("REQUEST_FAILED");
}

const POLICY_FIELDS: { key: string; label: string }[] = [
    { key: "min_funding_balance", label: "Source reserve (funding wallet)" },
    { key: "min_derivatives_reserve", label: "Source reserve (trading wallet)" },
    { key: "max_transfer_amount", label: "Maximum transfer amount" },
    { key: "max_transfer_pct", label: "Maximum transfer share of source (0-1]" },
    { key: "max_destination_balance", label: "Destination cap" },
    { key: "manual_approval_threshold", label: "Approval threshold (automated moves above it need you)" },
    { key: "daily_transfer_limit", label: "Daily transfer limit" },
];

export default function MultiAssetPanel({ accountId }: { accountId: string }) {
    const qc = useQueryClient();
    const [open, setOpen] = useState(false);
    const status = useQuery({
        queryKey: ["broker-market-status", accountId], enabled: open, retry: false,
        queryFn: () => api.getMarketStatus(accountId, { instruments_family: "FX" }).then(stripSecrets),
    });
    const settings = useQuery({
        queryKey: ["broker-transfer-settings", accountId], enabled: open, retry: false,
        queryFn: () => api.getTransferSettings(accountId).then(stripSecrets),
    });
    const transfers = useQuery({
        queryKey: ["broker-transfers", accountId], enabled: open, retry: false,
        queryFn: () => api.listInternalTransfers(accountId).then(stripSecrets),
    });
    const sync = useMutation({
        mutationFn: () => api.syncInstruments(accountId),
        onSuccess: () => qc.invalidateQueries({ queryKey: ["broker-market-status", accountId] }),
    });

    const [draft, setDraft] = useState<Record<string, any>>({});
    const [authorize, setAuthorize] = useState(false);
    const saveSettings = useMutation({
        mutationFn: (values: Record<string, any>) => api.updateTransferSettings(accountId, values),
        onSuccess: () => { setDraft({}); qc.invalidateQueries({ queryKey: ["broker-transfer-settings", accountId] }); },
    });

    const [planInput, setPlanInput] = useState({ asset: "USDT", amount: "", source_wallet: "", destination_wallet: "" });
    const plan = useMutation({ mutationFn: (body: Record<string, any>) => api.planCapitalTransfer(accountId, body) });
    const execute = useMutation({
        // the user's explicit approval: THIS plan instance is the idempotency key, so repeated clicks on the same
        // displayed plan can never submit a second transfer (a later, fresh plan is a new intent)
        mutationFn: (p: any) => api.createInternalTransfer(accountId, {
            asset: p.asset, amount: p.amount, source_wallet: p.route.source_wallet,
            destination_wallet: p.route.destination_wallet, idempotency_key: `${p.plan_id}-${p.observed_at_ms}` }),
        onSuccess: () => qc.invalidateQueries({ queryKey: ["broker-transfers", accountId] }),
    });

    const s = status.data;
    const cfg = settings.data || {};
    const pendingPolicy: Record<string, any> = { ...draft, ...(authorize ? { authorize_automated_reallocation: true } : {}) };
    const policyErrors = validateRoutingPolicy(pendingPolicy);
    const topo = s ? topologyLabel(s.topology) : null;
    const planResult = plan.data ? routeRequirement(plan.data) : null;

    return (
        <div className="w-full mt-4 border-t border-border/50 pt-3">
            <button onClick={() => setOpen(!open)} className="text-sm font-semibold flex items-center gap-2 hover:text-primary">
                <Layers className="w-4 h-4" /> Markets, permissions &amp; capital routing {open ? "▾" : "▸"}
            </button>
            {open && (
                <div className="mt-3 grid gap-4 md:grid-cols-2 text-sm">
                    {status.isError && <Reason reason={errorReason(status.error)} />}

                    {/* 21.1 markets this account can actually automate (backend capability state) */}
                    <section>
                        <div className="flex items-center justify-between mb-2">
                            <h4 className="font-semibold">Markets</h4>
                            <button onClick={() => sync.mutate()} disabled={sync.isPending}
                                    className="text-xs flex items-center gap-1 text-muted-foreground hover:text-primary">
                                <RefreshCw className={`w-3 h-3 ${sync.isPending ? "animate-spin" : ""}`} /> Sync instruments
                            </button>
                        </div>
                        {sync.data?.status === "THROTTLED" && <Reason reason={reasonView(sync.data.reason)} />}
                        {s && familyRows(s).map((f) => (
                            <div key={f.family} className="flex flex-col py-1">
                                <div className="flex items-center gap-2">
                                    <span className="w-24">{f.label}</span>
                                    <Badge tone={f.tone}>{f.availability === "AVAILABLE" ? "Available" : "Unavailable"}</Badge>
                                    <span className="text-xs text-muted-foreground">{f.marketsAvailable} listed · {f.apiTradable} API-tradable</span>
                                </div>
                                <Reason reason={f.reason} />
                            </div>
                        ))}
                    </section>

                    {/* 21.2 permission health: states only, never a key / secret / token */}
                    <section>
                        <h4 className="font-semibold mb-2 flex items-center gap-2"><ShieldCheck className="w-4 h-4" /> Permission health</h4>
                        {s && permissionRows(s.permission_health).map((p) => (
                            <div key={p.permission} className="flex items-center gap-2 py-0.5">
                                <span className="w-32">{p.label}</span><Badge tone={p.tone}>{p.state}</Badge>
                            </div>
                        ))}
                    </section>

                    {/* 21.3 topology + logical vs physical routing */}
                    <section>
                        <h4 className="font-semibold mb-2 flex items-center gap-2"><Route className="w-4 h-4" /> Account topology</h4>
                        {topo && (<><Badge tone={topo.tone}>{topo.label}</Badge> <Reason reason={topo.reason} /></>)}
                        {s?.topology?.logical_allocation && <p className="text-xs text-muted-foreground mt-1">{s.topology.logical_allocation}</p>}
                        {(s?.topology?.routes || []).map((r: any) => (
                            <div key={`${r.from}-${r.to}`} className="text-xs flex items-center gap-2 mt-1">
                                <span className="font-mono">{r.from} → {r.to}</span>
                                <Badge tone={r.available ? "ok" : "blocked"}>{r.available ? "route available" : "unavailable"}</Badge>
                                {!r.available && <Reason reason={reasonView(r.reason_code)} />}
                                <span className="text-muted-foreground">fee: {r.fee === null || r.fee === undefined ? "not published" : r.fee}</span>
                            </div>
                        ))}
                    </section>

                    {/* 21.4 / 21.5 independent toggles + supported routing policy only */}
                    <section>
                        <h4 className="font-semibold mb-2">Automation</h4>
                        <p className="text-xs"><b>Auto Trading</b> is controlled per bot (My Bots) and never enables capital movement.</p>
                        <p className="text-xs mt-1"><b>Auto Capital Routing</b>: <Badge tone={autoRoutingEnabled(cfg) ? "ok" : "neutral"}>
                            {autoRoutingEnabled(cfg) ? "ON" : "OFF"}</Badge>{cfg.emergency_disabled && <Badge tone="blocked">emergency disabled</Badge>}</p>
                        <p className="text-xs text-muted-foreground mt-1">{automationSummary(true, cfg)}</p>
                        <div className="grid grid-cols-2 gap-2 mt-2">
                            {POLICY_FIELDS.map((f) => (
                                <label key={f.key} className="text-xs flex flex-col gap-0.5">
                                    {f.label}
                                    <input className="bg-muted/40 rounded px-2 py-1" defaultValue={cfg[f.key] ?? ""}
                                           onChange={(e) => setDraft({ ...draft, [f.key]: e.target.value || null })} />
                                    {policyErrors[f.key] && <span className="text-red-400">{policyErrors[f.key]}</span>}
                                </label>
                            ))}
                        </div>
                        <div className="flex flex-wrap items-center gap-3 mt-2 text-xs">
                            <label className="flex items-center gap-1">
                                <input type="checkbox" checked={authorize} onChange={(e) => {
                                    setAuthorize(e.target.checked);
                                    setDraft({ ...draft, mode: e.target.checked ? "AUTOMATED_INTERNAL_REALLOCATION" : "MANUAL_TRANSFER",
                                               auto_rebalance_enabled: e.target.checked });
                                }} /> I authorize automatic internal reallocation (same account only, never a withdrawal)
                            </label>
                            <label className="flex items-center gap-1">
                                <input type="checkbox" defaultChecked={!!cfg.emergency_disabled}
                                       onChange={(e) => setDraft({ ...draft, emergency_disabled: e.target.checked })} /> Emergency disable
                            </label>
                            <button disabled={Object.keys(policyErrors).length > 0 || saveSettings.isPending}
                                    onClick={() => saveSettings.mutate(pendingPolicy)}
                                    className="px-3 py-1 rounded bg-primary text-primary-foreground disabled:opacity-50">Save policy</button>
                            {saveSettings.isError && <Reason reason={errorReason(saveSettings.error)} />}
                        </div>
                    </section>

                    {/* 21.6 plan -> approval -> execute intent -> broker submit -> confirm/reconcile */}
                    <section>
                        <h4 className="font-semibold mb-2">Internal transfer (plan first)</h4>
                        <div className="grid grid-cols-2 gap-2 text-xs">
                            {(["asset", "amount", "source_wallet", "destination_wallet"] as const).map((k) => (
                                <input key={k} placeholder={k} className="bg-muted/40 rounded px-2 py-1" value={planInput[k]}
                                       onChange={(e) => setPlanInput({ ...planInput, [k]: e.target.value })} />
                            ))}
                        </div>
                        <button onClick={() => plan.mutate(planInput)} className="mt-2 px-3 py-1 rounded border text-xs">Plan (dry run)</button>
                        {plan.isError && <Reason reason={errorReason(plan.error)} />}
                        {planResult && plan.data && (
                            <div className="mt-2 text-xs space-y-1">
                                <div>{planResult.text} <Reason reason={planResult.reason} /></div>
                                {(plan.data.reason_codes || []).map((c: string) => <div key={c}><Reason reason={reasonView(c)} /></div>)}
                                <div className="text-muted-foreground">Plan valid until {new Date(plan.data.valid_until_ms).toLocaleTimeString()} — nothing has moved.</div>
                                {planResult.requirement === "PHYSICAL_INTERNAL_TRANSFER" && (
                                    <button onClick={() => execute.mutate(plan.data)} disabled={execute.isPending}
                                            className="px-3 py-1 rounded bg-primary text-primary-foreground">Approve &amp; execute</button>
                                )}
                                {execute.isError && <Reason reason={errorReason(execute.error)} />}
                            </div>
                        )}
                        <div className="flex gap-1 mt-2 text-[10px] text-muted-foreground">{TRANSFER_FLOW.join(" → ")}</div>
                        {(transfers.data?.transfers || []).slice(0, 5).map((t: any) => {
                            const st = transferStage(t);
                            return (
                                <div key={t.id} className="text-xs flex items-center gap-2 mt-1">
                                    <span className="font-mono">{t.amount} {t.asset} {t.source_wallet}→{t.destination_wallet}</span>
                                    <Badge tone={st.confirmed ? "ok" : t.status === "BLOCKED" || t.status === "FAILED" ? "blocked" : "warn"}>{t.status}</Badge>
                                    <span className="text-muted-foreground">{st.text}</span>
                                    {t.failure_reason && <Reason reason={reasonView(t.failure_reason)} />}
                                </div>
                            );
                        })}
                    </section>

                    {/* 21.8 market availability is not research, certification or execution authority */}
                    <section className="md:col-span-2">
                        <h4 className="font-semibold mb-2 flex items-center gap-2"><ListChecks className="w-4 h-4" /> FX instruments</h4>
                        {(s?.instruments || []).slice(0, 12).map((i: any) => (
                            <div key={i.venue_symbol} className="flex flex-wrap items-center gap-2 py-0.5 text-xs">
                                <span className="font-mono w-40 truncate">{i.venue_symbol}</span>
                                {instrumentBadges(i.readiness).map((b) => (
                                    <span key={b.key} title={b.reason.code || ""}><Badge tone={b.tone}>{b.label}: {b.value}</Badge></span>
                                ))}
                            </div>
                        ))}
                        {s && !(s.instruments || []).length && <span className="text-xs text-muted-foreground">No FX instruments listed for this account.</span>}
                    </section>
                </div>
            )}
        </div>
    );
}
