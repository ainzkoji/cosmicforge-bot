/**
 * Step 1.9 -- what the engine itself knows about one bot, from its read model.
 *
 * Every block has its own loading, error, empty and stale state. A failed or
 * old read is shown as such; nothing is rendered as zero or as "healthy" when
 * the engine did not say so.
 */
import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Loader2, ShieldAlert, ShieldCheck } from "lucide-react";
import { api } from "@/api/client";
import { ageLabel, botStatusView, formatSigned, formatUsdt, protectionView, severityTone } from "@/lib/deployment";
import type { Tone } from "@/lib/deployment";

const TONE: Record<Tone, string> = {
    ok: "bg-green-500/10 text-green-400 border-green-500/20",
    info: "bg-blue-500/10 text-blue-300 border-blue-500/20",
    attention: "bg-amber-500/10 text-amber-300 border-amber-500/20",
    critical: "bg-red-500/10 text-red-300 border-red-500/20",
    muted: "bg-white/5 text-gray-400 border-white/10",
};

function Pill({ tone, children }: { tone: Tone; children: React.ReactNode }) {
    return <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold border ${TONE[tone]}`}>{children}</span>;
}

function Block({ title, query, empty, children }: {
    title: string;
    query: { isLoading: boolean; isError: boolean; error: unknown; refetch: () => unknown };
    empty?: string | null;
    children: React.ReactNode;
}) {
    return (
        <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-5 space-y-3">
            <h3 className="font-semibold text-white">{title}</h3>
            {query.isLoading ? (
                <div className="text-sm text-gray-400 flex items-center gap-2"><Loader2 className="w-4 h-4 animate-spin" /> Loading…</div>
            ) : query.isError ? (
                <div role="alert" className="text-sm text-red-300 flex items-start gap-2">
                    <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0" />
                    <span>
                        This could not be loaded{query.error instanceof Error && query.error.message ? `: ${query.error.message}` : "."}{" "}
                        <button type="button" onClick={() => query.refetch()} className="underline">Retry</button>
                    </span>
                </div>
            ) : empty ? (
                <p className="text-sm text-gray-500">{empty}</p>
            ) : children}
        </section>
    );
}

function time(ms: number | null | undefined): string {
    return ms ? new Date(ms).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "—";
}

function pnlClass(value: number | null | undefined): string {
    if (value === null || value === undefined || value === 0) return "text-gray-300";
    return value > 0 ? "text-green-400" : "text-red-400";
}

export function CatiBotPanel({ botId }: { botId: string }) {
    const [now, setNow] = useState(() => Date.now());
    useEffect(() => {
        const timer = window.setInterval(() => setNow(Date.now()), 5000);
        return () => window.clearInterval(timer);
    }, []);

    const status = useQuery({ queryKey: ["cati", botId, "status"], queryFn: () => api.getCatiBotStatus(botId), refetchInterval: 10_000 });
    const positions = useQuery({ queryKey: ["cati", botId, "positions"], queryFn: () => api.getCatiBotPositions(botId), refetchInterval: 10_000 });
    const summary = useQuery({ queryKey: ["cati", botId, "summary"], queryFn: () => api.getCatiBotSummary(botId), refetchInterval: 30_000 });
    const trades = useQuery({ queryKey: ["cati", botId, "trades"], queryFn: () => api.getCatiBotTrades(botId, 1, 10), refetchInterval: 30_000 });
    const events = useQuery({ queryKey: ["cati", botId, "events"], queryFn: () => api.getCatiBotEvents(botId, 15), refetchInterval: 15_000 });

    const s = status.data;
    const view = botStatusView(s?.status);
    const engine = s?.engine;
    const eligibility = s?.eligibility;
    const uncertain: any[] = s?.protection_uncertain ?? [];
    const windows = summary.data?.windows;

    return (
        <div className="space-y-4">
            <Block title="Engine status" query={status}>
                {s && (
                    <div className="space-y-3">
                        <div className="flex flex-wrap items-center gap-2">
                            <Pill tone={view.tone}>{view.label}</Pill>
                            {s.environment && <Pill tone={s.environment === "DEMO" ? "info" : "attention"}>{s.environment}</Pill>}
                            {s.exchange && <Pill tone="muted">{s.exchange}</Pill>}
                            {engine?.stale && <Pill tone="attention">engine data is stale</Pill>}
                            {s.kill_switch?.engaged && <Pill tone="critical">kill switch engaged</Pill>}
                            {s.daily_loss?.latched && <Pill tone="attention">daily loss pause</Pill>}
                        </div>
                        <p className="text-sm text-gray-400">{view.description}</p>
                        <p className="text-xs text-gray-500">
                            Last engine cycle for this account: {ageLabel(engine?.last_cycle_at, now)}
                            {engine?.stale ? " — older than two minutes, so the figures below may not be current." : ""}
                        </p>
                        {eligibility && (
                            <div className={`p-3 rounded-xl border text-sm ${TONE[eligibility.eligible_to_enter ? "ok" : severityTone(eligibility.severity)]}`}>
                                <div className="font-semibold">
                                    {eligibility.eligible_to_enter ? "Able to open a trade when a signal qualifies" : "Not opening new trades right now"}
                                </div>
                                <div className="mt-1">{eligibility.reason}</div>
                                {eligibility.suggested_action && <div className="mt-1 opacity-80">{eligibility.suggested_action}</div>}
                                {eligibility.reason_code && <div className="mt-1 text-[10px] font-mono opacity-60">{eligibility.reason_code}</div>}
                            </div>
                        )}
                        {uncertain.length > 0 && (
                            <div role="alert" className={`p-3 rounded-xl border text-sm flex gap-2 ${TONE.attention}`}>
                                <ShieldAlert className="w-4 h-4 mt-0.5 flex-shrink-0" />
                                <span>
                                    The exchange-side stop could not be verified for {uncertain.map((u) => u.symbol).join(", ")}. The position is
                                    kept and re-checked every cycle; no new trades are opened meanwhile.
                                </span>
                            </div>
                        )}
                    </div>
                )}
            </Block>

            <Block title="Results" query={summary}>
                {windows && (
                    <div className="space-y-3">
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                            {([["today", "Today"], ["seven_days", "7 days"], ["thirty_days", "30 days"], ["all_time", "All time"]] as const).map(([key, label]) => (
                                <div key={key} className="bg-[#0F1218] rounded-xl p-3 border border-white/5">
                                    <div className="text-xs text-gray-500">{label}</div>
                                    {windows[key].trades === 0 ? (
                                        <div className="text-sm text-gray-500 mt-1">No closed trades</div>
                                    ) : (
                                        <>
                                            <div className={`text-lg font-mono ${pnlClass(windows[key].net_pnl)}`}>{formatSigned(windows[key].net_pnl)} USDT</div>
                                            <div className="text-xs text-gray-500">
                                                {windows[key].trades} trades · {windows[key].wins} won · {windows[key].losses} lost
                                            </div>
                                        </>
                                    )}
                                </div>
                            ))}
                        </div>
                        <p className="text-xs text-gray-500">
                            Net result = realized profit or loss − fees + funding.{" "}
                            {summary.data.equity?.current !== null && summary.data.equity?.current !== undefined
                                ? `Account equity ${formatUsdt(summary.data.equity.current)} (read ${ageLabel(summary.data.equity.observed_at, now)}).`
                                : "Account equity has not been read yet."}
                        </p>
                    </div>
                )}
            </Block>

            <Block title="Open positions" query={positions}
                empty={positions.data && positions.data.positions.length === 0 ? "No open position." : null}>
                <div className="space-y-2">
                    {(positions.data?.positions ?? []).map((p: any) => {
                        const protection = protectionView(p.protection?.state);
                        return (
                            <div key={p.trade_plan_id} className="bg-[#0F1218] rounded-xl p-3 border border-white/5 text-sm">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                    <div className="font-semibold text-white">{p.symbol} <span className="text-gray-400 font-normal">{p.side}</span></div>
                                    <Pill tone={protection.tone}>
                                        {protection.tone === "ok" ? <ShieldCheck className="w-3 h-3" /> : <ShieldAlert className="w-3 h-3" />}
                                        {protection.label}
                                    </Pill>
                                </div>
                                <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mt-2 text-xs text-gray-400 font-mono">
                                    <div>Qty {p.quantity ?? "—"}</div>
                                    <div>Entry {p.entry_price ?? "—"}</div>
                                    <div>Stop {p.stop_price ?? "—"}</div>
                                    <div>Target {p.target_price ?? "—"}</div>
                                </div>
                                {p.protection?.description && protection.tone !== "ok" && (
                                    <div className="text-xs text-amber-300/80 mt-2">{p.protection.description}</div>
                                )}
                            </div>
                        );
                    })}
                </div>
            </Block>

            <Block title="Recent trades" query={trades}
                empty={trades.data && trades.data.trades.length === 0 ? "No trades yet." : null}>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="text-left text-xs text-gray-500">
                                <th className="py-1 pr-3">Symbol</th><th className="pr-3">Side</th><th className="pr-3">Opened</th>
                                <th className="pr-3">Closed</th><th className="pr-3 text-right">Net result</th><th className="text-right">Fees</th>
                            </tr>
                        </thead>
                        <tbody>
                            {(trades.data?.trades ?? []).map((t: any) => (
                                <tr key={t.trade_id} className="border-t border-white/5 text-gray-300">
                                    <td className="py-1.5 pr-3 font-mono">{t.symbol}</td>
                                    <td className="pr-3">{t.side}</td>
                                    <td className="pr-3">{time(t.entry_time)}</td>
                                    <td className="pr-3">{t.state === "OPEN" ? <Pill tone="info">open</Pill> : time(t.exit_time)}</td>
                                    <td className={`pr-3 text-right font-mono ${pnlClass(t.net_pnl)}`}>{t.net_pnl === null || t.net_pnl === undefined ? "—" : formatSigned(t.net_pnl, 4)}</td>
                                    <td className="text-right font-mono text-gray-500">{t.fees === null || t.fees === undefined ? "—" : Number(t.fees).toFixed(4)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </Block>

            <Block title="Activity" query={events}
                empty={events.data && events.data.events.length === 0 ? "Nothing has happened yet." : null}>
                <ul className="space-y-2">
                    {(events.data?.events ?? []).map((e: any) => (
                        <li key={e.event_id} className="text-sm flex gap-3">
                            <span className="text-xs text-gray-500 w-28 flex-shrink-0">{time(e.at)}</span>
                            <span className="text-gray-300">{e.payload?.message || e.event_type}</span>
                        </li>
                    ))}
                </ul>
            </Block>
        </div>
    );
}

export default CatiBotPanel;
