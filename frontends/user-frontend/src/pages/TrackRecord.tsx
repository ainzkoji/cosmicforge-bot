import { useState } from "react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { getTrackRecordIndex, getTrackRecordSeries } from "@/api/trackRecord";
import {
    KIND_NAMES, LIVE_NOT_AVAILABLE_REASON, NOT_AVAILABLE, TABS, deriveTabState, formatFreshness, formatOutages,
    formatPercent, formatPeriod, formatRatio, formatVersions, publishedMetrics, seriesLabel, tabDefinition,
} from "@/lib/trackRecord";
import type { SeriesSummary, TabKey } from "@/lib/trackRecord";

function Row({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-1 py-2 border-b border-gray-100 last:border-0">
            <dt className="text-sm font-medium text-gray-500">{label}</dt>
            <dd className="sm:col-span-2 text-sm text-gray-800">{children}</dd>
        </div>
    );
}

function Notice({ title, children }: { title: string; children: ReactNode }) {
    return (
        <div className="rounded-xl border border-gray-200 bg-gray-50 p-6 text-gray-700">
            <h3 className="font-semibold text-[#1E1B4B] mb-2">{title}</h3>
            <div className="text-sm space-y-2">{children}</div>
        </div>
    );
}

function SeriesDetail({ seriesId }: { seriesId: string }) {
    const { data, isLoading, isError } = useQuery({
        queryKey: ["track-record-series", seriesId],
        queryFn: () => getTrackRecordSeries(seriesId),
    });
    if (isLoading) return <p className="text-sm text-gray-500">Loading series…</p>;
    if (isError || !data) return <p className="text-sm text-gray-500">This series could not be loaded.</p>;
    return (
        <div className="mt-4 space-y-4">
            <div>
                <h4 className="font-semibold text-[#1E1B4B] mb-2">Groups (account class and risk level)</h4>
                <ul className="text-sm space-y-1">
                    {data.aggregates.map(g => (
                        <li key={`${g.account_class}-${g.risk_level}`}>
                            {g.account_class} / {g.risk_level}:{" "}
                            {g.status === "PUBLISHED" && g.metrics
                                ? `${g.account_count} accounts, return ${formatPercent(g.metrics.time_weighted_return)}, max drawdown ${formatPercent(g.metrics.max_drawdown)}`
                                : `withheld (fewer than ${g.minimum_accounts} accounts)`}
                        </li>
                    ))}
                </ul>
            </div>
            {data.daily_index.length > 0 && (
                <div className="overflow-x-auto">
                    <h4 className="font-semibold text-[#1E1B4B] mb-2">Daily index (starts at 1.00)</h4>
                    <table className="min-w-full text-sm">
                        <thead>
                            <tr className="text-left text-gray-500"><th className="pr-6">Date</th><th className="pr-6">Daily return</th><th>Index</th></tr>
                        </thead>
                        <tbody>
                            {data.daily_index.map(p => (
                                <tr key={p.date} className="border-t border-gray-100">
                                    <td className="pr-6 py-1">{p.date}</td>
                                    <td className="pr-6 py-1">{formatPercent(p.return, 3)}</td>
                                    <td className="py-1">{Number(p.index).toFixed(4)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
            <div>
                <h4 className="font-semibold text-[#1E1B4B] mb-2">Limitations of this series</h4>
                <ul className="list-disc ml-6 text-sm space-y-1">
                    {data.limitations.map(l => <li key={l}>{l}</li>)}
                </ul>
            </div>
            <p className="text-xs text-gray-500 break-all">
                Provenance: {data.provenance.rows_used} daily rows used from {data.provenance.chain_count} hash-chained
                series; chain head hash {data.provenance.chain_head_hash ?? NOT_AVAILABLE}.
            </p>
        </div>
    );
}

function SeriesCard({ series }: { series: SeriesSummary }) {
    const [open, setOpen] = useState(false);
    const m = publishedMetrics(series.composite);
    return (
        <article className="rounded-xl border border-gray-200 p-6">
            <header className="flex flex-wrap items-start justify-between gap-2 mb-4">
                <div>
                    <h3 className="text-lg font-semibold text-[#1E1B4B]">{series.family_id}</h3>
                    <p className="text-sm text-gray-500">{KIND_NAMES[series.source_kind]} · {series.venue}</p>
                </div>
                <span className="rounded-full bg-amber-50 border border-amber-200 px-3 py-1 text-xs font-semibold text-amber-800">
                    {seriesLabel(series)}
                </span>
            </header>
            <dl>
                <Row label="Strategy version">{formatVersions(series.strategy_versions)}</Row>
                <Row label="Observation period">{formatPeriod(series.observation_period)}</Row>
                <Row label="Return (time-weighted, net of fees and funding)">{m ? formatPercent(m.time_weighted_return) : NOT_AVAILABLE}</Row>
                <Row label="Maximum drawdown">{m ? formatPercent(m.max_drawdown) : NOT_AVAILABLE}</Row>
                <Row label="Risk level">See groups in the series details</Row>
                <Row label="Fee drag / funding drag">{m ? `${formatPercent(m.fee_drag)} / ${formatPercent(m.funding_drag)}` : NOT_AVAILABLE}</Row>
                <Row label="Average gross exposure">{m ? formatRatio(m.average_gross_exposure_ratio) : NOT_AVAILABLE}</Row>
                <Row label="Outages and suspensions">{m ? formatOutages(m) : NOT_AVAILABLE}</Row>
                <Row label="Data freshness">{formatFreshness(series.data_freshness)}</Row>
                <Row label="Methodology">{series.methodology_version}</Row>
            </dl>
            {!m && (
                <p className="mt-3 text-sm text-gray-500">
                    Composite figures are withheld because too few accounts contributed.
                </p>
            )}
            <button type="button" onClick={() => setOpen(o => !o)}
                    className="mt-4 text-sm font-medium text-[#1E1B4B] underline">
                {open ? "Hide details" : "Show details"}
            </button>
            {open && <SeriesDetail seriesId={series.series_id} />}
        </article>
    );
}

export default function TrackRecord() {
    const [tab, setTab] = useState<TabKey>("LIVE");
    const { data, isLoading, isError } = useQuery({ queryKey: ["track-record-index"], queryFn: getTrackRecordIndex });
    const state = deriveTabState(data, tab, { loading: isLoading, error: isError });
    const def = tabDefinition(tab);

    return (
        <div className="bg-white">
            <article className="max-w-4xl mx-auto px-6 pt-32 pb-24 space-y-10 text-gray-700 leading-relaxed">
                <header className="space-y-4">
                    <h1 className="text-4xl font-bold text-[#1E1B4B]">Track Record</h1>
                    <p className="text-lg">
                        Performance is published separately for backtests, demo trading and live trading. These are
                        different things and are never combined. Results are not audited, and past performance does
                        not predict future results.
                    </p>
                    <p className="text-sm text-gray-500">
                        Read the <a href="#methodology" className="underline">methodology</a> and
                        the <Link to="/risk-disclosure" className="underline">risk disclosure</Link> before relying
                        on any figure here.
                    </p>
                </header>

                <div role="tablist" aria-label="Track record type" className="flex gap-2 border-b border-gray-200">
                    {TABS.map(t => (
                        <button key={t.key} type="button" role="tab" aria-selected={tab === t.key}
                                onClick={() => setTab(t.key)}
                                className={`px-4 py-2 -mb-px border-b-2 font-medium ${tab === t.key
                                    ? "border-[#1E1B4B] text-[#1E1B4B]" : "border-transparent text-gray-500 hover:text-[#1E1B4B]"}`}>
                            {t.title}
                        </button>
                    ))}
                </div>

                <section role="tabpanel" className="space-y-6">
                    <p className="text-sm text-gray-500">{def.description}</p>
                    {state.state === "LOADING" && <p className="text-sm text-gray-500">Loading…</p>}
                    {state.state === "ERROR" && (
                        <Notice title="Track record unavailable">
                            <p>The track record could not be loaded right now. No figures are shown in its place.</p>
                        </Notice>
                    )}
                    {state.state === "NOT_YET_AVAILABLE" && (
                        <Notice title="Live track record: not yet available">
                            <p>{state.reason || LIVE_NOT_AVAILABLE_REASON}.</p>
                            <p>
                                Live results will appear here only after live trading has been authorized, and only
                                for days whose orders and fills have been reconciled with the exchange.
                            </p>
                        </Notice>
                    )}
                    {state.state === "EMPTY" && (
                        <Notice title="No published series">
                            <p>Nothing has been published under this tab yet.</p>
                        </Notice>
                    )}
                    {state.state === "AVAILABLE" && state.series.map(s => <SeriesCard key={s.series_id} series={s} />)}
                </section>

                <section id="methodology" className="space-y-4">
                    <h2 className="text-2xl font-bold text-[#1E1B4B]">Methodology</h2>
                    <ul className="list-disc ml-6 space-y-2 text-sm">
                        <li>
                            <strong>Daily return.</strong> r = (ending equity − starting equity − net deposits) ÷
                            (starting equity + net deposits). Deposits and withdrawals are treated as arriving at the
                            start of the day, so moving money in or out is never counted as a gain or a loss.
                        </li>
                        <li>
                            <strong>Time-weighted return.</strong> Daily returns are chain-linked into an index that
                            starts at 1.00. Maximum drawdown is the largest fall of that index from its previous peak.
                        </li>
                        <li>
                            <strong>Fees and funding.</strong> Returns are net of exchange fees and funding. Fee drag
                            and funding drag show how much each changed the period return; a negative funding drag
                            means funding was received.
                        </li>
                        <li>
                            <strong>Aggregation and privacy.</strong> Accounts are pooled by account class and risk
                            level. A group, or a day, with fewer than the published minimum of accounts (at least five
                            for customer and invited-beta accounts) is withheld. No account, user or amount is shown.
                        </li>
                        <li>
                            <strong>Outages and suspensions.</strong> Platform outage minutes and days on which the
                            strategy was suspended are reported as they occurred and are not adjusted for.
                        </li>
                        <li>
                            <strong>Provenance.</strong> Every daily row is hash-chained to the previous row of the
                            same series and is never edited; corrections are new rows that replace the original and
                            say why. Each published series lists the hash of its chain heads.
                        </li>
                        <li>
                            <strong>Labels.</strong> Backtest, research holdout, paper and demo results are labelled
                            “Simulated — not achieved trading results”. Only live, reconciled days appear under Live.
                        </li>
                    </ul>
                    <p className="text-sm text-gray-500">
                        The full methodology, including how to verify a published series, is maintained as the
                        public document “Track Record Methodology” (TRACK_RECORD_METHODOLOGY.md).
                    </p>
                </section>
            </article>
        </div>
    );
}
