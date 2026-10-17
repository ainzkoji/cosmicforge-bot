/**
 * Steps 6.4 / 8.6 -- the public track record, as the portal presents it.
 *
 * Pure functions and types only (no DOM, no network). The backend
 * (/api/v1/public/track-record) serves only published, aggregated documents;
 * this module decides what each tab may show and how it is labelled. It never
 * produces a number of its own: a missing value is shown as "Not available",
 * and a tab with no published series shows an empty state.
 *
 * No live trading has happened. The Live tab therefore shows the backend's
 * NOT_YET_AVAILABLE state, and it refuses to render any series that is not a
 * LIVE series -- simulated, paper or demo results never appear under Live.
 */

export type SourceKind = "BACKTEST" | "RESEARCH_HOLDOUT" | "PAPER" | "DEMO" | "LIVE";
export type TabKey = "BACKTEST" | "DEMO" | "LIVE";

export const SIMULATED_LABEL = "Simulated — not achieved trading results";
export const LIVE_LABEL = "Live trading results — unaudited";
export const LIVE_NOT_AVAILABLE_REASON = "No authorized live trading has occurred";
export const NOT_AVAILABLE = "Not available";

export interface TabDefinition {
    key: TabKey;
    title: string;
    kinds: SourceKind[];
    description: string;
}

export const TABS: TabDefinition[] = [
    {
        key: "BACKTEST", title: "Backtest", kinds: ["BACKTEST", "RESEARCH_HOLDOUT"],
        description: "Historical simulations and research holdout tests. Hypothetical results, not trading.",
    },
    {
        key: "DEMO", title: "Demo", kinds: ["PAPER", "DEMO"],
        description: "Paper trading and exchange demo accounts. No real money was at risk.",
    },
    {
        key: "LIVE", title: "Live", kinds: ["LIVE"],
        description: "Real-money trading on authorized accounts, recorded daily and reconciled with the exchange.",
    },
];

export const KIND_NAMES: Record<SourceKind, string> = {
    BACKTEST: "Backtest",
    RESEARCH_HOLDOUT: "Research holdout",
    PAPER: "Paper",
    DEMO: "Demo account",
    LIVE: "Live",
};

export interface StrategyVersion { family_version: string; first_date: string; last_date: string; }
export interface ObservationPeriod { start: string; end: string; calendar_days_with_data: number; }
export interface DataFreshness {
    latest_trading_date: string | null;
    last_recorded_at: string | null;
    generated_at: string | null;
}

export interface SeriesMetrics {
    time_weighted_return: string | null;
    max_drawdown: string | null;
    fee_drag: string | null;
    funding_drag: string | null;
    slippage_drag: string | null;
    slippage_measured_days: number;
    average_gross_exposure_ratio: string | null;
    max_gross_exposure_ratio: string | null;
    outage_minutes: number;
    outage_days: number;
    strategy_suspended_days: number;
    excluded_zero_capital_days: number;
    trading_days: number;
}

export interface Aggregate {
    status: "PUBLISHED" | "SUPPRESSED_MIN_COUNT";
    minimum_accounts: number;
    account_count?: number;
    days_suppressed_min_count?: number;
    metrics?: SeriesMetrics;
}

export interface SeriesSummary {
    series_id: string;
    source_kind: SourceKind;
    family_id: string;
    venue: string;
    label: string;
    is_live: boolean;
    strategy_versions: StrategyVersion[];
    observation_period: ObservationPeriod | null;
    methodology_version: string;
    composite: Aggregate | null;
    published_at: string | null;
    data_freshness: DataFreshness;
}

export interface IndexPoint { date: string; return: string; index: string; }

export interface SeriesDocument extends Omit<SeriesSummary, "data_freshness"> {
    state: string;
    audited: boolean;
    daily_index: IndexPoint[];
    aggregates: (Aggregate & { account_class: string; risk_level: string })[];
    limitations: string[];
    fees_and_funding_treatment: string;
    return_methodology: Record<string, string>;
    provenance: {
        chain_head_hash: string | null;
        chain_count: number;
        chain_row_count: number;
        rows_used: number;
        data_freshness: DataFreshness;
    };
}

export interface KindEntry {
    state: "AVAILABLE" | "NO_PUBLISHED_SERIES" | "NOT_YET_AVAILABLE";
    label: string;
    reason?: string;
    series: SeriesSummary[];
}

export interface TrackRecordIndex {
    methodology_version: string;
    audited: boolean;
    store: string;
    kinds: Partial<Record<SourceKind, KindEntry>>;
    live: { state: string; reason?: string };
}

export type TabState =
    | { state: "LOADING" }
    | { state: "ERROR" }
    | { state: "NOT_YET_AVAILABLE"; reason: string }
    | { state: "EMPTY" }
    | { state: "AVAILABLE"; series: SeriesSummary[]; refused: number };

export function tabDefinition(tab: TabKey): TabDefinition {
    const found = TABS.find(t => t.key === tab);
    if (!found) throw new Error(`unknown tab ${tab}`);
    return found;
}

/** May this series appear under this tab? Live requires a LIVE series flagged live; the others never accept one. */
export function canRenderUnderTab(series: Pick<SeriesSummary, "source_kind" | "is_live">, tab: TabKey): boolean {
    if (tab === "LIVE") return series.source_kind === "LIVE" && series.is_live === true;
    if (series.is_live !== false || series.source_kind === "LIVE") return false;
    return tabDefinition(tab).kinds.includes(series.source_kind);
}

/** What one tab shows. Never invents data: no published series means an empty (or, for Live, not-yet-available) state. */
export function deriveTabState(index: TrackRecordIndex | null | undefined, tab: TabKey,
                               opts: { loading?: boolean; error?: boolean } = {}): TabState {
    if (opts.loading) return { state: "LOADING" };
    if (opts.error || !index) return { state: "ERROR" };
    const all: SeriesSummary[] = [];
    for (const kind of tabDefinition(tab).kinds) {
        for (const s of index.kinds?.[kind]?.series ?? []) all.push(s);
    }
    const series = all.filter(s => canRenderUnderTab(s, tab));
    const refused = all.length - series.length;
    if (series.length > 0) return { state: "AVAILABLE", series, refused };
    if (tab === "LIVE") {
        return { state: "NOT_YET_AVAILABLE", reason: index.kinds?.LIVE?.reason || index.live?.reason || LIVE_NOT_AVAILABLE_REASON };
    }
    return { state: "EMPTY" };
}

/** The label shown on a series. Non-live series always carry the simulated label, whatever the payload says. */
export function seriesLabel(series: Pick<SeriesSummary, "source_kind" | "is_live">): string {
    return series.source_kind === "LIVE" && series.is_live === true ? LIVE_LABEL : SIMULATED_LABEL;
}

/** "0.0123000000" -> "1.23%". Missing or malformed values are "Not available", never zero. */
export function formatPercent(value: string | null | undefined, digits = 2): string {
    if (value === null || value === undefined || value === "") return NOT_AVAILABLE;
    const n = Number(value);
    if (!Number.isFinite(n)) return NOT_AVAILABLE;
    return `${(n * 100).toFixed(digits)}%`;
}

/** Exposure ratio "1.5000000000" -> "1.50x". */
export function formatRatio(value: string | null | undefined): string {
    if (value === null || value === undefined || value === "") return NOT_AVAILABLE;
    const n = Number(value);
    return Number.isFinite(n) ? `${n.toFixed(2)}x` : NOT_AVAILABLE;
}

export function formatPeriod(period: ObservationPeriod | null | undefined): string {
    if (!period) return NOT_AVAILABLE;
    return `${period.start} to ${period.end} (${period.calendar_days_with_data} days with data)`;
}

export function formatVersions(versions: StrategyVersion[] | null | undefined): string {
    if (!versions || versions.length === 0) return NOT_AVAILABLE;
    return versions.map(v => `v${v.family_version} (${v.first_date} to ${v.last_date})`).join(", ");
}

export function formatOutages(m: Pick<SeriesMetrics, "outage_minutes" | "outage_days" | "strategy_suspended_days"> | null | undefined): string {
    if (!m) return NOT_AVAILABLE;
    return `${m.outage_minutes} min across ${m.outage_days} day(s); strategy suspended on ${m.strategy_suspended_days} day(s)`;
}

export function formatFreshness(f: DataFreshness | null | undefined): string {
    if (!f || !f.latest_trading_date) return NOT_AVAILABLE;
    return `Data through ${f.latest_trading_date}${f.last_recorded_at ? `, last recorded ${f.last_recorded_at}` : ""}`;
}

/** The composite metrics a summary card may show, or null when the composite is suppressed or absent. */
export function publishedMetrics(agg: Aggregate | null | undefined): SeriesMetrics | null {
    if (!agg || agg.status !== "PUBLISHED" || !agg.metrics) return null;
    return agg.metrics;
}
