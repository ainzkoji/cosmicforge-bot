/** Steps 6.4 / 8.6 -- public track record: tab state, labelling, and no non-live series under Live. */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
    LIVE_LABEL, LIVE_NOT_AVAILABLE_REASON, NOT_AVAILABLE, SIMULATED_LABEL, canRenderUnderTab, deriveTabState,
    formatPercent, formatRatio, publishedMetrics, seriesLabel,
} from "../lib/trackRecord.ts";
import type { SeriesSummary, SourceKind, TrackRecordIndex } from "../lib/trackRecord.ts";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

const series = (kind: SourceKind, isLive = kind === "LIVE", id = `${kind.toLowerCase()}-x`): SeriesSummary => ({
    series_id: id, source_kind: kind, family_id: "trend_core", venue: "ALL", label: "whatever the payload says",
    is_live: isLive, strategy_versions: [], observation_period: null, methodology_version: "TR-METHOD-1.0",
    composite: null, published_at: null,
    data_freshness: { latest_trading_date: null, last_recorded_at: null, generated_at: null },
});

const index = (kinds: Partial<Record<SourceKind, SeriesSummary[]>> = {}): TrackRecordIndex => ({
    methodology_version: "TR-METHOD-1.0", audited: false, store: "AVAILABLE",
    live: { state: "NOT_YET_AVAILABLE", reason: LIVE_NOT_AVAILABLE_REASON },
    kinds: Object.fromEntries((["BACKTEST", "RESEARCH_HOLDOUT", "PAPER", "DEMO", "LIVE"] as SourceKind[]).map(k => [k, {
        state: (kinds[k]?.length ? "AVAILABLE" : k === "LIVE" ? "NOT_YET_AVAILABLE" : "NO_PUBLISHED_SERIES"),
        label: k === "LIVE" ? LIVE_LABEL : SIMULATED_LABEL,
        reason: k === "LIVE" && !kinds[k]?.length ? LIVE_NOT_AVAILABLE_REASON : undefined,
        series: kinds[k] ?? [],
    }])),
});

test("live tab shows NOT_YET_AVAILABLE with the backend reason when nothing is published", () => {
    assert.deepEqual(deriveTabState(index(), "LIVE"), { state: "NOT_YET_AVAILABLE", reason: LIVE_NOT_AVAILABLE_REASON });
});

test("empty non-live tabs are empty, loading and errors are not data", () => {
    assert.deepEqual(deriveTabState(index(), "BACKTEST"), { state: "EMPTY" });
    assert.deepEqual(deriveTabState(index(), "DEMO"), { state: "EMPTY" });
    assert.deepEqual(deriveTabState(undefined, "LIVE", { loading: true }), { state: "LOADING" });
    assert.deepEqual(deriveTabState(index(), "DEMO", { error: true }), { state: "ERROR" });
    assert.deepEqual(deriveTabState(null, "LIVE"), { state: "ERROR" });
});

test("tabs group kinds: backtest + research holdout, paper + demo", () => {
    const idx = index({ BACKTEST: [series("BACKTEST")], RESEARCH_HOLDOUT: [series("RESEARCH_HOLDOUT")],
                        PAPER: [series("PAPER")], DEMO: [series("DEMO")] });
    const bt = deriveTabState(idx, "BACKTEST");
    assert.equal(bt.state, "AVAILABLE");
    assert.deepEqual(bt.state === "AVAILABLE" && bt.series.map(s => s.source_kind), ["BACKTEST", "RESEARCH_HOLDOUT"]);
    const demo = deriveTabState(idx, "DEMO");
    assert.deepEqual(demo.state === "AVAILABLE" && demo.series.map(s => s.source_kind), ["PAPER", "DEMO"]);
    assert.equal(deriveTabState(idx, "LIVE").state, "NOT_YET_AVAILABLE");
});

test("a non-live series is refused under the Live tab, even if the payload misfiles it", () => {
    assert.equal(canRenderUnderTab(series("DEMO"), "LIVE"), false);
    assert.equal(canRenderUnderTab(series("BACKTEST"), "LIVE"), false);
    assert.equal(canRenderUnderTab(series("LIVE", false), "LIVE"), false);       // kind says live, flag does not
    assert.equal(canRenderUnderTab(series("DEMO", true), "LIVE"), false);        // flag says live, kind does not
    const misfiled = index({ LIVE: [series("DEMO", false, "demo-misfiled"), series("LIVE", false, "live-unflagged")] });
    assert.deepEqual(deriveTabState(misfiled, "LIVE"), { state: "NOT_YET_AVAILABLE", reason: LIVE_NOT_AVAILABLE_REASON });
    const mixed = index({ LIVE: [series("LIVE"), series("DEMO", false, "demo-misfiled")] });
    const live = deriveTabState(mixed, "LIVE");
    assert.equal(live.state, "AVAILABLE");
    assert.ok(live.state === "AVAILABLE" && live.series.length === 1 && live.refused === 1);
});

test("a live series never appears under Backtest or Demo", () => {
    assert.equal(canRenderUnderTab(series("LIVE"), "DEMO"), false);
    assert.equal(canRenderUnderTab(series("LIVE"), "BACKTEST"), false);
    assert.equal(canRenderUnderTab(series("DEMO", true), "DEMO"), false);
    assert.equal(canRenderUnderTab(series("DEMO"), "BACKTEST"), false);
});

test("labels: non-live always simulated, regardless of the payload label", () => {
    for (const k of ["BACKTEST", "RESEARCH_HOLDOUT", "PAPER", "DEMO"] as SourceKind[]) {
        assert.equal(seriesLabel(series(k)), "Simulated — not achieved trading results");
    }
    assert.equal(seriesLabel(series("LIVE")), LIVE_LABEL);
    assert.equal(seriesLabel(series("LIVE", false)), SIMULATED_LABEL);
});

test("formatting never invents a number", () => {
    assert.equal(formatPercent("0.0123000000"), "1.23%");
    assert.equal(formatPercent("-0.0405000000"), "-4.05%");
    for (const v of [null, undefined, "", "abc"]) assert.equal(formatPercent(v as string | null), NOT_AVAILABLE);
    assert.equal(formatRatio("1.5"), "1.50x");
    assert.equal(formatRatio(null), NOT_AVAILABLE);
    assert.equal(publishedMetrics({ status: "SUPPRESSED_MIN_COUNT", minimum_accounts: 5 }), null);
    assert.equal(publishedMetrics(null), null);
});

test("the page is public, under PublicLayout, and carries no hard-coded performance", () => {
    const app = src("App.tsx");
    const publicBlock = app.slice(app.indexOf("<Route element={<PublicLayout />}>"), app.indexOf("<Route element={<ProtectedRoute />}>"));
    assert.ok(publicBlock.includes('path="/track-record"'));
    const page = src("pages/TrackRecord.tsx");
    assert.ok(!/\d+(\.\d+)?%/.test(page), "no literal percentages in the page");
    assert.ok(page.includes("deriveTabState") && page.includes("seriesLabel"));
    assert.ok(src("api/trackRecord.ts").includes("/api/v1/public/track-record"));
    assert.ok(src("api/trackRecord.ts").includes('credentials: "omit"'));
});
