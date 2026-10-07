import test from "node:test";
import assert from "node:assert/strict";
import { isLiveMode, isPaperMode, summarizeTradingMode, tradingModeLabel } from "../utils/tradingMode.ts";

test("only an exact paper/demo value is paper", () => {
    for (const value of ["paper", "demo", " PAPER ", "Demo"]) assert.equal(isPaperMode(value), true);
});

test("everything else is treated as live", () => {
    for (const value of ["live", "LIVE", "", "  ", "papers", "paper-ish", "testnet", "unknown", undefined, null, 0, false, {}]) {
        assert.equal(isPaperMode(value), false);
        assert.equal(isLiveMode(value), true);
    }
});

test("an unrecognised mode is never labelled paper", () => {
    assert.equal(tradingModeLabel("paper"), "paper");
    assert.equal(tradingModeLabel("live"), "live");
    assert.match(tradingModeLabel(undefined), /treated as live/);
    assert.match(tradingModeLabel("sandbox"), /treated as live/);
});

test("banner summary: paper only when every bot is paper", () => {
    assert.equal(summarizeTradingMode([]).kind, "paper_only");
    assert.equal(summarizeTradingMode([{ mode: "paper", status: "active" }, { mode: "demo", status: "stopped" }]).kind, "paper_only");
});

test("banner summary: a stopped live bot still shows the live indicator", () => {
    const summary = summarizeTradingMode([{ mode: "paper", status: "active" }, { mode: "live", status: "stopped" }]);
    assert.deepEqual(summary, { kind: "live_stopped", liveCount: 1, liveActiveCount: 0 });
});

test("banner summary: running, paused or unknown-mode bots count as live", () => {
    assert.deepEqual(
        summarizeTradingMode([{ mode: "live", status: "active" }, { mode: "live", status: "paused" }, { mode: "live", status: "stopped" }]),
        { kind: "live_running", liveCount: 3, liveActiveCount: 1 },
    );
    assert.equal(summarizeTradingMode([{ status: "active" }]).kind, "live_running");
    assert.equal(summarizeTradingMode([{ mode: "mystery", status: "stopped" }]).kind, "live_stopped");
});
