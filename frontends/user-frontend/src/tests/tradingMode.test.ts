import test from "node:test";
import assert from "node:assert/strict";
import {
    classifyBotMoney, indexAccountEnvironments, isDemoEnvironment, isLiveMode, isPaperMode, isRealMoneyEnvironment,
    summarizeTradingMode, tradingModeLabel,
} from "../utils/tradingMode.ts";

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
    assert.deepEqual(summary, { kind: "live_stopped", liveCount: 1, liveActiveCount: 0, demoCount: 0, demoActiveCount: 0 });
});

test("banner summary: running, paused or unknown-mode bots count as live", () => {
    assert.deepEqual(
        summarizeTradingMode([{ mode: "live", status: "active" }, { mode: "live", status: "paused" }, { mode: "live", status: "stopped" }]),
        { kind: "live_running", liveCount: 3, liveActiveCount: 1, demoCount: 0, demoActiveCount: 0 },
    );
    assert.equal(summarizeTradingMode([{ status: "active" }]).kind, "live_running");
    assert.equal(summarizeTradingMode([{ mode: "mystery", status: "stopped" }]).kind, "live_stopped");
});

// Bot mode "live" means "executes at a broker"; the broker ACCOUNT's environment decides real money.
const accounts = indexAccountEnvironments([
    { id: "acc_demo", environment: "DEMO" },
    { id: "acc_testnet", environment: "testnet" },
    { id: "acc_live", environment: "LIVE" },
    { id: "acc_blank", environment: "" },
    { id: "acc_missing_env" },
]);

test("account environment: only a positive DEMO alias is not real money", () => {
    for (const value of ["demo", "DEMO", " testnet ", "sandbox", "paper"]) {
        assert.equal(isDemoEnvironment(value), true);
        assert.equal(isRealMoneyEnvironment(value), false);
    }
    for (const value of ["live", "LIVE", "mainnet", "", "unknown", undefined, null, 1, {}]) {
        assert.equal(isDemoEnvironment(value), false);
        assert.equal(isRealMoneyEnvironment(value), true);
    }
});

test("a live-mode bot on a DEMO account is demo (no real money)", () => {
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_demo" }, accounts), "demo");
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_testnet" }, accounts), "demo");
});

test("a live-mode bot on a LIVE account is live (real money)", () => {
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_live" }, accounts), "live");
});

test("a live-mode bot with an unknown account fails towards live", () => {
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_not_listed" }, accounts), "live");
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_blank" }, accounts), "live");
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_missing_env" }, accounts), "live");
    assert.equal(classifyBotMoney({ mode: "live" }, accounts), "live");
    // Accounts not loaded at all.
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_demo" }, null), "live");
    assert.equal(classifyBotMoney({ mode: "live", broker_account_id: "acc_demo" }), "live");
    // An unknown mode is never paper, even on a demo account it is at least a broker-executing bot.
    assert.equal(classifyBotMoney({ mode: "mystery", broker_account_id: "acc_live" }, accounts), "live");
});

test("a paper bot is paper whatever its account", () => {
    assert.equal(classifyBotMoney({ mode: "paper", broker_account_id: "acc_live" }, accounts), "paper");
    assert.equal(classifyBotMoney({ mode: "paper" }, null), "paper");
});

test("indexAccountEnvironments skips entries without an id", () => {
    const index = indexAccountEnvironments([{ environment: "demo" }, { account_id: "a2", environment: "live" }]);
    assert.deepEqual([...index.entries()], [["a2", "live"]]);
    assert.equal(indexAccountEnvironments(undefined).size, 0);
});

test("banner summary: live-mode bots on DEMO accounts are demo_exchange, not live", () => {
    assert.deepEqual(
        summarizeTradingMode([{ mode: "live", status: "active", broker_account_id: "acc_demo" }], accounts),
        { kind: "demo_exchange", liveCount: 0, liveActiveCount: 0, demoCount: 1, demoActiveCount: 1 },
    );
    assert.equal(summarizeTradingMode([{ mode: "paper", status: "active" }, { mode: "live", status: "stopped", broker_account_id: "acc_testnet" }], accounts).kind, "demo_exchange");
});

test("banner summary: one real-money bot outranks demo bots", () => {
    assert.deepEqual(
        summarizeTradingMode([
            { mode: "live", status: "active", broker_account_id: "acc_demo" },
            { mode: "live", status: "stopped", broker_account_id: "acc_live" },
        ], accounts),
        { kind: "live_stopped", liveCount: 1, liveActiveCount: 0, demoCount: 1, demoActiveCount: 1 },
    );
    assert.equal(summarizeTradingMode([{ mode: "live", status: "active", broker_account_id: "acc_not_listed" }], accounts).kind, "live_running");
});

test("banner summary: without loaded accounts a demo-account bot still counts as live", () => {
    assert.equal(summarizeTradingMode([{ mode: "live", status: "active", broker_account_id: "acc_demo" }], null).kind, "live_running");
});
