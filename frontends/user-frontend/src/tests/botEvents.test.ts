/** Section K3 -- live bot events refresh the right reads and nothing else. */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { BOT_EVENT_TYPES, parseBotEvent, queriesToRefresh } from "../lib/botEvents.ts";

const frame = (type: string, extra: Record<string, unknown> = {}) =>
    JSON.stringify({ event_id: "evt_1", event_type: type, timestamp: 1, user_id: "u", bot_id: "bot-1",
                     broker_account_id: "a", environment: "DEMO", payload: {}, ...extra });

test("all eight engine events are subscribed", () => {
    assert.deepEqual([...BOT_EVENT_TYPES].sort(), ["BOT_PAUSED", "BOT_RESUMED", "BOT_STOPPED", "DAILY_LOSS_PAUSE",
        "ENTRY_BLOCKED", "ENTRY_FILLED", "EXIT_FILLED", "POSITION_UNPROTECTED"]);
});

test("a fill refreshes the bot's positions, trades and results", () => {
    const keys = queriesToRefresh(parseBotEvent("EXIT_FILLED", frame("EXIT_FILLED"))!);
    for (const part of ["status", "positions", "trades", "summary", "events"]) {
        assert.ok(keys.some((k) => k.join("/") === `cati/bot-1/${part}`), part);
    }
    assert.ok(!keys.some((k) => k[0] === "botInstances"));
});

test("a pause or stop also refreshes the bot list", () => {
    for (const type of ["BOT_PAUSED", "BOT_STOPPED", "DAILY_LOSS_PAUSE"]) {
        assert.ok(queriesToRefresh(parseBotEvent(type, frame(type))!).some((k) => k[0] === "botInstances"), type);
    }
});

test("unknown types and malformed frames are ignored", () => {
    assert.equal(parseBotEvent("POSITION_OPENED", frame("POSITION_OPENED")), null);
    assert.equal(parseBotEvent("ENTRY_FILLED", "not json"), null);
    assert.equal(parseBotEvent("ENTRY_FILLED", JSON.stringify({ payload: {} })), null);
});

test("the streams do not close themselves on a transient error", () => {
    for (const hook of ["hooks/useBotEventStream.ts", "hooks/useAnalyticsStream.ts"]) {
        const text = readFileSync(join(process.cwd(), "src", hook), "utf8");
        assert.doesNotMatch(text, /onerror\s*=\s*\([^)]*\)\s*=>\s*\{[^}]*\.close\(\)/s, hook);
        assert.match(text, /withCredentials: true/);
        assert.doesNotMatch(text, /token=/);
    }
    assert.match(readFileSync(join(process.cwd(), "src", "components/BotInstance/CatiBotPanel.tsx"), "utf8"),
        /useBotEventStream\(botId\)/);
});
