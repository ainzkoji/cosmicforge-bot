/** Step 7.3 -- market scope: crypto only by default, traditional markets shown as unavailable with reasons. */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
    DEFAULT_MARKET_SCOPE, TRADFI_SELECTABLE_IN_THIS_BUILD, effectiveMarketScope, marketScopeLabel, marketScopeOptions,
} from "../lib/markets.ts";
import type { MarketAvailability } from "../lib/markets.ts";
import { buildDeploymentRequest, formFingerprint } from "../lib/deployment.ts";
import type { DeploymentForm } from "../lib/deployment.ts";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

const answer = (tradfiAvailable = false): MarketAvailability => ({
    version: "1.0.0", markets: [], default_scope: "CRYPTO_ONLY", live_trading: "DISABLED",
    scopes: {
        CRYPTO_ONLY: { available: true, reasons: [] },
        CRYPTO_AND_TRADFI: {
            available: tradfiAvailable,
            reasons: tradfiAvailable ? [] : [
                { code: "NO_TRADFI_MARKET_AVAILABLE", message: "Traditional markets (futures, FX, equities) are not available yet." },
                { code: "NO_CERTIFIED_FAMILY", message: "No strategy family has a research certification for this market." },
                { code: "CALENDAR_UNVERIFIED", message: "" },
            ],
        },
    },
});

const form = (over: Partial<DeploymentForm> = {}): DeploymentForm => ({
    brokerAccountId: "acct-1", riskLevel: "balanced", budgetType: "fixed_amount", budgetValue: "1000.00",
    maxPositionUsdt: "", dailyLossLimitPct: "", riskAcknowledged: true, ...over,
});

test("crypto only is the default and always selectable", () => {
    assert.equal(DEFAULT_MARKET_SCOPE, "CRYPTO_ONLY");
    const [crypto] = marketScopeOptions(answer());
    assert.equal(crypto.scope, "CRYPTO_ONLY");
    assert.equal(crypto.selectable, true);
    assert.equal(marketScopeOptions(null)[0].selectable, true);
});

test("traditional markets are shown as unavailable with the backend's reasons", () => {
    const tradfi = marketScopeOptions(answer())[1];
    assert.equal(tradfi.scope, "CRYPTO_AND_TRADFI");
    assert.equal(tradfi.selectable, false);
    assert.deepEqual(tradfi.reasons, [
        "Traditional markets (futures, FX, equities) are not available yet.",
        "No strategy family has a research certification for this market.",
        "CALENDAR_UNVERIFIED",
    ]);
    const unloaded = marketScopeOptions(undefined)[1];
    assert.equal(unloaded.selectable, false);
    assert.ok(unloaded.reasons[0].includes("could not be loaded"));
});

test("traditional markets are never selectable in this build, whatever an answer says", () => {
    assert.equal(TRADFI_SELECTABLE_IN_THIS_BUILD, false);
    assert.equal(marketScopeOptions(answer(true))[1].selectable, false);
    assert.equal(effectiveMarketScope("CRYPTO_AND_TRADFI", answer(true)), "CRYPTO_ONLY");
    assert.equal(effectiveMarketScope(undefined), "CRYPTO_ONLY");
});

test("the deployment body without a scope is byte-identical to the pre-Step-7.3 body", () => {
    const before = buildDeploymentRequest(form(), "dep-abc12345");
    const crypto = buildDeploymentRequest(form({ marketScope: "CRYPTO_ONLY" }), "dep-abc12345");
    assert.equal(JSON.stringify(crypto), JSON.stringify(before));
    assert.equal("market_scope" in before, false);
    assert.equal(formFingerprint(form({ marketScope: "CRYPTO_ONLY" })), formFingerprint(form()));
    const tradfi = buildDeploymentRequest(form({ marketScope: "CRYPTO_AND_TRADFI" }), "dep-abc12345");
    assert.equal(tradfi.market_scope, "CRYPTO_AND_TRADFI");
    assert.notEqual(formFingerprint(form({ marketScope: "CRYPTO_AND_TRADFI" })), formFingerprint(form()));
});

test("bot market scope labels", () => {
    assert.equal(marketScopeLabel("CRYPTO_ONLY"), "Crypto only");
    assert.equal(marketScopeLabel(undefined), "Crypto only");
    assert.equal(marketScopeLabel("CRYPTO_AND_TRADFI"), "Crypto and traditional markets");
    assert.equal(marketScopeLabel("LEGACY_FOREX"), "Legacy forex bot");
});

test("the deployment screen shows the market control and the bot row shows the scope", () => {
    const screen = src("components/AutoPilot/CatiDeployment.tsx");
    assert.ok(screen.includes("marketScopeOptions(") && screen.includes("getMarketAvailability"));
    assert.ok(screen.includes("disabled={!option.selectable}"));
    assert.ok(src("components/BotInstance/BotInstanceRow.tsx").includes("marketScopeLabel(cati.market_scope)"));
    assert.ok(src("api/client.ts").includes("/api/v1/markets/availability"));
    // the legacy forex form stays, and says it is not the CATI path
    const page = src("pages/AutoPilot.tsx");
    assert.ok(page.includes("legacy, not CATI") && page.includes("traditional markets are not available on the CATI path"));
});
