/**
 * The logic of the Experimental DEMO trading admin page (experimental DEMO
 * authorizations and the governed L-08 market-entry policy).
 *
 * These tests import src/utils/experimentalDemo.ts itself (no copy), so what
 * passes here is what the page runs. No React, DOM, network or backend:
 *
 *   node --experimental-strip-types src/tests/experimental_demo.test.ts
 *
 * The fixtures have the shape of the bot-backend answers
 * (governance/experimental_demo.py Grant.to_plain, scale/market_entry_policy.py
 * PolicyRecord.to_plain / PolicyEvent.to_plain); values are illustrative.
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
    ALL_SYMBOLS,
    DEFAULT_POLICY_DRAFT,
    EMPTY_GRANT_DRAFT,
    EXPERIMENTAL_DEMO_PATH,
    EXPERIMENTAL_DEMO_ROUTE,
    MARKET_POLICY_PATH,
    MARKET_POLICY_ROUTE,
    MAX_DURATION_HOURS,
    MAX_ENTRY_NOTIONAL_USDT,
    acknowledgementMatches,
    authorizationAnomalies,
    buildGrantRequest,
    buildPolicyRequest,
    describeDemoAction,
    familyEligibilityNotes,
    formatRemaining,
    parseExperimentalDemoState,
    parseMarketPolicyHistory,
    parseMarketPolicyState,
    readDemoAnswer,
    toDemoError,
} from "../utils/experimentalDemo.ts";
import type { DemoAction, GrantDraft, PolicyDraft } from "../utils/experimentalDemo.ts";
import { GovernanceApiError, UnexpectedAnswer } from "../utils/familyGovernance.ts";
import type { GovernanceFamily } from "../utils/familyGovernance.ts";

const NOW = 1_793_000_000_000;

function family(overrides: Partial<GovernanceFamily> = {}): GovernanceFamily {
    return {
        family_id: "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1",
        family_version: "1",
        description: "",
        mandate_id: null,
        hypothesis_id: null,
        rule_hash: null,
        timeframe: "1d",
        trading_direction: null,
        position_model: null,
        entry_contract: null,
        exit_contract: null,
        admission_route: null,
        pipeline_adapter: null,
        supported_markets: [],
        evidence_reference: [],
        kill_switch_position_policy: null,
        certification_status: "FAILED",
        certification_reason: null,
        research_status: "FAILED",
        failure_reasons: [],
        governance_stage: "M0",
        allowed_modes: ["RESEARCH_SIMULATION", "SHADOW"],
        kill_switch_state: "OFF",
        registered: true,
        disabled: false,
        executable: false,
        blockers: ["CERTIFICATION_NOT_CERTIFIED"],
        pending_request: null,
        recorded_evidence: [],
        next_stage: null,
        ...overrides,
    };
}

const GRANT = {
    authorization_id: "xdemo_0123456789abcdef0123",
    label: "EXPERIMENTAL_DEMO",
    environment: "DEMO",
    state: "ACTIVE",
    account_id: "brk_demo_1",
    user_id: "user-1",
    family_id: "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1",
    family_rule_hash: "abc",
    research_status_at_grant: "FAILED",
    stage_at_grant: "M0",
    venue: "binance",
    base_url: "https://demo-fapi.binance.com",
    approval_ref: "owner-msg-2026-11-01",
    reason: "watch the strategy on demo",
    granted_by: "operator:admin-1",
    max_entry_notional_usdt: "200",
    granted_at: NOW,
    expires_at: NOW + 3_600_000,
    seconds_remaining: 3600,
    revoked_at: null,
    revoked_by: null,
    revoke_reason: null,
    entries_claimed: 1,
    entries: [{ trade_plan_id: "tp1", attempt_id: "a1", symbol: "BTCUSDT", side: "LONG", notional_usdt: 40.0, recorded_at: NOW + 1 }],
    not_certification: true,
    grants_live_authority: false,
    promotes_family: false,
};

const POLICY = {
    policy_id: "0f".repeat(16),
    policy_version: `single-account-market-entry:${"0f".repeat(16)}`,
    venue: "BINANCE_USDM",
    symbol: "*",
    environment: "DEMO",
    account_id: "brk_demo_1",
    scope: "SINGLE_ACCOUNT",
    calibrated: false,
    approval_ref: "owner-ok",
    recorded_by: "operator:admin-1",
    reason: "demo market entries",
    terms: { max_participation: "0.05", max_slippage_bps: "10", max_spread_bps: "10", max_depth_age_ms: 2000, min_levels: 5,
        absolute_cap_notional: "200", expires_at_ms: NOW + 86_400_000 },
    recorded_at: NOW - 1000,
    revoked: null,
    active: true,
    future_field: "kept",
};

function grantDraft(overrides: Partial<GrantDraft> = {}): GrantDraft {
    return { ...EMPTY_GRANT_DRAFT, accountId: "brk_demo_1", familyId: "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", acknowledgedStatus: "FAILED",
        approvalRef: "owner-msg-1", reason: "watch it on demo", ...overrides };
}

function policyDraft(overrides: Partial<PolicyDraft> = {}): PolicyDraft {
    return { ...DEFAULT_POLICY_DRAFT, symbol: "BTCUSDT", accountId: "brk_demo_1", approvalRef: "owner-ok", reason: "demo entries", ...overrides };
}

describe("experimental DEMO ledger answers", () => {
    it("parses the list, its integrity and the release limits", () => {
        const state = parseExperimentalDemoState({ integrity: { ok: true, reason: "OK_2_EVENTS" }, max_duration_hours: 336,
            max_entry_notional_usdt: "1000", authorizations: [GRANT] });
        assert.deepEqual(state.integrity, { ok: true, reason: "OK_2_EVENTS" });
        assert.equal(state.max_duration_hours, 336);
        assert.equal(state.max_entry_notional_usdt, 1000);
        assert.equal(state.authorizations[0].entries_claimed, 1);
        assert.equal(state.authorizations[0].entries[0].symbol, "BTCUSDT");
        assert.deepEqual(authorizationAnomalies(state.authorizations[0]), []);
    });

    it("never offers more than this release's limits, even if the server reports more", () => {
        const state = parseExperimentalDemoState({ integrity: { ok: true }, max_duration_hours: 10_000, max_entry_notional_usdt: "50000", authorizations: [] });
        assert.equal(state.max_duration_hours, MAX_DURATION_HOURS);
        assert.equal(state.max_entry_notional_usdt, MAX_ENTRY_NOTIONAL_USDT);
    });

    it("refuses an answer without the API's shape", () => {
        assert.throws(() => parseExperimentalDemoState({ authorizations: [] }), UnexpectedAnswer);
        assert.throws(() => parseExperimentalDemoState({ integrity: { ok: true }, authorizations: [{ state: "ACTIVE" }] }), UnexpectedAnswer);
    });

    it("flags a grant that claims live authority, promotion or certification", () => {
        const state = parseExperimentalDemoState({ integrity: { ok: true }, authorizations: [
            { ...GRANT, environment: "LIVE", grants_live_authority: true, promotes_family: true, not_certification: false }] });
        assert.equal(authorizationAnomalies(state.authorizations[0]).length, 4);
    });
});

describe("grant form", () => {
    it("builds exactly the documented body", () => {
        const built = buildGrantRequest(grantDraft({ acknowledgedStatus: " failed " }), family());
        assert.equal(built.ok, true);
        if (!built.ok) return;
        assert.deepEqual(built.body, {
            account_id: "brk_demo_1",
            family_id: "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1",
            environment: "DEMO",
            acknowledged_research_status: "FAILED",
            approval_ref: "owner-msg-1",
            reason: "watch it on demo",
            duration_hours: 24,
            max_entry_notional_usdt: 200,
        });
    });

    it("refuses a mismatched acknowledgement and warns about it", () => {
        assert.equal(acknowledgementMatches("CERTIFIED", family()), false);
        assert.equal(acknowledgementMatches("failed", family()), true);
        const built = buildGrantRequest(grantDraft({ acknowledgedStatus: "CERTIFIED" }), family());
        assert.equal(built.ok, false);
        if (built.ok) return;
        assert.ok(built.problems.some((p) => p.includes("does not match") && p.includes("FAILED")));
    });

    it("enforces account id, duration, notional and text bounds", () => {
        const bad = buildGrantRequest(grantDraft({ accountId: "brk demo", durationHours: "337", maxEntryNotional: "1000.5",
            approvalRef: "ab", reason: "" }), family());
        assert.equal(bad.ok, false);
        if (bad.ok) return;
        assert.equal(bad.problems.length, 5);
        for (const [hours, ok] of [["0", false], ["1", true], ["336", true], ["2.5", false]] as const) {
            assert.equal(buildGrantRequest(grantDraft({ durationHours: hours }), family()).ok, ok, `hours ${hours}`);
        }
        for (const [notional, ok] of [["0.5", false], ["1", true], ["1000", true], ["1e3", false], ["-5", false]] as const) {
            assert.equal(buildGrantRequest(grantDraft({ maxEntryNotional: notional }), family()).ok, ok, `notional ${notional}`);
        }
        assert.equal(buildGrantRequest(grantDraft({ accountId: "_x" }), family()).ok, false);
        assert.equal(buildGrantRequest(grantDraft({ accountId: "a".repeat(129) }), family()).ok, false);
    });

    it("honours a tighter server limit", () => {
        assert.equal(buildGrantRequest(grantDraft({ durationHours: "48" }), family(), { maxDurationHours: 24, maxEntryNotional: 1000 }).ok, false);
    });

    it("needs a selected family", () => {
        assert.equal(buildGrantRequest(grantDraft(), undefined).ok, false);
    });

    it("notes families the engine will refuse", () => {
        assert.deepEqual(familyEligibilityNotes(family()), []);
        assert.equal(familyEligibilityNotes(family({ disabled: true, kill_switch_state: "ON", registered: false })).length, 3);
        assert.equal(familyEligibilityNotes(family({ certification_status: "CERTIFIED", allowed_modes: ["DEMO"] })).length, 1);
    });

    it("the confirmation shows what is sent, to the experimental-demo route, and says it is not certification", () => {
        const built = buildGrantRequest(grantDraft(), family());
        assert.ok(built.ok);
        if (!built.ok) return;
        const action: DemoAction = { kind: "grant", body: built.body, family: family(), requestedAtMs: NOW };
        const description = describeDemoAction(action, "");
        assert.equal(description.path, EXPERIMENTAL_DEMO_PATH);
        assert.deepEqual(description.body, built.body);
        assert.equal(description.asksReason, false);
        assert.ok(description.effects.some((e) => e.includes("NOT research certification")));
        assert.ok(description.effects.some((e) => e.includes("No LIVE authority")));
        assert.ok(description.summary.some(([label, value]) => label === "Current research status" && value === "FAILED"));
    });
});

describe("revocations", () => {
    const grant = parseExperimentalDemoState({ integrity: { ok: true }, authorizations: [GRANT] }).authorizations[0];
    const policy = parseMarketPolicyState({ integrity: { ok: true, reason: "OK" }, now_ms: NOW, policies: [POLICY] }).policies[0];

    it("send only the typed reason, to the id's revoke route", () => {
        const g = describeDemoAction({ kind: "revoke_grant", grant }, "  stop now  ");
        assert.equal(g.path, `${EXPERIMENTAL_DEMO_PATH}/${GRANT.authorization_id}/revoke`);
        assert.deepEqual(g.body, { reason: "stop now" });
        assert.equal(g.asksReason, true);
        const p = describeDemoAction({ kind: "revoke_policy", policy }, "done");
        assert.equal(p.path, `${MARKET_POLICY_PATH}/${POLICY.policy_id}/revoke`);
        assert.deepEqual(p.body, { reason: "done" });
    });

    it("refuse to build a request for an id that is not a plain identifier", () => {
        assert.throws(() => describeDemoAction({ kind: "revoke_grant", grant: { ...grant, authorization_id: "../x" } }, "why"));
    });

    it("are confirmed only when the server says so", () => {
        assert.match(readDemoAnswer({ kind: "revoke_grant", grant }, { authorization: { ...GRANT, state: "REVOKED" } }).summary, /REVOKED/);
        assert.throws(() => readDemoAnswer({ kind: "revoke_grant", grant }, { authorization: GRANT }), UnexpectedAnswer);
        assert.throws(() => readDemoAnswer({ kind: "revoke_grant", grant }, {}), UnexpectedAnswer);
        assert.match(readDemoAnswer({ kind: "revoke_policy", policy }, { event: { event_type: "POLICY_REVOKED", seq: 4 } }).summary, /event 4/);
        assert.throws(() => readDemoAnswer({ kind: "revoke_policy", policy }, { event: { event_type: "POLICY_RECORDED" } }), UnexpectedAnswer);
    });
});

describe("market-entry policy answers", () => {
    it("parses records defensively and derives their state", () => {
        const state = parseMarketPolicyState({ integrity: { ok: true, reason: "OK_3_EVENTS" }, now_ms: NOW, policies: [
            POLICY,
            { ...POLICY, policy_id: "b1", active: false },
            { ...POLICY, policy_id: "b2", active: false, revoked: { by: "operator:x", reason: "r", at: NOW } },
            { policy_id: "b3", terms: { expires_at_ms: NOW + 1 } },
            { policy_id: "b4" },
        ] });
        assert.deepEqual(state.policies.map((p) => p.state), ["ACTIVE", "EXPIRED", "REVOKED", "ACTIVE", "UNKNOWN"]);
        assert.equal(state.policies[0].terms.max_depth_age_ms, "2000");
        assert.deepEqual(state.policies[0].other, { future_field: "kept" });
        assert.equal(state.policies[2].revoked?.by, "operator:x");
        assert.throws(() => parseMarketPolicyState({ integrity: { ok: true }, policies: [{}] }), UnexpectedAnswer);
    });

    it("parses the history", () => {
        const history = parseMarketPolicyHistory({ integrity: { ok: false, reason: "BROKEN_AT_SEQ_2" }, events: [
            { seq: 1, event_type: "POLICY_RECORDED", policy_id: "p", calibrated: false, actor_ref: "operator:a", recorded_at: NOW }] });
        assert.equal(history.integrity.ok, false);
        assert.equal(history.events[0].event_type, "POLICY_RECORDED");
    });
});

describe("market-entry policy form", () => {
    it("starts from the conservative defaults", () => {
        assert.equal(DEFAULT_POLICY_DRAFT.maxParticipation, "0.05");
        assert.equal(DEFAULT_POLICY_DRAFT.maxSlippageBps, "10");
        assert.equal(DEFAULT_POLICY_DRAFT.maxSpreadBps, "10");
        assert.equal(DEFAULT_POLICY_DRAFT.maxDepthAgeMs, "2000");
        assert.equal(DEFAULT_POLICY_DRAFT.minLevels, "5");
        assert.equal(DEFAULT_POLICY_DRAFT.absoluteCapNotional, "200");
        assert.equal(DEFAULT_POLICY_DRAFT.expiryDays, "7");
    });

    it("builds the body with decimals as strings, DEMO only and never a calibration field", () => {
        const built = buildPolicyRequest(policyDraft({ venue: "binance_usdm", symbol: "btcusdt" }), NOW);
        assert.ok(built.ok);
        if (!built.ok) return;
        assert.deepEqual(built.body, {
            venue: "BINANCE_USDM",
            symbol: "BTCUSDT",
            environment: "DEMO",
            account_id: "brk_demo_1",
            terms: { max_participation: "0.05", max_slippage_bps: "10", max_spread_bps: "10", max_depth_age_ms: 2000, min_levels: 5,
                absolute_cap_notional: "200", expires_at_ms: NOW + 7 * 86_400_000 },
            approval_ref: "owner-ok",
            reason: "demo entries",
        });
        assert.ok(!JSON.stringify(built.body).includes("calibrat"));
        const description = describeDemoAction({ kind: "record_policy", body: built.body }, "");
        assert.equal(description.path, MARKET_POLICY_PATH);
        assert.deepEqual(description.body, built.body);
    });

    it("allows * only with an account id", () => {
        assert.equal(buildPolicyRequest(policyDraft({ symbol: ALL_SYMBOLS, accountId: "" }), NOW).ok, false);
        assert.equal(buildPolicyRequest(policyDraft({ symbol: ALL_SYMBOLS }), NOW).ok, true);
        const unscoped = buildPolicyRequest(policyDraft({ accountId: "" }), NOW);
        assert.ok(unscoped.ok);
        if (unscoped.ok) assert.equal("account_id" in unscoped.body, false);
    });

    it("enforces every bound the ledger enforces", () => {
        const cases: Array<[Partial<PolicyDraft>, boolean]> = [
            [{ maxParticipation: "0.10" }, true], [{ maxParticipation: "0.11" }, false], [{ maxParticipation: "0" }, false],
            [{ maxSlippageBps: "25" }, true], [{ maxSlippageBps: "25.01" }, false],
            [{ maxSpreadBps: "25" }, true], [{ maxSpreadBps: "26" }, false],
            [{ maxDepthAgeMs: "250" }, true], [{ maxDepthAgeMs: "249" }, false], [{ maxDepthAgeMs: "5000" }, true], [{ maxDepthAgeMs: "5001" }, false],
            [{ maxDepthAgeMs: "300.5" }, false],
            [{ minLevels: "5" }, true], [{ minLevels: "4" }, false], [{ minLevels: "50" }, true], [{ minLevels: "51" }, false],
            [{ absoluteCapNotional: "100000" }, true], [{ absoluteCapNotional: "100000.01" }, false], [{ absoluteCapNotional: "0" }, false],
            [{ expiryDays: "90" }, true], [{ expiryDays: "91" }, false], [{ expiryDays: "0" }, false],
            [{ approvalRef: "x".repeat(200) }, true], [{ approvalRef: "x".repeat(201) }, false], [{ reason: "no" }, false],
            [{ venue: "" }, false], [{ symbol: "BTC/USDT" }, false],
        ];
        for (const [overrides, ok] of cases) {
            assert.equal(buildPolicyRequest(policyDraft(overrides), NOW).ok, ok, JSON.stringify(overrides));
        }
    });
});

describe("errors and display", () => {
    it("keep the engine's reason code and detail verbatim, naming the route used", () => {
        const error = toDemoError({ response: { status: 409, data: { detail: "MARKET_POLICY_ALREADY_ACTIVE: abc", reason: "MARKET_POLICY_ALREADY_ACTIVE" } } },
            "Record", MARKET_POLICY_ROUTE);
        assert.ok(error instanceof GovernanceApiError);
        assert.equal(error.reason, "MARKET_POLICY_ALREADY_ACTIVE");
        assert.equal(error.detail, "MARKET_POLICY_ALREADY_ACTIVE: abc");
        assert.equal(error.outcomeUnknown, false);
        const missing = toDemoError({ response: { status: 404, data: { detail: "Not Found" } } }, "Load", EXPERIMENTAL_DEMO_ROUTE);
        assert.ok(missing.message.includes(EXPERIMENTAL_DEMO_ROUTE));
        const timeout = toDemoError(new Error("timeout"), "Grant", EXPERIMENTAL_DEMO_ROUTE);
        assert.equal(timeout.outcomeUnknown, true);
    });

    it("formats remaining time", () => {
        assert.equal(formatRemaining(null), "n/a");
        assert.equal(formatRemaining(0), "none");
        assert.equal(formatRemaining(45), "45s");
        assert.equal(formatRemaining(3600), "1h 00m");
        assert.equal(formatRemaining(2 * 86_400 + 3 * 3600 + 15 * 60), "2d 03h 15m");
    });
});

describe("routes and navigation", () => {
    it("the page is an admin-protected route with a sidebar entry, and offers no LIVE or calibrated option", () => {
        const root = join(import.meta.dirname, "..");
        const app = readFileSync(join(root, "App.tsx"), "utf8");
        const sidebar = readFileSync(join(root, "components/admin/layout/AdminSidebar.tsx"), "utf8");
        const page = readFileSync(join(root, "pages/admin/ExperimentalDemo.tsx"), "utf8");
        const protectedBlock = app.slice(app.indexOf("<AdminProtectedRoute />"));
        assert.ok(protectedBlock.includes('path="/admin/experimental-demo"'));
        assert.ok(sidebar.includes('"/admin/experimental-demo"'));
        assert.ok(page.includes("LIVE (not permitted in this release)"));
        assert.ok(/<input type="radio" name="mep-environment" disabled \/>/.test(page));
        assert.ok(page.includes("One account at a time per market; never"));
    });
});
