/**
 * CATI Section 21 UI safety tests (pure view-model; no React / DOM / network / API calls).
 * Run: npm test  (node --experimental-strip-types --test src/tests/*.test.ts)
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";

import {
    autoRoutingEnabled, automationSummary, familyRows, instrumentBadges, isTradeReady, permissionRows, reasonView,
    routeRequirement, stripSecrets, topologyLabel, transferStage, validateRoutingPolicy,
} from "../utils/multiAssetView.ts";

const status = {
    markets: {
        CRYPTO: { markets_available: 12, markets_api_tradable: 12, execution: { status: "ACTIVE", reason: null } },
        FX: { markets_available: 40, markets_api_tradable: 0, execution: { status: "ACCOUNT_NOT_ELIGIBLE", reason: "VENUE_API_NOT_SUPPORTED" } },
        STOCK: { markets_available: 0, markets_api_tradable: 0, execution: { status: "DATA_NOT_READY", reason: "NOT_LISTED_BY_VENUE" } },
    },
};

describe("account capabilities", () => {
    it("renders each family from backend state with its machine reason kept", () => {
        const rows = familyRows(status);
        assert.deepEqual(rows.map((r) => [r.family, r.availability]), [["CRYPTO", "AVAILABLE"], ["FX", "UNAVAILABLE"], ["STOCK", "UNAVAILABLE"]]);
        const fx = rows.find((r) => r.family === "FX")!;
        assert.equal(fx.reason.code, "VENUE_API_NOT_SUPPORTED");
        assert.match(fx.reason.text, /API-supported contract/);
    });
    it("never replaces an unknown reason by generic copy", () => {
        assert.deepEqual(reasonView("SOME_NEW_REASON"), { code: "SOME_NEW_REASON", text: "SOME_NEW_REASON" });
        assert.notEqual(reasonView("CAPITAL_NOT_READY").text, "Something went wrong");
    });
});

describe("secrets never render", () => {
    it("drops key / secret / passphrase / token / signature / masked key fields at any depth", () => {
        const out = stripSecrets({ api_key: "AK1234", apiSecret: "s", nested: { passphrase: "p", token: "t", ok: 1 },
            list: [{ signature: "x", masked_key: "...a1b2", state: "VERIFIED" }] });
        assert.deepEqual(out, { nested: { ok: 1 }, list: [{ state: "VERIFIED" }] });
        assert.doesNotMatch(JSON.stringify(out), /AK1234|a1b2/);
    });
    it("permission health shows states only; withdrawal is never required", () => {
        const rows = permissionRows({ TRADE: { state: "VERIFIED", use: "REQUIRED_FOR_TRADING" },
            INTERNAL_TRANSFER: { state: "UNVERIFIED", use: "REQUIRED_ONLY_FOR_PHYSICAL_TRANSFER" },
            WITHDRAW: { state: "ABSENT", use: "NEVER_REQUIRED" }, api_key: "leak" } as any);
        assert.equal(rows.find((r) => r.permission === "WITHDRAW")!.state, "Not required");
        assert.equal(rows.find((r) => r.permission === "INTERNAL_TRANSFER")!.tone, "warn");
        assert.doesNotMatch(JSON.stringify(rows), /leak/);
    });
    it("permission unavailable reason is shown", () => {
        assert.match(reasonView("PERMISSION_EVIDENCE_REQUIRED").text, /could not be verified/);
    });
});

describe("topology and routing", () => {
    it("labels every topology state", () => {
        assert.equal(topologyLabel({ class: "UNIFIED" }).label, "Unified account");
        assert.equal(topologyLabel({ class: "SEGMENTED" }).label, "Segmented wallets");
        assert.equal(topologyLabel({ class: "UNKNOWN", reason_code: "ACCOUNT_TOPOLOGY_UNKNOWN" }).label, "Topology unknown");
        assert.equal(topologyLabel(null).reason.code, "ACCOUNT_TOPOLOGY_UNKNOWN");
    });
    it("logical allocation never reads as moving funds; physical and no-route are distinct", () => {
        const logical = routeRequirement({ physical_transfer_required: false, allocation: "LOGICAL_ALLOCATION_NO_PHYSICAL_TRANSFER", policy_decision: { eligible: true } });
        assert.equal(logical.requirement, "LOGICAL_ALLOCATION");
        assert.match(logical.text, /no funds are moved/);
        assert.equal(routeRequirement({ physical_transfer_required: true, policy_decision: { eligible: true } }).requirement, "PHYSICAL_INTERNAL_TRANSFER");
        const none = routeRequirement({ physical_transfer_required: true, policy_decision: { eligible: false, reason_code: "ROUTE_UNSUPPORTED" } });
        assert.equal(none.requirement, "NO_ROUTE");
        assert.equal(none.reason.code, "ROUTE_UNSUPPORTED");
        assert.equal(routeRequirement(null).requirement, "UNKNOWN");
    });
});

describe("Auto Trading is independent from Auto Capital Routing", () => {
    const authorized = { mode: "AUTOMATED_INTERNAL_REALLOCATION", authorized_at: "t", auto_rebalance_enabled: true };
    it("routing is enabled only by its own policy (never by auto trading)", () => {
        assert.equal(autoRoutingEnabled({}), false);
        assert.equal(autoRoutingEnabled({ ...authorized, emergency_disabled: true }), false);
        assert.equal(autoRoutingEnabled({ ...authorized, authorized_at: null }), false);
        assert.equal(autoRoutingEnabled(authorized), true);
        assert.equal(autoRoutingEnabled.length, 1); // the function does not even accept an auto-trading input
    });
    it("describes the four combinations without letting a toggle bypass governance", () => {
        assert.match(automationSummary(true, {}), /wait for your manual action/);
        assert.match(automationSummary(false, authorized), /moves no funds on its own/);
        assert.match(automationSummary(true, authorized), /governance/);
        assert.match(automationSummary(false, {}), /No automatic trading/);
    });
});

describe("routing policy validation mirrors the server", () => {
    it("rejects bad numbers, percentages and routes; requires explicit authorization", () => {
        assert.deepEqual(validateRoutingPolicy({ max_transfer_amount: "100", max_transfer_pct: "0.5" }), {});
        const e = validateRoutingPolicy({ max_transfer_amount: "-1", max_transfer_pct: "1.5", allowed_routes: ["FUND"],
            mode: "AUTOMATED_INTERNAL_REALLOCATION" });
        assert.ok(e.max_transfer_amount && e.max_transfer_pct && e.allowed_routes && e.authorize_automated_reallocation);
        assert.ok(validateRoutingPolicy({ manual_approval_threshold: "abc" }).manual_approval_threshold);
    });
});

describe("transfer approval flow", () => {
    it("only a broker COMPLETED status is confirmation; approval / request / submit are not", () => {
        assert.equal(transferStage({ status: "COMPLETED" }).confirmed, true);
        for (const st of ["REQUESTED", "SUBMITTED", "CONFIRMATION_PENDING", "UNKNOWN", "RECONCILIATION_REQUIRED", "BLOCKED"]) {
            assert.equal(transferStage({ status: st }).confirmed, false, st);
        }
        assert.match(transferStage({ status: "UNKNOWN" }).text, /will not be re-submitted/);
        assert.equal(reasonView("MANUAL_APPROVAL_REQUIRED").code, "MANUAL_APPROVAL_REQUIRED");
    });
});

describe("research vs market availability", () => {
    const newlyListed = { market_available: { state: true }, research: { state: "RESEARCH_ONLY", reason: "INSTRUMENT_RESEARCH_ONLY" },
        certification: { state: "NOT_CERTIFIED", reason: "CERTIFICATION_NOT_READY" },
        execution_authorized: { state: false, reason: "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY" } };
    it("a newly discovered instrument is market-available, research-only, not certified, not authorized", () => {
        const b = instrumentBadges(newlyListed);
        assert.deepEqual(b.map((x) => x.value), ["Available", "Research only", "Not certified", "Not authorized"]);
        assert.equal(isTradeReady(newlyListed), false);
    });
    it("execution authority is shown separately and is the only trade-ready signal", () => {
        const certifiedButUnauthorized = { ...newlyListed, research: { state: "RESEARCH_READY" }, certification: { state: "CERTIFICATION_READY" } };
        assert.equal(isTradeReady(certifiedButUnauthorized), false);
        assert.equal(isTradeReady({ ...newlyListed, execution_authorized: { state: true } }), true);
    });
});

describe("cross-user data", () => {
    it("the view-model renders only what the (owner-scoped) response contains", () => {
        const rows = familyRows({ markets: {} });
        assert.deepEqual(rows, []);
        assert.deepEqual(permissionRows(undefined).map((r) => r.state).slice(0, 5), ["UNVERIFIED", "UNVERIFIED", "UNVERIFIED", "UNVERIFIED", "UNVERIFIED"]);
    });
});
