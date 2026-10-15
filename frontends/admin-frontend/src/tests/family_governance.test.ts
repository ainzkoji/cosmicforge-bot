/**
 * Step 3.11: the logic of the Family governance admin page.
 *
 * These tests import src/utils/familyGovernance.ts itself (no copy), so what
 * passes here is what the page runs. No React, DOM, network or backend is
 * involved:
 *
 *   node --experimental-strip-types src/tests/family_governance.test.ts
 *
 * Or via: npm test
 *
 * The fixtures have the shape of the bot-backend governance API's answers as
 * recorded from a throwaway database: the field names, the codes and the
 * refusal texts are the engine's; descriptive values are shortened or
 * illustrative. Nothing here promotes, registers or changes a family anywhere.
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";

import {
    GovernanceApiError,
    MAX_OPERABLE_STAGE,
    UnexpectedAnswer,
    actionProblem,
    describeAction,
    familyHasOrderAuthority,
    formatTimestamp,
    historyRows,
    integrityText,
    isPlainIdentifier,
    missingEvidenceKeys,
    orderAuthority,
    parseFamilyDetail,
    parseHistoryIntegrity,
    parseRegistry,
    promotionTarget,
    readActionAnswer,
    reasonReady,
    revocationTargets,
    stageLabel,
    toGovernanceError,
} from "../utils/familyGovernance.ts";
import type { GovernanceAction } from "../utils/familyGovernance.ts";

const OPERATOR = "operator:admin-42@admin_api:/api/v1/admin/governance";

function family(overrides: Record<string, unknown> = {}): Record<string, unknown> {
    return {
        family_id: "DAILY_TREND",
        family_version: "daily-trend-rules-1",
        mandate_id: "MANDATE_004",
        hypothesis_id: "H-018",
        rule_hash: "7795ad79de962300351fcf4d419cd511256dce356c9ee46cdbbc21842582cbb9",
        supported_markets: [["binance", "USDT_PERPETUAL"]],
        timeframe: "1d",
        trading_direction: "LONG_ONLY",
        position_model: "PORTFOLIO_TARGETS",
        entry_contract: "EntryIntent at the next daily open",
        exit_contract: "ExitIntent on signal, stop, data gap or drawdown stop",
        activation_policy: { demo_requires: ["CERTIFIED", "M6"], kill_switch_position_policy: "MANAGE_TO_EXIT", shadow_requires: "REGISTERED" },
        evidence_reference: ["research/trend_v1/SPEC.md", "docs/research/registry/research_register.jsonl"],
        admission_route: "RULE_BASED_PORTFOLIO_ADMISSION_V1",
        pipeline_adapter: "app.trading_intelligence.pipeline.daily_trend",
        description: "Long-or-flat breakout ensemble on daily bars.",
        certification_status: "NOT_CERTIFIED",
        certification_detail: { status: "NOT_CERTIFIED", reason: "RESEARCH_REGISTER_STATUS_REGISTERED", research_status: "REGISTERED", hypothesis_id: "H-018", failure_reasons: [] },
        governance_stage: "M0",
        allowed_modes: ["RESEARCH_SIMULATION", "SHADOW"],
        kill_switch_state: "OFF",
        registered: true,
        disabled: false,
        executable: false,
        blockers: ["FAMILY_NOT_CERTIFIED", "FAMILY_STAGE_M0_NO_ORDER_AUTHORITY"],
        registry_version: "1.0.0",
        pending_request: null,
        recorded_evidence: {},
        next_stage_prerequisites: { stage: "M1", missing: ["EVIDENCE_MISSING:v2_benchmark_tag", "EVIDENCE_MISSING:section9_shadow_verified"] },
        ...overrides,
    };
}

const RESIDUAL = family({
    family_id: "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1",
    family_version: "mandate-003-frozen",
    hypothesis_id: "H-015",
    pipeline_adapter: null,
    certification_status: "FAILED",
    certification_detail: { status: "FAILED", reason: "RESEARCH_REGISTER_STATUS_FAILED", research_status: "FAILED", failure_reasons: ["ECONOMIC_GATE"] },
    allowed_modes: ["RESEARCH_SIMULATION"],
    blockers: ["FAMILY_FAILED_RESEARCH", "NO_PLATFORM_PIPELINE_ADAPTER", "FAMILY_STAGE_M0_NO_ORDER_AUTHORITY"],
});

/** GET /families as the engine answers it today. */
function registryAnswer(families: unknown[] = [family(), RESIDUAL], executable: string[] = []): Record<string, unknown> {
    return { families, executable, log: { ok: true, records: 6, head_hash: "ff31d9be71b18bc2ad0cb24e2f730ea070e701aaf93b300d09ac33a3c8dd4d19" } };
}

function event(seq: number, eventType: string, fields: Record<string, unknown> = {}): Record<string, unknown> {
    return {
        seq, event_id: `fev_${seq}`, family_id: "DAILY_TREND", event_type: eventType, scope: "FAMILY", request_id: null,
        from_stage: null, to_stage: null, actor_ref: OPERATOR, recorded_at: 1791569668745 + seq, payload: {},
        prev_hash: "0".repeat(64), event_hash: "f".repeat(64), ...fields,
    };
}

/** The response half of an axios error. */
function httpError(status: number, data: unknown): unknown {
    return Object.assign(new Error(`Request failed with status code ${status}`), { response: { status, data } });
}

describe("Family governance: what the registry says", () => {
    it("reads the registry and finds no family with order authority today", () => {
        const registry = parseRegistry(registryAnswer());
        assert.deepEqual(registry.families.map((f) => [f.family_id, f.governance_stage, f.certification_status, f.executable]), [
            ["DAILY_TREND", "M0", "NOT_CERTIFIED", false],
            ["RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", "M0", "FAILED", false],
        ]);
        assert.deepEqual(registry.families[0].allowed_modes, ["RESEARCH_SIMULATION", "SHADOW"]);
        assert.equal(registry.families[0].kill_switch_state, "OFF");
        assert.equal(registry.families[0].kill_switch_position_policy, "MANAGE_TO_EXIT");
        assert.deepEqual(registry.families[1].blockers, ["FAMILY_FAILED_RESEARCH", "NO_PLATFORM_PIPELINE_ADAPTER", "FAMILY_STAGE_M0_NO_ORDER_AUTHORITY"]);
        assert.deepEqual(registry.families[1].failure_reasons, ["ECONOMIC_GATE"]);
        assert.deepEqual(orderAuthority(registry), []);
        assert.ok(registry.families.every((f) => !familyHasOrderAuthority(f)));
    });

    it("reports order authority whenever any part of the answer shows it", () => {
        const byMode = parseRegistry(registryAnswer([family({ allowed_modes: ["RESEARCH_SIMULATION", "SHADOW", "DEMO"] }), RESIDUAL]));
        assert.deepEqual(orderAuthority(byMode), [{ family_id: "DAILY_TREND", modes: ["DEMO"] }]);

        const byFlag = parseRegistry(registryAnswer([family({ executable: true }), RESIDUAL]));
        assert.deepEqual(orderAuthority(byFlag), [{ family_id: "DAILY_TREND", modes: [] }]);
        assert.equal(familyHasOrderAuthority(byFlag.families[0]), true);

        // the list and the rows disagree: the wider reading wins, never "none"
        const byList = parseRegistry(registryAnswer([family(), RESIDUAL], ["RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"]));
        assert.deepEqual(orderAuthority(byList), [{ family_id: "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", modes: [] }]);

        // a mode this page does not know is not assumed to be harmless
        const unknownMode = parseRegistry(registryAnswer([family({ allowed_modes: ["SHADOW", "DEMO_CERTIFICATION"] })]));
        assert.deepEqual(orderAuthority(unknownMode), [{ family_id: "DAILY_TREND", modes: ["DEMO_CERTIFICATION"] }]);
    });

    it("refuses an answer that does not say what a family may do", () => {
        for (const missing of ["allowed_modes", "executable", "blockers", "governance_stage", "certification_status", "kill_switch_state", "registered", "disabled", "family_id"]) {
            const broken = family();
            delete broken[missing];
            assert.throws(() => parseRegistry(registryAnswer([broken])), UnexpectedAnswer, missing);
        }
        assert.throws(() => parseRegistry({ families: [family()], log: { ok: true, records: 1 } }), UnexpectedAnswer);
        assert.throws(() => parseRegistry({ families: [family()], executable: [] }), UnexpectedAnswer);
        assert.throws(() => parseRegistry({ families: {}, executable: [], log: { ok: true } }), UnexpectedAnswer);
        assert.throws(() => parseRegistry(null), UnexpectedAnswer);
        assert.throws(() => parseRegistry("<html>"), UnexpectedAnswer);
    });

    it("lists what the next stage is missing from the engine's own list", () => {
        const [daily] = parseRegistry(registryAnswer()).families;
        assert.deepEqual(daily.next_stage, { stage: "M1", missing: ["EVIDENCE_MISSING:v2_benchmark_tag", "EVIDENCE_MISSING:section9_shadow_verified"] });
        assert.deepEqual(missingEvidenceKeys(daily), ["v2_benchmark_tag", "section9_shadow_verified"]);

        const [atM5] = parseRegistry(registryAnswer([family({
            governance_stage: "M5",
            next_stage_prerequisites: { stage: "M6", missing: ["EVIDENCE_MISSING:replay_gates_passed", "RESEARCH_CERTIFICATION_NOT_CERTIFIED", "ADMISSION_NOT_ELIGIBLE"] },
        })])).families;
        // only evidence can be recorded here; the certification and the admission are the register's
        assert.deepEqual(missingEvidenceKeys(atM5), ["replay_gates_passed"]);
        assert.deepEqual(atM5.next_stage?.missing.slice(1), ["RESEARCH_CERTIFICATION_NOT_CERTIFIED", "ADMISSION_NOT_ELIGIBLE"]);

        const [unregistered] = parseRegistry(registryAnswer([family({ registered: false, next_stage_prerequisites: null })])).families;
        assert.equal(unregistered.next_stage, null);
        assert.deepEqual(missingEvidenceKeys(unregistered), []);
    });

    it("states the integrity of the log, and a broken log is never called intact", () => {
        assert.equal(integrityText({ ok: true, records: 13, head_hash: "8831c79e6885047225a5a6624f2f270b" }), "13 records verified, head 8831c79e68850472.");
        assert.equal(integrityText({ ok: true, records: 1 }), "1 record verified.");
        const broken = integrityText({ ok: false, records: 4, broken_at_seq: 4 });
        assert.ok(broken.startsWith("Broken at record 4: a record was changed, removed or reordered."), broken);
        assert.ok(!broken.includes("verified"));
        assert.ok(integrityText({ ok: false, records: 0 }).startsWith("Broken: "));
        assert.deepEqual(parseHistoryIntegrity({ integrity: { ok: false, records: 4, broken_at_seq: 4 }, events: [] }), { ok: false, records: 4, broken_at_seq: 4 });
        assert.throws(() => parseHistoryIntegrity({ events: [] }), UnexpectedAnswer);
        assert.throws(() => parseHistoryIntegrity({ integrity: { records: 3 } }), UnexpectedAnswer);
    });

    it("labels stages with the titles of the governance phases, unchanged", () => {
        assert.equal(stageLabel("M0"), "M0 · CATI OBSERVE / ENTRY BLOCKED");
        assert.equal(stageLabel("M5"), "M5 · REPLAY CERTIFICATION");
        assert.equal(stageLabel("M6"), "M6 · DEMO CATI ACTIVE");
        assert.equal(stageLabel("X9"), "X9");
        assert.equal(stageLabel(null), "unknown");
    });
});

describe("Family governance: the history", () => {
    const answer = {
        family: family({
            governance_stage: "M0", kill_switch_state: "ON", disabled: true, allowed_modes: ["RESEARCH_SIMULATION"],
            recorded_evidence: {
                v2_benchmark_tag: { reference: "docs://shadow-week-1", value: true, seq: 8, actor_ref: OPERATOR, recorded_at: 1791569669050 },
                section9_shadow_verified: { reference: "docs://shadow-week-1", value: true, seq: 9, actor_ref: OPERATOR, recorded_at: 1791569669216 },
            },
        }),
        history: [
            event(2, "FAMILY_REGISTERED", { payload: { family_version: "daily-trend-rules-1", rule_hash: "7795ad79de962300351fcf4d419cd511", definition_hash: "09d3" } }),
            event(7, "PROMOTION_REQUESTED", { request_id: "fpr_252b0d74836901bc069d9f29", from_stage: "M0", to_stage: "M1", payload: { reason: "start the shadow stages", evidence: {} } }),
            event(8, "EVIDENCE_RECORDED", { payload: { key: "v2_benchmark_tag", reference: "docs://shadow-week-1", value: true } }),
            event(10, "PROMOTION_APPROVED", { request_id: "fpr_252b0d74836901bc069d9f29", from_stage: "M0", to_stage: "M1", payload: { reason: "shadow evidence reviewed", reviewer: "admin-42" } }),
            event(11, "FAMILY_KILL_SWITCH_ON", { payload: { reason: "incident 7" } }),
            event(12, "FAMILY_DISABLED", { payload: { reason: "maintenance" } }),
            event(13, "PROMOTION_REVOKED", { from_stage: "M1", to_stage: "M0", payload: { reason: "back to the start", reviewer: "admin-42" } }),
            event(14, "PROMOTION_REJECTED", { request_id: "fpr_x", from_stage: "M0", to_stage: "M0", payload: { reason: "not yet", reviewer: "admin-42" } }),
        ],
    };

    it("shows every record with its event type, stage change, actor, time and reason", () => {
        const detail = parseFamilyDetail(answer);
        const rows = historyRows(detail.history);
        assert.equal(rows.length, answer.history.length);
        assert.deepEqual(rows.map((row) => [row.seq, row.event_type, row.stage_change, row.reason]), [
            [2, "FAMILY_REGISTERED", "", ""],
            [7, "PROMOTION_REQUESTED", "M0 -> M1 requested", "start the shadow stages"],
            [8, "EVIDENCE_RECORDED", "", ""],
            [10, "PROMOTION_APPROVED", "M0 -> M1", "shadow evidence reviewed"],
            [11, "FAMILY_KILL_SWITCH_ON", "", "incident 7"],
            [12, "FAMILY_DISABLED", "", "maintenance"],
            [13, "PROMOTION_REVOKED", "M1 -> M0", "back to the start"],
            [14, "PROMOTION_REJECTED", "stays M0", "not yet"],
        ]);
        assert.ok(rows.every((row) => row.actor === OPERATOR));
        assert.equal(formatTimestamp(rows[0].recorded_at), "2026-10-09 18:14:28 UTC");
        assert.equal(formatTimestamp(null), "n/a");
        assert.equal(rows[1].request_id, "fpr_252b0d74836901bc069d9f29");
        assert.equal(rows[0].note, "version daily-trend-rules-1, rule hash 7795ad79de96");
        assert.equal(rows[2].note, 'v2_benchmark_tag = true (docs://shadow-week-1)');
    });

    it("reads the recorded evidence and the pending request of a family", () => {
        const detail = parseFamilyDetail(answer);
        assert.deepEqual(detail.family.recorded_evidence.map((item) => [item.key, item.value, item.reference, item.actor_ref]), [
            ["section9_shadow_verified", true, "docs://shadow-week-1", OPERATOR],
            ["v2_benchmark_tag", true, "docs://shadow-week-1", OPERATOR],
        ]);
        const pending = parseFamilyDetail({
            family: family({ pending_request: { request_id: "fpr_1", from_stage: "M0", to_stage: "M1", requested_by: OPERATOR, requested_at: 1791569668745, reason: "start", evidence: {} } }),
            history: [],
        }).family.pending_request;
        assert.deepEqual(pending, { request_id: "fpr_1", from_stage: "M0", to_stage: "M1", requested_by: OPERATOR, requested_at: 1791569668745, reason: "start" });
    });

    it("refuses a history it cannot read rather than showing part of it", () => {
        assert.throws(() => parseFamilyDetail({ family: family() }), UnexpectedAnswer);
        assert.throws(() => parseFamilyDetail({ family: family(), history: [{ seq: 1 }] }), UnexpectedAnswer);
        assert.throws(() => parseFamilyDetail({ history: [] }), UnexpectedAnswer);
    });
});

describe("Family governance: the actions offered", () => {
    /** Every action kind the page can open, with the request it must send. */
    const CASES: Array<{ action: GovernanceAction; path: string; body: Record<string, unknown>; records: string }> = [
        { action: { kind: "register" }, path: "/api/admin/governance/families/register", body: { reason: "first registration" }, records: "FAMILY_REGISTERED" },
        {
            action: { kind: "evidence", familyId: "DAILY_TREND", key: " section9_shadow_verified ", reference: " docs://shadow-week-1 ", value: true },
            path: "/api/admin/governance/families/DAILY_TREND/evidence",
            body: { key: "section9_shadow_verified", reference: "docs://shadow-week-1", value: true, reason: "first registration" },
            records: "EVIDENCE_RECORDED",
        },
        {
            action: { kind: "request_promotion", familyId: "DAILY_TREND", fromStage: "M0", toStage: "M1" },
            path: "/api/admin/governance/families/DAILY_TREND/promotion-requests",
            body: { to_stage: "M1", reason: "first registration" },
            records: "PROMOTION_REQUESTED",
        },
        {
            action: { kind: "approve", familyId: "DAILY_TREND", requestId: "fpr_252b0d74836901bc069d9f29", fromStage: "M0", toStage: "M1" },
            path: "/api/admin/governance/promotion-requests/fpr_252b0d74836901bc069d9f29/approve",
            body: { reason: "first registration" },
            records: "PROMOTION_APPROVED",
        },
        {
            action: { kind: "reject", familyId: "DAILY_TREND", requestId: "fpr_252b0d74836901bc069d9f29", fromStage: "M0", toStage: "M1" },
            path: "/api/admin/governance/promotion-requests/fpr_252b0d74836901bc069d9f29/reject",
            body: { reason: "first registration" },
            records: "PROMOTION_REJECTED",
        },
        {
            action: { kind: "revoke", familyId: "DAILY_TREND", fromStage: "M3", toStage: "M0" },
            path: "/api/admin/governance/families/DAILY_TREND/revoke",
            body: { to_stage: "M0", reason: "first registration" },
            records: "PROMOTION_REVOKED",
        },
        {
            action: { kind: "revoke_certification", familyId: "DAILY_TREND", fromStage: "M3" },
            path: "/api/admin/governance/families/DAILY_TREND/revoke-certification",
            body: { reason: "first registration" },
            records: "CERTIFICATION_REVOKED",
        },
        {
            action: { kind: "family_kill_switch", familyId: "DAILY_TREND", enabled: true, positionPolicy: "MANAGE_TO_EXIT" },
            path: "/api/admin/governance/families/DAILY_TREND/kill-switch",
            body: { enabled: true, reason: "first registration" },
            records: "FAMILY_KILL_SWITCH_ON",
        },
        {
            action: { kind: "family_kill_switch", familyId: "DAILY_TREND", enabled: false, positionPolicy: null },
            path: "/api/admin/governance/families/DAILY_TREND/kill-switch",
            body: { enabled: false, reason: "first registration" },
            records: "FAMILY_KILL_SWITCH_OFF",
        },
        {
            action: { kind: "family_disable", familyId: "DAILY_TREND", disabled: true },
            path: "/api/admin/governance/families/DAILY_TREND/disable",
            body: { enabled: true, reason: "first registration" },
            records: "FAMILY_DISABLED",
        },
        {
            action: { kind: "family_disable", familyId: "DAILY_TREND", disabled: false },
            path: "/api/admin/governance/families/DAILY_TREND/disable",
            body: { enabled: false, reason: "first registration" },
            records: "FAMILY_ENABLED",
        },
        {
            action: { kind: "account_kill_switch", accountId: "brk_9f2c41aa07b1", enabled: true },
            path: "/api/admin/governance/accounts/brk_9f2c41aa07b1/kill-switch",
            body: { enabled: true, reason: "first registration" },
            records: "kill-switch control record for account brk_9f2c41aa07b1",
        },
    ];

    /** The write routes of the governance API (bot-backend family_governance.py), as the proxy serves them. */
    const API_WRITE_ROUTES = [
        /^\/api\/admin\/governance\/families\/register$/,
        /^\/api\/admin\/governance\/families\/[A-Za-z0-9_-]+\/(evidence|promotion-requests|revoke|revoke-certification|kill-switch|disable)$/,
        /^\/api\/admin\/governance\/promotion-requests\/[A-Za-z0-9_-]+\/(approve|reject)$/,
        /^\/api\/admin\/governance\/accounts\/[A-Za-z0-9_-]+\/kill-switch$/,
    ];

    it("sends exactly the request the confirmation shows, with the typed reason", () => {
        for (const { action, path, body, records } of CASES) {
            const description = describeAction(action, "  first registration  ");
            assert.equal(description.method, "POST");
            assert.equal(description.path, path, action.kind);
            assert.deepEqual(description.body, body, action.kind);
            assert.ok(description.records.join(" ").includes(records), `${action.kind}: ${description.records.join(" ")}`);
            assert.ok(description.effects.length > 0 && description.title.length > 0 && description.confirmLabel.length > 0);
        }
    });

    it("offers nothing the API does not: every request is one of its write routes", () => {
        for (const { action } of CASES) {
            const { path, body } = describeAction(action, "reviewed");
            assert.ok(API_WRITE_ROUTES.some((route) => route.test(path)), path);
            // The only fields ever sent. There is none for a certification, an order-submission flag, a
            // prerequisite, a mode or an actor: the engine takes those from the register and the session.
            assert.deepEqual(Object.keys(body).filter((key) => !["reason", "to_stage", "enabled", "key", "reference", "value"].includes(key)), [], action.kind);
            if ("to_stage" in body) assert.ok(["M0", "M1", "M2", "M3", "M4", "M5", "M6"].includes(String(body.to_stage)), action.kind);
        }
        assert.equal(new Set(CASES.map((item) => item.action.kind)).size, 10);
    });

    it("never offers or sends a stage above M6", () => {
        assert.equal(MAX_OPERABLE_STAGE, "M6");
        assert.deepEqual(["M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M10", "", "m1"].map(promotionTarget),
            ["M1", "M2", "M3", "M4", "M5", "M6", null, null, null, null, null, null, null]);
        for (const toStage of ["M7", "M8", "M9"]) {
            const request: GovernanceAction = { kind: "request_promotion", familyId: "DAILY_TREND", fromStage: "M6", toStage };
            const approve: GovernanceAction = { kind: "approve", familyId: "DAILY_TREND", requestId: "fpr_1", fromStage: "M6", toStage };
            assert.match(actionProblem(request) ?? "", /above M6/);
            assert.match(actionProblem(approve) ?? "", /above M6/);
            assert.throws(() => describeAction(request, "live now"), /above M6/);
            assert.throws(() => describeAction(approve, "live now"), /above M6/);
        }
    });

    it("requests only the next stage and revokes only downwards", () => {
        const skipped: GovernanceAction = { kind: "request_promotion", familyId: "DAILY_TREND", fromStage: "M0", toStage: "M6" };
        assert.match(actionProblem(skipped) ?? "", /only stage that can be requested from M0 is M1/);
        assert.throws(() => describeAction(skipped, "skip ahead"));
        assert.equal(actionProblem({ kind: "request_promotion", familyId: "DAILY_TREND", fromStage: "M5", toStage: "M6" }), null);

        assert.deepEqual(revocationTargets("M0"), ["M0"]);
        assert.deepEqual(revocationTargets("M3"), ["M0", "M1", "M2", "M3"]);
        assert.deepEqual(revocationTargets("unknown"), ["M0"]);
        assert.match(actionProblem({ kind: "revoke", familyId: "DAILY_TREND", fromStage: "M1", toStage: "M4" }) ?? "", /not a revocation/);
        assert.match(actionProblem({ kind: "revoke", familyId: "DAILY_TREND", fromStage: "M1", toStage: "LIVE" }) ?? "", /not a governance stage/);
        assert.equal(actionProblem({ kind: "revoke", familyId: "DAILY_TREND", fromStage: "M1", toStage: "M0" }), null);
    });

    it("builds no request for an identifier that is not plain", () => {
        for (const good of ["DAILY_TREND", "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", "fpr_252b0d74836901bc069d9f29", "brk_9f2c41aa07b1", "acct-1", "a", "A".repeat(128)]) {
            assert.equal(isPlainIdentifier(good), true, good);
        }
        const hostile = ["", "..", ".", "a/b", "../emergency/flatten", "..%2f..%2femergency%2fflatten", "a%2fb", "a b", "a\nb", "DAILY_TREND\n",
            "a?b", "a#b", "a.b", "-a", "_a", "A".repeat(129), "ｆａｍ"];
        for (const bad of hostile) {
            assert.equal(isPlainIdentifier(bad), false, JSON.stringify(bad));
            assert.throws(() => describeAction({ kind: "family_kill_switch", familyId: bad, enabled: true, positionPolicy: null }, "incident 7"), /plain identifier/);
            assert.throws(() => describeAction({ kind: "approve", familyId: "DAILY_TREND", requestId: bad, fromStage: "M0", toStage: "M1" }, "reviewed"), /plain identifier/);
            assert.throws(() => describeAction({ kind: "account_kill_switch", accountId: bad, enabled: true }, "customer asked"), /plain identifier/);
        }
        for (const notText of [null, undefined, 7, {}, ["a"]]) assert.equal(isPlainIdentifier(notText), false);
    });

    it("needs a key and a reference for evidence, and a real reason for everything", () => {
        assert.match(actionProblem({ kind: "evidence", familyId: "DAILY_TREND", key: "  ", reference: "docs://x", value: true }) ?? "", /key is required/);
        assert.match(actionProblem({ kind: "evidence", familyId: "DAILY_TREND", key: "k", reference: " ", value: true }) ?? "", /reference is required/);
        assert.match(actionProblem({ kind: "evidence", familyId: "DAILY_TREND", key: "k".repeat(101), reference: "r", value: true }) ?? "", /key is required/);
        assert.match(actionProblem({ kind: "evidence", familyId: "DAILY_TREND", key: "k", reference: "r".repeat(501), value: true }) ?? "", /reference is required/);
        assert.deepEqual(["", "  ", "ab", " ab ", "abc", "a".repeat(500), "a".repeat(501)].map(reasonReady), [false, false, false, false, true, true, false]);
    });

    it("says what each action records and where the reason is kept", () => {
        const approve = describeAction({ kind: "approve", familyId: "DAILY_TREND", requestId: "fpr_1", fromStage: "M5", toStage: "M6" }, "reviewed");
        assert.ok(approve.records[0].includes("M5 -> M6") && approve.records[0].includes("naming you as the reviewer"));
        assert.ok(approve.records[1].includes("no governance record is written"));
        assert.ok(approve.effects[0].includes("DEMO accounts") && approve.effects[0].includes("research register"));
        assert.ok(approve.effects[1].includes("a reason and nothing else"));

        const shadow = describeAction({ kind: "approve", familyId: "DAILY_TREND", requestId: "fpr_1", fromStage: "M0", toStage: "M1" }, "reviewed");
        assert.ok(shadow.effects[0].includes("M1 grants no order authority"));

        const request = describeAction({ kind: "request_promotion", familyId: "DAILY_TREND", fromStage: "M0", toStage: "M1" }, "start");
        assert.ok(request.effects[0].includes("A request changes nothing"));

        const kill = describeAction({ kind: "family_kill_switch", familyId: "DAILY_TREND", enabled: true, positionPolicy: "MANAGE_TO_EXIT" }, "incident 7");
        assert.ok(kill.effects.join(" ").includes("MANAGE_TO_EXIT") && kill.tone === "danger");

        // the two calls whose governance record has no reason field say so
        for (const action of [{ kind: "register" }, { kind: "evidence", familyId: "DAILY_TREND", key: "k", reference: "r", value: true }] as GovernanceAction[]) {
            assert.ok(describeAction(action, "why").reasonNote.includes("not in the governance record"), action.kind);
        }
        assert.ok(request.reasonNote.includes("written into the governance record"));
        const withdrawn = describeAction({ kind: "evidence", familyId: "DAILY_TREND", key: "k", reference: "r", value: false }, "withdrawn");
        assert.ok(withdrawn.effects[0].includes("withdraws this key") && withdrawn.body.value === false);
    });
});

describe("Family governance: answers and refusals", () => {
    const request: GovernanceAction = { kind: "request_promotion", familyId: "DAILY_TREND", fromStage: "M0", toStage: "M1" };

    it("reports an action as recorded only from the record the server returned", () => {
        const recorded = readActionAnswer(request, { event: event(7, "PROMOTION_REQUESTED", { request_id: "fpr_252b", from_stage: "M0", to_stage: "M1" }) });
        assert.equal(recorded.summary, "Recorded: PROMOTION_REQUESTED for DAILY_TREND (M0 -> M1 requested) request fpr_252b as record 7.");
        const approved = readActionAnswer({ kind: "approve", familyId: "DAILY_TREND", requestId: "fpr_252b", fromStage: "M0", toStage: "M1" },
            { event: event(10, "PROMOTION_APPROVED", { request_id: "fpr_252b", from_stage: "M0", to_stage: "M1" }) });
        assert.equal(approved.summary, "Recorded: PROMOTION_APPROVED for DAILY_TREND (M0 -> M1) request fpr_252b as record 10.");

        for (const empty of [{}, { event: null }, { event: {} }, { ok: true }, null, "OK", []]) {
            assert.throws(() => readActionAnswer(request, empty), UnexpectedAnswer);
        }
    });

    it("reads registration and the account kill switch from what the server confirmed", () => {
        const first = readActionAnswer({ kind: "register" }, { events: [event(1, "FAMILY_REGISTERED"), { family_id: "X", event_type: "FAMILY_REGISTERED", unchanged: true }] });
        assert.equal(first.summary, "Recorded: FAMILY_REGISTERED for DAILY_TREND; 1 already on record and unchanged.");
        const again = readActionAnswer({ kind: "register" }, { events: [{ family_id: "X", unchanged: true }, { family_id: "Y", unchanged: true }] });
        assert.equal(again.summary, "Nothing new to record: all 2 code-defined families are already on record and unchanged.");
        assert.throws(() => readActionAnswer({ kind: "register" }, { event: event(1, "FAMILY_REGISTERED") }), UnexpectedAnswer);

        const on: GovernanceAction = { kind: "account_kill_switch", accountId: "acct-1", enabled: true };
        const status = { enabled: true, reason: "customer asked", scope: "acct-1", set_at_ms: 1791569669906, set_by: OPERATOR };
        assert.equal(readActionAnswer(on, { account_kill_switch: status }).summary, "Confirmed by the server: the kill switch of account acct-1 is ON.");
        // asked ON, reported OFF: not shown as done
        assert.throws(() => readActionAnswer(on, { account_kill_switch: { ...status, enabled: false } }), UnexpectedAnswer);
        assert.throws(() => readActionAnswer(on, { event: event(1, "X") }), UnexpectedAnswer);
    });

    it("keeps the API's refusal reason and detail verbatim", () => {
        const refusals: Array<[number, Record<string, unknown>]> = [
            [409, { detail: "PREREQUISITES_NOT_MET: EVIDENCE_MISSING:v2_benchmark_tag, EVIDENCE_MISSING:section9_shadow_verified", reason: "PREREQUISITES_NOT_MET" }],
            [409, { detail: "STAGE_SKIPPED: M0 -> M6: the next stage is M1", reason: "STAGE_SKIPPED" }],
            [409, { detail: "STAGE_NOT_OPERABLE_HERE: M7 is beyond M6; live promotion is a separate decision", reason: "STAGE_NOT_OPERABLE_HERE" }],
            [404, { detail: "FAMILY_NOT_REGISTERED: DAILY_TREND", reason: "FAMILY_NOT_REGISTERED" }],
            [404, { detail: "REQUEST_NOT_FOUND: unknown", reason: "REQUEST_NOT_FOUND" }],
            [404, { detail: "acct-1", reason: "ACCOUNT_UNKNOWN" }],
            [400, { detail: "the platform kill switch is on the emergency router", reason: "SCOPE_INVALID" }],
            [400, { detail: "family_id is not a plain identifier (letters, digits, '_' and '-'; at most 128 characters). The request was NOT sent and nothing was changed.", reason: "IDENTIFIER_INVALID" }],
            [502, { detail: "The engine is unreachable (ConnectError): the request was NOT sent and nothing was done. Nothing was changed by this call.", reason: "UPSTREAM_UNREACHABLE" }],
            [503, { detail: "the change could not be persisted", reason: "KILL_SWITCH_NOT_PERSISTED" }],
        ];
        for (const [status, body] of refusals) {
            const error = toGovernanceError(httpError(status, body), "Approve");
            assert.ok(error instanceof GovernanceApiError);
            assert.equal(error.status, status);
            assert.equal(error.reason, body.reason);
            assert.equal(error.detail, body.detail);
            assert.equal(error.outcomeUnknown, false, String(body.reason));
            assert.equal(error.message, `Approve was refused (HTTP ${status}): ${String(body.detail)}`);
        }
    });

    it("reads a validation error and an error without a reason", () => {
        const invalid = toGovernanceError(httpError(422, {
            detail: [{ type: "string_too_short", loc: ["body", "reason"], msg: "String should have at least 3 characters", input: "x", ctx: { min_length: 3 } }],
        }), "Turn ON");
        assert.equal(invalid.detail, "reason: String should have at least 3 characters");
        assert.equal(invalid.reason, null);
        assert.equal(invalid.outcomeUnknown, false);

        const forbidden = toGovernanceError(httpError(403, { detail: "Emergency controls require the admin emergency service credential" }), "Approve");
        assert.equal(forbidden.detail, "Emergency controls require the admin emergency service credential");

        const noRoute = toGovernanceError(httpError(404, { detail: "Not Found" }), "Loading the family registry");
        assert.ok(noRoute.message.includes("is the governance API reachable at /api/admin/governance"), noRoute.message);
        const unknownFamily = toGovernanceError(httpError(404, { detail: "NOPE", reason: "FAMILY_UNKNOWN" }), "Loading family NOPE");
        assert.equal(unknownFamily.message, "Loading family NOPE was refused (HTTP 404): NOPE");
    });

    it("never reads 'no definite answer' as done or as refused", () => {
        const timeout = toGovernanceError(httpError(504, {
            detail: "The bot service did not answer within 30s. The outcome is UNKNOWN: the request may still be running. Reload the family registry and its history before doing anything else.",
            reason: "UPSTREAM_TIMEOUT",
        }), "Approve");
        assert.equal(timeout.outcomeUnknown, true);
        assert.equal(timeout.reason, "UPSTREAM_TIMEOUT");
        assert.ok(timeout.message.startsWith("Approve: no definite answer (HTTP 504)"));

        for (const status of [500, 408]) assert.equal(toGovernanceError(httpError(status, "Internal Server Error"), "Approve").outcomeUnknown, true);

        const noResponse = toGovernanceError(new Error("Request timed out. Please check that the backend is running and try again."), "Approve");
        assert.equal(noResponse.outcomeUnknown, true);
        assert.equal(noResponse.status, null);
        assert.ok(noResponse.message.includes("no response from the server"));

        // a 2xx without the record: unknown, not done
        const odd = toGovernanceError(new UnexpectedAnswer("no governance record"), "Approve");
        assert.equal(odd.outcomeUnknown, true);
        assert.ok(odd.message.includes("unexpected response"));

        const same = new GovernanceApiError("x", 409, "STAGE_SKIPPED", "d", false);
        assert.equal(toGovernanceError(same, "Approve"), same);
    });
});
