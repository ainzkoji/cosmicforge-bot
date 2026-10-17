/**
 * Section H Step 8.5: logic of the admin Operations and Support desk pages (src/utils/operations.ts itself).
 *
 *   node --experimental-strip-types src/tests/operations.test.ts
 *
 * Fixtures have the shape of the user-backend /api/admin/operations answers; no person here is real.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
    OWNER_REQUIRED_TEXT, alertActions, drillHeadline, formatMs, operationsError, ownerLine, publicTextProblems,
    replyProblems, rotationOwnerText, sortAlerts, ticketActions, unassignedRotations, validateIncident,
    validateMember,
} from "../utils/operations.ts";
import type { Alert, AdminTicket, Oncall } from "../utils/operations.ts";

const alert = (over: Partial<Alert>): Alert => ({
    alert_id: "a", category: "QUEUE_BACKLOG", severity: "HIGH", status: "OPEN", title: "t", rotation_key: "platform-infrastructure",
    escalation_level: 0, owner_required: false, occurrences: 1, first_seen_at: 1, last_seen_at: 1, acknowledged_by: null,
    resolved_by: null, scope: "PLATFORM", ...over,
});

const EMPTY_ONCALL: Oncall = {
    rotations: [
        { rotation_key: "operations-owner", description: "d", status: "OPERATIONS_OWNER_REQUIRED", members: [] },
        { rotation_key: "trading-operations", description: "d", status: "OPERATIONS_OWNER_REQUIRED", members: [] },
    ],
    findings: [{ code: "OPERATIONS_OWNER_REQUIRED", severity: "CRITICAL", message: "m", rotation_key: "operations-owner" }],
    owner_assigned: false, status: "OPERATIONS_OWNER_REQUIRED",
};

describe("on-call ownership", () => {
    it("shows OPERATIONS OWNER REQUIRED until an admin assigns a person, never a default", () => {
        assert.equal(ownerLine(null), OWNER_REQUIRED_TEXT);
        assert.equal(ownerLine(EMPTY_ONCALL), OWNER_REQUIRED_TEXT);
        assert.equal(rotationOwnerText(EMPTY_ONCALL.rotations[1]), OWNER_REQUIRED_TEXT);
        assert.deepEqual(unassignedRotations(EMPTY_ONCALL), ["operations-owner", "trading-operations"]);
        const staffed: Oncall = { ...EMPTY_ONCALL, rotations: [{ ...EMPTY_ONCALL.rotations[0], status: "ASSIGNED", members: [
            { member_id: "m", rotation_key: "operations-owner", person_name: "Named Owner", level: 0, level_name: "PRIMARY", contact_channel: "EMAIL" }] }] };
        assert.equal(ownerLine(staffed), "Operations owner: Named Owner");
    });
    it("member form requires a named person and contact", () => {
        const rotations = ["operations-owner"];
        assert.deepEqual(validateMember({ rotation_key: "operations-owner", level: 0, person_name: "A", contact_channel: "EMAIL",
            contact_address: "a@example.test" }, rotations), []);
        assert.equal(validateMember({ rotation_key: "x", level: 5, person_name: " ", contact_channel: "FAX", contact_address: "" },
            rotations).length, 5);
    });
});

describe("alerts", () => {
    it("sorts open before acknowledged, then by severity", () => {
        const sorted = sortAlerts([alert({ alert_id: "1", status: "ACKNOWLEDGED", severity: "CRITICAL" }),
            alert({ alert_id: "2", severity: "LOW" }), alert({ alert_id: "3", severity: "CRITICAL" })]);
        assert.deepEqual(sorted.map((a) => a.alert_id), ["3", "2", "1"]);
    });
    it("acknowledge only when open; resolve until resolved", () => {
        assert.deepEqual(alertActions(alert({})), { acknowledge: true, resolve: true });
        assert.deepEqual(alertActions(alert({ status: "ACKNOWLEDGED" })), { acknowledge: false, resolve: true });
        assert.deepEqual(alertActions(alert({ status: "RESOLVED" })), { acknowledge: false, resolve: false });
    });
    it("errors show the backend code", () => {
        assert.equal(operationsError({ response: { data: { detail: { error_code: "OPERATIONS_SCHEMA_MISSING", message: "m" } } } }),
            "OPERATIONS_SCHEMA_MISSING: m");
        assert.equal(operationsError({ response: { data: { detail: { error_code: "PUBLIC_TEXT_REFUSED", problems: ["IP_ADDRESS"] } } } }),
            "PUBLIC_TEXT_REFUSED (IP_ADDRESS)");
    });
});

describe("status incidents are public text", () => {
    it("refuses internal details before sending", () => {
        assert.deepEqual(publicTextProblems("Orders are delayed."), []);
        assert.ok(publicTextProblems("db at 10.0.0.4").includes("IP_ADDRESS"));
        assert.ok(publicTextProblems("db1.internal restarted").includes("INTERNAL_HOSTNAME"));
        assert.ok(publicTextProblems("account_id: 42 affected").includes("ACCOUNT_IDENTIFIER"));
        assert.ok(publicTextProblems("mail ops@example.test").includes("EMAIL_ADDRESS"));
        assert.deepEqual(validateIncident({ title: "Delays", components: ["TRADING_ENGINE"], impact: "DEGRADED", message: "We are investigating." }), []);
        assert.equal(validateIncident({ title: "", components: ["NOPE"], impact: "BAD", message: "" }).length, 4);
    });
});

describe("backup drill", () => {
    it("headline and units", () => {
        assert.equal(drillHeadline(null), "Drill status unavailable");
        assert.equal(drillHeadline({ status: "NO_DRILL_RECORDED", rpo_target_minutes: "UNSET", rto_target_minutes: "UNSET" }),
            "No restore drill has been recorded");
        assert.match(drillHeadline({ status: "PASSED", age_hours: 2, rpo_target_minutes: "UNSET", rto_target_minutes: "UNSET" }), /PASSED \(2 h ago\)/);
        assert.match(drillHeadline({ status: "FAILED", checks_failed: ["integrity_check_ok"], rpo_target_minutes: "UNSET", rto_target_minutes: "UNSET" }), /integrity_check_ok/);
        assert.equal(formatMs(250), "250 ms");
        assert.equal(formatMs(null), "n/a");
    });
});

describe("support desk", () => {
    it("closed tickets accept no action; replies never carry secrets", () => {
        const t = { status: "CLOSED" } as AdminTicket;
        assert.deepEqual(ticketActions(t), { reply: false, assign: false, close: false });
        assert.deepEqual(ticketActions({ ...t, status: "OPEN" }), { reply: true, assign: true, close: true });
        assert.deepEqual(replyProblems("Please enable trading permission on the key."), []);
        assert.equal(replyProblems("send your api key=XyZ1234567890abc").length, 1);
        assert.equal(replyProblems("vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A").length, 1);
    });
});

describe("routes and navigation", () => {
    const src = (p: string) => readFileSync(join(process.cwd(), "src", p), "utf8");
    it("operations and support desk are admin-protected routes with sidebar entries", () => {
        const app = src("App.tsx");
        const protectedStart = app.indexOf("<AdminProtectedRoute />");
        assert.ok(app.indexOf('path="/admin/operations"') > protectedStart);
        assert.ok(app.indexOf('path="/admin/support"') > protectedStart);
        const sidebar = src("components/admin/layout/AdminSidebar.tsx");
        assert.match(sidebar, /path: "\/admin\/operations"/);
        assert.match(sidebar, /path: "\/admin\/support"/);
    });
});
