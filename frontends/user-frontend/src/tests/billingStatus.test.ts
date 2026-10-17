import test from "node:test";
import assert from "node:assert/strict";
import { deriveNotices, deriveSubscriptionView, isPaidPlan } from "../utils/billingStatus.ts";

const fmt = (value: string | null | undefined) => (value ? value.slice(0, 10) : "—");
const pro = { id: "plan_pro", name: "Nebula Voyager", price: 29, interval: "month" };

test("free plan: neutral, nothing to cancel, no notices", () => {
    const view = deriveSubscriptionView({ plan: { id: "plan_free", name: "Star Gazer" }, status: "active" }, fmt);
    assert.equal(view.isPaid, false);
    assert.equal(view.statusLabel, "Free plan");
    assert.equal(view.canCancel, false);
    assert.equal(view.canResume, false);
    assert.deepEqual(view.notices, []);
    assert.equal(view.renewalLabel, "No billing scheduled.");
});

test("active paid plan shows its renewal date", () => {
    const view = deriveSubscriptionView({ plan: pro, status: "active", current_period_end: "2026-11-17T00:00:00Z" }, fmt);
    assert.equal(view.statusLabel, "Active");
    assert.equal(view.tone, "ok");
    assert.equal(view.renewalLabel, "Renews on 2026-11-17.");
    assert.equal(view.canCancel, true);
});

test("past_due shows the payment-failure notice and the grace end", () => {
    const view = deriveSubscriptionView({
        plan: pro, status: "past_due", grace_period_end: "2026-10-24T00:00:00Z", current_period_end: "2026-10-17T00:00:00Z",
    }, fmt);
    assert.equal(view.statusLabel, "Payment failed");
    assert.equal(view.tone, "error");
    assert.match(view.renewalLabel, /2026-10-24/);
    assert.deepEqual(view.notices.map((n) => n.code), ["PAYMENT_FAILED"]);
    assert.equal(view.notices[0].until, "2026-10-24T00:00:00Z");
    assert.equal(view.canCancel, true);
});

test("scheduled cancellation: resume offered, cancel not", () => {
    const view = deriveSubscriptionView({
        plan: pro, status: "active", cancel_at_period_end: true, current_period_end: "2026-11-01T00:00:00Z",
    }, fmt);
    assert.equal(view.statusLabel, "Cancels at period end");
    assert.equal(view.canCancel, false);
    assert.equal(view.canResume, true);
    assert.deepEqual(view.notices.map((n) => n.code), ["CANCELLATION_SCHEDULED"]);
    assert.equal(view.renewalLabel, "Access continues until 2026-11-01.");
});

test("a billing hold overrides everything and blocks cancel/resume", () => {
    const view = deriveSubscriptionView({
        plan: { id: "plan_free" }, status: "active", billing_hold: true, cancel_at_period_end: true,
        notices: [{ code: "PAYMENT_UNDER_REVIEW", severity: "error", message: "Under review (server text)" }],
    }, fmt);
    assert.equal(view.statusLabel, "Under review");
    assert.equal(view.tone, "error");
    assert.equal(view.canCancel, false);
    assert.equal(view.canResume, false);
    assert.deepEqual(view.notices, [{ code: "PAYMENT_UNDER_REVIEW", tone: "error", message: "Under review (server text)", until: null }]);
});

test("server notices are kept, de-duplicated with implied ones, and ordered by severity", () => {
    const notices = deriveNotices({
        plan: pro, status: "past_due", cancel_at_period_end: true, billing_hold: true,
        notices: [
            { code: "CANCELLATION_SCHEDULED", severity: "info", message: "ends soon" },
            { code: "PAYMENT_FAILED", severity: "error", message: "card declined", until: "2026-10-24" },
        ],
    });
    assert.deepEqual(notices.map((n) => n.code), ["PAYMENT_UNDER_REVIEW", "PAYMENT_FAILED", "CANCELLATION_SCHEDULED"]);
    assert.equal(notices[1].message, "card declined");
    assert.equal(notices[2].tone, "warning");
});

test("unknown notice codes are shown with their server message; malformed ones dropped", () => {
    const notices = deriveNotices({
        plan: pro, status: "active",
        notices: [{ code: "SOMETHING_NEW", severity: "warning", message: "new thing" }, { code: "" }, null as never],
    });
    assert.deepEqual(notices, [{ code: "SOMETHING_NEW", tone: "warning", message: "new thing", until: null }]);
});

test("portal link only when the server says a provider customer exists", () => {
    assert.equal(deriveSubscriptionView({ plan: pro, status: "active" }).canOpenPortal, false);
    assert.equal(deriveSubscriptionView({ plan: pro, status: "active", portal_available: true }).canOpenPortal, true);
});

test("nothing client-side makes a plan paid", () => {
    assert.equal(isPaidPlan(null), false);
    assert.equal(isPaidPlan({}), false);
    assert.equal(isPaidPlan({ plan: { id: "" } }), false);
    assert.equal(deriveSubscriptionView(undefined).planId, "plan_free");
});
