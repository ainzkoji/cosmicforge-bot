/** Step 1.9 -- the deployment form builds the shared contract and shows only what the backend said. */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
    DEPLOYMENT_SCHEMA_VERSION, ageLabel, botStatusView, buildDeploymentRequest, decimalText, formFingerprint, formatPct,
    deploymentPrefillPath, formatUsdt, isStale, newRequestId, parseRefusal, protectionView, riskLevelFromAppetite, severityTone,
    typicalPositionView, validateForm,
} from "../lib/deployment.ts";
import type { DeploymentForm } from "../lib/deployment.ts";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

const form = (over: Partial<DeploymentForm> = {}): DeploymentForm => ({
    brokerAccountId: "acct-1", riskLevel: "balanced", budgetType: "fixed_amount", budgetValue: "1000.00",
    maxPositionUsdt: "", dailyLossLimitPct: "", riskAcknowledged: true, ...over,
});

test("the request is the shared contract with decimal strings and nothing the backend derives", () => {
    const body = buildDeploymentRequest(form({ maxPositionUsdt: "200", dailyLossLimitPct: "1.5" }), "dep-abc12345");
    assert.deepEqual(body, {
        schema_version: DEPLOYMENT_SCHEMA_VERSION, broker_account_id: "acct-1", budget: { type: "fixed_amount", value: "1000.00" },
        risk_level: "balanced", advanced: { max_position_usdt: "200", daily_loss_limit_pct: "1.5", symbols: [] },
        risk_acknowledged: true, request_id: "dep-abc12345",
    });
    for (const forbidden of ["environment", "leverage", "execution_mode", "risk_per_trade", "api_key"]) {
        assert.equal(JSON.stringify(body).includes(forbidden), false, forbidden);
    }
});

test("empty advanced fields are null, not zero", () => {
    const body = buildDeploymentRequest(form(), "dep-abc12345");
    assert.equal(body.advanced.max_position_usdt, null);
    assert.equal(body.advanced.daily_loss_limit_pct, null);
});

test("amounts are validated as plain positive decimals", () => {
    assert.equal(decimalText(" 1,250.50 "), "1250.50");
    for (const bad of ["", "0", "-5", "1e9", "abc", "1.2.3", "NaN", "Infinity"]) assert.equal(decimalText(bad), null, bad);
    assert.match(validateForm(form({ brokerAccountId: "" })) ?? "", /account/);
    assert.match(validateForm(form({ budgetValue: "0" })) ?? "", /budget/i);
    assert.match(validateForm(form({ budgetType: "percent_balance", budgetValue: "150" })) ?? "", /100/);
    assert.match(validateForm(form({ maxPositionUsdt: "-1" })) ?? "", /position/i);
    assert.match(validateForm(form({ dailyLossLimitPct: "100" })) ?? "", /loss/i);
    assert.equal(validateForm(form({ budgetType: "percent_balance", budgetValue: "25" })), null);
    assert.throws(() => buildDeploymentRequest(form({ budgetValue: "x" }), "dep-abc12345"));
});

test("a request id is unique per intent and stable while the intent is unchanged", () => {
    const a = newRequestId(), b = newRequestId();
    assert.notEqual(a, b);
    assert.ok(a.length >= 8 && a.length <= 128);
    assert.equal(newRequestId(() => "fixed"), "dep-fixed");
    assert.equal(formFingerprint(form()), formFingerprint(form({ riskAcknowledged: false })));     // ticking the box is not a new intent
    assert.notEqual(formFingerprint(form()), formFingerprint(form({ budgetValue: "2000" })));
    assert.notEqual(formFingerprint(form()), formFingerprint(form({ riskLevel: "aggressive" })));
});

test("a refusal shows the backend's blockers", () => {
    const refused = parseRefusal({ detail: { can_deploy: false, blockers: [
        { code: "BUDGET_TOO_SMALL_FOR_LEVEL", message: "The budget is too small.", action: "Increase it.", minimum_deployable_budget: "250" }] } });
    assert.equal(refused.blockers[0].code, "BUDGET_TOO_SMALL_FOR_LEVEL");
    assert.equal(refused.preview?.can_deploy, false);
    assert.equal(refused.message, "The budget is too small.");
    assert.equal(parseRefusal({ detail: { blockers: [{ code: "ACCOUNT_NOT_CONNECTED", message: "" }] } }).message, "ACCOUNT_NOT_CONNECTED");
    assert.equal(parseRefusal({ detail: "Not authenticated" }).message, "Not authenticated");
    assert.equal(parseRefusal({ detail: [{ msg: "field required" }, { msg: "bad value" }] }).message, "field required; bad value");
    assert.equal(parseRefusal(null).message, "The request could not be completed.");
});

test("money is shown as stated and never rounded up", () => {
    assert.equal(formatUsdt("4.999"), "4.99 USDT");
    assert.equal(formatUsdt("1234.5"), "1,234.50 USDT");
    assert.equal(formatUsdt(null), "—");
    assert.equal(formatUsdt("not-a-number"), "—");
    assert.equal(formatPct("0.50"), "0.5 %");
    assert.equal(formatPct(undefined), "—");
});

test("a missing or old observation is stale, never current", () => {
    const now = 1_000_000_000;
    assert.equal(isStale(null, now), true);
    assert.equal(isStale(undefined, now), true);
    assert.equal(isStale(now - 30_000, now), false);
    assert.equal(isStale(now - 121_000, now), true);
    assert.equal(ageLabel(null, now), "never");
    assert.equal(ageLabel(now - 30_000, now), "30s ago");
    assert.equal(ageLabel(now - 600_000, now), "10 min ago");
});

test("statuses, severities and protection states have honest labels", () => {
    assert.equal(botStatusView("running").label, "Running");
    assert.equal(botStatusView("deploying").tone, "info");
    assert.equal(botStatusView("paused").tone, "attention");
    assert.equal(botStatusView(undefined).label, "Unknown");
    assert.equal(severityTone("critical"), "critical");
    assert.equal(severityTone("something-new"), "muted");
    assert.equal(protectionView("CONFIRMED").tone, "ok");
    assert.equal(protectionView("ABSENT").tone, "critical");
    assert.equal(protectionView("UNKNOWN").label, "Could not be verified");       // unknown is never shown as protected
    assert.notEqual(protectionView(undefined).tone, "ok");
});

test("onboarding risk appetite maps to a profile and unknown answers stay conservative", () => {
    assert.equal(riskLevelFromAppetite("aggressive"), "aggressive");
    assert.equal(riskLevelFromAppetite("Moderate"), "balanced");
    assert.equal(riskLevelFromAppetite("medium"), "balanced");
    assert.equal(riskLevelFromAppetite("conservative"), "conservative");
    assert.equal(riskLevelFromAppetite(""), "conservative");
    assert.equal(riskLevelFromAppetite(null), "conservative");
});

test("onboarding hands over a pre-filled deployment link and nothing else", () => {
    assert.equal(deploymentPrefillPath({ risk_level: "balanced", budget_type: "fixed_amount", budget_value: "1000" }),
        "/dashboard/auto-pilot?risk=balanced&budget=1000");
    assert.equal(deploymentPrefillPath({ risk_level: "high", budget_type: null, budget_value: null }), "/dashboard/auto-pilot?risk=aggressive");
    assert.equal(deploymentPrefillPath({ risk_level: "low", budget_type: "percent_balance", budget_value: "25" }), "/dashboard/auto-pilot?risk=conservative");
    assert.equal(deploymentPrefillPath({ risk_level: "balanced", budget_type: "fixed_amount", budget_value: "1e9" }), "/dashboard/auto-pilot?risk=balanced");
    assert.equal(deploymentPrefillPath(null), "/dashboard/auto-pilot");
});

test("the wizard submits the step names the service accepts and never deploys", () => {
    const wizard = src("pages/OnboardingWizard.tsx");
    for (const step of ["welcome", "experience_level", "risk_tolerance", "strategy_preference", "capital_allocation", "summary"]) {
        assert.ok(wizard.includes(`'${step}'`), step);
    }
    for (const file of ["pages/OnboardingWizard.tsx", "components/onboarding/SummaryStep.tsx", "api/onboarding.ts"]) {
        const text = src(file);
        assert.equal(/deployBot|deployAutoPilot|auto-pilot\/deploy/.test(text), false, file);
    }
    assert.ok(wizard.includes("deploymentPrefillPath"));
});

test("the deployment screen has no leverage or environment control and goes through the shared client", () => {
    const screen = src("components/AutoPilot/CatiDeployment.tsx");
    assert.equal(/<input[^>]*leverage/i.test(screen), false);
    assert.equal(/setLeverage|setEnvironment|setMode\(/.test(screen), false);
    assert.ok(screen.includes("api.previewDeployment") && screen.includes("api.deployBot"));
    assert.ok(screen.includes("risk_acknowledged") || screen.includes("riskAcknowledged"));
    const client = src("api/client.ts");
    for (const path of ["/api/v1/auto-pilot/preview", "/api/v1/auto-pilot/bots", "/api/v1/cati/bots/${botId}/status",
        "/api/v1/cati/bots/${botId}/positions", "/api/v1/cati/bots/${botId}/trades", "/api/v1/cati/bots/${botId}/summary",
        "/api/v1/cati/bots/${botId}/equity", "/api/v1/cati/bots/${botId}/events"]) {
        assert.ok(client.includes(path), path);
    }
});

test("a typical position is a figure only when the engine approved one", () => {
    const base = { schema_version: "v", request_id: "r", broker_account_id: "a", evaluated_at: 1, blockers: [], requirements: [], can_deploy: true };
    const sized = (approved: boolean, reason: string | null) =>
        ({ ...base, typical_position: { typical: { notional_usdt: "62.5", margin_usdt: "62.5", leverage: "1", approved, reason } } });
    assert.equal(typicalPositionView(sized(true, null)).value, "62.50 USDT");
    assert.equal(typicalPositionView(sized(false, "RISK_SIZE_BELOW_EXCHANGE_MINIMUM")).value, "below exchange minimum");
    // any other refusal is not an exchange-minimum problem and is not described as one
    const invalid = typicalPositionView(sized(false, "RISK_SIZE_STOP_DISTANCE_INVALID"));
    assert.equal(invalid.value, "not estimated");
    assert.ok(invalid.hint.includes("RISK_SIZE_STOP_DISTANCE_INVALID"));
    assert.equal(typicalPositionView({ ...base, typical_position: null }).value, "not estimated");
});

