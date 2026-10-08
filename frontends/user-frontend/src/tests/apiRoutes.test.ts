/**
 * Step 1.7 -- the portal talks to the user backend only, on paths it serves.
 *
 * Source-level guards (the unit runner has no DOM): every engine call in the
 * API client goes through the user backend under /api/v1, the CATI panel does
 * not address the engine directly, and the SSE hook uses the same base URL
 * variable as every other call.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

test("bot-instance engine status is requested under /api/v1", () => {
    const client = src("api/client.ts");
    assert.equal(client.includes("/api/bot-instances/${instanceId}/engine-status"), false);
    assert.ok(client.includes("/api/v1/bot-instances/${instanceId}/engine-status"));
});

test("no client function targets a monitoring route the backends do not serve", () => {
    const client = src("api/client.ts");
    for (const dead of ["/api/v1/monitoring/traces", "/api/v1/monitoring/trace/", "/api/v1/monitoring/violations"]) {
        assert.equal(client.includes(dead), false, dead);
    }
});

test("the CATI panel reaches the engine through the user backend only", () => {
    const panel = src("components/Dashboard/CatiTrading.tsx");
    assert.equal(panel.includes("VITE_CATI_API_BASE"), false);
    assert.equal(panel.includes("9000"), false);
    assert.ok(panel.includes("/api/v1/cati/runtime/status"));
});

test("the analytics stream uses the shared API base variable", () => {
    const hook = src("hooks/useAnalyticsStream.ts");
    assert.equal(hook.includes("VITE_API_URL"), false);
    assert.ok(hook.includes("VITE_API_BASE"));
});

test("no source file addresses the engine port directly", () => {
    for (const file of ["api/client.ts", "api/http.ts", "components/Dashboard/CatiTrading.tsx", "hooks/useAnalyticsStream.ts"]) {
        assert.equal(src(file).includes("localhost:9000"), false, file);
    }
});
