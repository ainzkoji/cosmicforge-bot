/**
 * Section H 8.4 -- the portal keeps no token in browser storage.
 *
 * Pure helpers (src/lib/sessionAuth.ts) plus source-level guards: no file
 * writes a token to localStorage, every call sends the session cookies and
 * the SSE stream no longer puts a token in its URL.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
    CSRF_HEADER,
    LEGACY_TOKEN_KEYS,
    isPlaceholderAuthorization,
    isUnsafeMethod,
    purgeLegacyTokens,
    readCookie,
    refreshedSince,
} from "../lib/sessionAuth.ts";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

test("state-changing methods need the CSRF header, safe ones do not", () => {
    for (const method of ["POST", "put", "PATCH", "delete"]) assert.equal(isUnsafeMethod(method), true, method);
    for (const method of ["GET", "head", "OPTIONS", undefined, null]) assert.equal(isUnsafeMethod(method), false);
    assert.equal(CSRF_HEADER, "X-CSRF-Token");
});

test("the CSRF cookie is read by exact name", () => {
    const jar = "cf_admin_csrf=admin; cf_csrf=user%2Dtoken; other=1";
    assert.equal(readCookie(jar, "cf_csrf"), "user-token");
    assert.equal(readCookie(jar, "cf_admin_csrf"), "admin");
    assert.equal(readCookie(jar, "csrf"), null);
    assert.equal(readCookie("", "cf_csrf"), null);
});

test("placeholder Authorization headers are recognised", () => {
    for (const value of ["Bearer null", "Bearer undefined", "Bearer ", "bearer null", ""]) {
        assert.equal(isPlaceholderAuthorization(value), true, JSON.stringify(value));
    }
    assert.equal(isPlaceholderAuthorization("Bearer eyJhbGciOi"), false);
    assert.equal(isPlaceholderAuthorization(null), false);
});

test("legacy tokens are purged and storage errors are ignored", () => {
    const removed: string[] = [];
    purgeLegacyTokens({ removeItem: (key: string) => removed.push(key) });
    assert.deepEqual(removed, [...LEGACY_TOKEN_KEYS]);
    assert.ok(LEGACY_TOKEN_KEYS.includes("access_token") && LEGACY_TOKEN_KEYS.includes("refresh_token"));
    purgeLegacyTokens({ removeItem: () => { throw new Error("denied"); } });
    purgeLegacyTokens(null);
});

test("a refresh made after the request started is reused, not repeated", () => {
    assert.equal(refreshedSince(2000, 1000), true);
    assert.equal(refreshedSince(1000, 2000), false);
    assert.equal(refreshedSince(Number(null), 0), false);
    assert.equal(refreshedSince(Number("garbage"), 0), false);
});

test("no source file stores a token in localStorage", () => {
    for (const file of ["api/http.ts", "api/client.ts", "auth/AuthContext.tsx", "hooks/useAnalyticsStream.ts",
        "pages/PaymentSuccess.tsx"]) {
        const text = src(file);
        assert.equal(/localStorage\.setItem\(\s*["'](admin_)?(access|refresh)_token/.test(text), false, file);
        assert.equal(/localStorage\.getItem\(\s*["'](admin_)?(access|refresh)_token/.test(text), false, file);
    }
});

test("every API call sends the session cookies", () => {
    assert.ok(src("api/http.ts").includes('credentials: "include"'));
    assert.ok(src("api/client.ts").includes("withCredentials: true"));
});

test("the analytics stream authenticates with the cookie, not a URL token", () => {
    const hook = src("hooks/useAnalyticsStream.ts");
    assert.equal(hook.includes("?token="), false);
    assert.ok(hook.includes("withCredentials: true"));
});
