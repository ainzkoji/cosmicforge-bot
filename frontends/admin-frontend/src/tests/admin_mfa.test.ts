/**
 * Section H 8.4 -- admin console: MFA sign-in steps and cookie sessions.
 *
 * Pure helpers (src/lib/adminMfa.ts, src/lib/sessionAuth.ts) plus source
 * guards: no admin token is ever written to localStorage and every client
 * sends the session cookies.
 *
 *   node --experimental-strip-types src/tests/admin_mfa.test.ts
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { classifyAdminLogin, errorCode, isCompleteTotp, looksLikeRecoveryCode, normaliseTotpInput } from "../lib/adminMfa.ts";
import { isPlaceholderAuthorization, isUnsafeMethod, purgeLegacyTokens, readCookie, refreshedSince } from "../lib/sessionAuth.ts";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

describe("admin sign-in steps", () => {
    it("a session answer signs in and keeps the CSRF token", () => {
        assert.deepEqual(classifyAdminLogin({ token_type: "bearer", csrf_token: "c1", access_token: "a" }),
            { kind: "session", csrfToken: "c1" });
        assert.deepEqual(classifyAdminLogin({ token_type: "bearer", csrf_token: "c2" }), { kind: "session", csrfToken: "c2" });
    });

    it("an MFA challenge asks for the second factor", () => {
        assert.deepEqual(classifyAdminLogin({ mfa_required: true, mfa_token: "m", token_type: "mfa_challenge" }),
            { kind: "mfa", mfaToken: "m" });
    });

    it("mandatory MFA without enrolment leads to enrolment", () => {
        assert.deepEqual(classifyAdminLogin({ mfa_enrollment_required: true, enrollment_token: "e" }),
            { kind: "enroll", enrollmentToken: "e" });
    });

    it("anything else is not a sign-in answer", () => {
        for (const body of [null, "x", [], {}, { mfa_required: true }, { mfa_enrollment_required: true, enrollment_token: "" }]) {
            assert.equal(classifyAdminLogin(body), null, JSON.stringify(body));
        }
    });

    it("code inputs are normalised and validated", () => {
        assert.equal(normaliseTotpInput(" 12 34-56 789"), "123456");
        assert.equal(isCompleteTotp("123456"), true);
        assert.equal(isCompleteTotp("12345"), false);
        assert.equal(looksLikeRecoveryCode("abcde-fghjk"), true);
        assert.equal(looksLikeRecoveryCode("ABCDE FGHJK"), true);
        assert.equal(looksLikeRecoveryCode("abc"), false);
        assert.equal(errorCode({ detail: { code: "MFA_INVALID", message: "x" } }), "MFA_INVALID");
        assert.equal(errorCode({ detail: "Incorrect email or password" }), null);
    });
});

describe("admin cookie session", () => {
    it("shares the session helpers", () => {
        assert.equal(isUnsafeMethod("POST"), true);
        assert.equal(isUnsafeMethod("GET"), false);
        assert.equal(readCookie("cf_csrf=u; cf_admin_csrf=a", "cf_admin_csrf"), "a");
        assert.equal(isPlaceholderAuthorization("Bearer null"), true);
        assert.equal(refreshedSince(5, 1), true);
        const removed: string[] = [];
        purgeLegacyTokens({ removeItem: (key: string) => removed.push(key) });
        assert.ok(removed.includes("admin_access_token") && removed.includes("admin_refresh_token"));
    });

    it("no admin token is written to or read from localStorage", () => {
        for (const file of ["api/http.ts", "api/client.ts", "auth/AuthContext.tsx", "pages/Login.tsx"]) {
            const text = src(file);
            assert.equal(/localStorage\.(set|get)Item\(\s*["'](admin_)?(access|refresh)_token/.test(text), false, file);
            assert.equal(/localStorage\.setItem\([^)]*(mfa|enrollment)_?token/i.test(text), false, file);
        }
    });

    it("every client sends the session cookies and the CSRF header", () => {
        const http = src("api/http.ts");
        assert.ok(http.includes('credentials: "include"'));
        assert.ok(http.includes("/api/v1/admin-auth/refresh"));
        const client = src("api/client.ts");
        assert.ok(client.includes("config.withCredentials = true"));
        assert.ok(client.includes("config.headers[CSRF_HEADER]"));
    });

    it("the logout button signs out", () => {
        assert.equal(src("components/admin/layout/AdminSidebar.tsx").includes("Handle logout"), false);
    });
});
