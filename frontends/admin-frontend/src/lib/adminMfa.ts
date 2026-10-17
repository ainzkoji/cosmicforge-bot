/**
 * Admin sign-in steps (Section H 8.4). Pure: unit-tested with node:test
 * (src/tests/admin_mfa.test.ts).
 *
 * POST /api/v1/admin-auth/login answers one of:
 *   - a session ({ csrf_token, ... })                         -> signed in
 *   - { mfa_required: true, mfa_token }                       -> ask for the authenticator code
 *   - { mfa_enrollment_required: true, enrollment_token }     -> MFA is mandatory: enrol first
 * The step tokens are kept in component memory only, never in storage.
 */

export type AdminLoginStep =
    | { kind: "session"; csrfToken: string | null }
    | { kind: "mfa"; mfaToken: string }
    | { kind: "enroll"; enrollmentToken: string };

function record(value: unknown): Record<string, unknown> | null {
    return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

/** What the login (or /mfa/verify, /mfa/enroll) answer asks the console to do next. */
export function classifyAdminLogin(body: unknown): AdminLoginStep | null {
    const data = record(body);
    if (!data) return null;
    if (data.mfa_required === true && typeof data.mfa_token === "string" && data.mfa_token) {
        return { kind: "mfa", mfaToken: data.mfa_token };
    }
    if (data.mfa_enrollment_required === true && typeof data.enrollment_token === "string" && data.enrollment_token) {
        return { kind: "enroll", enrollmentToken: data.enrollment_token };
    }
    if ("csrf_token" in data || "access_token" in data) {
        return { kind: "session", csrfToken: typeof data.csrf_token === "string" ? data.csrf_token : null };
    }
    return null;
}

/** Digits only, at most six: what an authenticator shows. */
export function normaliseTotpInput(value: string): string {
    return String(value || "").replace(/\D/g, "").slice(0, 6);
}

export function isCompleteTotp(value: string): boolean {
    return /^\d{6}$/.test(value);
}

/** Recovery codes look like "abcde-fghjk"; spacing and case do not matter. */
export function looksLikeRecoveryCode(value: string): boolean {
    return String(value || "").replace(/[^a-z0-9]/gi, "").length === 10;
}

/** The `code` field of a FastAPI error detail ({ detail: { code, message } }), if any. */
export function errorCode(body: unknown): string | null {
    const detail = record(record(body)?.detail);
    return detail && typeof detail.code === "string" ? detail.code : null;
}
