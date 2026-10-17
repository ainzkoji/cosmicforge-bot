/**
 * Pure helpers for the cookie session (Section H 8.4). No DOM, no React:
 * unit-tested with node:test (src/tests/sessionAuth.test.ts).
 *
 * The session lives in httpOnly cookies the page cannot read. The page only
 * handles the CSRF token (double submit: readable cookie + X-CSRF-Token header
 * on every state-changing request) and a non-secret "signed in" hint.
 */

export const CSRF_HEADER = "X-CSRF-Token";

/** Keys earlier releases kept tokens under. Removed on load, never written again. */
export const LEGACY_TOKEN_KEYS: readonly string[] = [
    "access_token",
    "refresh_token",
    "admin_access_token",
    "admin_refresh_token",
];

const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS", "TRACE"]);
const PLACEHOLDER_TOKENS = new Set(["", "null", "undefined", "none", "false"]);

/** True for methods that change state and therefore need the CSRF header. */
export function isUnsafeMethod(method: string | undefined | null): boolean {
    return !SAFE_METHODS.has(String(method || "GET").toUpperCase());
}

/** Value of one cookie in a `document.cookie` style string, or null. */
export function readCookie(cookieString: string | null | undefined, name: string): string | null {
    for (const part of String(cookieString || "").split(";")) {
        const index = part.indexOf("=");
        if (index < 0) continue;
        if (part.slice(0, index).trim() === name) {
            const value = part.slice(index + 1).trim();
            try {
                return decodeURIComponent(value) || null;
            } catch {
                return value || null;
            }
        }
    }
    return null;
}

/** An Authorization header that carries no usable token ("Bearer null" and friends). */
export function isPlaceholderAuthorization(value: string | null | undefined): boolean {
    if (value === null || value === undefined) return false;
    const text = value.trim();
    if (!text) return true;
    const [scheme, ...rest] = text.split(" ");
    return scheme.toLowerCase() === "bearer" && PLACEHOLDER_TOKENS.has(rest.join(" ").trim().toLowerCase());
}

/** Remove every legacy token key. Storage errors (private mode) are ignored. */
export function purgeLegacyTokens(
    storage: { removeItem(key: string): void } | null | undefined,
    keys: readonly string[] = LEGACY_TOKEN_KEYS,
): void {
    if (!storage) return;
    for (const key of keys) {
        try {
            storage.removeItem(key);
        } catch {
            // Storage unavailable: nothing to remove.
        }
    }
}

/**
 * True when the session was already refreshed (by another request or tab)
 * after the failed request was sent: the cookie jar is shared, so the request
 * should simply be retried. Refreshing again would replay a rotated refresh
 * token, which the backend treats as theft and answers by ending every session.
 */
export function refreshedSince(lastRefreshAt: number, requestStartedAt: number): boolean {
    return Number.isFinite(lastRefreshAt) && lastRefreshAt > 0 && lastRefreshAt >= requestStartedAt;
}
