/**
 * Shared authenticated HTTP plumbing for the customer app.
 *
 * `apiFetch` has the same signature as `fetch`. It attaches the stored access
 * token and, when the backend answers 401, exchanges the stored refresh token
 * for a new pair (the backend ROTATES refresh tokens, so both are replaced),
 * retries the original request once, and ends the session when the refresh
 * token is no longer accepted.
 *
 * Tokens stay in localStorage for now. Moving them to httpOnly cookies needs
 * backend support and is tracked separately.
 */

export const API_BASE: string = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const REFRESH_URL = `${API_BASE}/api/v1/auth/refresh`;

const ACCESS_TOKEN_KEY = "access_token";
const REFRESH_TOKEN_KEY = "refresh_token";
const SESSION_KEYS = [ACCESS_TOKEN_KEY, REFRESH_TOKEN_KEY, "user_email", "user_name"];
const REFRESH_LOCK_NAME = "cosmicforge-user-token-refresh";

/** Dispatched on `window` when the session has ended; AuthContext logs the user out. */
export const AUTH_UNAUTHORIZED_EVENT = "auth:unauthorized";

/** A 401 from these endpoints means "wrong credentials/code", not "expired session". */
const NO_REFRESH_PATHS = [
    "/api/v1/auth/login",
    "/api/v1/auth/register",
    "/api/v1/auth/refresh",
    "/api/v1/auth/verify-email",
    "/api/v1/auth/resend-verification",
    "/api/v1/auth/forgot-password",
    "/api/v1/auth/reset-password",
];

export type RefreshOutcome =
    | { status: "refreshed"; token: string }
    /** The refresh token is missing, expired or revoked: the session is over. */
    | { status: "rejected" }
    /** The refresh endpoint could not be reached or failed; the session is kept. */
    | { status: "unavailable" };

function readStorage(key: string): string | null {
    try {
        return localStorage.getItem(key);
    } catch {
        return null;
    }
}

export function getAccessToken(): string | null {
    return readStorage(ACCESS_TOKEN_KEY);
}

/** Remove the stored session and tell the app to return to the login screen. */
export function clearSession(): void {
    try {
        for (const key of SESSION_KEYS) {
            localStorage.removeItem(key);
        }
    } catch {
        // Storage unavailable: nothing to clear.
    }
    window.dispatchEvent(new Event(AUTH_UNAUTHORIZED_EVENT));
}

/** True when a 401 from this URL should trigger a token refresh. */
export function isRefreshableUrl(url: string): boolean {
    return !NO_REFRESH_PATHS.some((path) => url.includes(path));
}

function isTokenPair(value: unknown): value is { access_token: string; refresh_token: string } {
    if (typeof value !== "object" || value === null) return false;
    const record = value as Record<string, unknown>;
    return typeof record.access_token === "string" && typeof record.refresh_token === "string";
}

async function performRefresh(staleAccessToken: string | null): Promise<RefreshOutcome> {
    // Another request or another tab may already have rotated the tokens.
    const currentAccess = readStorage(ACCESS_TOKEN_KEY);
    if (currentAccess && staleAccessToken && currentAccess !== staleAccessToken) {
        return { status: "refreshed", token: currentAccess };
    }

    const refreshToken = readStorage(REFRESH_TOKEN_KEY);
    if (!refreshToken) return { status: "rejected" };

    let response: Response;
    try {
        response = await fetch(REFRESH_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ refresh_token: refreshToken }),
        });
    } catch {
        return { status: "unavailable" };
    }

    if (response.ok) {
        const data: unknown = await response.json().catch(() => null);
        if (!isTokenPair(data)) return { status: "unavailable" };
        try {
            localStorage.setItem(ACCESS_TOKEN_KEY, data.access_token);
            localStorage.setItem(REFRESH_TOKEN_KEY, data.refresh_token);
        } catch {
            return { status: "unavailable" };
        }
        return { status: "refreshed", token: data.access_token };
    }

    if (response.status === 401 || response.status === 403) {
        return { status: "rejected" };
    }
    return { status: "unavailable" };
}

/**
 * Serialise refreshes across browser tabs. Re-using an already rotated refresh
 * token makes the backend revoke every session of the user, so two tabs must
 * never refresh with the same token at the same time.
 */
async function runExclusive(task: () => Promise<RefreshOutcome>): Promise<RefreshOutcome> {
    if (typeof navigator !== "undefined" && "locks" in navigator) {
        // `locks.request` resolves with the callback's (awaited) result.
        return await navigator.locks.request(REFRESH_LOCK_NAME, () => task());
    }
    return await runWithStorageLock(task);
}

/*
 * Fallback for browsers without the Web Locks API: a small localStorage mutex.
 * localStorage has no compare-and-set, so after writing a claim we wait a
 * moment and confirm it is still ours; the loser waits for the winner. A claim
 * expires after FALLBACK_LOCK_TTL_MS so a crashed tab cannot block refreshes.
 * If the lock cannot be taken in time (or storage is unavailable) the task
 * runs anyway: performRefresh first re-reads the stored tokens and adopts a
 * pair that another tab has just rotated instead of refreshing again.
 */
const FALLBACK_LOCK_KEY = `${REFRESH_LOCK_NAME}:lock`;
const FALLBACK_LOCK_TTL_MS = 10_000;

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
        window.setTimeout(resolve, ms);
    });
}

/** Owner id of the current, unexpired claim ("<owner>|<expiresAtMs>"), if any. */
function storageLockOwner(): string | null {
    const raw = readStorage(FALLBACK_LOCK_KEY);
    if (!raw) return null;
    const [owner, expires] = raw.split("|");
    const expiresAt = Number(expires);
    if (!owner || !Number.isFinite(expiresAt) || expiresAt <= Date.now()) return null;
    return owner;
}

async function runWithStorageLock(task: () => Promise<RefreshOutcome>): Promise<RefreshOutcome> {
    const owner = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const giveUpAt = Date.now() + FALLBACK_LOCK_TTL_MS;
    let held = false;
    while (!held && Date.now() < giveUpAt) {
        if (storageLockOwner() !== null) {
            await sleep(100);
            continue;
        }
        try {
            localStorage.setItem(FALLBACK_LOCK_KEY, `${owner}|${Date.now() + FALLBACK_LOCK_TTL_MS}`);
        } catch {
            break; // Storage unavailable: there is no cross-tab state to protect.
        }
        await sleep(25 + Math.random() * 50);
        held = storageLockOwner() === owner;
    }
    try {
        return await task();
    } finally {
        if (held && storageLockOwner() === owner) {
            try {
                localStorage.removeItem(FALLBACK_LOCK_KEY);
            } catch {
                // Storage unavailable: the claim expires on its own.
            }
        }
    }
}

let refreshInFlight: Promise<RefreshOutcome> | null = null;

/**
 * Single-flight token refresh: concurrent callers share one request.
 * `staleAccessToken` is the token the failed request was sent with.
 */
export function refreshAccessToken(staleAccessToken: string | null): Promise<RefreshOutcome> {
    if (!refreshInFlight) {
        refreshInFlight = runExclusive(() => performRefresh(staleAccessToken)).finally(() => {
            refreshInFlight = null;
        });
    }
    return refreshInFlight;
}

function requestUrl(input: RequestInfo | URL): string {
    if (typeof input === "string") return input;
    if (input instanceof URL) return input.href;
    return input.url;
}

/** Origin of a URL (relative URLs resolve against the page); null when unusable. */
function originOf(url: string): string | null {
    try {
        const origin = new URL(url, window.location.href).origin;
        return origin === "null" ? null : origin;
    } catch {
        return null;
    }
}

let apiOrigins: Set<string> | null = null;

/**
 * Only our own backend ever receives the bearer token. URLs are compared by
 * ORIGIN, not by string prefix: "//evil.host/x" and
 * "http://localhost:8000.evil.host/x" resolve to a foreign origin and get a
 * plain fetch without the token. A relative URL is the page's own origin, so
 * it counts as the API only when VITE_API_BASE is on that same origin (the
 * production nginx layout); every API call in this app uses an absolute
 * API_BASE URL.
 */
function isApiUrl(url: string): boolean {
    if (apiOrigins === null) {
        apiOrigins = new Set(
            [API_BASE].map(originOf).filter((origin): origin is string => origin !== null),
        );
    }
    const origin = originOf(url);
    return origin !== null && apiOrigins.has(origin);
}

function withAuth(input: RequestInfo | URL, init: RequestInit, token: string | null): RequestInit {
    const headers = new Headers(init.headers ?? (input instanceof Request ? input.headers : undefined));
    if (token) {
        headers.set("Authorization", `Bearer ${token}`);
    } else if (headers.get("Authorization") === "Bearer null") {
        headers.delete("Authorization");
    }
    return { ...init, headers };
}

/** Drop-in replacement for `fetch` for calls to the CosmicForge backend. */
export async function apiFetch(input: RequestInfo | URL, init: RequestInit = {}): Promise<Response> {
    const url = requestUrl(input);
    if (!isApiUrl(url)) return fetch(input, init);

    const sentToken = getAccessToken();
    const retryInput = input instanceof Request ? input.clone() : input;

    const response = await fetch(input, withAuth(input, init, sentToken));
    if (response.status !== 401 || !isRefreshableUrl(url)) return response;

    // With no refresh token (never signed in, or signed out in another tab) the
    // outcome is "rejected" and the app is told the session is over.
    const outcome = await refreshAccessToken(sentToken);
    if (outcome.status === "refreshed") {
        return fetch(retryInput, withAuth(retryInput, init, outcome.token));
    }
    if (outcome.status === "rejected") {
        clearSession();
    }
    return response;
}

/** Extract the server's error text from a failed response (FastAPI `detail`). */
export async function responseErrorMessage(response: Response, fallback: string): Promise<string> {
    const body: unknown = await response.json().catch(() => null);
    if (typeof body === "object" && body !== null) {
        const detail = (body as Record<string, unknown>).detail;
        if (typeof detail === "string" && detail.trim()) return detail;
        if (Array.isArray(detail)) {
            const messages = detail
                .map((item: unknown) =>
                    typeof item === "object" && item !== null && typeof (item as Record<string, unknown>).msg === "string"
                        ? String((item as Record<string, unknown>).msg)
                        : "")
                .filter(Boolean);
            if (messages.length > 0) return messages.join("; ");
        }
        if (typeof detail === "object" && detail !== null) {
            const record = detail as Record<string, unknown>;
            if (typeof record.message === "string" && record.message.trim()) return record.message;
            if (typeof record.reason === "string" && record.reason.trim()) return record.reason;
        }
    }
    return `${fallback} (HTTP ${response.status})`;
}

/**
 * Download a file through an authenticated request and hand it to the browser
 * as a save. Never put the access token in a URL.
 */
export async function downloadAuthenticated(url: string, fallbackFilename: string): Promise<void> {
    const response = await apiFetch(url);
    if (!response.ok) {
        throw new Error(await responseErrorMessage(response, "Download failed"));
    }
    const disposition = response.headers.get("Content-Disposition") || "";
    const match = /filename="?([^";]+)"?/i.exec(disposition);
    const filename = match ? match[1] : fallbackFilename;

    const blob = await response.blob();
    const objectUrl = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = objectUrl;
    anchor.download = filename;
    anchor.style.display = "none";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
}
