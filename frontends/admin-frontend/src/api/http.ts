/**
 * Shared authenticated HTTP plumbing for the admin console.
 *
 * Section H 8.4: the session lives in httpOnly cookies set by the backend
 * (cf_admin_access / cf_admin_refresh), which page JavaScript cannot read. Nothing here
 * stores a token. `apiFetch` (same signature as `fetch`) sends the cookies
 * (`credentials: "include"`), adds the CSRF token to every state-changing
 * request (double submit: readable cf_admin_csrf cookie + X-CSRF-Token header) and,
 * when the backend answers 401, refreshes the session once through the refresh
 * cookie (the backend ROTATES it) and retries the request. When the refresh is
 * refused the session is over and the app returns to the login screen.
 *
 * Tokens that earlier releases kept in localStorage are deleted on load.
 */
import { CSRF_HEADER, isPlaceholderAuthorization, isUnsafeMethod, purgeLegacyTokens, readCookie, refreshedSince } from "../lib/sessionAuth";

export const API_BASE: string = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const ADMIN_API_BASE: string = import.meta.env.VITE_ADMIN_API_BASE || API_BASE;
// Admin sessions are always issued and refreshed by the main backend.
const REFRESH_URL = `${API_BASE}/api/v1/admin-auth/refresh`;
const CSRF_URL = `${API_BASE}/api/v1/admin-auth/csrf`;

const CSRF_COOKIE = "cf_admin_csrf";
/** "1" while signed in. Not a credential: it only saves a pointless /me call when signed out. */
const SESSION_HINT_KEY = "cf_admin_session";
/** Time of the last successful refresh in any tab (cookies are shared between tabs). */
const LAST_REFRESH_KEY = "cf_admin_session_refreshed_at";
const PROFILE_KEYS = ["admin_email", "admin_name"];
const REFRESH_LOCK_NAME = "cosmicforge-admin-token-refresh";

/** Dispatched on `window` when the session has ended; AuthContext logs the admin out. */
export const AUTH_UNAUTHORIZED_EVENT = "auth:unauthorized";

/** A 401 from these endpoints means "wrong credentials/code", not "expired session". */
const NO_REFRESH_PATHS = [
    "/api/v1/admin-auth/login",
    "/api/v1/admin-auth/refresh",
    "/api/v1/admin-auth/logout",
    "/api/v1/admin-auth/mfa/verify",
    "/api/v1/admin-auth/mfa/setup",
    "/api/v1/admin-auth/mfa/enroll",
];

export type RefreshOutcome =
    | { status: "refreshed" }
    /** The refresh cookie is missing, expired or revoked: the session is over. */
    | { status: "rejected" }
    /** The refresh endpoint could not be reached or failed; the session is kept. */
    | { status: "unavailable" };

function storage(): Storage | null {
    try {
        return typeof localStorage === "undefined" ? null : localStorage;
    } catch {
        return null;
    }
}

function readStorage(key: string): string | null {
    try {
        return storage()?.getItem(key) ?? null;
    } catch {
        return null;
    }
}

function writeStorage(key: string, value: string | null): void {
    try {
        const store = storage();
        if (!store) return;
        if (value === null) store.removeItem(key);
        else store.setItem(key, value);
    } catch {
        // Storage unavailable (private mode): the hint is only an optimisation.
    }
}

// Tokens written by earlier releases: gone on first load of this one.
purgeLegacyTokens(storage());

let csrfToken: string | null = null;

/** The CSRF token from the last login/refresh answer, else the readable cookie. */
export function getCsrfToken(): string | null {
    if (csrfToken) return csrfToken;
    try {
        return readCookie(typeof document === "undefined" ? "" : document.cookie, CSRF_COOKIE);
    } catch {
        return null;
    }
}

export function setCsrfToken(token: string | null | undefined): void {
    csrfToken = token || null;
}

/**
 * CSRF token for a state-changing request. When the portal is served from
 * another host than the API the cookie is not readable here, so it is asked
 * for once (GET /csrf; CORS keeps other sites from reading the answer).
 */
async function ensureCsrfToken(force = false): Promise<string | null> {
    if (!force) {
        const known = getCsrfToken();
        if (known) return known;
    }
    try {
        const response = await fetch(CSRF_URL, { credentials: "include" });
        if (!response.ok) return null;
        const body: unknown = await response.json().catch(() => null);
        const token = typeof body === "object" && body !== null ? (body as Record<string, unknown>).csrf_token : null;
        setCsrfToken(typeof token === "string" ? token : null);
        return csrfToken;
    } catch {
        return null;
    }
}

/** True when this browser has (or recently had) a session. */
export function hasSessionHint(): boolean {
    return readStorage(SESSION_HINT_KEY) === "1";
}

/** Record a fresh session: the CSRF token from the login answer plus the hint. */
export function markSignedIn(csrf: string | null | undefined): void {
    setCsrfToken(csrf);
    writeStorage(SESSION_HINT_KEY, "1");
}

/** Forget the local session state (the cookies are cleared by the backend's /logout). */
export function forgetLocalSession(): void {
    csrfToken = null;
    writeStorage(SESSION_HINT_KEY, null);
    writeStorage(LAST_REFRESH_KEY, null);
    for (const key of PROFILE_KEYS) writeStorage(key, null);
    purgeLegacyTokens(storage());
}

/** Remove the local session and tell the app to return to the login screen. */
export function clearSession(): void {
    forgetLocalSession();
    window.dispatchEvent(new Event(AUTH_UNAUTHORIZED_EVENT));
}

/** True when a 401 from this URL should trigger a session refresh. */
export function isRefreshableUrl(url: string): boolean {
    return !NO_REFRESH_PATHS.some((path) => url.includes(path));
}

async function performRefresh(requestStartedAt: number): Promise<RefreshOutcome> {
    // Another request or another tab may already have rotated the session.
    if (refreshedSince(Number(readStorage(LAST_REFRESH_KEY)), requestStartedAt)) {
        return { status: "refreshed" };
    }

    const csrf = await ensureCsrfToken();
    let response: Response;
    try {
        response = await fetch(REFRESH_URL, {
            method: "POST",
            credentials: "include",
            headers: csrf ? { [CSRF_HEADER]: csrf } : {},
        });
    } catch {
        return { status: "unavailable" };
    }

    if (response.ok) {
        const data: unknown = await response.json().catch(() => null);
        const token = typeof data === "object" && data !== null ? (data as Record<string, unknown>).csrf_token : null;
        if (typeof token === "string" && token) setCsrfToken(token);
        writeStorage(LAST_REFRESH_KEY, String(Date.now()));
        writeStorage(SESSION_HINT_KEY, "1");
        return { status: "refreshed" };
    }

    if (response.status === 401 || response.status === 403) {
        return { status: "rejected" };
    }
    return { status: "unavailable" };
}

/**
 * Serialise refreshes across browser tabs. Re-using an already rotated refresh
 * token makes the backend revoke every session of the admin, so two tabs must
 * never refresh at the same time.
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
 * runs anyway: performRefresh first checks whether another tab refreshed in
 * the meantime instead of refreshing again.
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
 * Single-flight session refresh: concurrent callers share one request.
 * `requestStartedAt` is when the failed request was sent (Date.now()); a
 * refresh that happened after that point is reused instead of repeated.
 */
export function refreshAccessToken(requestStartedAt = 0): Promise<RefreshOutcome> {
    if (!refreshInFlight) {
        refreshInFlight = runExclusive(() => performRefresh(requestStartedAt)).finally(() => {
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

function requestMethod(input: RequestInfo | URL, init: RequestInit): string {
    if (init.method) return init.method;
    if (typeof Request !== "undefined" && input instanceof Request) return input.method;
    return "GET";
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
 * Only our own backends ever receive the session cookies and the CSRF token.
 * URLs are compared by ORIGIN, not by string prefix: "//evil.host/x" and
 * "http://localhost:8000.evil.host/x" resolve to a foreign origin and get a
 * plain fetch. A relative URL is the page's own origin, so it counts as the
 * API only when a configured API base is on that same origin (the production
 * nginx layout); every API call in this app uses an absolute API_BASE /
 * ADMIN_API_BASE URL.
 */
export function isApiUrl(url: string): boolean {
    if (apiOrigins === null) {
        apiOrigins = new Set(
            [API_BASE, ADMIN_API_BASE].map(originOf).filter((origin): origin is string => origin !== null),
        );
    }
    const origin = originOf(url);
    return origin !== null && apiOrigins.has(origin);
}

function withSession(input: RequestInfo | URL, init: RequestInit): RequestInit {
    const headers = new Headers(init.headers ?? (input instanceof Request ? input.headers : undefined));
    if (isPlaceholderAuthorization(headers.get("Authorization"))) {
        headers.delete("Authorization");
    }
    if (isUnsafeMethod(requestMethod(input, init))) {
        const csrf = getCsrfToken();
        if (csrf) headers.set(CSRF_HEADER, csrf);
    }
    return { ...init, headers, credentials: "include" };
}

async function isCsrfRefusal(response: Response): Promise<boolean> {
    if (response.status !== 403) return false;
    const body: unknown = await response.clone().json().catch(() => null);
    const detail = typeof body === "object" && body !== null ? (body as Record<string, unknown>).detail : null;
    return typeof detail === "object" && detail !== null && (detail as Record<string, unknown>).code === "CSRF_FAILED";
}

/** Drop-in replacement for `fetch` for calls to the CosmicForge backend. */
export async function apiFetch(input: RequestInfo | URL, init: RequestInit = {}): Promise<Response> {
    const url = requestUrl(input);
    if (!isApiUrl(url)) return fetch(input, init);

    const unsafe = isUnsafeMethod(requestMethod(input, init));
    if (unsafe && hasSessionHint()) await ensureCsrfToken();

    const startedAt = Date.now();
    const retryInput = input instanceof Request ? input.clone() : input;
    let response = await fetch(input, withSession(input, init));

    if (unsafe && (await isCsrfRefusal(response))) {
        // The CSRF token we held is stale (e.g. a session opened in another tab): fetch it once more.
        if (await ensureCsrfToken(true)) {
            response = await fetch(retryInput instanceof Request ? retryInput.clone() : retryInput, withSession(retryInput, init));
        }
    }
    if (response.status !== 401 || !isRefreshableUrl(url)) return response;

    // Never signed in, or signed out in another tab: the refresh is "rejected"
    // and the app is told the session is over.
    const outcome = await refreshAccessToken(startedAt);
    if (outcome.status === "refreshed") {
        return fetch(retryInput, withSession(retryInput, init));
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
