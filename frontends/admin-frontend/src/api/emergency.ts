/**
 * Admin emergency controls (kill switch, flatten).
 *
 * The API lives in bot-backend under /api/v1/admin/emergency/*. The admin app
 * has no direct bot-backend connection: like the other admin families (for
 * example /api/admin/cati/*) it goes through the main backend, which verifies
 * the admin and calls bot-backend with its own short-lived service credential
 * (the only credential bot-backend accepts for this API):
 *
 *   {VITE_API_BASE}/api/admin/emergency/status        -> GET  /api/v1/admin/emergency/status
 *   {VITE_API_BASE}/api/admin/emergency/kill-switch   -> POST /api/v1/admin/emergency/kill-switch
 *   {VITE_API_BASE}/api/admin/emergency/flatten       -> POST /api/v1/admin/emergency/flatten
 */
import { apiClient } from "./client";

export interface EmergencyOpenPosition {
    account_id: string;
    user_id: string | null;
    symbol: string;
    side: string;
    qty: number;
}

export interface EmergencyStatus {
    kill_switch: {
        enabled: boolean;
        reason: string | null;
        set_at: string | null;
        set_by: string | null;
    };
    live_order_submission_enabled: boolean;
    demo_order_submission_enabled: boolean;
    /** null when bot-backend could not read positions. */
    open_positions: EmergencyOpenPosition[] | null;
    generated_at: string;
}

export type FlattenResultStatus = "closed" | "submitted" | "failed" | "no_position";

export interface FlattenResult {
    account_id: string;
    symbol: string;
    status: FlattenResultStatus | string;
    detail: string;
}

export interface FlattenResponse {
    ok: boolean;
    kill_switch_enabled: boolean;
    results: FlattenResult[];
}

/** A failed emergency request. Never treat this as "the action happened". */
export class EmergencyApiError extends Error {
    /** HTTP status, or null when no response was received (network error / timeout). */
    status: number | null;
    /** Per-account results, when the backend included them in the error body. */
    results: FlattenResult[];
    /**
     * True when the request was (or may have been) sent but no definite answer
     * came back: client timeout/abort, a network failure, HTTP 504 from the
     * proxy, or an unreadable reply. The action MAY have happened. False means
     * the server definitely refused or failed it (400/409/502/503 with its text).
     */
    outcomeUnknown: boolean;

    constructor(message: string, status: number | null, results: FlattenResult[] = [], outcomeUnknown = false) {
        super(message);
        this.name = "EmergencyApiError";
        this.status = status;
        this.results = results;
        this.outcomeUnknown = outcomeUnknown;
    }
}

const EMERGENCY_PATH = "/api/admin/emergency";

/** Human-readable description of the route in use, shown with errors. */
export const EMERGENCY_API_ROUTE = `${EMERGENCY_PATH} (main backend proxy to bot-backend)`;

const STATUS_TIMEOUT_MS = 15_000;
const KILL_SWITCH_TIMEOUT_MS = 30_000;
// Flattening closes positions on the exchange account by account and can be
// slow. The proxy gives bot-backend 120 s and then answers 504; the browser
// must wait LONGER than that, otherwise it gives up at the same moment and
// never sees the proxy's answer.
const FLATTEN_TIMEOUT_MS = 140_000;

/**
 * Statuses that do not tell us whether the action ran: the gateway timed out
 * (504/408) or the server failed without a handled answer (500).
 */
const UNKNOWN_OUTCOME_STATUSES: ReadonlySet<number> = new Set([408, 500, 504]);

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFlattenResult(value: unknown): value is FlattenResult {
    return isRecord(value)
        && typeof value.account_id === "string"
        && typeof value.symbol === "string"
        && typeof value.status === "string";
}

function parseResults(value: unknown): FlattenResult[] {
    if (!Array.isArray(value)) return [];
    return value.filter(isFlattenResult).map((item) => ({
        account_id: item.account_id,
        symbol: item.symbol,
        status: item.status,
        detail: typeof item.detail === "string" ? item.detail : "",
    }));
}

function isEmergencyStatus(value: unknown): value is EmergencyStatus {
    if (!isRecord(value) || !isRecord(value.kill_switch)) return false;
    return typeof value.kill_switch.enabled === "boolean"
        && typeof value.live_order_submission_enabled === "boolean"
        && typeof value.demo_order_submission_enabled === "boolean"
        && (value.open_positions === null || Array.isArray(value.open_positions));
}

function detailMessage(detail: unknown): string | null {
    if (typeof detail === "string" && detail.trim()) return detail;
    if (Array.isArray(detail)) {
        const parts = detail
            .map((item) => (isRecord(item) && typeof item.msg === "string" ? item.msg : ""))
            .filter(Boolean);
        return parts.length > 0 ? parts.join("; ") : JSON.stringify(detail);
    }
    if (isRecord(detail)) {
        for (const key of ["message", "error", "reason", "reason_code"]) {
            const candidate = detail[key];
            if (typeof candidate === "string" && candidate.trim()) return candidate;
        }
        return JSON.stringify(detail);
    }
    return null;
}

/** Convert anything thrown by axios into an EmergencyApiError with the server's reason. */
function toEmergencyError(error: unknown, action: string): EmergencyApiError {
    if (error instanceof EmergencyApiError) return error;
    const err = isRecord(error) ? error : {};
    const response = isRecord(err.response) ? err.response : null;
    const fallback = error instanceof Error && error.message ? error.message : "Request failed";

    if (!response) {
        // No response after the request left the browser: network error,
        // client timeout/abort, CORS. The server may still have acted on it.
        return new EmergencyApiError(`${action}: no response from the server (${fallback}).`, null, [], true);
    }

    const status = typeof response.status === "number" ? response.status : null;
    const body = response.data;
    const detail = isRecord(body) ? body.detail : undefined;
    const results = isRecord(detail) ? parseResults(detail.results) : isRecord(body) ? parseResults(body.results) : [];
    let message = detailMessage(detail) ?? (typeof body === "string" && body.trim() ? body.slice(0, 300) : fallback);
    if (status === 404) {
        message = `${message} — the emergency API is not reachable at ${EMERGENCY_API_ROUTE}.`;
    }
    const outcomeUnknown = status === null || UNKNOWN_OUTCOME_STATUSES.has(status);
    return new EmergencyApiError(
        outcomeUnknown
            ? `${action}: no definite answer (HTTP ${status ?? "?"}): ${message}`
            : `${action} failed (HTTP ${status ?? "?"}): ${message}`,
        status,
        results,
        outcomeUnknown,
    );
}

async function request(method: "get" | "post", path: string, data: unknown, timeout: number, action: string): Promise<unknown> {
    // Known offline before sending: a definite "nothing was sent", not an unknown outcome.
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
        throw new EmergencyApiError(`${action} failed: this device is offline, so the request was NOT sent.`, null);
    }
    try {
        const response = await apiClient.request({
            method,
            url: `${EMERGENCY_PATH}${path}`,
            data,
            timeout,
        });
        return response.data;
    } catch (error) {
        throw toEmergencyError(error, action);
    }
}

export async function getEmergencyStatus(): Promise<EmergencyStatus> {
    const data = await request("get", "/status", undefined, STATUS_TIMEOUT_MS, "Loading emergency status");
    if (!isEmergencyStatus(data)) {
        throw new EmergencyApiError("Loading emergency status failed: the server returned an unexpected response.", null);
    }
    return data;
}

export async function setKillSwitch(enabled: boolean, reason: string): Promise<EmergencyStatus> {
    const action = enabled ? "Enabling the kill switch" : "Disabling the kill switch";
    const data = await request("post", "/kill-switch", { enabled, reason }, KILL_SWITCH_TIMEOUT_MS, action);
    if (!isEmergencyStatus(data)) {
        throw new EmergencyApiError(
            `${action}: the server returned an unexpected response. Reload the status to see the real state.`,
            null,
            [],
            true,
        );
    }
    if (data.kill_switch.enabled !== enabled) {
        throw new EmergencyApiError(
            `${action} failed: the server reports the kill switch is still ${data.kill_switch.enabled ? "ENABLED" : "DISABLED"}.`,
            null,
        );
    }
    return data;
}

/**
 * Flatten every open position on every account. Resolves with the server's
 * response for any 2xx; callers must still check `ok === true` before telling
 * the operator that it worked.
 */
export async function flattenAllPositions(reason: string): Promise<FlattenResponse> {
    const data = await request(
        "post",
        "/flatten",
        { scope: "all", account_id: null, confirm: "FLATTEN", reason },
        FLATTEN_TIMEOUT_MS,
        "Flatten",
    );
    if (!isRecord(data) || typeof data.ok !== "boolean") {
        throw new EmergencyApiError(
            "Flatten: the server returned an unexpected response. Positions may or may not have been closed — verify on the exchange.",
            null,
            [],
            true,
        );
    }
    return {
        ok: data.ok,
        kill_switch_enabled: data.kill_switch_enabled === true,
        results: parseResults(data.results),
    };
}
