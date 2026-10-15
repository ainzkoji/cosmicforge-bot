/**
 * Family governance (Step 3.11): registry, promotion, revocation, family and
 * account kill switches.
 *
 * The API lives in bot-backend under /api/v1/admin/governance/*. Like the
 * emergency controls, the admin app reaches it through the main backend, which
 * verifies the admin and calls bot-backend with its own short-lived service
 * credential naming that admin (the only credential bot-backend accepts here):
 *
 *   {VITE_API_BASE}/api/admin/governance/families                      -> GET  .../families
 *   {VITE_API_BASE}/api/admin/governance/families/{id}                 -> GET  .../families/{id}
 *   {VITE_API_BASE}/api/admin/governance/history                       -> GET  .../history
 *   {VITE_API_BASE}/api/admin/governance/families/register             -> POST
 *   {VITE_API_BASE}/api/admin/governance/families/{id}/evidence        -> POST
 *   {VITE_API_BASE}/api/admin/governance/families/{id}/promotion-requests
 *   {VITE_API_BASE}/api/admin/governance/promotion-requests/{id}/approve | reject
 *   {VITE_API_BASE}/api/admin/governance/families/{id}/revoke | revoke-certification | kill-switch | disable
 *   {VITE_API_BASE}/api/admin/governance/accounts/{id}/kill-switch
 *
 * What a request is, what it records and how an answer is read is decided in
 * src/utils/familyGovernance.ts (which the unit tests run); this file only
 * sends and receives.
 */
import { apiClient } from "./client";
import {
    GOVERNANCE_PATH,
    GovernanceApiError,
    describeAction,
    isPlainIdentifier,
    parseFamilyDetail,
    parseHistoryIntegrity,
    parseRegistry,
    readActionAnswer,
    toGovernanceError,
} from "@/utils/familyGovernance";
import type {
    ActionOutcome,
    FamilyDetail,
    GovernanceAction,
    GovernanceRegistry,
    LogIntegrity,
} from "@/utils/familyGovernance";

/** Prefix of every governance query, so that one invalidation reloads all of them after an action. */
export const GOVERNANCE_QUERY_KEY = ["admin-governance"] as const;

// The proxy gives bot-backend 30 s and then answers 504. The browser must wait
// LONGER than that, otherwise it gives up at the same moment and never sees
// the proxy's answer.
const READ_TIMEOUT_MS = 35_000;
const WRITE_TIMEOUT_MS = 35_000;

async function request(method: "get" | "post", url: string, data: unknown, timeout: number, action: string): Promise<unknown> {
    // Known offline before sending: a definite "nothing was sent", not an unknown outcome.
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
        throw new GovernanceApiError(`${action} failed: this device is offline, so the request was NOT sent.`, null, null, null, false);
    }
    try {
        const response = await apiClient.request({ method, url, data, timeout });
        return response.data;
    } catch (error) {
        throw toGovernanceError(error, action);
    }
}

/** A read whose answer does not have the API's shape is an error, never a partly shown page. */
function parsed<T>(parse: (data: unknown) => T, data: unknown, action: string): T {
    try {
        return parse(data);
    } catch (error) {
        throw toGovernanceError(error, action);
    }
}

export async function getGovernanceRegistry(): Promise<GovernanceRegistry> {
    const action = "Loading the family registry";
    return parsed(parseRegistry, await request("get", `${GOVERNANCE_PATH}/families`, undefined, READ_TIMEOUT_MS, action), action);
}

export async function getGovernanceFamily(familyId: string): Promise<FamilyDetail> {
    const action = `Loading family ${familyId}`;
    if (!isPlainIdentifier(familyId)) {
        throw new GovernanceApiError(`${action} failed: this is not a family id, so the request was NOT sent.`, null, null, null, false);
    }
    const url = `${GOVERNANCE_PATH}/families/${encodeURIComponent(familyId)}`;
    return parsed(parseFamilyDetail, await request("get", url, undefined, READ_TIMEOUT_MS, action), action);
}

/** The integrity result of the whole hash-chained governance log. */
export async function getGovernanceIntegrity(): Promise<LogIntegrity> {
    const action = "Verifying the governance log";
    return parsed(parseHistoryIntegrity, await request("get", `${GOVERNANCE_PATH}/history`, undefined, READ_TIMEOUT_MS, action), action);
}

/**
 * Send one operator action. Resolves only when the server returned the record
 * it wrote; a refusal rejects with the API's own reason and detail, and a 2xx
 * without the expected record rejects as an unknown outcome.
 */
export async function submitGovernanceAction(action: GovernanceAction, reason: string): Promise<ActionOutcome> {
    let description;
    try {
        description = describeAction(action, reason);
    } catch (error) {
        const why = error instanceof Error ? error.message : "the request could not be built";
        throw new GovernanceApiError(`Not sent: ${why}`, null, null, null, false);
    }
    const data = await request("post", description.path, description.body, WRITE_TIMEOUT_MS, description.title);
    return parsed((answer) => readActionAnswer(action, answer), data, description.title);
}
