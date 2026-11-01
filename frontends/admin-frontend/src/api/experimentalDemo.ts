/**
 * Experimental DEMO authorizations and the governed L-08 market-entry policy.
 *
 * Both APIs live in bot-backend. Like family governance (src/api/governance.ts),
 * the admin app reaches them through the main backend, which verifies the admin
 * and calls bot-backend with its own short-lived service credential naming that
 * admin:
 *
 *   {VITE_API_BASE}/api/admin/governance/experimental-demo                 -> GET | POST
 *   {VITE_API_BASE}/api/admin/governance/experimental-demo/{id}/revoke     -> POST
 *   {VITE_API_BASE}/api/admin/order-spreading/market-entry-policies        -> GET | POST
 *   {VITE_API_BASE}/api/admin/order-spreading/market-entry-policies/history -> GET
 *   {VITE_API_BASE}/api/admin/order-spreading/market-entry-policies/{id}/revoke -> POST
 *
 * What a request is and how an answer is read is decided in
 * src/utils/experimentalDemo.ts (which the unit tests run); this file only
 * sends and receives.
 */
import { apiClient } from "./client";
import { GovernanceApiError } from "@/utils/familyGovernance";
import {
    EXPERIMENTAL_DEMO_PATH,
    EXPERIMENTAL_DEMO_ROUTE,
    MARKET_POLICY_PATH,
    MARKET_POLICY_ROUTE,
    describeDemoAction,
    parseExperimentalDemoState,
    parseMarketPolicyHistory,
    parseMarketPolicyState,
    readDemoAnswer,
    toDemoError,
} from "@/utils/experimentalDemo";
import type {
    DemoAction,
    DemoActionOutcome,
    ExperimentalDemoState,
    MarketPolicyHistory,
    MarketPolicyState,
} from "@/utils/experimentalDemo";

/** Prefix of every query on the page, so that one invalidation reloads all of them after an action. */
export const EXPERIMENTAL_DEMO_QUERY_KEY = ["admin-experimental-demo"] as const;

// The proxy gives bot-backend 30 s and then answers 504: the browser waits longer.
const TIMEOUT_MS = 35_000;

async function request(method: "get" | "post", url: string, data: unknown, action: string, route: string): Promise<unknown> {
    // Known offline before sending: a definite "nothing was sent", not an unknown outcome.
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
        throw new GovernanceApiError(`${action} failed: this device is offline, so the request was NOT sent.`, null, null, null, false);
    }
    try {
        const response = await apiClient.request({ method, url, data, timeout: TIMEOUT_MS });
        return response.data;
    } catch (error) {
        throw toDemoError(error, action, route);
    }
}

/** A read whose answer does not have the API's shape is an error, never a partly shown page. */
function parsed<T>(parse: (data: unknown) => T, data: unknown, action: string, route: string): T {
    try {
        return parse(data);
    } catch (error) {
        throw toDemoError(error, action, route);
    }
}

export async function getExperimentalDemo(): Promise<ExperimentalDemoState> {
    const action = "Loading the experimental DEMO authorizations";
    const data = await request("get", EXPERIMENTAL_DEMO_PATH, undefined, action, EXPERIMENTAL_DEMO_ROUTE);
    return parsed(parseExperimentalDemoState, data, action, EXPERIMENTAL_DEMO_ROUTE);
}

export async function getMarketEntryPolicies(): Promise<MarketPolicyState> {
    const action = "Loading the market-entry policies";
    const data = await request("get", MARKET_POLICY_PATH, undefined, action, MARKET_POLICY_ROUTE);
    return parsed(parseMarketPolicyState, data, action, MARKET_POLICY_ROUTE);
}

export async function getMarketEntryPolicyHistory(): Promise<MarketPolicyHistory> {
    const action = "Loading the market-entry ledger history";
    const data = await request("get", `${MARKET_POLICY_PATH}/history`, undefined, action, MARKET_POLICY_ROUTE);
    return parsed(parseMarketPolicyHistory, data, action, MARKET_POLICY_ROUTE);
}

/**
 * Send one operator action. Resolves only when the server returned the record
 * it wrote; a refusal rejects with the API's own reason and detail, and a 2xx
 * without the expected record rejects as an unknown outcome.
 */
export async function submitDemoAction(action: DemoAction, reason: string): Promise<DemoActionOutcome> {
    let description;
    try {
        description = describeDemoAction(action, reason);
    } catch (error) {
        const why = error instanceof Error ? error.message : "the request could not be built";
        throw new GovernanceApiError(`Not sent: ${why}`, null, null, null, false);
    }
    const route = action.kind === "grant" || action.kind === "revoke_grant" ? EXPERIMENTAL_DEMO_ROUTE : MARKET_POLICY_ROUTE;
    const data = await request("post", description.path, description.body, description.title, route);
    return parsed((answer) => readDemoAnswer(action, answer), data, description.title, route);
}
