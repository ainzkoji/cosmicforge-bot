/**
 * Experimental DEMO trading and the governed L-08 market-entry policy: everything
 * the admin page decides, kept free of React, axios and the DOM so that `npm test`
 * runs this file exactly as it ships
 * (node --experimental-strip-types src/tests/experimental_demo.test.ts).
 *
 * The APIs live in bot-backend and are reached through the main backend's admin
 * proxy, exactly like family governance (src/api/governance.ts):
 *
 *   {VITE_API_BASE}/api/admin/governance/experimental-demo                          GET | POST
 *   {VITE_API_BASE}/api/admin/governance/experimental-demo/{authorization_id}/revoke POST
 *   {VITE_API_BASE}/api/admin/order-spreading/market-entry-policies                 GET | POST
 *   {VITE_API_BASE}/api/admin/order-spreading/market-entry-policies/history         GET
 *   {VITE_API_BASE}/api/admin/order-spreading/market-entry-policies/{policy_id}/revoke POST
 *
 * This module never decides what is allowed: the engine does, and its refusal is
 * shown with its own `reason` code and `detail`. What is here only (a) checks a
 * form against the bounds the engine enforces, so that a request the engine is
 * certain to refuse is not sent, (b) builds the exact request the confirmation
 * dialog shows and the call sends, and (c) reads answers.
 */
import {
    GOVERNANCE_API_ROUTE,
    GOVERNANCE_PATH,
    UnexpectedAnswer,
    isPlainIdentifier,
    toGovernanceError,
} from "./familyGovernance.ts";
import type { GovernanceApiError, GovernanceFamily } from "./familyGovernance.ts";

export const EXPERIMENTAL_DEMO_PATH = `${GOVERNANCE_PATH}/experimental-demo`;
export const EXPERIMENTAL_DEMO_ROUTE = `${EXPERIMENTAL_DEMO_PATH} (main backend proxy to bot-backend)`;
export const MARKET_POLICY_PATH = "/api/admin/order-spreading/market-entry-policies";
export const MARKET_POLICY_ROUTE = `${MARKET_POLICY_PATH} (main backend proxy to bot-backend)`;

/** The only environment either API grants anything for in this release. */
export const DEMO = "DEMO";

// ------------------------------------------------------------------------------------- experimental DEMO bounds

/** bot-backend governance/experimental_demo.py MAX_DURATION_HOURS (14 days). */
export const MAX_DURATION_HOURS = 336;
export const MIN_DURATION_HOURS = 1;
/** bot-backend governance/experimental_demo.py MAX_ENTRY_NOTIONAL_USDT. */
export const MAX_ENTRY_NOTIONAL_USDT = 1000;
export const MIN_ENTRY_NOTIONAL_USDT = 1;
export const MIN_TEXT_LENGTH = 3;
export const MAX_TEXT_LENGTH = 500;
/** The market-entry ledger keeps an approval reference of at most 200 characters. */
export const MAX_POLICY_APPROVAL_REF_LENGTH = 200;

// --------------------------------------------------------------------------------------- market-entry bounds

/** bot-backend scale/market_entry_policy.py: a governed policy is never looser than the conservative default. */
export const POLICY_BOUNDS = {
    max_participation: { max: 0.1 },
    max_slippage_bps: { max: 25 },
    max_spread_bps: { max: 25 },
    max_depth_age_ms: { min: 250, max: 5000 },
    min_levels: { min: 5, max: 50 },
    absolute_cap_notional: { max: 100_000 },
    expiry_days: { min: 1, max: 90 },
} as const;

/** The conservative defaults the record form starts from. */
export const POLICY_DEFAULTS = {
    venue: "BINANCE_USDM",
    max_participation: "0.05",
    max_slippage_bps: "10",
    max_spread_bps: "10",
    max_depth_age_ms: "2000",
    min_levels: "5",
    absolute_cap_notional: "200",
    expiry_days: "7",
} as const;

export const ALL_SYMBOLS = "*";
const DAY_MS = 86_400_000;
const HOUR_MS = 3_600_000;

// ------------------------------------------------------------------------------------------------- helpers

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

function text(value: unknown): string | null {
    return typeof value === "string" && value !== "" ? value : null;
}

function count(value: unknown): number | null {
    return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** A decimal the API returns as a string ("200") or a number; shown as given. */
function decimalText(value: unknown): string | null {
    if (typeof value === "string" && value.trim() !== "") return value;
    if (typeof value === "number" && Number.isFinite(value)) return String(value);
    return null;
}

/** An account id is a plain identifier: the rule the proxy enforces on path and body ids. */
export function isAccountId(value: string): boolean {
    return isPlainIdentifier(value);
}

function textLengthOk(value: string, max = MAX_TEXT_LENGTH): boolean {
    const length = value.trim().length;
    return length >= MIN_TEXT_LENGTH && length <= max;
}

/** A plain non-negative decimal as typed: "0.05", "10", "200.5". No exponent, no sign. */
const DECIMAL = /^\d+(\.\d+)?$/;
const INTEGER = /^\d+$/;

// ------------------------------------------------------------------------------------------------- answers

export interface LedgerIntegrity {
    ok: boolean;
    /** The engine's own text, e.g. OK_2_EVENTS or BROKEN_AT_SEQ_3. */
    reason: string;
}

export interface ClaimedEntry {
    trade_plan_id: string | null;
    attempt_id: string | null;
    symbol: string | null;
    side: string | null;
    notional_usdt: number | null;
    recorded_at: number | null;
}

export interface ExperimentalAuthorization {
    authorization_id: string;
    label: string | null;
    environment: string | null;
    state: string;
    account_id: string;
    user_id: string | null;
    family_id: string;
    family_rule_hash: string | null;
    research_status_at_grant: string | null;
    stage_at_grant: string | null;
    venue: string | null;
    base_url: string | null;
    approval_ref: string | null;
    reason: string | null;
    granted_by: string | null;
    max_entry_notional_usdt: string | null;
    granted_at: number | null;
    expires_at: number | null;
    seconds_remaining: number | null;
    revoked_at: number | null;
    revoked_by: string | null;
    revoke_reason: string | null;
    entries_claimed: number;
    entries: ClaimedEntry[];
    /** The three statements the engine attaches to every grant; anything else is flagged on the page. */
    not_certification: boolean | null;
    grants_live_authority: boolean | null;
    promotes_family: boolean | null;
}

export interface ExperimentalDemoState {
    integrity: LedgerIntegrity;
    max_duration_hours: number;
    max_entry_notional_usdt: number;
    authorizations: ExperimentalAuthorization[];
}

function parseLedgerIntegrity(value: unknown, what: string): LedgerIntegrity {
    if (!isRecord(value) || typeof value.ok !== "boolean") throw new UnexpectedAnswer(what);
    return { ok: value.ok, reason: text(value.reason) ?? (value.ok ? "OK" : "BROKEN") };
}

function optionalBoolean(value: unknown): boolean | null {
    return typeof value === "boolean" ? value : null;
}

function parseEntry(value: unknown): ClaimedEntry {
    const item = isRecord(value) ? value : {};
    return {
        trade_plan_id: text(item.trade_plan_id),
        attempt_id: text(item.attempt_id),
        symbol: text(item.symbol),
        side: text(item.side),
        notional_usdt: count(item.notional_usdt) ?? (typeof item.notional_usdt === "string" ? Number(item.notional_usdt) : null),
        recorded_at: count(item.recorded_at),
    };
}

export function parseAuthorization(value: unknown): ExperimentalAuthorization {
    if (!isRecord(value)) throw new UnexpectedAnswer("an authorization is not an object");
    const id = text(value.authorization_id);
    const state = text(value.state);
    const account = text(value.account_id);
    const family = text(value.family_id);
    if (id === null || state === null || account === null || family === null) {
        throw new UnexpectedAnswer(`authorization ${id ?? "without an id"} is incomplete`);
    }
    const entries = Array.isArray(value.entries) ? value.entries.map(parseEntry) : [];
    return {
        authorization_id: id,
        label: text(value.label),
        environment: text(value.environment),
        state,
        account_id: account,
        user_id: text(value.user_id),
        family_id: family,
        family_rule_hash: text(value.family_rule_hash),
        research_status_at_grant: text(value.research_status_at_grant),
        stage_at_grant: text(value.stage_at_grant),
        venue: text(value.venue),
        base_url: text(value.base_url),
        approval_ref: text(value.approval_ref),
        reason: text(value.reason),
        granted_by: text(value.granted_by),
        max_entry_notional_usdt: decimalText(value.max_entry_notional_usdt),
        granted_at: count(value.granted_at),
        expires_at: count(value.expires_at),
        seconds_remaining: count(value.seconds_remaining),
        revoked_at: count(value.revoked_at),
        revoked_by: text(value.revoked_by),
        revoke_reason: text(value.revoke_reason),
        entries_claimed: count(value.entries_claimed) ?? entries.length,
        entries,
        not_certification: optionalBoolean(value.not_certification),
        grants_live_authority: optionalBoolean(value.grants_live_authority),
        promotes_family: optionalBoolean(value.promotes_family),
    };
}

function positiveNumber(value: unknown): number | null {
    const n = typeof value === "string" ? Number(value) : value;
    return typeof n === "number" && Number.isFinite(n) && n > 0 ? n : null;
}

/** GET /experimental-demo */
export function parseExperimentalDemoState(data: unknown): ExperimentalDemoState {
    if (!isRecord(data) || !Array.isArray(data.authorizations)) throw new UnexpectedAnswer("the experimental DEMO ledger");
    // The page never offers more than this release's bounds, even if a server reported more.
    const serverHours = positiveNumber(data.max_duration_hours);
    const serverNotional = positiveNumber(data.max_entry_notional_usdt);
    return {
        integrity: parseLedgerIntegrity(data.integrity, "the integrity of the experimental DEMO ledger"),
        max_duration_hours: Math.min(serverHours ?? MAX_DURATION_HOURS, MAX_DURATION_HOURS),
        max_entry_notional_usdt: Math.min(serverNotional ?? MAX_ENTRY_NOTIONAL_USDT, MAX_ENTRY_NOTIONAL_USDT),
        authorizations: data.authorizations.map(parseAuthorization),
    };
}

/** Problems with a grant as the engine reported it: any statement other than "not certification, no live, no promotion". */
export function authorizationAnomalies(grant: ExperimentalAuthorization): string[] {
    const found: string[] = [];
    if (grant.environment !== null && grant.environment !== DEMO) found.push(`environment reported as ${grant.environment}`);
    if (grant.not_certification === false) found.push("not_certification is false");
    if (grant.grants_live_authority === true) found.push("grants_live_authority is true");
    if (grant.promotes_family === true) found.push("promotes_family is true");
    return found;
}

export interface PolicyTermsView {
    max_participation: string | null;
    max_slippage_bps: string | null;
    max_spread_bps: string | null;
    max_depth_age_ms: string | null;
    min_levels: string | null;
    absolute_cap_notional: string | null;
    expires_at_ms: number | null;
    /** Any term the engine returned that this page does not know, shown as given. */
    other: Record<string, unknown>;
}

export interface PolicyRevocation {
    by: string | null;
    reason: string | null;
    at: number | null;
}

export interface MarketEntryPolicy {
    policy_id: string;
    policy_version: string | null;
    venue: string | null;
    symbol: string | null;
    environment: string | null;
    account_id: string | null;
    scope: string | null;
    calibrated: boolean | null;
    approval_ref: string | null;
    recorded_by: string | null;
    reason: string | null;
    terms: PolicyTermsView;
    recorded_at: number | null;
    revoked: PolicyRevocation | null;
    active: boolean | null;
    /** ACTIVE, REVOKED or EXPIRED, derived from `revoked`, `active` and the server clock. */
    state: string;
    /** Fields of the record this page does not know, shown as given. */
    other: Record<string, unknown>;
}

export interface MarketPolicyState {
    integrity: LedgerIntegrity;
    now_ms: number | null;
    policies: MarketEntryPolicy[];
}

const KNOWN_TERMS = new Set(["max_participation", "max_slippage_bps", "max_spread_bps", "max_depth_age_ms", "min_levels",
    "absolute_cap_notional", "expires_at_ms"]);
const KNOWN_POLICY_FIELDS = new Set(["policy_id", "policy_version", "venue", "symbol", "environment", "account_id", "scope",
    "calibrated", "approval_ref", "recorded_by", "actor_ref", "reason", "terms", "recorded_at", "revoked", "active"]);

function parseTerms(value: unknown): PolicyTermsView {
    const terms = isRecord(value) ? value : {};
    const other: Record<string, unknown> = {};
    for (const key of Object.keys(terms)) if (!KNOWN_TERMS.has(key)) other[key] = terms[key];
    return {
        max_participation: decimalText(terms.max_participation),
        max_slippage_bps: decimalText(terms.max_slippage_bps),
        max_spread_bps: decimalText(terms.max_spread_bps),
        max_depth_age_ms: decimalText(terms.max_depth_age_ms),
        min_levels: decimalText(terms.min_levels),
        absolute_cap_notional: decimalText(terms.absolute_cap_notional),
        expires_at_ms: count(terms.expires_at_ms),
        other,
    };
}

function parseRevocation(value: unknown): PolicyRevocation | null {
    if (value === null || value === undefined || value === false) return null;
    if (!isRecord(value)) return { by: null, reason: null, at: null };
    return { by: text(value.by), reason: text(value.reason), at: count(value.at) };
}

export function policyState(revoked: PolicyRevocation | null, active: boolean | null, expiresAt: number | null, nowMs: number | null): string {
    if (revoked !== null) return "REVOKED";
    if (active === true) return "ACTIVE";
    if (active === false) return "EXPIRED";
    if (expiresAt !== null && nowMs !== null) return nowMs < expiresAt ? "ACTIVE" : "EXPIRED";
    return "UNKNOWN";
}

export function parsePolicy(value: unknown, nowMs: number | null): MarketEntryPolicy {
    if (!isRecord(value)) throw new UnexpectedAnswer("a market-entry policy is not an object");
    const id = text(value.policy_id);
    if (id === null) throw new UnexpectedAnswer("a market-entry policy has no id");
    const terms = parseTerms(value.terms);
    const revoked = parseRevocation(value.revoked);
    const active = optionalBoolean(value.active);
    const other: Record<string, unknown> = {};
    for (const key of Object.keys(value)) if (!KNOWN_POLICY_FIELDS.has(key)) other[key] = value[key];
    return {
        policy_id: id,
        policy_version: text(value.policy_version),
        venue: text(value.venue),
        symbol: text(value.symbol),
        environment: text(value.environment),
        account_id: text(value.account_id),
        scope: text(value.scope),
        calibrated: optionalBoolean(value.calibrated),
        approval_ref: text(value.approval_ref),
        recorded_by: text(value.recorded_by) ?? text(value.actor_ref),
        reason: text(value.reason),
        terms,
        recorded_at: count(value.recorded_at),
        revoked,
        active,
        state: policyState(revoked, active, terms.expires_at_ms, nowMs),
        other,
    };
}

/** GET /market-entry-policies */
export function parseMarketPolicyState(data: unknown): MarketPolicyState {
    if (!isRecord(data) || !Array.isArray(data.policies)) throw new UnexpectedAnswer("the market-entry policies");
    const now = count(data.now_ms);
    return {
        integrity: parseLedgerIntegrity(data.integrity, "the integrity of the market-entry ledger"),
        now_ms: now,
        policies: data.policies.map((item) => parsePolicy(item, now)),
    };
}

export interface PolicyEventRow {
    seq: number | null;
    event_type: string;
    policy_id: string | null;
    venue: string | null;
    symbol: string | null;
    environment: string | null;
    account_id: string | null;
    actor_ref: string | null;
    reason: string | null;
    approval_ref: string | null;
    calibrated: boolean | null;
    recorded_at: number | null;
    hash: string | null;
}

export interface MarketPolicyHistory {
    integrity: LedgerIntegrity;
    events: PolicyEventRow[];
}

/** GET /market-entry-policies/history */
export function parseMarketPolicyHistory(data: unknown): MarketPolicyHistory {
    if (!isRecord(data) || !Array.isArray(data.events)) throw new UnexpectedAnswer("the market-entry ledger history");
    return {
        integrity: parseLedgerIntegrity(data.integrity, "the integrity of the market-entry ledger"),
        events: data.events.map((value) => {
            const item = isRecord(value) ? value : {};
            return {
                seq: count(item.seq),
                event_type: text(item.event_type) ?? "UNKNOWN",
                policy_id: text(item.policy_id),
                venue: text(item.venue),
                symbol: text(item.symbol),
                environment: text(item.environment),
                account_id: text(item.account_id),
                actor_ref: text(item.actor_ref),
                reason: text(item.reason),
                approval_ref: text(item.approval_ref),
                calibrated: optionalBoolean(item.calibrated),
                recorded_at: count(item.recorded_at),
                hash: text(item.hash),
            };
        }),
    };
}

// ------------------------------------------------------------------------------------------- what is shown

/** "2d 03h 15m" for a positive number of seconds; "expired" at 0; "n/a" when unknown. */
export function formatRemaining(seconds: number | null | undefined): string {
    if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return "n/a";
    if (seconds <= 0) return "none";
    const total = Math.floor(seconds);
    const days = Math.floor(total / 86_400);
    const hours = Math.floor((total % 86_400) / 3_600);
    const minutes = Math.floor((total % 3_600) / 60);
    if (days > 0) return `${days}d ${String(hours).padStart(2, "0")}h ${String(minutes).padStart(2, "0")}m`;
    if (hours > 0) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
    if (minutes > 0) return `${minutes}m`;
    return `${total}s`;
}

export function integrityLabel(integrity: LedgerIntegrity): string {
    return integrity.ok ? "INTACT" : "BROKEN";
}

// ------------------------------------------------------------------------------------- experimental DEMO grant

export interface GrantDraft {
    accountId: string;
    familyId: string;
    acknowledgedStatus: string;
    durationHours: string;
    maxEntryNotional: string;
    approvalRef: string;
    reason: string;
}

export const EMPTY_GRANT_DRAFT: GrantDraft = {
    accountId: "",
    familyId: "",
    acknowledgedStatus: "",
    durationHours: "24",
    maxEntryNotional: "200",
    approvalRef: "",
    reason: "",
};

export interface GrantLimits {
    maxDurationHours: number;
    maxEntryNotional: number;
}

export const DEFAULT_GRANT_LIMITS: GrantLimits = { maxDurationHours: MAX_DURATION_HOURS, maxEntryNotional: MAX_ENTRY_NOTIONAL_USDT };

export interface GrantRequestBody {
    account_id: string;
    family_id: string;
    environment: "DEMO";
    acknowledged_research_status: string;
    approval_ref: string;
    reason: string;
    duration_hours: number;
    max_entry_notional_usdt: number;
}

/**
 * True when the typed acknowledgement equals the family's current research
 * status (the engine compares them trimmed and case-insensitively).
 */
export function acknowledgementMatches(typed: string, family: GovernanceFamily | undefined): boolean {
    if (!family) return false;
    return typed.trim().toUpperCase() === family.certification_status.trim().toUpperCase();
}

/**
 * Warnings about a family that the engine will most likely refuse to authorize
 * (shown, never decided here: the engine has the last word).
 */
export function familyEligibilityNotes(family: GovernanceFamily): string[] {
    const notes: string[] = [];
    if (!family.registered) notes.push("The family is not registered: the engine refuses EXPERIMENTAL_DEMO_FAMILY_NOT_ELIGIBLE.");
    if (family.disabled) notes.push("The family is disabled: the engine refuses EXPERIMENTAL_DEMO_FAMILY_NOT_ELIGIBLE.");
    if (family.kill_switch_state === "ON") notes.push("The family kill switch is ON: the engine refuses EXPERIMENTAL_DEMO_FAMILY_NOT_ELIGIBLE.");
    if (family.certification_status === "CERTIFIED" && family.allowed_modes.includes("DEMO")) {
        notes.push("The family is certified and already has DEMO authority: no experimental authorization is needed (EXPERIMENTAL_DEMO_FAMILY_ALREADY_CERTIFIED_FOR_DEMO).");
    }
    return notes;
}

export type Built<T> = { ok: true; body: T } | { ok: false; problems: string[] };

/**
 * Check a grant form against the engine's bounds and build the exact body to
 * send. The body is only built when nothing is wrong.
 */
export function buildGrantRequest(draft: GrantDraft, family: GovernanceFamily | undefined, limits: GrantLimits = DEFAULT_GRANT_LIMITS): Built<GrantRequestBody> {
    const problems: string[] = [];
    const account = draft.accountId.trim();
    const maxHours = Math.min(limits.maxDurationHours, MAX_DURATION_HOURS);
    const maxNotional = Math.min(limits.maxEntryNotional, MAX_ENTRY_NOTIONAL_USDT);

    if (!isAccountId(account)) problems.push("Account id: letters, digits, '_' and '-' only (1 to 128 characters, starting with a letter or digit).");
    if (!family) problems.push("Family: select a family from the registry.");
    else if (!isPlainIdentifier(family.family_id)) problems.push("Family: the selected family id is not a plain identifier.");

    const acknowledged = draft.acknowledgedStatus.trim().toUpperCase();
    if (acknowledged === "") problems.push("Acknowledged research status: type the family's current research status.");
    else if (family && !acknowledgementMatches(acknowledged, family)) {
        problems.push(`Acknowledged research status "${acknowledged}" does not match the family's current status ${family.certification_status}.`);
    }

    const hoursText = draft.durationHours.trim();
    const hours = Number(hoursText);
    if (!INTEGER.test(hoursText) || hours < MIN_DURATION_HOURS || hours > maxHours) {
        problems.push(`Duration: a whole number of hours from ${MIN_DURATION_HOURS} to ${maxHours}.`);
    }

    const notionalText = draft.maxEntryNotional.trim();
    const notional = Number(notionalText);
    if (!DECIMAL.test(notionalText) || !Number.isFinite(notional) || notional < MIN_ENTRY_NOTIONAL_USDT || notional > maxNotional) {
        problems.push(`Maximum entry notional: a number from ${MIN_ENTRY_NOTIONAL_USDT} to ${maxNotional} USDT.`);
    }

    if (!textLengthOk(draft.approvalRef)) problems.push(`Approval reference: ${MIN_TEXT_LENGTH} to ${MAX_TEXT_LENGTH} characters (the owner's authorization).`);
    if (!textLengthOk(draft.reason)) problems.push(`Reason: ${MIN_TEXT_LENGTH} to ${MAX_TEXT_LENGTH} characters.`);

    if (problems.length > 0 || !family) return { ok: false, problems };
    return {
        ok: true,
        body: {
            account_id: account,
            family_id: family.family_id,
            environment: "DEMO",
            acknowledged_research_status: acknowledged,
            approval_ref: draft.approvalRef.trim(),
            reason: draft.reason.trim(),
            duration_hours: hours,
            max_entry_notional_usdt: notional,
        },
    };
}

// --------------------------------------------------------------------------------- market-entry policy record

export interface PolicyDraft {
    venue: string;
    symbol: string;
    accountId: string;
    maxParticipation: string;
    maxSlippageBps: string;
    maxSpreadBps: string;
    maxDepthAgeMs: string;
    minLevels: string;
    absoluteCapNotional: string;
    expiryDays: string;
    approvalRef: string;
    reason: string;
}

export const DEFAULT_POLICY_DRAFT: PolicyDraft = {
    venue: POLICY_DEFAULTS.venue,
    symbol: "",
    accountId: "",
    maxParticipation: POLICY_DEFAULTS.max_participation,
    maxSlippageBps: POLICY_DEFAULTS.max_slippage_bps,
    maxSpreadBps: POLICY_DEFAULTS.max_spread_bps,
    maxDepthAgeMs: POLICY_DEFAULTS.max_depth_age_ms,
    minLevels: POLICY_DEFAULTS.min_levels,
    absoluteCapNotional: POLICY_DEFAULTS.absolute_cap_notional,
    expiryDays: POLICY_DEFAULTS.expiry_days,
    approvalRef: "",
    reason: "",
};

export interface PolicyRequestBody {
    venue: string;
    symbol: string;
    environment: "DEMO";
    account_id?: string;
    terms: {
        max_participation: string;
        max_slippage_bps: string;
        max_spread_bps: string;
        max_depth_age_ms: number;
        min_levels: number;
        absolute_cap_notional: string;
        expires_at_ms: number;
    };
    approval_ref: string;
    reason: string;
}

const VENUE = /^[A-Z0-9][A-Z0-9_]{0,63}$/;
const SYMBOL = /^[A-Z0-9][A-Z0-9_-]{0,63}$/;

function decimalInRange(value: string, label: string, max: number, unit: string, problems: string[]): string | null {
    const typed = value.trim();
    const n = Number(typed);
    if (!DECIMAL.test(typed) || !Number.isFinite(n) || n <= 0 || n > max) {
        problems.push(`${label}: a number greater than 0 and at most ${max}${unit}.`);
        return null;
    }
    return typed;
}

function integerInRange(value: string, label: string, min: number, max: number, unit: string, problems: string[]): number | null {
    const typed = value.trim();
    const n = Number(typed);
    if (!INTEGER.test(typed) || n < min || n > max) {
        problems.push(`${label}: a whole number from ${min} to ${max}${unit}.`);
        return null;
    }
    return n;
}

/**
 * Check a policy form against the engine's bounds and build the exact body to
 * send. Decimals are sent as strings (the ledger refuses floats); the expiry is
 * computed from `nowMs` here, once, so the dialog shows what will be sent.
 * There is no field for calibration and no LIVE form: neither can be built.
 */
export function buildPolicyRequest(draft: PolicyDraft, nowMs: number): Built<PolicyRequestBody> {
    const problems: string[] = [];
    const venue = draft.venue.trim().toUpperCase();
    const symbol = draft.symbol.trim().toUpperCase();
    const account = draft.accountId.trim();

    if (!VENUE.test(venue)) problems.push("Venue: an exchange venue code such as BINANCE_USDM (letters, digits, '_').");
    if (symbol !== ALL_SYMBOLS && !SYMBOL.test(symbol)) problems.push("Symbol: one market symbol such as BTCUSDT, or * for all symbols (DEMO with one account only).");
    if (account !== "" && !isAccountId(account)) problems.push("Account id: letters, digits, '_' and '-' only.");
    if (symbol === ALL_SYMBOLS && account === "") problems.push("The all-symbols scope (*) exists for DEMO only and must name its one account.");

    const participation = decimalInRange(draft.maxParticipation, "Max participation", POLICY_BOUNDS.max_participation.max, "", problems);
    const slippage = decimalInRange(draft.maxSlippageBps, "Max slippage", POLICY_BOUNDS.max_slippage_bps.max, " bps", problems);
    const spread = decimalInRange(draft.maxSpreadBps, "Max spread", POLICY_BOUNDS.max_spread_bps.max, " bps", problems);
    const depthAge = integerInRange(draft.maxDepthAgeMs, "Max depth age", POLICY_BOUNDS.max_depth_age_ms.min, POLICY_BOUNDS.max_depth_age_ms.max, " ms", problems);
    const levels = integerInRange(draft.minLevels, "Min depth levels", POLICY_BOUNDS.min_levels.min, POLICY_BOUNDS.min_levels.max, "", problems);
    const cap = decimalInRange(draft.absoluteCapNotional, "Absolute cap notional", POLICY_BOUNDS.absolute_cap_notional.max, " USDT", problems);
    const days = integerInRange(draft.expiryDays, "Expires in", POLICY_BOUNDS.expiry_days.min, POLICY_BOUNDS.expiry_days.max, " days", problems);

    if (!textLengthOk(draft.approvalRef, MAX_POLICY_APPROVAL_REF_LENGTH)) {
        problems.push(`Approval reference: ${MIN_TEXT_LENGTH} to ${MAX_POLICY_APPROVAL_REF_LENGTH} characters (the owner's approval).`);
    }
    if (!textLengthOk(draft.reason)) problems.push(`Reason: ${MIN_TEXT_LENGTH} to ${MAX_TEXT_LENGTH} characters.`);

    if (problems.length > 0 || participation === null || slippage === null || spread === null || depthAge === null
        || levels === null || cap === null || days === null) {
        return { ok: false, problems };
    }
    const body: PolicyRequestBody = {
        venue,
        symbol,
        environment: "DEMO",
        terms: {
            max_participation: participation,
            max_slippage_bps: slippage,
            max_spread_bps: spread,
            max_depth_age_ms: depthAge,
            min_levels: levels,
            absolute_cap_notional: cap,
            expires_at_ms: Math.floor(nowMs) + days * DAY_MS,
        },
        approval_ref: draft.approvalRef.trim(),
        reason: draft.reason.trim(),
    };
    if (account !== "") body.account_id = account;
    return { ok: true, body };
}

// ------------------------------------------------------------------------------------------------ actions

export type DemoAction =
    | { kind: "grant"; body: GrantRequestBody; family: GovernanceFamily; requestedAtMs: number }
    | { kind: "revoke_grant"; grant: ExperimentalAuthorization }
    | { kind: "record_policy"; body: PolicyRequestBody }
    | { kind: "revoke_policy"; policy: MarketEntryPolicy };

export interface DemoActionDescription {
    title: string;
    confirmLabel: string;
    tone: "danger" | "primary";
    method: "POST";
    path: string;
    /** The JSON body, exactly as it will be sent. */
    body: Record<string, unknown>;
    /** What will be authorized or revoked, line by line. */
    summary: Array<[string, string]>;
    /** What the action does and does not do. */
    effects: string[];
    /** True when the dialog asks for the reason (revocations); a grant or record carries the form's reason. */
    asksReason: boolean;
    /** Which API answers, for the refusal panel. */
    apiName: string;
}

export function revokeProblem(action: DemoAction): string | null {
    if (action.kind === "revoke_grant" && !isPlainIdentifier(action.grant.authorization_id)) {
        return "The authorization id is not a plain identifier.";
    }
    if (action.kind === "revoke_policy" && !isPlainIdentifier(action.policy.policy_id)) {
        return "The policy id is not a plain identifier.";
    }
    return null;
}

export function describeDemoAction(action: DemoAction, dialogReason: string): DemoActionDescription {
    const problem = revokeProblem(action);
    if (problem !== null) throw new Error(problem);
    const why = dialogReason.trim();
    switch (action.kind) {
        case "grant": {
            const b = action.body;
            const expires = action.requestedAtMs + b.duration_hours * HOUR_MS;
            return {
                title: `Authorize EXPERIMENTAL DEMO trading of ${b.family_id}`,
                confirmLabel: "Grant experimental DEMO authorization",
                tone: "danger",
                method: "POST",
                path: EXPERIMENTAL_DEMO_PATH,
                body: { ...b },
                summary: [
                    ["Family", b.family_id],
                    ["Current research status", action.family.certification_status],
                    ["Acknowledged research status", b.acknowledged_research_status],
                    ["Governance stage (unchanged)", action.family.governance_stage],
                    ["Account", b.account_id],
                    ["Environment", "DEMO (verified DEMO account only)"],
                    ["Duration", `${b.duration_hours} h, expires about ${new Date(expires).toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC")}`],
                    ["Max entry notional", `${b.max_entry_notional_usdt} USDT per entry`],
                    ["Approval reference", b.approval_ref],
                    ["Reason", b.reason],
                ],
                effects: [
                    `${b.family_id} may submit entries to DEMO account ${b.account_id} only, until the authorization expires or is revoked. Every entry is recorded and labelled EXPERIMENTAL DEMO.`,
                    "This is NOT research certification. The family keeps its research verdict and its stage, and is not promoted.",
                    "No LIVE authority of any kind. The engine verifies the account is a connected DEMO account on the venue's DEMO host, and re-checks it before every order.",
                    "Every other safety gate still applies: registration, rule hash, kill switches, venue certification, account consent, the DEMO order switch, risk and sizing, market-data freshness, the market-entry (L-08) policy, the account fence and stop protection.",
                ],
                asksReason: false,
                apiName: "governance API",
            };
        }
        case "revoke_grant": {
            const g = action.grant;
            return {
                title: `Revoke experimental DEMO authorization ${g.authorization_id}`,
                confirmLabel: "Revoke authorization",
                tone: "danger",
                method: "POST",
                path: `${EXPERIMENTAL_DEMO_PATH}/${encodeURIComponent(g.authorization_id)}/revoke`,
                body: { reason: why },
                summary: [
                    ["Authorization", g.authorization_id],
                    ["Family", g.family_id],
                    ["Account", g.account_id],
                    ["State now", g.state],
                    ["Entries claimed", String(g.entries_claimed)],
                ],
                effects: [
                    "New entries under this authorization are refused from the next check: the execution boundary re-checks right before every order.",
                    "Revocation submits no order. It does not close positions the account already holds; they keep their protection and exits.",
                    "The authorization stays on the ledger as REVOKED, with your administrator id and the reason.",
                ],
                asksReason: true,
                apiName: "governance API",
            };
        }
        case "record_policy": {
            const b = action.body;
            const t = b.terms;
            return {
                title: `Record market-entry policy (L-08) for ${b.venue} ${b.symbol === ALL_SYMBOLS ? "all symbols (*)" : b.symbol}`,
                confirmLabel: "Record policy",
                tone: "danger",
                method: "POST",
                path: MARKET_POLICY_PATH,
                body: { ...b, terms: { ...t } },
                summary: [
                    ["Venue", b.venue],
                    ["Symbol", b.symbol === ALL_SYMBOLS ? "* (all symbols, DEMO, one account)" : b.symbol],
                    ["Environment", "DEMO"],
                    ["Account", b.account_id ?? "not named (any one account; the uncalibrated budget still admits only one per market)"],
                    ["Max participation", t.max_participation],
                    ["Max slippage", `${t.max_slippage_bps} bps`],
                    ["Max spread", `${t.max_spread_bps} bps`],
                    ["Max depth age", `${t.max_depth_age_ms} ms`],
                    ["Min depth levels", String(t.min_levels)],
                    ["Absolute cap notional", `${t.absolute_cap_notional} USDT`],
                    ["Expires", new Date(t.expires_at_ms).toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC")],
                    ["Calibrated", "never"],
                    ["Approval reference", b.approval_ref],
                    ["Reason", b.reason],
                ],
                effects: [
                    "Permits MARKET entries on this market for one account at a time, within these bounds. It is never calibrated: a second account on the same venue/symbol/side is still refused.",
                    "Every other gate of the execution boundary still applies (family, adapter, go-live, risk, fence, submission). The record submits no order.",
                    "Only one policy may be active per venue/symbol/environment: revoke the active one first.",
                ],
                asksReason: false,
                apiName: "market-entry policy API",
            };
        }
        case "revoke_policy": {
            const p = action.policy;
            return {
                title: `Revoke market-entry policy ${p.policy_id}`,
                confirmLabel: "Revoke policy",
                tone: "danger",
                method: "POST",
                path: `${MARKET_POLICY_PATH}/${encodeURIComponent(p.policy_id)}/revoke`,
                body: { reason: why },
                summary: [
                    ["Policy", p.policy_id],
                    ["Venue / symbol", `${p.venue ?? "?"} / ${p.symbol ?? "?"}`],
                    ["Environment", p.environment ?? "?"],
                    ["Account", p.account_id ?? "not named"],
                    ["State now", p.state],
                ],
                effects: [
                    "MARKET entries on this market are refused (MARKET_POLICY_REVOKED) from the moment the revocation commits.",
                    "Revocation submits no order and closes no position.",
                ],
                asksReason: true,
                apiName: "market-entry policy API",
            };
        }
    }
}

export interface DemoActionOutcome {
    summary: string;
}

/**
 * Read the answer to an action. Throws UnexpectedAnswer when the server said
 * 2xx without the record it should have returned: that is never shown as done.
 */
export function readDemoAnswer(action: DemoAction, data: unknown): DemoActionOutcome {
    if (action.kind === "grant" || action.kind === "revoke_grant") {
        const grant = parseAuthorization(isRecord(data) ? data.authorization : undefined);
        if (action.kind === "grant") {
            return { summary: `Recorded by the server: experimental DEMO authorization ${grant.authorization_id} for ${grant.family_id} on account ${grant.account_id}, state ${grant.state}.` };
        }
        if (grant.authorization_id !== action.grant.authorization_id) throw new UnexpectedAnswer("a different authorization was returned");
        if (grant.state !== "REVOKED") throw new UnexpectedAnswer(`the authorization is reported ${grant.state}, not REVOKED`);
        return { summary: `Confirmed by the server: authorization ${grant.authorization_id} is REVOKED.` };
    }
    if (action.kind === "record_policy") {
        const policy = parsePolicy(isRecord(data) ? data.policy : undefined, null);
        return { summary: `Recorded by the server: market-entry policy ${policy.policy_id} for ${policy.venue ?? "?"} ${policy.symbol ?? "?"} ${policy.environment ?? "?"}.` };
    }
    const event = isRecord(data) && isRecord(data.event) ? data.event : null;
    const eventType = event ? text(event.event_type) : null;
    if (!event || eventType === null) throw new UnexpectedAnswer("no ledger event");
    if (eventType !== "POLICY_REVOKED") throw new UnexpectedAnswer(`the ledger recorded ${eventType}, not POLICY_REVOKED`);
    const seq = count(event.seq);
    return { summary: `Recorded by the server: POLICY_REVOKED for ${action.policy.policy_id}${seq !== null ? ` as event ${seq}` : ""}.` };
}

// ------------------------------------------------------------------------------------------------- errors

/**
 * Convert anything thrown by the HTTP client into a GovernanceApiError carrying
 * the server's own `reason` and `detail` (the governance page's conversion),
 * naming the route this page actually used.
 */
export function toDemoError(error: unknown, action: string, route: string): GovernanceApiError {
    const converted = toGovernanceError(error, action);
    if (route !== GOVERNANCE_API_ROUTE && converted.message.includes(GOVERNANCE_API_ROUTE)) {
        converted.message = converted.message.replace(GOVERNANCE_API_ROUTE, route);
    }
    return converted;
}
