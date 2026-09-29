/**
 * Multi-asset account view-model (CATI Section 21) — PURE functions, no React / DOM / network.
 *
 * Everything here maps BACKEND state to display state. The server stays authoritative: nothing here
 * decides eligibility, and nothing here can turn market availability into trade readiness.
 *
 *   - market availability, research readiness, certification and execution authority are FOUR separate
 *     states; only execution authority may ever read as "ready to trade";
 *   - Auto Trading and Auto Capital Routing are independent: routing is enabled ONLY by the routing policy
 *     (mode + explicit authorization + switch, not emergency-disabled) — never by Auto Trading;
 *   - machine reason codes are preserved next to the human copy (never replaced by "Something went wrong");
 *   - permission health shows states only; key / secret / passphrase / token values never render, not even
 *     partially masked.
 */

export type Tone = "ok" | "warn" | "blocked" | "neutral";

export interface ReasonView {
    code: string | null;
    text: string;
}

/** Human copy for stable backend reason codes. Unknown codes keep their machine code as the text. */
export const REASON_COPY: Record<string, string> = {
    ACCOUNT_NOT_ELIGIBLE: "This account is not eligible to automate this market.",
    VENUE_API_NOT_SUPPORTED: "The venue does not expose an API-supported contract for this market.",
    BROKER_EXECUTION_UNVALIDATED_FOR_LIVE: "Trading blocked: venue execution has not completed external validation.",
    EXTERNAL_VALIDATION_REQUIRED: "Trading blocked: venue execution has not completed external validation.",
    ACCOUNT_TOPOLOGY_UNKNOWN: "The broker account mode has not been read yet, so no wallet structure is assumed.",
    PERMISSION_EVIDENCE_REQUIRED: "Permission could not be verified with the venue.",
    TRANSFER_PERMISSION_UNVERIFIED: "Internal-transfer permission is not verified for this API key.",
    INTERNAL_TRANSFER_ROUTE_UNAVAILABLE: "Capital route unavailable: no official internal route exists.",
    ROUTE_UNSUPPORTED: "Capital route unavailable: no official internal route exists.",
    TRANSFER_COST_UNAVAILABLE: "The venue does not publish a verified fee for this internal route.",
    CAPITAL_NOT_READY: "Capital is not ready: the required internal transfer is not confirmed by the broker.",
    CERTIFICATION_NOT_READY: "Not certified: research certification for this market is not complete.",
    DATASET_ACQUIRING: "Research data is still being acquired.",
    HOLDOUT_CLOSED: "The certification holdout is closed (unopened).",
    INSTRUMENT_RESEARCH_ONLY: "Research only: this instrument is not part of a frozen research universe.",
    NOT_LISTED_BY_VENUE: "The venue does not list this market for this account.",
    DISCOVERY_NOT_SYNCED: "The venue catalog has not been synchronised yet.",
    MANUAL_APPROVAL_REQUIRED: "This transfer exceeds your approval threshold and needs your approval.",
    AUTOMATION_EMERGENCY_DISABLED: "Automatic capital routing is emergency-disabled.",
    AUTOMATION_NOT_AUTHORIZED: "Automatic capital routing has not been authorized for this account.",
    SHARED_COLLATERAL_NO_TRANSFER_NEEDED: "Unified collateral: no transfer is needed.",
    INSUFFICIENT_TRANSFERABLE_BALANCE: "The source wallet does not hold enough transferable balance.",
    SYNC_COOLDOWN: "The instrument catalog was synchronised recently; try again shortly.",
};

export function reasonView(code: string | null | undefined): ReasonView {
    if (!code) return { code: null, text: "" };
    const base = String(code).split(":")[0];
    return { code: String(code), text: REASON_COPY[String(code)] ?? REASON_COPY[base] ?? String(code) };
}

// --- secrets ---------------------------------------------------------------------------------------------
const SECRET_KEY = /(secret|passphrase|password|api[_-]?key|apikey|token|signature|authorization|private|masked[_-]?key)/i;

/** Defence in depth: drop any secret-looking field from a structure before it can reach the screen. */
export function stripSecrets<T>(value: T): T {
    if (Array.isArray(value)) return value.map((v) => stripSecrets(v)) as unknown as T;
    if (value && typeof value === "object") {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
            if (SECRET_KEY.test(k)) continue;
            out[k] = stripSecrets(v);
        }
        return out as T;
    }
    return value;
}

// --- market families ------------------------------------------------------------------------------------
export const FAMILY_LABEL: Record<string, string> = {
    CRYPTO: "Crypto", FX: "FX", COMMODITIES: "Commodities", STOCK: "Stocks", INDEX: "Indices",
};

export interface FamilyRow {
    family: string;
    label: string;
    availability: "AVAILABLE" | "UNAVAILABLE";
    marketsAvailable: number;
    apiTradable: number;
    reason: ReasonView;
    tone: Tone;
}

export function familyRows(status: any): FamilyRow[] {
    const markets = (status && status.markets) || {};
    return Object.keys(FAMILY_LABEL).filter((f) => markets[f]).map((f) => {
        const m = markets[f];
        const reason = m.execution && m.execution.reason ? m.execution.reason : null;
        const available = !reason;
        return {
            family: f, label: FAMILY_LABEL[f], availability: available ? "AVAILABLE" : "UNAVAILABLE",
            marketsAvailable: Number(m.markets_available || 0), apiTradable: Number(m.markets_api_tradable || 0),
            reason: reasonView(reason), tone: available ? "ok" : "blocked",
        };
    });
}

// --- permission health ----------------------------------------------------------------------------------
export interface PermissionRow {
    permission: string;
    label: string;
    state: string;
    use: string;
    tone: Tone;
}

const PERMISSION_LABEL: Record<string, string> = {
    READ_ACCOUNT: "Read account", READ_POSITIONS: "Read positions", READ_ORDERS: "Read orders",
    TRADE: "Trading", INTERNAL_TRANSFER: "Internal transfer", WITHDRAW: "Withdrawal",
};

/** Only the abstract state + purpose of each permission. Withdrawal always reads "Not required". */
export function permissionRows(health: any): PermissionRow[] {
    const h = (health && typeof health === "object") ? health : {};
    return Object.keys(PERMISSION_LABEL).map((p) => {
        const entry = h[p] || {};
        const state = String(entry.state || "UNVERIFIED");
        if (p === "WITHDRAW") {
            return {
                permission: p, label: PERMISSION_LABEL[p], use: "NEVER_REQUIRED",
                state: state === "PRESENT_KEY_REFUSED" ? "Present — key refused (use a trade-only key)" : "Not required",
                tone: state === "PRESENT_KEY_REFUSED" ? "blocked" : "neutral",
            };
        }
        return {
            permission: p, label: PERMISSION_LABEL[p], state, use: String(entry.use || ""),
            tone: state === "VERIFIED" ? "ok" : state === "MISSING" ? "blocked" : "warn",
        };
    });
}

// --- topology / routes ----------------------------------------------------------------------------------
export function topologyLabel(topology: any): { label: string; reason: ReasonView; tone: Tone } {
    const cls = String((topology && topology.class) || "UNKNOWN");
    if (cls === "UNIFIED") return { label: "Unified account", reason: reasonView(null), tone: "ok" };
    if (cls === "SEGMENTED") return { label: "Segmented wallets", reason: reasonView(null), tone: "neutral" };
    if (cls === "UNSUPPORTED") return { label: "Topology unsupported", reason: reasonView(topology && topology.reason_code), tone: "blocked" };
    return { label: "Topology unknown", reason: reasonView((topology && topology.reason_code) || "ACCOUNT_TOPOLOGY_UNKNOWN"), tone: "warn" };
}

export type RouteRequirement = "LOGICAL_ALLOCATION" | "PHYSICAL_INTERNAL_TRANSFER" | "NO_ROUTE" | "UNKNOWN";

/** What a (dry-run) capital plan needs: never implies money moved for a logical allocation. */
export function routeRequirement(plan: any): { requirement: RouteRequirement; text: string; reason: ReasonView } {
    if (!plan) return { requirement: "UNKNOWN", text: "Not planned yet.", reason: reasonView(null) };
    if (plan.physical_transfer_required) {
        const eligible = plan.policy_decision && plan.policy_decision.eligible;
        return eligible
            ? { requirement: "PHYSICAL_INTERNAL_TRANSFER", text: "Requires a physical internal transfer between wallets of this account.", reason: reasonView(null) }
            : { requirement: "NO_ROUTE", text: "No usable internal route for this plan.", reason: reasonView(plan.policy_decision && plan.policy_decision.reason_code) };
    }
    if (plan.allocation === "LOGICAL_ALLOCATION_NO_PHYSICAL_TRANSFER" && plan.policy_decision && plan.policy_decision.eligible) {
        return {
            requirement: "LOGICAL_ALLOCATION",
            text: "Logical allocation: the capital is assigned by policy within one collateral pool — no funds are moved.",
            reason: reasonView(null),
        };
    }
    return { requirement: "UNKNOWN", text: "Capital route not determined.", reason: reasonView(plan.policy_decision && plan.policy_decision.reason_code) };
}

// --- Auto Trading vs Auto Capital Routing ---------------------------------------------------------------
/** Auto Capital Routing is ON only through its own policy. ``autoTrading`` is deliberately NOT an input. */
export function autoRoutingEnabled(settings: any): boolean {
    const s = settings || {};
    return s.mode === "AUTOMATED_INTERNAL_REALLOCATION" && !!s.authorized_at && !!s.auto_rebalance_enabled
        && !s.emergency_disabled;
}

export function automationSummary(autoTrading: boolean, settings: any): string {
    const routing = autoRoutingEnabled(settings);
    if (!autoTrading && !routing) return "No automatic trading and no automatic capital routing.";
    if (autoTrading && !routing) {
        return "Plans needing no physical transfer may proceed only if separately execution-authorized; plans that need an internal transfer wait for your manual action.";
    }
    if (!autoTrading && routing) return "Routing moves no funds on its own: it only funds an approved trade intent.";
    return "Still subject to governance, capability, certification, economics, risk, routing policy, reservation and broker confirmation.";
}

// --- routing policy validation (mirrors the server; the server remains authoritative) -------------------
export interface PolicyErrors { [field: string]: string }

const DECIMAL_FIELDS = ["max_transfer_amount", "min_funding_balance", "min_derivatives_reserve", "min_free_margin",
    "daily_transfer_limit", "max_destination_balance", "manual_approval_threshold", "max_transfer_pct"];

export function validateRoutingPolicy(values: Record<string, any>): PolicyErrors {
    const errors: PolicyErrors = {};
    for (const key of DECIMAL_FIELDS) {
        const raw = values[key];
        if (raw === undefined || raw === null || raw === "") continue;
        const n = Number(raw);
        if (!Number.isFinite(n) || n < 0 || (key === "max_transfer_pct" && !(n > 0 && n <= 1))) {
            errors[key] = key === "max_transfer_pct" ? "Must be greater than 0 and at most 1." : "Must be a non-negative number.";
        }
    }
    if (values.mode !== undefined && values.mode !== "MANUAL_TRANSFER" && values.mode !== "AUTOMATED_INTERNAL_REALLOCATION") {
        errors.mode = "Invalid mode.";
    }
    if (values.mode === "AUTOMATED_INTERNAL_REALLOCATION" && !values.authorize_automated_reallocation) {
        errors.authorize_automated_reallocation = "Automatic capital routing needs your explicit authorization.";
    }
    if (Array.isArray(values.allowed_routes)) {
        const bad = values.allowed_routes.map((r: string) => String(r).trim().toUpperCase().replace(/ /g, ""))
            .filter((r: string) => r.split("->").length !== 2 || r.startsWith("->") || r.endsWith("->") || r.length > 64);
        if (bad.length) errors.allowed_routes = "Routes look like SOURCE->DESTINATION.";
    }
    return errors;
}

// --- transfer approval flow -----------------------------------------------------------------------------
export const TRANSFER_FLOW = ["PLAN", "APPROVAL", "EXECUTE_INTENT", "BROKER_SUBMIT", "CONFIRM_RECONCILE"] as const;

/** Where a transfer is in PLAN -> APPROVAL -> EXECUTE INTENT -> BROKER SUBMIT -> CONFIRM/RECONCILE.
 *  Only a broker COMPLETED status is "confirmed"; a UI approval or a local request is never confirmation. */
export function transferStage(t: any): { stage: typeof TRANSFER_FLOW[number]; confirmed: boolean; text: string } {
    const st = String((t && t.status) || "");
    if (st === "COMPLETED") return { stage: "CONFIRM_RECONCILE", confirmed: true, text: "Confirmed by the broker." };
    if (st === "UNKNOWN" || st === "RECONCILIATION_REQUIRED") {
        return { stage: "CONFIRM_RECONCILE", confirmed: false, text: "Outcome unknown — reconciling with the broker; it will not be re-submitted." };
    }
    if (st === "SUBMITTED" || st === "CONFIRMATION_PENDING" || st === "SUBMITTING") {
        return { stage: "BROKER_SUBMIT", confirmed: false, text: "Submitted — awaiting broker confirmation." };
    }
    if (st === "REQUESTED" || st === "VALIDATING") return { stage: "EXECUTE_INTENT", confirmed: false, text: "Intent recorded." };
    if (st === "BLOCKED" || st === "FAILED") return { stage: "EXECUTE_INTENT", confirmed: false, text: "Not executed." };
    return { stage: "PLAN", confirmed: false, text: "Planned (dry run) — nothing moved." };
}

// --- instrument readiness (four separate states) --------------------------------------------------------
export interface ReadinessBadge { key: string; label: string; value: string; tone: Tone; reason: ReasonView }

export function instrumentBadges(readiness: any): ReadinessBadge[] {
    const r = readiness || {};
    const market = r.market_available || {};
    const research = r.research || {};
    const cert = r.certification || {};
    const exec = r.execution_authorized || {};
    return [
        { key: "market", label: "Market", value: market.state ? "Available" : "Unavailable", tone: market.state ? "ok" : "blocked", reason: reasonView(market.reason) },
        { key: "research", label: "Research", value: research.state === "RESEARCH_READY" ? "Ready" : research.state === "DATASET_ACQUIRING" ? "Acquiring" : "Research only",
          tone: research.state === "RESEARCH_READY" ? "ok" : "warn", reason: reasonView(research.reason) },
        { key: "certification", label: "Certification", value: cert.state === "CERTIFICATION_READY" ? "Certified" : "Not certified",
          tone: cert.state === "CERTIFICATION_READY" ? "ok" : "warn", reason: reasonView(cert.reason) },
        { key: "execution", label: "Execution", value: exec.state === true ? "Authorized" : "Not authorized",
          tone: exec.state === true ? "ok" : "blocked", reason: reasonView(exec.reason) },
    ];
}

/** "Trade ready" is ONLY execution authority. Discovery / market availability never implies it. */
export function isTradeReady(readiness: any): boolean {
    return !!(readiness && readiness.execution_authorized && readiness.execution_authorized.state === true);
}
