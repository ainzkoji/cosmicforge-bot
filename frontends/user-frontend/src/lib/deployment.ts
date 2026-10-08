/**
 * Step 1.9 -- the portal side of the deployment contract and the CATI read model.
 *
 * Pure functions and types only (no DOM, no network), so the unit runner can
 * exercise them. The backend is the authority for every number: this module
 * builds the request the user typed, and formats what the backend answered.
 * It never computes risk, leverage or eligibility itself.
 */

export const DEPLOYMENT_SCHEMA_VERSION = "2026-10-08.v1";

export type RiskLevel = "conservative" | "balanced" | "aggressive";
export type BudgetType = "fixed_amount" | "percent_balance";

export interface DeploymentForm {
    brokerAccountId: string;
    riskLevel: RiskLevel;
    budgetType: BudgetType;
    budgetValue: string;
    maxPositionUsdt: string;
    dailyLossLimitPct: string;
    riskAcknowledged: boolean;
}

export interface DeploymentRequestBody {
    schema_version: string;
    broker_account_id: string;
    budget: { type: BudgetType; value: string };
    risk_level: RiskLevel;
    advanced: { max_position_usdt: string | null; daily_loss_limit_pct: string | null; symbols: string[] };
    risk_acknowledged: boolean;
    request_id: string;
}

export interface Blocker {
    code: string;
    message: string;
    action?: string;
    [detail: string]: unknown;
}

export interface MoneyView {
    risk_level: string;
    risk_profile_version: string;
    budget: { currency: string; amount: string };
    per_trade_risk_pct: string;
    effective_per_trade_risk_pct: string;
    ceiling_applied: boolean;
    risk_per_trade: string;
    max_open_risk_pct: string;
    max_open_risk: string;
    daily_loss_pause_pct: string;
    daily_loss_pause: string;
    drawdown_reduce_pct: string;
    drawdown_reduce_threshold: string;
    drawdown_stop_pct: string;
    drawdown_stop_threshold: string;
    leverage_ceiling: string | number;
    max_positions: number;
}

export interface DeploymentPreview {
    schema_version: string;
    request_id: string;
    broker_account_id: string;
    evaluated_at: number;
    exchange?: string;
    environment?: string;
    account_label?: string | null;
    account?: { equity: string; available_balance: string; wallet: string; source: string; observed_at: number };
    effective_budget?: string;
    money?: MoneyView;
    minimum_deployable_budget?: string;
    exchange_minimum_notional?: string;
    typical_position?: Record<string, { notional_usdt: string; margin_usdt: string; leverage: string; approved: boolean; reason: string | null }> | null;
    typical_position_note?: string;
    ceiling_conflict?: { system_per_trade_risk_ceiling_pct: string; ceiling_applied: boolean; note: string } | null;
    blockers: Blocker[];
    requirements: string[];
    can_deploy: boolean;
    consent?: { version: string; text: string };
    disclaimer?: string;
}

export interface CatiBot {
    id: string;
    name: string;
    exchange: string;
    environment: string | null;
    broker_account_id: string;
    risk_level: string;
    risk_profile_version?: string | null;
    allocation_type: string;
    budget: { type: string; value: string } | null;
    money: MoneyView | null;
    max_position_usdt?: number | null;
    status: "deploying" | "running" | "paused" | "stopped" | string;
    engine_status: string;
    stopped_reason: string | null;
    created_at: string;
}

/** A refused preview/deploy: the backend's blockers, never a guessed message. */
export class DeploymentRefusedError extends Error {
    status: number;
    blockers: Blocker[];
    preview: DeploymentPreview | null;
    constructor(message: string, status: number, blockers: Blocker[], preview: DeploymentPreview | null) {
        super(message);
        this.name = "DeploymentRefusedError";
        this.status = status;
        this.blockers = blockers;
        this.preview = preview;
    }
}

const DECIMAL = /^\d+(\.\d+)?$/;

/** A plain positive decimal as the user typed it (no exponent, no sign), or null. */
export function decimalText(value: string): string | null {
    const text = String(value ?? "").trim().replace(/,/g, "");
    if (!DECIMAL.test(text)) return null;
    return Number(text) > 0 ? text : null;
}

/** What is wrong with the form before anything is sent; null when it can be previewed. */
export function validateForm(form: DeploymentForm): string | null {
    if (!form.brokerAccountId) return "Select an exchange account.";
    const budget = decimalText(form.budgetValue);
    if (budget === null) return "Enter a budget greater than zero.";
    if (form.budgetType === "percent_balance" && Number(budget) > 100) return "A percentage budget cannot exceed 100 % of the account balance.";
    if (form.maxPositionUsdt.trim() !== "" && decimalText(form.maxPositionUsdt) === null) return "Maximum position size must be a positive amount, or empty.";
    if (form.dailyLossLimitPct.trim() !== "") {
        const loss = decimalText(form.dailyLossLimitPct);
        if (loss === null || Number(loss) >= 100) return "Daily loss limit must be a percentage above 0 and below 100, or empty.";
    }
    return null;
}

/**
 * The request body. Amounts stay decimal strings exactly as typed; the
 * environment, leverage and risk figures are never sent (the backend derives them).
 */
export function buildDeploymentRequest(form: DeploymentForm, requestId: string): DeploymentRequestBody {
    const problem = validateForm(form);
    if (problem) throw new Error(problem);
    return {
        schema_version: DEPLOYMENT_SCHEMA_VERSION,
        broker_account_id: form.brokerAccountId,
        budget: { type: form.budgetType, value: decimalText(form.budgetValue) as string },
        risk_level: form.riskLevel,
        advanced: {
            max_position_usdt: form.maxPositionUsdt.trim() === "" ? null : decimalText(form.maxPositionUsdt),
            daily_loss_limit_pct: form.dailyLossLimitPct.trim() === "" ? null : decimalText(form.dailyLossLimitPct),
            symbols: [],
        },
        risk_acknowledged: form.riskAcknowledged,
        request_id: requestId,
    };
}

/** Identity of one deploy intent. Reused on retry so a retry can never create a second bot. */
export function newRequestId(random: () => string = defaultRandom): string {
    return `dep-${random()}`;
}

function defaultRandom(): string {
    const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
    if (c?.randomUUID) return c.randomUUID();
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}-${Math.random().toString(36).slice(2, 12)}`;
}

/** The part of the form that changes the deploy intent (acknowledging does not). */
export function formFingerprint(form: DeploymentForm): string {
    return JSON.stringify([form.brokerAccountId, form.riskLevel, form.budgetType, form.budgetValue.trim(),
        form.maxPositionUsdt.trim(), form.dailyLossLimitPct.trim()]);
}

/** Blockers and the full preview out of a refusal body, whatever FastAPI wrapped it in. */
export function parseRefusal(body: unknown): { blockers: Blocker[]; preview: DeploymentPreview | null; message: string } {
    const detail = (body as { detail?: unknown } | null)?.detail ?? body;
    if (detail && typeof detail === "object" && !Array.isArray(detail) && Array.isArray((detail as { blockers?: unknown }).blockers)) {
        const blockers = (detail as { blockers: Blocker[] }).blockers;
        const preview = "can_deploy" in (detail as object) ? (detail as DeploymentPreview) : null;
        return { blockers, preview, message: blockers.map((b) => b.message || b.code).join(" ") || "The deployment was refused." };
    }
    if (Array.isArray(detail)) {
        return { blockers: [], preview: null, message: detail.map((d) => (d as { msg?: string })?.msg ?? JSON.stringify(d)).join("; ") };
    }
    return { blockers: [], preview: null, message: typeof detail === "string" && detail ? detail : "The request could not be completed." };
}

/** Money as the backend stated it: grouped, at most two decimals, never rounded up. */
export function formatUsdt(value: string | number | null | undefined, currency = "USDT"): string {
    if (value === null || value === undefined || value === "") return "—";
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    const truncated = Math.trunc(n * 100) / 100;
    return `${truncated.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${currency}`;
}

export function formatPct(value: string | number | null | undefined): string {
    if (value === null || value === undefined || value === "") return "—";
    const n = Number(value);
    return Number.isFinite(n) ? `${String(Number(n.toFixed(4)))} %` : "—";
}

export function formatSigned(value: number | null | undefined, digits = 2): string {
    if (value === null || value === undefined || !Number.isFinite(value)) return "—";
    return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
}

export type Tone = "ok" | "info" | "attention" | "critical" | "muted";

export function botStatusView(status: string | null | undefined): { label: string; tone: Tone; description: string } {
    switch (String(status || "").toLowerCase()) {
        case "running": return { label: "Running", tone: "ok", description: "The engine is evaluating this account every cycle." };
        case "deploying": return { label: "Deploying", tone: "info", description: "Created; waiting for the engine's first evaluation of this account." };
        case "paused": return { label: "Paused", tone: "attention", description: "No new entries. Open positions keep their exchange-side protection." };
        case "stopped": return { label: "Stopped", tone: "muted", description: "The bot does not trade until it is started again." };
        default: return { label: status ? String(status) : "Unknown", tone: "muted", description: "The engine has not reported a state for this bot." };
    }
}

export function severityTone(severity: string | null | undefined): Tone {
    switch (String(severity || "").toLowerCase()) {
        case "ok": case "info": return "info";
        case "attention": case "warning": return "attention";
        case "critical": case "error": return "critical";
        default: return "muted";
    }
}

export function protectionView(state: string | null | undefined): { label: string; tone: Tone } {
    switch (String(state || "").toUpperCase()) {
        case "CONFIRMED": return { label: "Stop confirmed on exchange", tone: "ok" };
        case "ABSENT": return { label: "Stop missing", tone: "critical" };
        case "NOT_REQUIRED_FLAT": return { label: "Flat", tone: "muted" };
        case "UNCONFIRMED": return { label: "Not yet confirmed", tone: "attention" };
        default: return { label: "Could not be verified", tone: "attention" };
    }
}

/** Data older than this is shown as stale, never as current. */
export const STALE_AFTER_MS = 120_000;

export function isStale(observedAtMs: number | null | undefined, nowMs: number, limitMs: number = STALE_AFTER_MS): boolean {
    if (observedAtMs === null || observedAtMs === undefined || !Number.isFinite(observedAtMs)) return true;
    return nowMs - observedAtMs > limitMs;
}

export function ageLabel(observedAtMs: number | null | undefined, nowMs: number): string {
    if (observedAtMs === null || observedAtMs === undefined || !Number.isFinite(observedAtMs)) return "never";
    const seconds = Math.max(0, Math.round((nowMs - observedAtMs) / 1000));
    if (seconds < 90) return `${seconds}s ago`;
    if (seconds < 5400) return `${Math.round(seconds / 60)} min ago`;
    if (seconds < 172_800) return `${Math.round(seconds / 3600)} h ago`;
    return `${Math.round(seconds / 86_400)} d ago`;
}

/** Onboarding risk appetite -> risk level (unknown answers fall to the lowest-risk profile). */
export function riskLevelFromAppetite(appetite: string | null | undefined): RiskLevel {
    switch (String(appetite || "").trim().toLowerCase()) {
        case "aggressive": case "high": return "aggressive";
        case "balanced": case "moderate": case "medium": return "balanced";
        default: return "conservative";
    }
}
