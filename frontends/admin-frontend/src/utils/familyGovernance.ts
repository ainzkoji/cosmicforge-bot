/**
 * Family governance (Step 3.11): everything the admin page decides, kept free of
 * React, axios and the DOM so that `npm test` runs this file exactly as it ships
 * (node --experimental-strip-types src/tests/family_governance.test.ts).
 *
 * The API is bot-backend /api/v1/admin/governance/*, reached through the main
 * backend at /api/admin/governance/* (see src/api/governance.ts). This module
 * never decides what is allowed: the engine does, and its refusal is shown as
 * it was given. What is here only (a) describes a request before it is sent,
 * (b) refuses to build one the API does not offer, and (c) reads answers.
 */

export const GOVERNANCE_PATH = "/api/admin/governance";

/** Human-readable description of the route in use, shown with errors. */
export const GOVERNANCE_API_ROUTE = `${GOVERNANCE_PATH} (main backend proxy to bot-backend)`;

/** The API requires a reason of 3 to 500 characters on every decision. */
export const MIN_REASON_LENGTH = 3;
export const MAX_REASON_LENGTH = 500;
export const MAX_EVIDENCE_KEY_LENGTH = 100;
export const MAX_EVIDENCE_REFERENCE_LENGTH = 500;

/**
 * The phases of bot-backend governance/phases.py, with their titles verbatim.
 * They are shown as labels only; nothing here is derived from a title.
 */
export const STAGE_TITLES: Readonly<Record<string, string>> = {
    M0: "CATI OBSERVE / ENTRY BLOCKED",
    M1: "SECTION 9 SHADOW",
    M2: "SECTIONS 10-12 SHADOW",
    M3: "SECTIONS 13-17 SHADOW",
    M4: "SECTION 19 SHADOW",
    M5: "REPLAY CERTIFICATION",
    M6: "DEMO CATI ACTIVE",
    M7: "LIMITED PRODUCTION",
    M8: "FULL PROMOTION",
    M9: "CLEANUP",
};

const STAGE_ORDER: readonly string[] = ["M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9"];

/** The highest stage the governance API operates. Nothing above it is ever offered or sent. */
export const MAX_OPERABLE_STAGE = "M6";

/** The modes in which a family submits no order. Any other mode is order authority. */
const NON_EXECUTING_MODES: ReadonlySet<string> = new Set(["RESEARCH_SIMULATION", "SHADOW"]);

// ----------------------------------------------------------------------------------------------- answers

export interface LogIntegrity {
    ok: boolean;
    records: number;
    head_hash?: string;
    broken_at_seq?: number;
}

export interface PendingRequest {
    request_id: string;
    from_stage: string | null;
    to_stage: string | null;
    requested_by: string | null;
    requested_at: number | null;
    reason: string | null;
}

export interface RecordedEvidence {
    key: string;
    reference: string | null;
    value: unknown;
    actor_ref: string | null;
    recorded_at: number | null;
}

export interface NextStage {
    stage: string;
    missing: string[];
}

export interface GovernanceFamily {
    family_id: string;
    family_version: string | null;
    description: string;
    mandate_id: string | null;
    hypothesis_id: string | null;
    rule_hash: string | null;
    timeframe: string | null;
    trading_direction: string | null;
    position_model: string | null;
    entry_contract: string | null;
    exit_contract: string | null;
    admission_route: string | null;
    pipeline_adapter: string | null;
    supported_markets: string[];
    evidence_reference: string[];
    kill_switch_position_policy: string | null;
    certification_status: string;
    certification_reason: string | null;
    research_status: string | null;
    failure_reasons: string[];
    governance_stage: string;
    allowed_modes: string[];
    kill_switch_state: string;
    registered: boolean;
    disabled: boolean;
    /** The engine's own statement that the family may submit orders. */
    executable: boolean;
    blockers: string[];
    pending_request: PendingRequest | null;
    recorded_evidence: RecordedEvidence[];
    next_stage: NextStage | null;
}

export interface GovernanceRegistry {
    families: GovernanceFamily[];
    /** Family ids the engine lists as executable (may submit orders). */
    executable: string[];
    log: LogIntegrity;
}

export interface GovernanceEvent {
    seq: number;
    event_id: string;
    family_id: string;
    event_type: string;
    request_id: string | null;
    from_stage: string | null;
    to_stage: string | null;
    actor_ref: string;
    recorded_at: number | null;
    payload: Record<string, unknown>;
}

export interface FamilyDetail {
    family: GovernanceFamily;
    history: GovernanceEvent[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

function text(value: unknown): string | null {
    return typeof value === "string" && value !== "" ? value : null;
}

function count(value: unknown): number | null {
    return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function strings(value: unknown): string[] {
    return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

/** The answer did not have the shape of the governance API: nothing in it is shown as fact. */
export class UnexpectedAnswer extends Error {
    constructor(what: string) {
        super(`The server returned an unexpected response (${what}).`);
        this.name = "UnexpectedAnswer";
    }
}

function parseIntegrity(value: unknown, what: string): LogIntegrity {
    if (!isRecord(value) || typeof value.ok !== "boolean") throw new UnexpectedAnswer(what);
    const integrity: LogIntegrity = { ok: value.ok, records: count(value.records) ?? 0 };
    const head = text(value.head_hash);
    if (head !== null) integrity.head_hash = head;
    const broken = count(value.broken_at_seq);
    if (broken !== null) integrity.broken_at_seq = broken;
    return integrity;
}

function parsePending(value: unknown): PendingRequest | null {
    if (!isRecord(value)) return null;
    const requestId = text(value.request_id);
    if (requestId === null) return null;
    return {
        request_id: requestId,
        from_stage: text(value.from_stage),
        to_stage: text(value.to_stage),
        requested_by: text(value.requested_by),
        requested_at: count(value.requested_at),
        reason: text(value.reason),
    };
}

function parseEvidence(value: unknown): RecordedEvidence[] {
    if (!isRecord(value)) return [];
    return Object.keys(value).sort().map((key) => {
        const item = isRecord(value[key]) ? value[key] : {};
        return {
            key,
            reference: text(item.reference),
            value: item.value,
            actor_ref: text(item.actor_ref),
            recorded_at: count(item.recorded_at),
        };
    });
}

function parseFamily(value: unknown): GovernanceFamily {
    if (!isRecord(value)) throw new UnexpectedAnswer("a family is not an object");
    const familyId = text(value.family_id);
    const stage = text(value.governance_stage);
    const certification = text(value.certification_status);
    // The fields that say what a family may do must be present and of the right type.
    if (familyId === null || stage === null || certification === null || !Array.isArray(value.allowed_modes)
        || !Array.isArray(value.blockers) || typeof value.executable !== "boolean"
        || typeof value.registered !== "boolean" || typeof value.disabled !== "boolean"
        || text(value.kill_switch_state) === null) {
        throw new UnexpectedAnswer(`family ${familyId ?? "without an id"} is incomplete`);
    }
    const detail = isRecord(value.certification_detail) ? value.certification_detail : {};
    const policy = isRecord(value.activation_policy) ? value.activation_policy : {};
    const next = isRecord(value.next_stage_prerequisites) ? value.next_stage_prerequisites : null;
    const nextStage = next ? text(next.stage) : null;
    return {
        family_id: familyId,
        family_version: text(value.family_version),
        description: text(value.description) ?? "",
        mandate_id: text(value.mandate_id),
        hypothesis_id: text(value.hypothesis_id),
        rule_hash: text(value.rule_hash),
        timeframe: text(value.timeframe),
        trading_direction: text(value.trading_direction),
        position_model: text(value.position_model),
        entry_contract: text(value.entry_contract),
        exit_contract: text(value.exit_contract),
        admission_route: text(value.admission_route),
        pipeline_adapter: text(value.pipeline_adapter),
        supported_markets: Array.isArray(value.supported_markets)
            ? value.supported_markets.map((market) => strings(market).join(" ")).filter(Boolean)
            : [],
        evidence_reference: strings(value.evidence_reference),
        kill_switch_position_policy: text(policy.kill_switch_position_policy),
        certification_status: certification,
        certification_reason: text(detail.reason),
        research_status: text(detail.research_status),
        failure_reasons: Array.isArray(detail.failure_reasons)
            ? detail.failure_reasons.map((reason) => (typeof reason === "string" ? reason : JSON.stringify(reason)))
            : [],
        governance_stage: stage,
        allowed_modes: strings(value.allowed_modes),
        kill_switch_state: text(value.kill_switch_state) ?? "UNKNOWN",
        registered: value.registered,
        disabled: value.disabled,
        executable: value.executable,
        blockers: strings(value.blockers),
        pending_request: parsePending(value.pending_request),
        recorded_evidence: parseEvidence(value.recorded_evidence),
        next_stage: next && nextStage !== null ? { stage: nextStage, missing: strings(next.missing) } : null,
    };
}

function parseEvent(value: unknown): GovernanceEvent {
    if (!isRecord(value)) throw new UnexpectedAnswer("a history record is not an object");
    const seq = count(value.seq);
    const eventType = text(value.event_type);
    const familyId = text(value.family_id);
    if (seq === null || eventType === null || familyId === null) {
        throw new UnexpectedAnswer("a history record is incomplete");
    }
    return {
        seq,
        event_id: text(value.event_id) ?? "",
        family_id: familyId,
        event_type: eventType,
        request_id: text(value.request_id),
        from_stage: text(value.from_stage),
        to_stage: text(value.to_stage),
        actor_ref: text(value.actor_ref) ?? "",
        recorded_at: count(value.recorded_at),
        payload: isRecord(value.payload) ? value.payload : {},
    };
}

/** GET /families */
export function parseRegistry(data: unknown): GovernanceRegistry {
    if (!isRecord(data) || !Array.isArray(data.families) || !Array.isArray(data.executable)) {
        throw new UnexpectedAnswer("the registry");
    }
    return {
        families: data.families.map(parseFamily),
        executable: strings(data.executable),
        log: parseIntegrity(data.log, "the log integrity of the registry"),
    };
}

/** GET /families/{family_id} */
export function parseFamilyDetail(data: unknown): FamilyDetail {
    if (!isRecord(data) || !Array.isArray(data.history)) throw new UnexpectedAnswer("the family");
    return { family: parseFamily(data.family), history: data.history.map(parseEvent) };
}

/** GET /history: only the integrity result is used (the family view carries the family's own records). */
export function parseHistoryIntegrity(data: unknown): LogIntegrity {
    if (!isRecord(data)) throw new UnexpectedAnswer("the governance log");
    return parseIntegrity(data.integrity, "the integrity of the governance log");
}

// ------------------------------------------------------------------------------------------ what is shown

export function stageIndex(stage: string | null | undefined): number {
    return stage ? STAGE_ORDER.indexOf(stage) : -1;
}

export function stageLabel(stage: string | null | undefined): string {
    if (!stage) return "unknown";
    const title = STAGE_TITLES[stage];
    return title ? `${stage} · ${title}` : stage;
}

/**
 * The only stage a promotion request may name: the next one, and never above
 * M6. null when there is none to offer (at M6, above it, or an unknown stage).
 */
export function promotionTarget(stage: string): string | null {
    const index = stageIndex(stage);
    if (index < 0 || index >= stageIndex(MAX_OPERABLE_STAGE)) return null;
    return STAGE_ORDER[index + 1];
}

/** Stages a revocation may name: M0 up to the current stage. M0 first; it is the API's default. */
export function revocationTargets(stage: string): string[] {
    const index = stageIndex(stage);
    return index < 0 ? ["M0"] : STAGE_ORDER.slice(0, index + 1);
}

/** Evidence keys the next stage still lacks, read from the engine's own list of what is missing. */
export function missingEvidenceKeys(family: GovernanceFamily): string[] {
    const prefix = "EVIDENCE_MISSING:";
    return (family.next_stage?.missing ?? [])
        .filter((item) => item.startsWith(prefix))
        .map((item) => item.slice(prefix.length))
        .filter(Boolean);
}

export interface FamilyOrderAuthority {
    family_id: string;
    /** The modes that carry order authority; empty when the engine only flagged the family as executable. */
    modes: string[];
}

/**
 * Which families may submit orders, read from the registry answer. Deliberately
 * the widest reading: a family counts when the engine lists it as executable,
 * flags it executable, or allows it any mode other than research simulation and
 * shadow. An inconsistent answer therefore never reads as "no authority".
 */
export function orderAuthority(registry: GovernanceRegistry): FamilyOrderAuthority[] {
    const found = new Map<string, string[]>();
    for (const family of registry.families) {
        const modes = family.allowed_modes.filter((mode) => !NON_EXECUTING_MODES.has(mode));
        if (family.executable || modes.length > 0) found.set(family.family_id, modes);
    }
    for (const familyId of registry.executable) {
        if (!found.has(familyId)) found.set(familyId, []);
    }
    return Array.from(found.entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([family_id, modes]) => ({ family_id, modes }));
}

export function familyHasOrderAuthority(family: GovernanceFamily): boolean {
    return family.executable || family.allowed_modes.some((mode) => !NON_EXECUTING_MODES.has(mode));
}

export interface HistoryRow {
    seq: number;
    event_id: string;
    event_type: string;
    /** "M0 -> M1", "stays M0", or "" when the record carries no stage. */
    stage_change: string;
    actor: string;
    recorded_at: number | null;
    reason: string;
    request_id: string;
    /** What else the record says (evidence key and reference, registered version). */
    note: string;
}

function stageChange(event: GovernanceEvent): string {
    const from = event.from_stage;
    const to = event.to_stage;
    if (event.event_type === "PROMOTION_REQUESTED") return from && to ? `${from} -> ${to} requested` : "";
    if (from && to) return from === to ? `stays ${from}` : `${from} -> ${to}`;
    if (to) return `-> ${to}`;
    if (from) return `from ${from}`;
    return "";
}

function eventNote(event: GovernanceEvent): string {
    const payload = event.payload;
    if (event.event_type === "EVIDENCE_RECORDED") {
        return `${String(payload.key ?? "?")} = ${JSON.stringify(payload.value ?? null)} (${String(payload.reference ?? "no reference")})`;
    }
    if (event.event_type === "FAMILY_REGISTERED") {
        const version = text(payload.family_version);
        const ruleHash = text(payload.rule_hash);
        return [version ? `version ${version}` : "", ruleHash ? `rule hash ${ruleHash.slice(0, 12)}` : ""].filter(Boolean).join(", ");
    }
    if (event.event_type === "CERTIFICATION_VERIFIED") {
        return text(payload.status) ? `research register said ${String(payload.status)}` : "";
    }
    return "";
}

/** The history as it is listed: every record, in the order of the log, nothing merged or dropped. */
export function historyRows(events: readonly GovernanceEvent[]): HistoryRow[] {
    return events.map((event) => ({
        seq: event.seq,
        event_id: event.event_id,
        event_type: event.event_type,
        stage_change: stageChange(event),
        actor: event.actor_ref,
        recorded_at: event.recorded_at,
        reason: text(event.payload.reason) ?? "",
        request_id: event.request_id ?? "",
        note: eventNote(event),
    }));
}

/** What the verification of the hash-chained governance log found, shown next to its INTACT / BROKEN badge. */
export function integrityText(log: LogIntegrity): string {
    if (log.ok) {
        const head = log.head_hash ? `, head ${log.head_hash.slice(0, 16)}` : "";
        return `${log.records} record${log.records === 1 ? "" : "s"} verified${head}.`;
    }
    const where = log.broken_at_seq !== undefined ? `Broken at record ${log.broken_at_seq}` : "Broken";
    return `${where}: a record was changed, removed or reordered. Do not rely on the states shown until this is investigated.`;
}

// ------------------------------------------------------------------------------------------------ actions

/** A path parameter is a plain identifier: the same rule the proxy enforces. */
export function isPlainIdentifier(value: unknown): value is string {
    return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(value);
}

/**
 * Everything this page can ask the governance API to do. There is no action
 * that certifies a family, names a stage above M6 or touches an order-submission
 * flag, because the API has none.
 */
export type GovernanceAction =
    | { kind: "register" }
    | { kind: "evidence"; familyId: string; key: string; reference: string; value: boolean }
    | { kind: "request_promotion"; familyId: string; fromStage: string; toStage: string }
    | { kind: "approve"; familyId: string; requestId: string; fromStage: string; toStage: string }
    | { kind: "reject"; familyId: string; requestId: string; fromStage: string; toStage: string }
    | { kind: "revoke"; familyId: string; fromStage: string; toStage: string }
    | { kind: "revoke_certification"; familyId: string; fromStage: string }
    | { kind: "family_kill_switch"; familyId: string; enabled: boolean; positionPolicy: string | null }
    | { kind: "family_disable"; familyId: string; disabled: boolean }
    | { kind: "account_kill_switch"; accountId: string; enabled: boolean };

export type ActionTone = "danger" | "primary";

export interface ActionDescription {
    title: string;
    confirmLabel: string;
    tone: ActionTone;
    method: "POST";
    path: string;
    /** The JSON body, exactly as it will be sent. */
    body: Record<string, unknown>;
    /** What the action writes, record by record. */
    records: string[];
    /** What the action does and does not change. */
    effects: string[];
    /** Where the typed reason is kept. */
    reasonNote: string;
}

const REVIEWER_NOTE = "The actor on the record is you: the engine takes the administrator id from your session, never from this page.";
const REASON_IN_LOG = "The reason is written into the governance record and into the administrative audit trail.";
const REASON_IN_AUDIT_ONLY = "This API call has no reason field: the reason is kept in the administrative audit trail (auth_audit_log) with your administrator id, not in the governance record.";

/**
 * Why an action may not be sent at all, or null. This is not the engine's
 * decision (it still decides); it only keeps the page from building a request
 * the API does not offer.
 */
export function actionProblem(action: GovernanceAction): string | null {
    if ("familyId" in action && !isPlainIdentifier(action.familyId)) {
        return "The family id is not a plain identifier (letters, digits, '_' and '-').";
    }
    if ("requestId" in action && !isPlainIdentifier(action.requestId)) {
        return "The request id is not a plain identifier (letters, digits, '_' and '-').";
    }
    if (action.kind === "account_kill_switch" && !isPlainIdentifier(action.accountId)) {
        return "The account id is not a plain identifier (letters, digits, '_' and '-').";
    }
    if (action.kind === "request_promotion" || action.kind === "approve") {
        if (stageIndex(action.toStage) < 0) return `${action.toStage} is not a governance stage.`;
        if (stageIndex(action.toStage) > stageIndex(MAX_OPERABLE_STAGE)) {
            return `${action.toStage} is above ${MAX_OPERABLE_STAGE}: this interface does not operate it.`;
        }
    }
    if (action.kind === "request_promotion" && promotionTarget(action.fromStage) !== action.toStage) {
        return `The only stage that can be requested from ${action.fromStage} is ${promotionTarget(action.fromStage) ?? "none"}.`;
    }
    if (action.kind === "revoke") {
        if (stageIndex(action.toStage) < 0) return `${action.toStage} is not a governance stage.`;
        if (stageIndex(action.fromStage) >= 0 && stageIndex(action.toStage) > stageIndex(action.fromStage)) {
            return `${action.fromStage} -> ${action.toStage} is not a revocation.`;
        }
    }
    if (action.kind === "evidence") {
        const key = action.key.trim();
        const reference = action.reference.trim();
        if (key.length < 1 || key.length > MAX_EVIDENCE_KEY_LENGTH) return "The evidence key is required (at most 100 characters).";
        if (reference.length < 1 || reference.length > MAX_EVIDENCE_REFERENCE_LENGTH) {
            return "The evidence reference is required (at most 500 characters): where the evidence is.";
        }
    }
    return null;
}

/** True when the typed reason satisfies the API's rule (3 to 500 characters). */
export function reasonReady(reason: string): boolean {
    const length = reason.trim().length;
    return length >= MIN_REASON_LENGTH && length <= MAX_REASON_LENGTH;
}

function familyPath(familyId: string, tail: string): string {
    return `${GOVERNANCE_PATH}/families/${encodeURIComponent(familyId)}${tail}`;
}

/**
 * The request an action sends and what it records, for the confirmation dialog
 * and for the call itself: the dialog shows this object and the call sends it,
 * so the two cannot differ. Throws when the action may not be sent.
 */
export function describeAction(action: GovernanceAction, reason: string): ActionDescription {
    const problem = actionProblem(action);
    if (problem !== null) throw new Error(problem);
    const why = reason.trim();
    const base = { method: "POST" as const, reasonNote: REASON_IN_LOG };

    switch (action.kind) {
        case "register":
            return {
                ...base,
                title: "Register the code-defined families",
                confirmLabel: "Register families",
                tone: "primary",
                path: `${GOVERNANCE_PATH}/families/register`,
                body: { reason: why },
                records: [
                    "One FAMILY_REGISTERED record for every family the code defines whose definition is not on record yet. A family whose definition is unchanged gets no new record.",
                    REVIEWER_NOTE,
                ],
                effects: [
                    "Registration gives a family an identity and stage M0. It grants no certification, no stage and no order authority.",
                    "A family whose rules or version changed since its last registration is recorded again and returns to M0.",
                ],
                reasonNote: REASON_IN_AUDIT_ONLY,
            };
        case "evidence":
            return {
                ...base,
                title: `Record evidence for ${action.familyId}`,
                confirmLabel: "Record evidence",
                tone: "primary",
                path: familyPath(action.familyId, "/evidence"),
                body: { key: action.key.trim(), reference: action.reference.trim(), value: action.value, reason: why },
                records: [
                    `One EVIDENCE_RECORDED record for ${action.familyId}: key "${action.key.trim()}", reference "${action.reference.trim()}", value ${String(action.value)}.`,
                    REVIEWER_NOTE,
                ],
                effects: [
                    action.value
                        ? "Evidence is a reference to where the proof is. It moves no stage and grants nothing by itself."
                        : "The value false withdraws this key: the stage that requires it lists it as missing again.",
                    "From M5 the engine verifies the family's mandate and its evaluation run, and at M6 the research certification itself, against the research register at the moment of approval. No recorded evidence stands in for them.",
                ],
                reasonNote: REASON_IN_AUDIT_ONLY,
            };
        case "request_promotion":
            return {
                ...base,
                title: `Request promotion of ${action.familyId} to ${action.toStage}`,
                confirmLabel: "Record the request",
                tone: "primary",
                path: familyPath(action.familyId, "/promotion-requests"),
                body: { to_stage: action.toStage, reason: why },
                records: [
                    `One PROMOTION_REQUESTED record for ${action.familyId}: ${action.fromStage} -> ${action.toStage} requested, under a new request id.`,
                    REVIEWER_NOTE,
                ],
                effects: [
                    `A request changes nothing: ${action.familyId} stays at ${action.fromStage} until an administrator approves the request and the engine finds every prerequisite of ${action.toStage} met at that moment.`,
                ],
            };
        case "approve":
            return {
                ...base,
                title: `Approve the promotion of ${action.familyId} to ${action.toStage}`,
                confirmLabel: "Approve",
                tone: "primary",
                path: `${GOVERNANCE_PATH}/promotion-requests/${encodeURIComponent(action.requestId)}/approve`,
                body: { reason: why },
                records: [
                    `If the engine finds every prerequisite of ${action.toStage} met now: one PROMOTION_APPROVED record for ${action.familyId}, ${action.fromStage} -> ${action.toStage}, request ${action.requestId}, naming you as the reviewer, with the prerequisites it verified.`,
                    "If it does not: no governance record is written, the family stays where it is, and the refusal is shown here as the engine gave it.",
                ],
                effects: [
                    action.toStage === MAX_OPERABLE_STAGE
                        ? "M6 is the stage from which a certified family may submit orders to DEMO accounts. The engine approves it only when the research register says the family's hypothesis passed and, for a rule-based family, its admission route answers eligible. Nothing typed here can replace that."
                        : `${action.toStage} grants no order authority: below M6 a family runs in research simulation and shadow only.`,
                    "This page sends a reason and nothing else: the engine reads the recorded evidence from the governance log and verifies everything else against the research register itself.",
                ],
            };
        case "reject":
            return {
                ...base,
                title: `Reject the promotion of ${action.familyId} to ${action.toStage}`,
                confirmLabel: "Reject",
                tone: "danger",
                path: `${GOVERNANCE_PATH}/promotion-requests/${encodeURIComponent(action.requestId)}/reject`,
                body: { reason: why },
                records: [
                    `One PROMOTION_REJECTED record for ${action.familyId}: request ${action.requestId} is closed and the stage stays ${action.fromStage}, naming you as the reviewer.`,
                ],
                effects: ["The family keeps its stage. A new request can be recorded afterwards."],
            };
        case "revoke":
            return {
                ...base,
                title: `Revoke the promotion of ${action.familyId}`,
                confirmLabel: `Revoke to ${action.toStage}`,
                tone: "danger",
                path: familyPath(action.familyId, "/revoke"),
                body: { to_stage: action.toStage, reason: why },
                records: [
                    `One PROMOTION_REVOKED record for ${action.familyId}: ${action.fromStage} -> ${action.toStage}, naming you as the reviewer.`,
                ],
                effects: [
                    "Revocation needs no evidence and is never blocked by a missing prerequisite.",
                    "A pending promotion request of this family is closed by it. Recorded evidence stays on record.",
                ],
            };
        case "revoke_certification":
            return {
                ...base,
                title: `Revoke the certification of ${action.familyId}`,
                confirmLabel: "Revoke certification",
                tone: "danger",
                path: familyPath(action.familyId, "/revoke-certification"),
                body: { reason: why },
                records: [
                    `One CERTIFICATION_REVOKED record for ${action.familyId}: ${action.fromStage} -> M0, naming you as the reviewer.`,
                ],
                effects: [
                    "The platform stops accepting this family's research certification: its status becomes REVOKED and it returns to M0.",
                    "The research register is not changed. The family stays REVOKED until it is registered again with a changed rule set or version.",
                ],
            };
        case "family_kill_switch":
            return {
                ...base,
                title: `${action.enabled ? "Turn ON" : "Turn OFF"} the kill switch of ${action.familyId}`,
                confirmLabel: action.enabled ? "Turn kill switch ON" : "Turn kill switch OFF",
                tone: action.enabled ? "danger" : "primary",
                path: familyPath(action.familyId, "/kill-switch"),
                body: { enabled: action.enabled, reason: why },
                records: [
                    `One ${action.enabled ? "FAMILY_KILL_SWITCH_ON" : "FAMILY_KILL_SWITCH_OFF"} record for ${action.familyId}.`,
                    REVIEWER_NOTE,
                ],
                effects: action.enabled
                    ? [
                        "No new entry and no increase from this family, on any account, from its next decision.",
                        `Positions it already holds follow the family's registered policy: ${action.positionPolicy ?? "not stated in the registry answer"}.`,
                    ]
                    : [
                        "Only this family's own stop is lifted. Its stage, its certification, the platform kill switch and every account kill switch still apply.",
                    ],
            };
        case "family_disable":
            return {
                ...base,
                title: `${action.disabled ? "Disable" : "Enable"} ${action.familyId}`,
                confirmLabel: action.disabled ? "Disable family" : "Enable family",
                tone: action.disabled ? "danger" : "primary",
                path: familyPath(action.familyId, "/disable"),
                // The API's field is `enabled`: true means "disabled is in force".
                body: { enabled: action.disabled, reason: why },
                records: [
                    `One ${action.disabled ? "FAMILY_DISABLED" : "FAMILY_ENABLED"} record for ${action.familyId}.`,
                    REVIEWER_NOTE,
                ],
                effects: action.disabled
                    ? ["A disabled family is not evaluated at all (no shadow decisions either) and cannot be promoted."]
                    : ["The family is evaluated again, in the modes its registration, certification and stage allow. Enabling grants nothing else."],
            };
        case "account_kill_switch":
            return {
                ...base,
                title: `${action.enabled ? "Turn ON" : "Turn OFF"} the kill switch of account ${action.accountId}`,
                confirmLabel: action.enabled ? "Turn account kill switch ON" : "Turn account kill switch OFF",
                tone: action.enabled ? "danger" : "primary",
                path: `${GOVERNANCE_PATH}/accounts/${encodeURIComponent(action.accountId)}/kill-switch`,
                body: { enabled: action.enabled, reason: why },
                records: [
                    `One kill-switch control record for account ${action.accountId} in the engine's append-only control log: new entries ${action.enabled ? "stopped" : "allowed again"}.`,
                    REVIEWER_NOTE,
                ],
                effects: action.enabled
                    ? ["No new entry for this one account, from any family. Its open positions keep their protection and their exits."]
                    : ["New entries are allowed again for this account, subject to the platform kill switch and to each family's kill switch, stage and certification."],
                reasonNote: "The reason is written into the control record and into the administrative audit trail.",
            };
    }
}

export interface ActionOutcome {
    /** One sentence for the operator, built from what the server confirmed. */
    summary: string;
}

/**
 * Read the answer to an action. Throws UnexpectedAnswer when the server said
 * 2xx without the record it should have returned: that is never shown as done.
 */
export function readActionAnswer(action: GovernanceAction, data: unknown): ActionOutcome {
    if (action.kind === "register") {
        if (!isRecord(data) || !Array.isArray(data.events)) throw new UnexpectedAnswer("no registration records");
        const registered = data.events.filter((event) => isRecord(event) && event.unchanged !== true)
            .map((event) => (isRecord(event) ? text(event.family_id) ?? "?" : "?"));
        const unchanged = data.events.length - registered.length;
        return {
            summary: registered.length > 0
                ? `Recorded: FAMILY_REGISTERED for ${registered.join(", ")}${unchanged > 0 ? `; ${unchanged} already on record and unchanged` : ""}.`
                : `Nothing new to record: all ${unchanged} code-defined families are already on record and unchanged.`,
        };
    }
    if (action.kind === "account_kill_switch") {
        const status = isRecord(data) && isRecord(data.account_kill_switch) ? data.account_kill_switch : null;
        if (!status || typeof status.enabled !== "boolean") throw new UnexpectedAnswer("no account kill-switch state");
        if (status.enabled !== action.enabled) {
            throw new UnexpectedAnswer(`the account kill switch is reported ${status.enabled ? "ON" : "OFF"}, not what was asked`);
        }
        return { summary: `Confirmed by the server: the kill switch of account ${action.accountId} is ${status.enabled ? "ON" : "OFF"}.` };
    }
    const event = isRecord(data) && isRecord(data.event) ? data.event : null;
    const eventType = event ? text(event.event_type) : null;
    if (!event || eventType === null) throw new UnexpectedAnswer("no governance record");
    const parts = [`Recorded: ${eventType}`];
    const familyId = text(event.family_id);
    if (familyId) parts.push(`for ${familyId}`);
    const from = text(event.from_stage);
    const to = text(event.to_stage);
    if (eventType === "PROMOTION_REQUESTED" && from && to) parts.push(`(${from} -> ${to} requested)`);
    else if (from && to) parts.push(from === to ? `(stays ${from})` : `(${from} -> ${to})`);
    const requestId = text(event.request_id);
    if (requestId) parts.push(`request ${requestId}`);
    const seq = count(event.seq);
    if (seq !== null) parts.push(`as record ${seq}`);
    return { summary: `${parts.join(" ")}.` };
}

// ------------------------------------------------------------------------------------------------- errors

/** A failed governance request. Never treat this as "the action happened". */
export class GovernanceApiError extends Error {
    /** HTTP status, or null when no response was received. */
    status: number | null;
    /** The API's `reason` code, exactly as it was given (for example PREREQUISITES_NOT_MET). */
    reason: string | null;
    /** The API's `detail` text, exactly as it was given. */
    detail: string | null;
    /**
     * True when the request was (or may have been) sent and no definite answer
     * came back: a client timeout, a network failure, HTTP 504/408/500, or a
     * 2xx without the expected record. The record MAY have been written.
     */
    outcomeUnknown: boolean;

    constructor(message: string, status: number | null, reason: string | null, detail: string | null, outcomeUnknown: boolean) {
        super(message);
        this.name = "GovernanceApiError";
        this.status = status;
        this.reason = reason;
        this.detail = detail;
        this.outcomeUnknown = outcomeUnknown;
    }
}

/** Statuses that do not say whether the engine acted. */
const UNKNOWN_OUTCOME_STATUSES: ReadonlySet<number> = new Set([408, 500, 504]);

function detailText(detail: unknown): string | null {
    if (typeof detail === "string" && detail.trim()) return detail;
    if (Array.isArray(detail)) {
        // FastAPI validation errors: [{loc, msg, type}, ...]
        const parts = detail.map((item) => {
            if (!isRecord(item) || typeof item.msg !== "string") return "";
            const where = Array.isArray(item.loc) ? item.loc.filter((part) => part !== "body").join(".") : "";
            return where ? `${where}: ${item.msg}` : item.msg;
        }).filter(Boolean);
        return parts.length > 0 ? parts.join("; ") : JSON.stringify(detail);
    }
    if (isRecord(detail)) return JSON.stringify(detail);
    return null;
}

/** Convert anything thrown by the HTTP client into a GovernanceApiError carrying the server's own words. */
export function toGovernanceError(error: unknown, action: string): GovernanceApiError {
    if (error instanceof GovernanceApiError) return error;
    if (error instanceof UnexpectedAnswer) {
        return new GovernanceApiError(`${action}: ${error.message} Reload before relying on anything shown.`, null, null, null, true);
    }
    const err = isRecord(error) ? error : {};
    const response = isRecord(err.response) ? err.response : null;
    const fallback = error instanceof Error && error.message ? error.message : "Request failed";

    if (!response) {
        // No response after the request left the browser: the server may still have acted on it.
        return new GovernanceApiError(`${action}: no response from the server (${fallback}).`, null, null, null, true);
    }

    const status = typeof response.status === "number" ? response.status : null;
    const body = response.data;
    const reason = isRecord(body) ? text(body.reason) : null;
    let detail = isRecord(body) ? detailText(body.detail) : null;
    if (detail === null && typeof body === "string" && body.trim()) detail = body.slice(0, 300);
    const outcomeUnknown = status === null || UNKNOWN_OUTCOME_STATUSES.has(status);
    const said = detail ?? reason ?? fallback;
    const where = status === 404 && reason === null ? ` (is the governance API reachable at ${GOVERNANCE_API_ROUTE}?)` : "";
    return new GovernanceApiError(
        outcomeUnknown
            ? `${action}: no definite answer (HTTP ${status ?? "?"}): ${said}`
            : `${action} was refused (HTTP ${status ?? "?"}): ${said}${where}`,
        status,
        reason,
        detail,
        outcomeUnknown,
    );
}

export function formatTimestamp(ms: number | null | undefined): string {
    if (ms === null || ms === undefined) return "n/a";
    const date = new Date(ms);
    return Number.isNaN(date.getTime()) ? String(ms) : date.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC");
}
