import { useState } from "react";
import type { ReactNode } from "react";
import { Link, useParams } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, RefreshCw, X } from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { GovernanceActionDialog } from "@/components/admin/governance/GovernanceActionDialog";
import {
    Badge,
    CannotDoCard,
    CertificationBadge,
    CodeList,
    ErrorPanel,
    IntegrityLine,
    KillSwitchBadge,
} from "@/components/admin/governance/shared";
import { GOVERNANCE_QUERY_KEY, getGovernanceFamily, getGovernanceIntegrity } from "@/api/governance";
import {
    MAX_EVIDENCE_KEY_LENGTH,
    MAX_EVIDENCE_REFERENCE_LENGTH,
    MAX_OPERABLE_STAGE,
    familyHasOrderAuthority,
    formatTimestamp,
    historyRows,
    missingEvidenceKeys,
    promotionTarget,
    revocationTargets,
    stageLabel,
} from "@/utils/familyGovernance";
import type { ActionOutcome, GovernanceAction, GovernanceEvent, GovernanceFamily } from "@/utils/familyGovernance";

function Field({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div>
            <div className="admin-metric-label mb-1">{label}</div>
            <div className="break-words text-sm" style={{ color: "var(--admin-text-primary)" }}>{children}</div>
        </div>
    );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
    return (
        <div className="admin-card space-y-4">
            <h2 className="text-lg font-semibold" style={{ color: "var(--admin-text-primary)" }}>{title}</h2>
            {children}
        </div>
    );
}

const MUTED = { color: "var(--admin-text-secondary)" } as const;
const ACTION_BUTTON = "admin-btn disabled:cursor-not-allowed disabled:opacity-50";

function HistoryTable({ events }: { events: GovernanceEvent[] }) {
    if (events.length === 0) {
        return <div className="admin-empty-state">No governance record exists for this family.</div>;
    }
    return (
        <div className="overflow-x-auto">
            <table className="admin-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Time</th>
                        <th>Event</th>
                        <th>Stage change</th>
                        <th>Actor</th>
                        <th>Reason</th>
                        <th>Record</th>
                    </tr>
                </thead>
                <tbody>
                    {historyRows(events).map((row) => (
                        <tr key={`${row.seq}-${row.event_id}`} className="align-top">
                            <td className="font-mono text-xs">{row.seq}</td>
                            <td className="whitespace-nowrap text-xs">{formatTimestamp(row.recorded_at)}</td>
                            <td className="font-mono text-xs">{row.event_type}</td>
                            <td className="whitespace-nowrap font-mono text-xs">{row.stage_change || "none"}</td>
                            <td className="min-w-[14rem] break-all font-mono text-xs">{row.actor || "not recorded"}</td>
                            <td className="min-w-[12rem] text-xs">
                                {row.reason || <span style={MUTED}>none recorded</span>}
                                {row.note && <div className="mt-1 break-all font-mono" style={MUTED}>{row.note}</div>}
                            </td>
                            <td className="min-w-[11rem] break-all font-mono text-xs">
                                {row.event_id}
                                {row.request_id && <div style={MUTED}>request {row.request_id}</div>}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

function FamilyView({ family, open }: { family: GovernanceFamily; open: (action: GovernanceAction) => void }) {
    const familyId = family.family_id;
    const stage = family.governance_stage;
    const target = promotionTarget(stage);
    const pending = family.pending_request;
    const missingKeys = missingEvidenceKeys(family);
    const revocable = revocationTargets(stage);
    const authority = familyHasOrderAuthority(family);
    // Every action on a family needs it to be registered; the API answers FAMILY_NOT_REGISTERED otherwise.
    const usable = family.registered;

    const [evidenceKey, setEvidenceKey] = useState("");
    const [evidenceReference, setEvidenceReference] = useState("");
    const [evidenceValue, setEvidenceValue] = useState(true);
    const [revokeTo, setRevokeTo] = useState("M0");

    const evidenceReady = usable && evidenceKey.trim() !== "" && evidenceReference.trim() !== "";

    return (
        <>
            <div
                role={authority ? "alert" : "status"}
                className={`rounded-xl border px-5 py-4 ${authority ? "border-red-500/60 bg-red-500/15 text-red-100" : "border-green-500/40 bg-green-500/10 text-green-100"}`}
            >
                <div className="text-lg font-bold">
                    {authority ? "This family HAS order authority" : "This family has no order authority"}
                </div>
                <div className="mt-1 text-sm">
                    Allowed modes: {family.allowed_modes.length > 0 ? family.allowed_modes.join(", ") : "none"}.{" "}
                    {authority
                        ? "The engine reports that it may submit orders."
                        : "It may not submit an order to any account, demo or live."}
                </div>
            </div>

            {!usable && (
                <div role="status" className="rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    This family is defined in the code but not registered in the governance log. Nothing can be recorded for
                    it until the code-defined families are registered on the{" "}
                    <Link to="/admin/governance" className="underline">Family governance</Link> page.
                </div>
            )}

            <Section title="State">
                <div className="grid grid-cols-2 gap-6 md:grid-cols-4">
                    <Field label="Certification">
                        <CertificationBadge status={family.certification_status} />
                        {family.certification_reason && <div className="mt-1 break-words font-mono text-xs">{family.certification_reason}</div>}
                    </Field>
                    <Field label="Governance stage">{stageLabel(stage)}</Field>
                    <Field label="Kill switch"><KillSwitchBadge state={family.kill_switch_state} /></Field>
                    <Field label="Registered / enabled">
                        <Badge tone={family.registered ? "info" : "warning"}>{family.registered ? "Registered" : "Not registered"}</Badge>{" "}
                        <Badge tone={family.disabled ? "danger" : "info"}>{family.disabled ? "Disabled" : "Enabled"}</Badge>
                    </Field>
                </div>
                <div className="grid gap-6 md:grid-cols-2">
                    <Field label="Blockers (why it may not execute)">
                        <CodeList codes={family.blockers} empty="none reported" />
                    </Field>
                    <Field label="Research register">
                        {family.research_status ?? "no status"}
                        {family.failure_reasons.length > 0 && <CodeList codes={family.failure_reasons} empty="" />}
                    </Field>
                </div>
            </Section>

            <Section title="Promotion">
                <div className="grid gap-6 md:grid-cols-2">
                    <Field label="Next stage">
                        {family.next_stage ? (
                            <>
                                <div>{stageLabel(family.next_stage.stage)} is missing:</div>
                                <CodeList codes={family.next_stage.missing} empty="nothing the registry can see (the engine decides at approval)" />
                            </>
                        ) : target === null ? (
                            `None: this interface operates no stage above ${MAX_OPERABLE_STAGE}.`
                        ) : (
                            "Not reported for an unregistered family."
                        )}
                    </Field>
                    <Field label="Pending request">
                        {pending ? (
                            <div className="space-y-1">
                                <div className="font-mono text-xs">{pending.request_id}</div>
                                <div>{pending.from_stage ?? "?"} to {pending.to_stage ?? "?"}</div>
                                <div className="text-xs" style={MUTED}>
                                    requested by <span className="break-all font-mono">{pending.requested_by ?? "unknown"}</span> at {formatTimestamp(pending.requested_at)}
                                </div>
                                <div className="text-xs">Reason: {pending.reason ?? "none recorded"}</div>
                            </div>
                        ) : "none"}
                    </Field>
                </div>
                <div className="flex flex-wrap items-center gap-3">
                    {pending && pending.from_stage && pending.to_stage && (
                        <>
                            <button
                                type="button"
                                className={`${ACTION_BUTTON} admin-btn-primary`}
                                disabled={!usable}
                                onClick={() => open({ kind: "approve", familyId, requestId: pending.request_id, fromStage: pending.from_stage ?? stage, toStage: pending.to_stage ?? "" })}
                            >
                                Approve the pending request
                            </button>
                            <button
                                type="button"
                                className={`${ACTION_BUTTON} admin-btn-secondary`}
                                disabled={!usable}
                                onClick={() => open({ kind: "reject", familyId, requestId: pending.request_id, fromStage: stage, toStage: pending.to_stage ?? "" })}
                            >
                                Reject the pending request
                            </button>
                        </>
                    )}
                    {target === null ? (
                        <span className="text-sm" style={MUTED}>
                            No promotion can be requested from {stage} here: nothing above {MAX_OPERABLE_STAGE} is operated by this interface.
                        </span>
                    ) : pending ? (
                        <span className="text-sm" style={MUTED}>
                            One request at a time: approve or reject the pending request before another is recorded.
                        </span>
                    ) : (
                        <button
                            type="button"
                            className={`${ACTION_BUTTON} admin-btn-primary`}
                            disabled={!usable}
                            onClick={() => open({ kind: "request_promotion", familyId, fromStage: stage, toStage: target })}
                        >
                            Request promotion to {target}
                        </button>
                    )}
                </div>
                <div className="flex flex-wrap items-end gap-3 border-t pt-4" style={{ borderColor: "var(--admin-border-color)" }}>
                    <div>
                        <label htmlFor="governance-revoke-to" className="admin-metric-label mb-1 block">Revoke to</label>
                        <select
                            id="governance-revoke-to"
                            value={revocable.includes(revokeTo) ? revokeTo : "M0"}
                            onChange={(event) => setRevokeTo(event.target.value)}
                            className="admin-input"
                            style={{ width: "auto" }}
                        >
                            {revocable.map((option) => <option key={option} value={option}>{option}</option>)}
                        </select>
                    </div>
                    <button
                        type="button"
                        className={`${ACTION_BUTTON} admin-btn-danger`}
                        disabled={!usable}
                        onClick={() => open({ kind: "revoke", familyId, fromStage: stage, toStage: revocable.includes(revokeTo) ? revokeTo : "M0" })}
                    >
                        Revoke promotion
                    </button>
                    <button
                        type="button"
                        className={`${ACTION_BUTTON} admin-btn-danger`}
                        disabled={!usable}
                        onClick={() => open({ kind: "revoke_certification", familyId, fromStage: stage })}
                    >
                        Revoke certification
                    </button>
                </div>
            </Section>

            <Section title="Evidence">
                {family.recorded_evidence.length === 0 ? (
                    <div className="text-sm" style={MUTED}>No evidence is recorded for this family.</div>
                ) : (
                    <div className="overflow-x-auto">
                        <table className="admin-table">
                            <thead>
                                <tr>
                                    <th>Key</th>
                                    <th>Value</th>
                                    <th>Reference</th>
                                    <th>Recorded by</th>
                                    <th>Time</th>
                                </tr>
                            </thead>
                            <tbody>
                                {family.recorded_evidence.map((item) => (
                                    <tr key={item.key} className="align-top">
                                        <td className="font-mono text-xs">{item.key}</td>
                                        <td className="font-mono text-xs">{JSON.stringify(item.value ?? null)}</td>
                                        <td className="min-w-[12rem] break-all text-xs">{item.reference ?? "none"}</td>
                                        <td className="min-w-[14rem] break-all font-mono text-xs">{item.actor_ref ?? "not recorded"}</td>
                                        <td className="whitespace-nowrap text-xs">{formatTimestamp(item.recorded_at)}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
                <div className="grid gap-3 md:grid-cols-4">
                    <div>
                        <label htmlFor="governance-evidence-key" className="admin-metric-label mb-1 block">Requirement key</label>
                        <input
                            id="governance-evidence-key"
                            list="governance-evidence-keys"
                            value={evidenceKey}
                            onChange={(event) => setEvidenceKey(event.target.value)}
                            maxLength={MAX_EVIDENCE_KEY_LENGTH}
                            className="admin-input font-mono"
                            placeholder={missingKeys[0] ?? "requirement key"}
                            autoComplete="off"
                            spellCheck={false}
                        />
                        <datalist id="governance-evidence-keys">
                            {missingKeys.map((key) => <option key={key} value={key} />)}
                        </datalist>
                    </div>
                    <div className="md:col-span-2">
                        <label htmlFor="governance-evidence-reference" className="admin-metric-label mb-1 block">Reference (where the evidence is)</label>
                        <input
                            id="governance-evidence-reference"
                            value={evidenceReference}
                            onChange={(event) => setEvidenceReference(event.target.value)}
                            maxLength={MAX_EVIDENCE_REFERENCE_LENGTH}
                            className="admin-input"
                            placeholder="a file, a run id or a report"
                            autoComplete="off"
                        />
                    </div>
                    <div>
                        <label htmlFor="governance-evidence-value" className="admin-metric-label mb-1 block">Value</label>
                        <select
                            id="governance-evidence-value"
                            value={evidenceValue ? "true" : "false"}
                            onChange={(event) => setEvidenceValue(event.target.value === "true")}
                            className="admin-input"
                        >
                            <option value="true">true (the evidence holds)</option>
                            <option value="false">false (withdraw this key)</option>
                        </select>
                    </div>
                </div>
                <button
                    type="button"
                    className={`${ACTION_BUTTON} admin-btn-primary`}
                    disabled={!evidenceReady}
                    onClick={() => open({ kind: "evidence", familyId, key: evidenceKey, reference: evidenceReference, value: evidenceValue })}
                >
                    Record evidence
                </button>
            </Section>

            <Section title="Stop and disable">
                <p className="text-sm" style={MUTED}>
                    The family kill switch stops new entries and increases from this family on every account; its open
                    positions follow the registered policy ({family.kill_switch_position_policy ?? "not stated"}). Disabling
                    stops the family from being evaluated at all. Both are always possible and need no evidence.
                </p>
                <div className="flex flex-wrap gap-3">
                    {family.kill_switch_state !== "ON" && (
                        <button
                            type="button"
                            className={`${ACTION_BUTTON} admin-btn-danger`}
                            disabled={!usable}
                            onClick={() => open({ kind: "family_kill_switch", familyId, enabled: true, positionPolicy: family.kill_switch_position_policy })}
                        >
                            Turn family kill switch ON
                        </button>
                    )}
                    {family.kill_switch_state !== "OFF" && (
                        <button
                            type="button"
                            className={`${ACTION_BUTTON} admin-btn-secondary`}
                            disabled={!usable}
                            onClick={() => open({ kind: "family_kill_switch", familyId, enabled: false, positionPolicy: family.kill_switch_position_policy })}
                        >
                            Turn family kill switch OFF
                        </button>
                    )}
                    {family.disabled ? (
                        <button type="button" className={`${ACTION_BUTTON} admin-btn-secondary`} disabled={!usable} onClick={() => open({ kind: "family_disable", familyId, disabled: false })}>
                            Enable family
                        </button>
                    ) : (
                        <button type="button" className={`${ACTION_BUTTON} admin-btn-danger`} disabled={!usable} onClick={() => open({ kind: "family_disable", familyId, disabled: true })}>
                            Disable family
                        </button>
                    )}
                </div>
            </Section>

            <Section title="Definition">
                {family.description && <p className="text-sm" style={MUTED}>{family.description}</p>}
                <div className="grid grid-cols-2 gap-6 md:grid-cols-4">
                    <Field label="Version">{family.family_version ?? "none"}</Field>
                    <Field label="Mandate">{family.mandate_id ?? "none registered"}</Field>
                    <Field label="Hypothesis">{family.hypothesis_id ?? "none registered"}</Field>
                    <Field label="Admission route">{family.admission_route ?? "not stated"}</Field>
                    <Field label="Timeframe">{family.timeframe ?? "not stated"}</Field>
                    <Field label="Direction">{family.trading_direction ?? "not stated"}</Field>
                    <Field label="Position model">{family.position_model ?? "not stated"}</Field>
                    <Field label="Markets">{family.supported_markets.join(", ") || "not stated"}</Field>
                </div>
                <div className="grid gap-6 md:grid-cols-2">
                    <Field label="Entry">{family.entry_contract ?? "not stated"}</Field>
                    <Field label="Exit">{family.exit_contract ?? "not stated"}</Field>
                    <Field label="Rule hash"><span className="break-all font-mono text-xs">{family.rule_hash ?? "none"}</span></Field>
                    <Field label="Evidence references"><CodeList codes={family.evidence_reference} empty="none" /></Field>
                </div>
            </Section>
        </>
    );
}

/** One family: its state, every action the governance API offers for it, and its whole history. */
export default function FamilyGovernanceDetail() {
    const { familyId = "" } = useParams<{ familyId: string }>();
    const queryClient = useQueryClient();

    const familyQuery = useQuery({
        queryKey: [...GOVERNANCE_QUERY_KEY, "family", familyId],
        queryFn: () => getGovernanceFamily(familyId),
        retry: false,
    });
    const integrityQuery = useQuery({
        queryKey: [...GOVERNANCE_QUERY_KEY, "integrity"],
        queryFn: getGovernanceIntegrity,
        retry: false,
    });
    // A failed reload never leaves an older answer on screen as if it were current.
    const detail = familyQuery.isError ? undefined : familyQuery.data;
    const integrity = integrityQuery.isError ? undefined : integrityQuery.data;

    const [action, setAction] = useState<GovernanceAction | null>(null);
    const [notice, setNotice] = useState<string | null>(null);

    const open = (next: GovernanceAction) => {
        setNotice(null);
        setAction(next);
    };
    const recorded = (outcome: ActionOutcome) => {
        setNotice(outcome.summary);
        setAction(null);
    };
    const reload = () => queryClient.invalidateQueries({ queryKey: GOVERNANCE_QUERY_KEY });
    const fetching = familyQuery.isFetching || integrityQuery.isFetching;

    return (
        <AdminLayout>
            <div className="space-y-6">
                <div className="flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <Link to="/admin/governance" className="mb-2 flex items-center gap-1 text-sm underline" style={MUTED}>
                            <ArrowLeft className="h-4 w-4" />
                            Family governance
                        </Link>
                        <h1 className="break-all font-mono text-2xl font-bold" style={{ color: "var(--admin-text-primary)" }}>{familyId}</h1>
                    </div>
                    <button type="button" className="admin-btn admin-btn-secondary flex items-center gap-2 text-sm" onClick={reload} disabled={fetching}>
                        <RefreshCw className={`h-4 w-4 ${fetching ? "animate-spin" : ""}`} />
                        Reload
                    </button>
                </div>

                {familyQuery.isError && (
                    <ErrorPanel title="This family could not be read: its state is unknown." error={familyQuery.error} />
                )}
                {!detail && !familyQuery.isError && <div className="admin-empty-state">Loading the family.</div>}

                {notice && (
                    <div role="status" className="flex items-start justify-between gap-3 rounded-lg border border-green-500/30 bg-green-500/10 px-4 py-3 text-sm text-green-200">
                        <span className="break-words">{notice}</span>
                        <button type="button" onClick={() => setNotice(null)} aria-label="Dismiss">
                            <X className="h-4 w-4" />
                        </button>
                    </div>
                )}

                {detail && (
                    <>
                        <FamilyView family={detail.family} open={open} />
                        <Section title="History">
                            <IntegrityLine log={integrity} error={integrityQuery.error} loading={integrityQuery.isPending} />
                            <p className="text-xs" style={MUTED}>
                                Every record of this family, oldest first, as the append-only log holds it. The integrity result
                                is for the whole log (all families).
                            </p>
                            <HistoryTable events={detail.history} />
                        </Section>
                    </>
                )}

                <CannotDoCard />

                <GovernanceActionDialog action={action} onClose={() => setAction(null)} onRecorded={recorded} onSettled={reload} />
            </div>
        </AdminLayout>
    );
}
