import { useEffect, useState } from "react";
import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { FlaskConical, RefreshCw, TriangleAlert, X } from "lucide-react";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { ExperimentalDemoActionDialog } from "@/components/admin/governance/ExperimentalDemoActionDialog";
import { Badge, CertificationBadge, CodeList, ErrorPanel } from "@/components/admin/governance/shared";
import type { BadgeTone } from "@/components/admin/governance/shared";
import { GOVERNANCE_QUERY_KEY, getGovernanceRegistry } from "@/api/governance";
import {
    EXPERIMENTAL_DEMO_QUERY_KEY,
    getExperimentalDemo,
    getMarketEntryPolicies,
    getMarketEntryPolicyHistory,
} from "@/api/experimentalDemo";
import { formatTimestamp, stageLabel } from "@/utils/familyGovernance";
import type { GovernanceFamily } from "@/utils/familyGovernance";
import {
    ALL_SYMBOLS,
    DEFAULT_GRANT_LIMITS,
    DEFAULT_POLICY_DRAFT,
    EMPTY_GRANT_DRAFT,
    EXPERIMENTAL_DEMO_ROUTE,
    MARKET_POLICY_ROUTE,
    MAX_POLICY_APPROVAL_REF_LENGTH,
    MAX_TEXT_LENGTH,
    MIN_DURATION_HOURS,
    MIN_ENTRY_NOTIONAL_USDT,
    POLICY_BOUNDS,
    acknowledgementMatches,
    authorizationAnomalies,
    buildGrantRequest,
    buildPolicyRequest,
    familyEligibilityNotes,
    formatRemaining,
    integrityLabel,
    isAccountId,
} from "@/utils/experimentalDemo";
import type {
    DemoAction,
    DemoActionOutcome,
    ExperimentalAuthorization,
    GrantDraft,
    LedgerIntegrity,
    MarketEntryPolicy,
    PolicyDraft,
} from "@/utils/experimentalDemo";

const PRIMARY = { color: "var(--admin-text-primary)" } as const;
const SECONDARY = { color: "var(--admin-text-secondary)" } as const;
const BUTTON_DISABLED = "disabled:cursor-not-allowed disabled:opacity-50";

/** The browser clock, read only in event handlers (when an operator opens a confirmation). */
function currentTimeMs(): number {
    return Date.now();
}

function stateTone(state: string): BadgeTone {
    if (state === "ACTIVE") return "danger";
    if (state === "REVOKED" || state === "EXPIRED") return "info";
    return "warning";
}

/** A clock that ticks, so that remaining times count down between reloads. */
function useNow(intervalMs: number): number {
    const [now, setNow] = useState(() => Date.now());
    useEffect(() => {
        const timer = window.setInterval(() => setNow(Date.now()), intervalMs);
        return () => window.clearInterval(timer);
    }, [intervalMs]);
    return now;
}

function LedgerIntegrityLine({ label, integrity, loading }: {
    label: string;
    integrity: LedgerIntegrity | undefined;
    loading: boolean;
}) {
    if (integrity === undefined) {
        return (
            <div className="text-sm" style={SECONDARY}>
                {label}: <Badge tone="warning">{loading ? "CHECKING" : "UNKNOWN"}</Badge>
            </div>
        );
    }
    return (
        <div className="text-sm" style={{ color: integrity.ok ? "var(--admin-text-secondary)" : "var(--admin-red)" }}>
            {label}: <Badge tone={integrity.ok ? "success" : "danger"}>{integrityLabel(integrity)}</Badge>
            <span className="ml-2 font-mono text-xs">{integrity.reason}</span>
            {!integrity.ok && (
                <span className="ml-2">A record was changed, removed or reordered. Do not rely on the states shown until this is investigated.</span>
            )}
        </div>
    );
}

function Field({ id, label, hint, children }: { id: string; label: string; hint?: ReactNode; children: ReactNode }) {
    return (
        <div>
            <label htmlFor={id} className="admin-metric-label mb-1 block">{label}</label>
            {children}
            {hint && <div className="mt-1 text-xs" style={SECONDARY}>{hint}</div>}
        </div>
    );
}

function Problems({ problems }: { problems: string[] }) {
    if (problems.length === 0) return null;
    return (
        <div role="alert" className="rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
            <div className="font-semibold">Not sent. Correct the following first:</div>
            <ul className="mt-1 list-disc space-y-0.5 pl-5">
                {problems.map((problem) => <li key={problem}>{problem}</li>)}
            </ul>
        </div>
    );
}

function Banner() {
    return (
        <div role="note" className="rounded-xl border border-amber-500/60 bg-amber-500/10 px-5 py-4 text-amber-100">
            <div className="flex items-center gap-2 text-lg font-bold">
                <TriangleAlert className="h-5 w-5" />
                EXPERIMENTAL DEMO: an exception for one DEMO account, not a certification
            </div>
            <ul className="mt-2 list-disc space-y-1 pl-5 text-sm">
                <li><strong>DEMO accounts only.</strong> The engine verifies the account is a connected DEMO account on the venue&apos;s DEMO host, and re-checks it before every order. There is no LIVE form.</li>
                <li><strong>Not research certification.</strong> The family keeps its research verdict (for example FAILED). Nothing here is evidence for certification or for the holdout.</li>
                <li><strong>Does not promote the family.</strong> Its governance stage (for example M0) is unchanged.</li>
                <li><strong>No live authority</strong> of any kind is granted or implied.</li>
                <li><strong>Expires automatically</strong> (at most 336 hours) and can be revoked at any time.</li>
                <li><strong>Every other safety gate still applies:</strong> registration and rule hash, kill switches, venue certification, account consent, the DEMO order switch, risk and sizing, market-data freshness, the market-entry (L-08) policy below, the account fence and stop protection.</li>
                <li><strong>All activity is labelled EXPERIMENTAL DEMO</strong>: every entry is recorded on the authorization with its trade plan, attempt and notional.</li>
            </ul>
        </div>
    );
}

function AuthorizationRow({ grant, elapsedSeconds, onRevoke }: {
    grant: ExperimentalAuthorization;
    elapsedSeconds: number;
    onRevoke: (grant: ExperimentalAuthorization) => void;
}) {
    const anomalies = authorizationAnomalies(grant);
    const remaining = grant.state === "ACTIVE" && grant.seconds_remaining !== null
        ? Math.max(0, grant.seconds_remaining - elapsedSeconds)
        : null;
    return (
        <tr className="align-top">
            <td>
                <Badge tone={stateTone(grant.state)}>{grant.state}</Badge>
                <div className="mt-1"><Badge tone="warning">{grant.label ?? "EXPERIMENTAL_DEMO"}</Badge></div>
                <div className="mt-1 break-all font-mono text-xs" style={SECONDARY}>{grant.authorization_id}</div>
                {anomalies.length > 0 && (
                    <div className="mt-1 text-xs text-red-300">Unexpected: {anomalies.join("; ")}</div>
                )}
            </td>
            <td>
                <div className="break-all font-mono text-xs">{grant.account_id}</div>
                <div className="mt-1 text-xs" style={SECONDARY}>
                    {grant.environment ?? "?"}{grant.venue ? ` · ${grant.venue}` : ""}
                </div>
                {grant.base_url && <div className="break-all text-xs" style={SECONDARY}>{grant.base_url}</div>}
            </td>
            <td>
                <div className="font-mono text-xs">{grant.family_id}</div>
                <div className="mt-1 flex flex-wrap items-center gap-1 text-xs">
                    {grant.research_status_at_grant ? <CertificationBadge status={grant.research_status_at_grant} /> : <span>?</span>}
                    <span style={SECONDARY}>{grant.stage_at_grant ?? "?"}</span>
                </div>
            </td>
            <td className="font-mono text-xs">{grant.max_entry_notional_usdt ?? "?"} USDT</td>
            <td className="text-xs">
                {grant.state === "ACTIVE" ? (
                    <div className="font-semibold">{formatRemaining(remaining)}</div>
                ) : (
                    <div style={SECONDARY}>{grant.state === "REVOKED" ? "revoked" : "expired"}</div>
                )}
                <div style={SECONDARY}>granted {formatTimestamp(grant.granted_at)}</div>
                <div style={SECONDARY}>expires {formatTimestamp(grant.expires_at)}</div>
                {grant.revoked_at !== null && (
                    <div className="mt-1" style={SECONDARY}>
                        revoked {formatTimestamp(grant.revoked_at)} by <span className="font-mono">{grant.revoked_by ?? "?"}</span>
                        {grant.revoke_reason ? `: ${grant.revoke_reason}` : ""}
                    </div>
                )}
            </td>
            <td className="text-xs">
                <div className="break-all font-mono">{grant.granted_by ?? "?"}</div>
                <div className="mt-1 break-words" style={SECONDARY}>Approval: {grant.approval_ref ?? "?"}</div>
                {grant.reason && <div className="mt-1 break-words" style={SECONDARY}>Reason: {grant.reason}</div>}
            </td>
            <td className="text-xs">
                <div className="font-semibold">{grant.entries_claimed}</div>
                {grant.entries.length > 0 && (
                    <details className="mt-1">
                        <summary className="cursor-pointer" style={SECONDARY}>entries</summary>
                        <ul className="mt-1 space-y-1">
                            {grant.entries.map((entry, index) => (
                                <li key={`${entry.attempt_id ?? ""}-${index}`} className="font-mono">
                                    {entry.symbol ?? "?"} {entry.side ?? "?"} {entry.notional_usdt ?? "?"} USDT
                                    <div style={SECONDARY}>{formatTimestamp(entry.recorded_at)} · plan {entry.trade_plan_id ?? "?"}</div>
                                </li>
                            ))}
                        </ul>
                    </details>
                )}
            </td>
            <td>
                {grant.state !== "REVOKED" && (
                    <button type="button" className="admin-btn admin-btn-danger text-sm" onClick={() => onRevoke(grant)}>
                        Revoke
                    </button>
                )}
            </td>
        </tr>
    );
}

function termsLine(policy: MarketEntryPolicy): string {
    const t = policy.terms;
    return [
        `participation ≤ ${t.max_participation ?? "?"}`,
        `slippage ≤ ${t.max_slippage_bps ?? "?"} bps`,
        `spread ≤ ${t.max_spread_bps ?? "?"} bps`,
        `depth age ≤ ${t.max_depth_age_ms ?? "?"} ms`,
        `levels ≥ ${t.min_levels ?? "?"}`,
        `cap ${t.absolute_cap_notional ?? "?"}`,
    ].join(" · ");
}

function PolicyRow({ policy, onRevoke }: { policy: MarketEntryPolicy; onRevoke: (policy: MarketEntryPolicy) => void }) {
    const extraTerms = Object.keys(policy.terms.other).length > 0;
    const extraFields = Object.keys(policy.other).length > 0;
    return (
        <tr className="align-top">
            <td>
                <Badge tone={stateTone(policy.state)}>{policy.state}</Badge>
                {policy.calibrated === true && <div className="mt-1"><Badge tone="danger">CALIBRATED (unexpected)</Badge></div>}
                <div className="mt-1 break-all font-mono text-xs" style={SECONDARY}>{policy.policy_id}</div>
            </td>
            <td className="text-xs">
                <div className="font-mono">{policy.venue ?? "?"}</div>
                <div className="font-mono font-semibold">{policy.symbol === ALL_SYMBOLS ? "* (all symbols)" : policy.symbol ?? "?"}</div>
                <div style={SECONDARY}>{policy.environment ?? "?"}{policy.scope ? ` · ${policy.scope}` : ""}</div>
            </td>
            <td className="break-all font-mono text-xs">{policy.account_id ?? <span style={SECONDARY}>not named</span>}</td>
            <td className="text-xs">
                <div>{termsLine(policy)}</div>
                {extraTerms && <pre className="mt-1 whitespace-pre-wrap break-all font-mono">{JSON.stringify(policy.terms.other)}</pre>}
                {extraFields && <pre className="mt-1 whitespace-pre-wrap break-all font-mono" style={SECONDARY}>{JSON.stringify(policy.other)}</pre>}
            </td>
            <td className="text-xs">
                <div>expires {formatTimestamp(policy.terms.expires_at_ms)}</div>
                <div style={SECONDARY}>recorded {formatTimestamp(policy.recorded_at)}</div>
                {policy.revoked && (
                    <div className="mt-1" style={SECONDARY}>
                        revoked {formatTimestamp(policy.revoked.at)} by <span className="font-mono">{policy.revoked.by ?? "?"}</span>
                        {policy.revoked.reason ? `: ${policy.revoked.reason}` : ""}
                    </div>
                )}
            </td>
            <td className="text-xs">
                <div className="break-all font-mono">{policy.recorded_by ?? "?"}</div>
                <div className="mt-1 break-words" style={SECONDARY}>Approval: {policy.approval_ref ?? "?"}</div>
                {policy.reason && <div className="mt-1 break-words" style={SECONDARY}>Reason: {policy.reason}</div>}
            </td>
            <td>
                {policy.state !== "REVOKED" && (
                    <button type="button" className="admin-btn admin-btn-danger text-sm" onClick={() => onRevoke(policy)}>
                        Revoke
                    </button>
                )}
            </td>
        </tr>
    );
}

function GrantForm({ families, registryReady, limits, onReview }: {
    families: GovernanceFamily[];
    registryReady: boolean;
    limits: { maxDurationHours: number; maxEntryNotional: number };
    onReview: (action: DemoAction) => void;
}) {
    const [draft, setDraft] = useState<GrantDraft>(EMPTY_GRANT_DRAFT);
    const [problems, setProblems] = useState<string[]>([]);
    const set = (key: keyof GrantDraft) => (event: { target: { value: string } }) => setDraft({ ...draft, [key]: event.target.value });

    const family = families.find((item) => item.family_id === draft.familyId);
    const account = draft.accountId.trim();
    const typedStatus = draft.acknowledgedStatus.trim();
    const mismatch = family !== undefined && typedStatus !== "" && !acknowledgementMatches(typedStatus, family);

    const review = () => {
        const built = buildGrantRequest(draft, family, limits);
        if (!built.ok) {
            setProblems(built.problems);
            return;
        }
        setProblems([]);
        if (family) onReview({ kind: "grant", body: built.body, family, requestedAtMs: currentTimeMs() });
    };

    return (
        <div className="admin-card space-y-4">
            <div>
                <h2 className="text-xl font-semibold" style={PRIMARY}>Grant an experimental DEMO authorization</h2>
                <p className="mt-1 text-sm" style={SECONDARY}>
                    One family, one verified DEMO account, for a bounded time. Environment is fixed to DEMO.
                </p>
            </div>
            <div className="grid gap-4 md:grid-cols-2">
                <Field id="xdemo-family" label="Strategy family" hint={!registryReady ? "The family registry is not available: no family can be selected." : undefined}>
                    <select
                        id="xdemo-family"
                        value={draft.familyId}
                        onChange={set("familyId")}
                        className="admin-input"
                        disabled={!registryReady}
                    >
                        <option value="">Select a family</option>
                        {families.map((item) => (
                            <option key={item.family_id} value={item.family_id}>
                                {item.family_id} · {item.certification_status} · {item.governance_stage}
                            </option>
                        ))}
                    </select>
                </Field>
                <Field id="xdemo-account" label="DEMO account id" hint="The customer's broker account id (brk_...). The engine verifies it is a connected DEMO account.">
                    <input
                        id="xdemo-account"
                        value={draft.accountId}
                        onChange={set("accountId")}
                        className="admin-input font-mono"
                        placeholder="brk_..."
                        autoComplete="off"
                        spellCheck={false}
                    />
                    {account !== "" && !isAccountId(account) && (
                        <div className="mt-1 text-xs text-red-300">An account id is letters, digits, &apos;_&apos; and &apos;-&apos; only.</div>
                    )}
                </Field>
            </div>

            {family && (
                <div className="rounded-lg border px-4 py-3 text-sm" style={{ borderColor: "var(--admin-border-color)" }}>
                    <div className="flex flex-wrap items-center gap-2">
                        <span className="font-mono font-semibold">{family.family_id}</span>
                        <span style={SECONDARY}>current research status</span>
                        <CertificationBadge status={family.certification_status} />
                        <span style={SECONDARY}>stage</span>
                        <span>{stageLabel(family.governance_stage)}</span>
                    </div>
                    {family.certification_reason && <div className="mt-1 break-words font-mono text-xs">{family.certification_reason}</div>}
                    <div className="mt-2">
                        <div className="admin-metric-label mb-1">Blockers (unchanged by an experimental authorization except certification and stage)</div>
                        <CodeList inline codes={family.blockers} empty="none reported" />
                    </div>
                    {familyEligibilityNotes(family).map((note) => (
                        <div key={note} className="mt-2 text-xs text-amber-200">{note}</div>
                    ))}
                </div>
            )}

            <div className="grid gap-4 md:grid-cols-3">
                <Field id="xdemo-ack" label="Acknowledged research status" hint="Type the family's current research status exactly as shown above (for example FAILED).">
                    <input
                        id="xdemo-ack"
                        value={draft.acknowledgedStatus}
                        onChange={set("acknowledgedStatus")}
                        className="admin-input font-mono"
                        autoComplete="off"
                        spellCheck={false}
                    />
                    {mismatch && family && (
                        <div role="alert" className="mt-1 text-xs font-semibold text-red-300">
                            Mismatch: the family&apos;s current research status is {family.certification_status}, not {typedStatus.toUpperCase()}.
                        </div>
                    )}
                </Field>
                <Field id="xdemo-duration" label="Duration (hours)" hint={`${MIN_DURATION_HOURS} to ${limits.maxDurationHours} hours; expires automatically.`}>
                    <input
                        id="xdemo-duration"
                        type="number"
                        min={MIN_DURATION_HOURS}
                        max={limits.maxDurationHours}
                        step={1}
                        value={draft.durationHours}
                        onChange={set("durationHours")}
                        className="admin-input"
                    />
                </Field>
                <Field id="xdemo-notional" label="Max entry notional (USDT)" hint={`${MIN_ENTRY_NOTIONAL_USDT} to ${limits.maxEntryNotional} USDT per entry.`}>
                    <input
                        id="xdemo-notional"
                        type="number"
                        min={MIN_ENTRY_NOTIONAL_USDT}
                        max={limits.maxEntryNotional}
                        step="any"
                        value={draft.maxEntryNotional}
                        onChange={set("maxEntryNotional")}
                        className="admin-input"
                    />
                </Field>
            </div>
            <div className="grid gap-4 md:grid-cols-2">
                <Field id="xdemo-approval" label="Owner authorization reference" hint={`3 to ${MAX_TEXT_LENGTH} characters: where the owner's authorization is recorded.`}>
                    <input
                        id="xdemo-approval"
                        value={draft.approvalRef}
                        onChange={set("approvalRef")}
                        maxLength={MAX_TEXT_LENGTH}
                        className="admin-input"
                        autoComplete="off"
                    />
                </Field>
                <Field id="xdemo-environment" label="Environment">
                    <input id="xdemo-environment" value="DEMO" readOnly disabled className="admin-input font-mono" />
                </Field>
            </div>
            <Field id="xdemo-reason" label="Reason" hint={`3 to ${MAX_TEXT_LENGTH} characters.`}>
                <textarea
                    id="xdemo-reason"
                    value={draft.reason}
                    onChange={set("reason")}
                    maxLength={MAX_TEXT_LENGTH}
                    className="admin-input min-h-[80px]"
                    placeholder="Why is this family being watched on this DEMO account?"
                />
            </Field>

            <Problems problems={problems} />

            <div className="flex flex-wrap gap-3">
                <button type="button" className={`admin-btn admin-btn-danger ${BUTTON_DISABLED}`} disabled={!registryReady} onClick={review}>
                    Review authorization
                </button>
                <button type="button" className="admin-btn admin-btn-secondary" onClick={() => { setDraft(EMPTY_GRANT_DRAFT); setProblems([]); }}>
                    Clear
                </button>
            </div>
        </div>
    );
}

function PolicyForm({ onReview }: { onReview: (action: DemoAction) => void }) {
    const [draft, setDraft] = useState<PolicyDraft>(DEFAULT_POLICY_DRAFT);
    const [problems, setProblems] = useState<string[]>([]);
    const set = (key: keyof PolicyDraft) => (event: { target: { value: string } }) => setDraft({ ...draft, [key]: event.target.value });
    const allSymbols = draft.symbol.trim() === ALL_SYMBOLS;

    const review = () => {
        const built = buildPolicyRequest(draft, currentTimeMs());
        if (!built.ok) {
            setProblems(built.problems);
            return;
        }
        setProblems([]);
        onReview({ kind: "record_policy", body: built.body });
    };

    return (
        <div className="space-y-4 border-t pt-4" style={{ borderColor: "var(--admin-border-color)" }}>
            <h3 className="text-lg font-semibold" style={PRIMARY}>Record a market-entry policy</h3>
            <div className="grid gap-4 md:grid-cols-3">
                <Field id="mep-venue" label="Venue" hint="Venue code of the execution adapter, e.g. BINANCE_USDM.">
                    <input id="mep-venue" value={draft.venue} onChange={set("venue")} className="admin-input font-mono" autoComplete="off" spellCheck={false} />
                </Field>
                <Field id="mep-symbol" label="Symbol" hint="One symbol (BTCUSDT), or * for all symbols: DEMO only, and only with an account id.">
                    <input id="mep-symbol" value={draft.symbol} onChange={set("symbol")} className="admin-input font-mono" placeholder="BTCUSDT or *" autoComplete="off" spellCheck={false} />
                </Field>
                <Field id="mep-account" label={allSymbols ? "Account id (required for *)" : "Account id"} hint="The one account this record permits. Strongly recommended; required for *.">
                    <input id="mep-account" value={draft.accountId} onChange={set("accountId")} className="admin-input font-mono" placeholder="brk_..." autoComplete="off" spellCheck={false} />
                    {draft.accountId.trim() !== "" && !isAccountId(draft.accountId.trim()) && (
                        <div className="mt-1 text-xs text-red-300">An account id is letters, digits, &apos;_&apos; and &apos;-&apos; only.</div>
                    )}
                </Field>
            </div>
            <fieldset>
                <legend className="admin-metric-label mb-1">Environment</legend>
                <div className="flex flex-wrap gap-6 text-sm">
                    <label className="flex items-center gap-2">
                        <input type="radio" name="mep-environment" checked readOnly /> DEMO
                    </label>
                    <label className="flex items-center gap-2 opacity-50" title="Not permitted in this release">
                        <input type="radio" name="mep-environment" disabled /> LIVE (not permitted in this release)
                    </label>
                </div>
            </fieldset>
            <div className="grid gap-4 md:grid-cols-4">
                <Field id="mep-participation" label="Max participation" hint={`> 0 and ≤ ${POLICY_BOUNDS.max_participation.max}`}>
                    <input id="mep-participation" value={draft.maxParticipation} onChange={set("maxParticipation")} className="admin-input font-mono" inputMode="decimal" />
                </Field>
                <Field id="mep-slippage" label="Max slippage (bps)" hint={`> 0 and ≤ ${POLICY_BOUNDS.max_slippage_bps.max}`}>
                    <input id="mep-slippage" value={draft.maxSlippageBps} onChange={set("maxSlippageBps")} className="admin-input font-mono" inputMode="decimal" />
                </Field>
                <Field id="mep-spread" label="Max spread (bps)" hint={`> 0 and ≤ ${POLICY_BOUNDS.max_spread_bps.max}`}>
                    <input id="mep-spread" value={draft.maxSpreadBps} onChange={set("maxSpreadBps")} className="admin-input font-mono" inputMode="decimal" />
                </Field>
                <Field id="mep-depth-age" label="Max depth age (ms)" hint={`${POLICY_BOUNDS.max_depth_age_ms.min} to ${POLICY_BOUNDS.max_depth_age_ms.max}`}>
                    <input id="mep-depth-age" value={draft.maxDepthAgeMs} onChange={set("maxDepthAgeMs")} className="admin-input font-mono" inputMode="numeric" />
                </Field>
                <Field id="mep-levels" label="Min depth levels" hint={`${POLICY_BOUNDS.min_levels.min} to ${POLICY_BOUNDS.min_levels.max}`}>
                    <input id="mep-levels" value={draft.minLevels} onChange={set("minLevels")} className="admin-input font-mono" inputMode="numeric" />
                </Field>
                <Field id="mep-cap" label="Absolute cap notional (USDT)" hint={`> 0 and ≤ ${POLICY_BOUNDS.absolute_cap_notional.max.toLocaleString("en-US")}`}>
                    <input id="mep-cap" value={draft.absoluteCapNotional} onChange={set("absoluteCapNotional")} className="admin-input font-mono" inputMode="decimal" />
                </Field>
                <Field id="mep-expiry" label="Expires in (days)" hint={`${POLICY_BOUNDS.expiry_days.min} to ${POLICY_BOUNDS.expiry_days.max} days from confirmation`}>
                    <input id="mep-expiry" value={draft.expiryDays} onChange={set("expiryDays")} className="admin-input font-mono" inputMode="numeric" />
                </Field>
                <Field id="mep-calibrated" label="Calibrated">
                    <input id="mep-calibrated" value="never" readOnly disabled className="admin-input font-mono" />
                </Field>
            </div>
            <div className="grid gap-4 md:grid-cols-2">
                <Field id="mep-approval" label="Owner approval reference" hint={`3 to ${MAX_POLICY_APPROVAL_REF_LENGTH} characters.`}>
                    <input id="mep-approval" value={draft.approvalRef} onChange={set("approvalRef")} maxLength={MAX_POLICY_APPROVAL_REF_LENGTH} className="admin-input" autoComplete="off" />
                </Field>
                <Field id="mep-reason" label="Reason" hint={`3 to ${MAX_TEXT_LENGTH} characters.`}>
                    <input id="mep-reason" value={draft.reason} onChange={set("reason")} maxLength={MAX_TEXT_LENGTH} className="admin-input" autoComplete="off" />
                </Field>
            </div>

            <Problems problems={problems} />

            <div className="flex flex-wrap gap-3">
                <button type="button" className="admin-btn admin-btn-danger" onClick={review}>Review policy</button>
                <button type="button" className="admin-btn admin-btn-secondary" onClick={() => { setDraft(DEFAULT_POLICY_DRAFT); setProblems([]); }}>
                    Reset to conservative defaults
                </button>
            </div>
        </div>
    );
}

/**
 * Experimental DEMO trading: operator-granted, expiring, revocable permission
 * for an uncertified family to trade one verified DEMO account, and the governed
 * L-08 market-entry policy that permits MARKET entries for one account. Every
 * list is the server's answer, reloaded after each action; every refusal is
 * shown with the engine's own reason code and detail.
 */
export default function ExperimentalDemo() {
    const queryClient = useQueryClient();
    const registryQuery = useQuery({ queryKey: [...GOVERNANCE_QUERY_KEY, "registry"], queryFn: getGovernanceRegistry, retry: false });
    const grantsQuery = useQuery({ queryKey: [...EXPERIMENTAL_DEMO_QUERY_KEY, "grants"], queryFn: getExperimentalDemo, retry: false });
    const policiesQuery = useQuery({ queryKey: [...EXPERIMENTAL_DEMO_QUERY_KEY, "policies"], queryFn: getMarketEntryPolicies, retry: false });
    const historyQuery = useQuery({ queryKey: [...EXPERIMENTAL_DEMO_QUERY_KEY, "policy-history"], queryFn: getMarketEntryPolicyHistory, retry: false });

    // A failed reload never leaves an older answer on screen as if it were current.
    const registry = registryQuery.isError ? undefined : registryQuery.data;
    const grants = grantsQuery.isError ? undefined : grantsQuery.data;
    const policies = policiesQuery.isError ? undefined : policiesQuery.data;
    const history = historyQuery.isError ? undefined : historyQuery.data;

    const now = useNow(30_000);
    const elapsedSeconds = grantsQuery.dataUpdatedAt > 0 ? Math.max(0, Math.floor((now - grantsQuery.dataUpdatedAt) / 1000)) : 0;

    const [action, setAction] = useState<DemoAction | null>(null);
    const [notice, setNotice] = useState<string | null>(null);
    // A form is emptied only after the server confirmed what it sent; a refusal leaves it as typed.
    const [formKeys, setFormKeys] = useState({ grant: 0, policy: 0 });

    const open = (next: DemoAction) => {
        setNotice(null);
        setAction(next);
    };
    const recorded = (outcome: DemoActionOutcome) => {
        setNotice(outcome.summary);
        if (action?.kind === "grant") setFormKeys((keys) => ({ ...keys, grant: keys.grant + 1 }));
        if (action?.kind === "record_policy") setFormKeys((keys) => ({ ...keys, policy: keys.policy + 1 }));
        setAction(null);
    };
    const reload = () => {
        void queryClient.invalidateQueries({ queryKey: EXPERIMENTAL_DEMO_QUERY_KEY });
        void queryClient.invalidateQueries({ queryKey: GOVERNANCE_QUERY_KEY });
    };
    const fetching = registryQuery.isFetching || grantsQuery.isFetching || policiesQuery.isFetching || historyQuery.isFetching;

    const limits = grants
        ? { maxDurationHours: grants.max_duration_hours, maxEntryNotional: grants.max_entry_notional_usdt }
        : DEFAULT_GRANT_LIMITS;

    return (
        <AdminLayout>
            <div className="space-y-6">
                <div className="flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <h1 className="flex items-center gap-2 text-3xl font-bold" style={PRIMARY}>
                            <FlaskConical className="h-7 w-7" />
                            Experimental DEMO trading
                        </h1>
                        <p className="mt-1 text-sm" style={SECONDARY}>
                            Activation and revocation of experimental DEMO authorizations and of the market-entry (L-08) policy.
                            Family certification and stages are on{" "}
                            <Link to="/admin/governance" className="underline">Family Governance</Link>.
                        </p>
                    </div>
                    <button type="button" className="admin-btn admin-btn-secondary flex items-center gap-2 text-sm" onClick={reload} disabled={fetching}>
                        <RefreshCw className={`h-4 w-4 ${fetching ? "animate-spin" : ""}`} />
                        Reload
                    </button>
                </div>

                <Banner />

                {notice && (
                    <div role="status" className="flex items-start justify-between gap-3 rounded-lg border border-green-500/30 bg-green-500/10 px-4 py-3 text-sm text-green-200">
                        <span className="break-words">{notice} The lists below were reloaded from the server.</span>
                        <button type="button" onClick={() => setNotice(null)} aria-label="Dismiss">
                            <X className="h-4 w-4" />
                        </button>
                    </div>
                )}

                {/* ── Authorizations ─────────────────────────────────────────────── */}
                <div className="admin-card">
                    <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                        <div>
                            <h2 className="text-xl font-semibold" style={PRIMARY}>Experimental DEMO authorizations</h2>
                            <div className="mt-1 text-xs" style={SECONDARY}>
                                Limits: at most {limits.maxDurationHours} hours, at most {limits.maxEntryNotional} USDT per entry. Remaining times count down from the last reload.
                            </div>
                        </div>
                        <LedgerIntegrityLine label="Ledger integrity" integrity={grants?.integrity} loading={grantsQuery.isPending} />
                    </div>
                    {grantsQuery.isError && (
                        <ErrorPanel title="The experimental DEMO ledger is UNAVAILABLE: no authorization state is known." error={grantsQuery.error} route={EXPERIMENTAL_DEMO_ROUTE} />
                    )}
                    {grants && grants.authorizations.length === 0 && (
                        <div className="admin-empty-state">No experimental DEMO authorization has been granted.</div>
                    )}
                    {grants && grants.authorizations.length > 0 && (
                        <div className="overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>State</th>
                                        <th>Account</th>
                                        <th>Family / status at grant</th>
                                        <th>Notional cap</th>
                                        <th>Remaining</th>
                                        <th>Granted by / approval</th>
                                        <th>Entries</th>
                                        <th />
                                    </tr>
                                </thead>
                                <tbody>
                                    {grants.authorizations.map((grant) => (
                                        <AuthorizationRow
                                            key={grant.authorization_id}
                                            grant={grant}
                                            elapsedSeconds={elapsedSeconds}
                                            onRevoke={(item) => open({ kind: "revoke_grant", grant: item })}
                                        />
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                    {!grants && !grantsQuery.isError && <div className="admin-empty-state">Loading the authorizations.</div>}
                </div>

                {registryQuery.isError && (
                    <ErrorPanel title="The family registry is UNAVAILABLE: no family can be selected for a grant." error={registryQuery.error} />
                )}
                <GrantForm
                    key={formKeys.grant}
                    families={registry?.families ?? []}
                    registryReady={registry !== undefined}
                    limits={limits}
                    onReview={open}
                />

                {/* ── Market-entry policy (L-08) ─────────────────────────────────── */}
                <div className="admin-card space-y-4">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                        <div>
                            <h2 className="text-xl font-semibold" style={PRIMARY}>Market-entry policy (L-08)</h2>
                            <p className="mt-1 text-sm" style={SECONDARY}>
                                Permits MARKET entries for one account under conservative bounds. One account at a time per market; never
                                calibrated. Without an active policy, MARKET entries are refused (MARKET_FALLBACK_DISABLED_BY_POLICY).
                            </p>
                        </div>
                        <LedgerIntegrityLine label="Ledger integrity" integrity={policies?.integrity} loading={policiesQuery.isPending} />
                    </div>
                    {policiesQuery.isError && (
                        <ErrorPanel title="The market-entry ledger is UNAVAILABLE: no policy state is known." error={policiesQuery.error} route={MARKET_POLICY_ROUTE} />
                    )}
                    {policies && policies.policies.length === 0 && (
                        <div className="admin-empty-state">No market-entry policy has been recorded.</div>
                    )}
                    {policies && policies.policies.length > 0 && (
                        <div className="overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>State</th>
                                        <th>Market</th>
                                        <th>Account</th>
                                        <th>Terms</th>
                                        <th>Expiry</th>
                                        <th>Recorded by / approval</th>
                                        <th />
                                    </tr>
                                </thead>
                                <tbody>
                                    {policies.policies.map((policy) => (
                                        <PolicyRow key={policy.policy_id} policy={policy} onRevoke={(item) => open({ kind: "revoke_policy", policy: item })} />
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                    {!policies && !policiesQuery.isError && <div className="admin-empty-state">Loading the policies.</div>}

                    <PolicyForm key={formKeys.policy} onReview={open} />

                    <details className="border-t pt-4" style={{ borderColor: "var(--admin-border-color)" }}>
                        <summary className="cursor-pointer text-sm font-semibold" style={PRIMARY}>
                            Ledger history{history ? ` (${history.events.length} events)` : ""}
                        </summary>
                        <div className="mt-3 space-y-3">
                            {historyQuery.isError && (
                                <ErrorPanel title="The market-entry ledger history is UNAVAILABLE." error={historyQuery.error} route={MARKET_POLICY_ROUTE} />
                            )}
                            {history && (
                                <>
                                    <LedgerIntegrityLine label="History integrity" integrity={history.integrity} loading={false} />
                                    {history.events.length === 0 ? (
                                        <div className="admin-empty-state">The ledger has no events.</div>
                                    ) : (
                                        <div className="overflow-x-auto">
                                            <table className="admin-table">
                                                <thead>
                                                    <tr>
                                                        <th>Seq</th>
                                                        <th>Event</th>
                                                        <th>Policy</th>
                                                        <th>Market</th>
                                                        <th>Account</th>
                                                        <th>Actor</th>
                                                        <th>Recorded</th>
                                                        <th>Reason / approval</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {history.events.map((event, index) => (
                                                        <tr key={`${event.seq ?? "?"}-${index}`} className="align-top text-xs">
                                                            <td>{event.seq ?? "?"}</td>
                                                            <td className="font-mono">{event.event_type}{event.calibrated === true ? " (CALIBRATED)" : ""}</td>
                                                            <td className="break-all font-mono">{event.policy_id ?? "?"}</td>
                                                            <td className="font-mono">{event.venue ?? "?"} {event.symbol ?? "?"} {event.environment ?? "?"}</td>
                                                            <td className="break-all font-mono">{event.account_id ?? "—"}</td>
                                                            <td className="break-all font-mono">{event.actor_ref ?? "?"}</td>
                                                            <td>{formatTimestamp(event.recorded_at)}</td>
                                                            <td className="break-words">
                                                                {event.reason ?? ""}
                                                                {event.approval_ref ? <div style={SECONDARY}>Approval: {event.approval_ref}</div> : null}
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </>
                            )}
                            {!history && !historyQuery.isError && <div className="admin-empty-state">Loading the history.</div>}
                        </div>
                    </details>
                </div>

                <ExperimentalDemoActionDialog action={action} onClose={() => setAction(null)} onRecorded={recorded} onSettled={reload} />
            </div>
        </AdminLayout>
    );
}
