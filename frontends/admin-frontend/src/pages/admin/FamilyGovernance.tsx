import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Gavel, RefreshCw, X } from "lucide-react";
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
    OrderAuthorityBanner,
} from "@/components/admin/governance/shared";
import { GOVERNANCE_QUERY_KEY, getGovernanceRegistry } from "@/api/governance";
import { familyHasOrderAuthority, isPlainIdentifier, orderAuthority, stageLabel } from "@/utils/familyGovernance";
import type { ActionOutcome, GovernanceAction, GovernanceFamily } from "@/utils/familyGovernance";

const COLUMNS = 6;
const SECONDARY = { color: "var(--admin-text-secondary)" } as const;
/** The first of a family's two rows carries no rule of its own: the pair reads as one entry. */
const JOINED = { borderBottom: "none" } as const;

/** Two rows per family: what it is and may do, then why not more (blockers, what the next stage is missing). */
function FamilyRows({ family }: { family: GovernanceFamily }) {
    const authority = familyHasOrderAuthority(family);
    return (
        <>
            <tr className="align-top">
                <td style={JOINED}>
                    <Link to={`/admin/governance/${encodeURIComponent(family.family_id)}`} className="font-mono text-sm font-semibold underline">
                        {family.family_id}
                    </Link>
                    <div className="mt-1 text-xs" style={SECONDARY}>
                        {family.family_version ? `version ${family.family_version}` : "no version"}
                        {family.timeframe ? ` · ${family.timeframe}` : ""}
                    </div>
                    <div className="mt-1 flex flex-wrap gap-1">
                        {!family.registered && <Badge tone="warning">Not registered</Badge>}
                        {family.disabled && <Badge tone="danger">Disabled</Badge>}
                        {family.pending_request && <Badge tone="info">Request pending: {family.pending_request.to_stage ?? "?"}</Badge>}
                    </div>
                </td>
                <td style={JOINED}>
                    <CertificationBadge status={family.certification_status} />
                    {family.certification_reason && <div className="mt-1 break-words font-mono text-xs">{family.certification_reason}</div>}
                </td>
                <td className="text-sm" style={JOINED}>{stageLabel(family.governance_stage)}</td>
                <td style={JOINED}><CodeList codes={family.allowed_modes} empty="none" /></td>
                <td style={JOINED}><KillSwitchBadge state={family.kill_switch_state} /></td>
                <td style={JOINED}><Badge tone={authority ? "danger" : "info"}>{authority ? "YES" : "None"}</Badge></td>
            </tr>
            <tr className="align-top">
                <td colSpan={COLUMNS} style={{ paddingTop: 0 }}>
                    <div className="grid gap-4 md:grid-cols-2">
                        <div>
                            <div className="admin-metric-label mb-1">Blockers</div>
                            <CodeList inline codes={family.blockers} empty="none reported" />
                        </div>
                        <div>
                            <div className="admin-metric-label mb-1">
                                {family.next_stage ? `Next stage ${family.next_stage.stage} is missing` : "Next stage"}
                            </div>
                            {family.next_stage ? (
                                <CodeList inline codes={family.next_stage.missing} empty="nothing the registry can see (the engine decides at approval)" />
                            ) : (
                                <span className="text-xs" style={SECONDARY}>
                                    {family.registered ? "No further stage is operated by this interface." : "Not reported: register the family first."}
                                </span>
                            )}
                        </div>
                    </div>
                </td>
            </tr>
        </>
    );
}

/**
 * Family governance (Step 3.11): the registry as the engine reports it, the
 * integrity of its log, and the two actions that are not about one family
 * (registration, account kill switch). Everything about one family is on its
 * detail page.
 */
export default function FamilyGovernance() {
    const queryClient = useQueryClient();
    const registryQuery = useQuery({
        queryKey: [...GOVERNANCE_QUERY_KEY, "registry"],
        queryFn: getGovernanceRegistry,
        retry: false,
    });
    // A failed reload never leaves an older answer on screen as if it were current.
    const registry = registryQuery.isError ? undefined : registryQuery.data;

    const [action, setAction] = useState<GovernanceAction | null>(null);
    const [notice, setNotice] = useState<string | null>(null);
    const [accountId, setAccountId] = useState("");

    const account = accountId.trim();
    const accountReady = isPlainIdentifier(account);

    const open = (next: GovernanceAction) => {
        setNotice(null);
        setAction(next);
    };
    const recorded = (outcome: ActionOutcome) => {
        setNotice(outcome.summary);
        setAction(null);
    };
    const reload = () => queryClient.invalidateQueries({ queryKey: GOVERNANCE_QUERY_KEY });

    return (
        <AdminLayout>
            <div className="space-y-6">
                <div className="flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <h1 className="flex items-center gap-2 text-3xl font-bold" style={{ color: "var(--admin-text-primary)" }}>
                            <Gavel className="h-7 w-7" />
                            Family governance
                        </h1>
                        <p className="mt-1 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            Every strategy family, its research certification, its governance stage (M0 to M6) and what it may do.
                        </p>
                    </div>
                    <button
                        type="button"
                        className="admin-btn admin-btn-secondary flex items-center gap-2 text-sm"
                        onClick={() => registryQuery.refetch()}
                        disabled={registryQuery.isFetching}
                    >
                        <RefreshCw className={`h-4 w-4 ${registryQuery.isFetching ? "animate-spin" : ""}`} />
                        Reload
                    </button>
                </div>

                <OrderAuthorityBanner
                    authority={registry ? orderAuthority(registry) : undefined}
                    familyCount={registry ? registry.families.length : 0}
                    loading={registryQuery.isPending}
                />

                {registryQuery.isError && (
                    <ErrorPanel title="The family registry is UNAVAILABLE: the state of every family is unknown." error={registryQuery.error} />
                )}

                {notice && (
                    <div role="status" className="flex items-start justify-between gap-3 rounded-lg border border-green-500/30 bg-green-500/10 px-4 py-3 text-sm text-green-200">
                        <span className="break-words">{notice}</span>
                        <button type="button" onClick={() => setNotice(null)} aria-label="Dismiss">
                            <X className="h-4 w-4" />
                        </button>
                    </div>
                )}

                <div className="admin-card">
                    <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                        <h2 className="text-xl font-semibold" style={{ color: "var(--admin-text-primary)" }}>Families</h2>
                        <IntegrityLine log={registry?.log} loading={registryQuery.isPending} />
                    </div>
                    {registry && registry.families.length === 0 && (
                        <div className="admin-empty-state">The engine reports no family.</div>
                    )}
                    {registry && registry.families.length > 0 && (
                        <div className="overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>Family</th>
                                        <th>Certification</th>
                                        <th>Stage</th>
                                        <th>Allowed modes</th>
                                        <th>Kill switch</th>
                                        <th>Order authority</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {registry.families.map((family) => <FamilyRows key={family.family_id} family={family} />)}
                                </tbody>
                            </table>
                        </div>
                    )}
                    {!registry && !registryQuery.isError && (
                        <div className="admin-empty-state">Loading the registry.</div>
                    )}
                </div>

                <div className="grid gap-6 lg:grid-cols-2">
                    <div className="admin-card space-y-3">
                        <h2 className="text-lg font-semibold" style={{ color: "var(--admin-text-primary)" }}>Register families</h2>
                        <p className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            Records every family the code defines in the governance log. A family must be registered before
                            evidence, a promotion request or a kill switch can be recorded for it. Registration gives stage M0
                            and nothing else: no certification, no order authority.
                        </p>
                        <button type="button" className="admin-btn admin-btn-primary" onClick={() => open({ kind: "register" })}>
                            Register code-defined families
                        </button>
                    </div>

                    <div className="admin-card space-y-3">
                        <h2 className="text-lg font-semibold" style={{ color: "var(--admin-text-primary)" }}>Account kill switch</h2>
                        <p className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                            Stops new entries for one account, from any family. Its open positions keep their protection and
                            their exits. The governance API has no read for account kill switches, so their current state is
                            not shown here: the answer to each change states the state the server recorded.
                        </p>
                        <label htmlFor="governance-account-id" className="block text-sm font-medium" style={{ color: "var(--admin-text-primary)" }}>
                            Account id
                        </label>
                        <input
                            id="governance-account-id"
                            value={accountId}
                            onChange={(event) => setAccountId(event.target.value)}
                            className="admin-input font-mono"
                            placeholder="brk_..."
                            autoComplete="off"
                            spellCheck={false}
                        />
                        {account !== "" && !accountReady && (
                            <div className="text-xs text-red-300">An account id is letters, digits, &apos;_&apos; and &apos;-&apos; only.</div>
                        )}
                        <div className="flex flex-wrap gap-3">
                            <button
                                type="button"
                                className="admin-btn admin-btn-danger disabled:cursor-not-allowed disabled:opacity-50"
                                disabled={!accountReady}
                                onClick={() => open({ kind: "account_kill_switch", accountId: account, enabled: true })}
                            >
                                Turn ON for this account
                            </button>
                            <button
                                type="button"
                                className="admin-btn admin-btn-secondary disabled:cursor-not-allowed disabled:opacity-50"
                                disabled={!accountReady}
                                onClick={() => open({ kind: "account_kill_switch", accountId: account, enabled: false })}
                            >
                                Turn OFF for this account
                            </button>
                        </div>
                    </div>
                </div>

                <CannotDoCard />

                <GovernanceActionDialog action={action} onClose={() => setAction(null)} onRecorded={recorded} onSettled={reload} />
            </div>
        </AdminLayout>
    );
}
