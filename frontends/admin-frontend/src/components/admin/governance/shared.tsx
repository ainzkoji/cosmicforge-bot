import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { AlertCircle, ShieldCheck, TriangleAlert } from "lucide-react";
import { GOVERNANCE_API_ROUTE, MAX_OPERABLE_STAGE, integrityText } from "@/utils/familyGovernance";
import type { FamilyOrderAuthority, LogIntegrity } from "@/utils/familyGovernance";

export type BadgeTone = "danger" | "success" | "warning" | "info";

export function Badge({ tone, children }: { tone: BadgeTone; children: ReactNode }) {
    return <span className={`admin-badge admin-badge-${tone}`}>{children}</span>;
}

/** Green only for CERTIFIED; a failed or revoked family is red; everything else is "not yet". */
export function CertificationBadge({ status }: { status: string }) {
    const tone: BadgeTone = status === "CERTIFIED" ? "success" : status === "FAILED" || status === "REVOKED" ? "danger" : "warning";
    return <Badge tone={tone}>{status}</Badge>;
}

export function KillSwitchBadge({ state }: { state: string }) {
    return <Badge tone={state === "ON" ? "danger" : state === "OFF" ? "info" : "warning"}>{state}</Badge>;
}

/**
 * A list of machine-readable codes, exactly as the API gave them: one per line,
 * or side by side (`inline`) where the list has the full width of a row.
 */
export function CodeList({ codes, empty, inline = false }: { codes: string[]; empty: string; inline?: boolean }) {
    if (codes.length === 0) {
        return <span className="text-xs" style={{ color: "var(--admin-text-secondary)" }}>{empty}</span>;
    }
    return (
        <ul className={inline ? "flex flex-wrap gap-x-4 gap-y-1" : "space-y-0.5"}>
            {codes.map((code) => <li key={code} className="break-words font-mono text-xs">{code}</li>)}
        </ul>
    );
}

export function ErrorPanel({ title, error }: { title: string; error: unknown }) {
    return (
        <div role="alert" className="rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
            <div className="flex items-start gap-2">
                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                <div>
                    <div className="font-semibold">{title}</div>
                    <div className="mt-1 break-words">{error instanceof Error && error.message ? error.message : "Request failed"}</div>
                    <div className="mt-1 text-xs opacity-80">Route: {GOVERNANCE_API_ROUTE}</div>
                </div>
            </div>
        </div>
    );
}

/**
 * The first thing on the page: who may submit orders. `authority` is undefined
 * while the registry is not known, and that is said, never assumed to be "none".
 */
export function OrderAuthorityBanner({ authority, familyCount, loading }: {
    authority: FamilyOrderAuthority[] | undefined;
    familyCount: number;
    loading: boolean;
}) {
    if (authority === undefined) {
        return (
            <div role="status" className="rounded-xl border border-amber-500/50 bg-amber-500/10 px-5 py-4 text-amber-100">
                <div className="text-lg font-bold">
                    {loading ? "Order authority: loading the registry" : "Order authority UNKNOWN: the registry could not be read"}
                </div>
                <div className="mt-1 text-sm">Nothing is assumed until the engine answers.</div>
            </div>
        );
    }
    if (authority.length === 0) {
        return (
            <div role="status" className="rounded-xl border border-green-500/40 bg-green-500/10 px-5 py-4 text-green-100">
                <div className="flex items-center gap-2 text-lg font-bold">
                    <ShieldCheck className="h-5 w-5" />
                    No family has order authority
                </div>
                <div className="mt-1 text-sm">
                    The engine reports {familyCount} {familyCount === 1 ? "family" : "families"} and lists none as executable.
                    Every one is limited to research simulation and shadow operation: no family may submit an order to
                    any account, demo or live.
                </div>
            </div>
        );
    }
    return (
        <div role="alert" className="rounded-xl border border-red-500/60 bg-red-500/15 px-5 py-4 text-red-100">
            <div className="flex items-center gap-2 text-lg font-bold">
                <TriangleAlert className="h-5 w-5" />
                ORDER AUTHORITY PRESENT: {authority.length} {authority.length === 1 ? "family" : "families"} may submit orders
            </div>
            <ul className="mt-2 space-y-1 text-sm">
                {authority.map((item) => (
                    <li key={item.family_id}>
                        <span className="font-mono font-semibold">{item.family_id}</span>
                        {": "}
                        {item.modes.length > 0 ? item.modes.join(", ") : "listed as executable by the engine"}
                    </li>
                ))}
            </ul>
        </div>
    );
}

export function IntegrityLine({ log, error, loading }: { log: LogIntegrity | undefined; error?: unknown; loading?: boolean }) {
    if (log === undefined) {
        return (
            <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                Governance log integrity:{" "}
                <Badge tone="warning">{loading ? "CHECKING" : "UNKNOWN"}</Badge>
                {error instanceof Error && !loading ? <span className="ml-2">{error.message}</span> : null}
            </div>
        );
    }
    return (
        <div className="text-sm" style={{ color: log.ok ? "var(--admin-text-secondary)" : "var(--admin-red)" }}>
            Governance log integrity: <Badge tone={log.ok ? "success" : "danger"}>{log.ok ? "INTACT" : "BROKEN"}</Badge>
            <span className="ml-2">{integrityText(log)}</span>
        </div>
    );
}

/** What the interface cannot do, stated on the page so that nobody looks for it. */
export function CannotDoCard() {
    return (
        <div className="admin-card">
            <h2 className="mb-3 text-lg font-semibold" style={{ color: "var(--admin-text-primary)" }}>What this page cannot do</h2>
            <ul className="list-disc space-y-1 pl-5 text-sm" style={{ color: "var(--admin-text-secondary)" }}>
                <li>
                    It cannot promote a family above {MAX_OPERABLE_STAGE}. Limited production and everything after it is a
                    separate decision that this interface does not operate.
                </li>
                <li>
                    It cannot certify a family. Certification is the research register&apos;s record; the engine reads it
                    again at every approval, and nothing typed here replaces it.
                </li>
                <li>It cannot enable order submission. The submission flags are configuration and are not shown or changed here.</li>
                <li>
                    It cannot approve a promotion by itself: an approval is recorded only when the engine finds every
                    prerequisite met at that moment. Otherwise the refusal is shown as the engine gave it.
                </li>
                <li>
                    The platform kill switch (all families, all accounts) and emergency flatten are on the{" "}
                    <Link to="/admin/bot-monitor" className="underline">Bot Monitor</Link> page, not here.
                </li>
            </ul>
        </div>
    );
}
