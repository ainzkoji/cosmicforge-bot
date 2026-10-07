import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, Loader2, RefreshCw, Square, TriangleAlert, X } from "lucide-react";
import Modal from "@/components/UI/Modal";
import {
    EMERGENCY_API_ROUTE,
    EmergencyApiError,
    flattenAllPositions,
    getEmergencyStatus,
    setKillSwitch,
} from "@/api/emergency";
import type { EmergencyStatus, FlattenResponse, FlattenResult } from "@/api/emergency";

const STATUS_QUERY_KEY = ["admin-emergency-status"] as const;
const FLATTEN_PHRASE = "FLATTEN";
const MIN_REASON_LENGTH = 3;

type FlattenOutcome =
    | { kind: "response"; response: FlattenResponse; at: string }
    | { kind: "error"; message: string; results: FlattenResult[]; at: string };

function errorText(error: unknown): string {
    return error instanceof Error && error.message ? error.message : "Request failed";
}

function formatDateTime(iso: string | null | undefined): string {
    if (!iso) return "n/a";
    const date = new Date(iso);
    return Number.isNaN(date.getTime()) ? iso : date.toLocaleString();
}

type FlagTone = "danger" | "success" | "warning" | "info";

function Flag({ label, text, tone }: { label: string; text: string; tone: FlagTone }) {
    return (
        <div>
            <div className="admin-metric-label mb-2">{label}</div>
            <span className={`admin-badge admin-badge-${tone}`}>{text}</span>
        </div>
    );
}

function ResultsTable({ results }: { results: FlattenResult[] }) {
    if (results.length === 0) {
        return <div className="text-sm" style={{ color: "var(--admin-text-secondary)" }}>The server returned no per-account results.</div>;
    }
    return (
        <table className="admin-table">
            <thead>
                <tr>
                    <th>Account</th>
                    <th>Symbol</th>
                    <th>Result</th>
                    <th>Detail</th>
                </tr>
            </thead>
            <tbody>
                {results.map((row, index) => (
                    <tr key={`${row.account_id}-${row.symbol}-${index}`}>
                        <td className="font-mono text-xs">{row.account_id}</td>
                        <td className="font-mono text-xs">{row.symbol}</td>
                        <td>
                            <span className={`admin-badge ${row.status === "closed" || row.status === "no_position"
                                ? "admin-badge-success"
                                : row.status === "submitted"
                                    ? "admin-badge-warning"
                                    : "admin-badge-danger"}`}>
                                {row.status}
                            </span>
                        </td>
                        <td className="text-xs">{row.detail || "—"}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}

/**
 * Kill switch and flatten controls for the bot monitor. Every action goes to
 * the emergency API; nothing here reports success unless the server confirms it.
 */
export function EmergencyControls() {
    const queryClient = useQueryClient();

    const statusQuery = useQuery({
        queryKey: STATUS_QUERY_KEY,
        queryFn: getEmergencyStatus,
        refetchInterval: 10_000,
        retry: false,
    });
    const status: EmergencyStatus | undefined = statusQuery.isError ? undefined : statusQuery.data;

    // Kill switch dialog: the target state is chosen explicitly, never inferred from stale data.
    const [killTarget, setKillTarget] = useState<boolean | null>(null);
    const [killReason, setKillReason] = useState("");
    const [killNotice, setKillNotice] = useState<{ ok: boolean; text: string } | null>(null);

    // Flatten dialog
    const [flattenOpen, setFlattenOpen] = useState(false);
    const [flattenPhrase, setFlattenPhrase] = useState("");
    const [flattenReason, setFlattenReason] = useState("");
    const [flattenOutcome, setFlattenOutcome] = useState<FlattenOutcome | null>(null);

    const killMutation = useMutation({
        mutationFn: (vars: { enabled: boolean; reason: string }) => setKillSwitch(vars.enabled, vars.reason),
        onSuccess: (data: EmergencyStatus) => {
            queryClient.setQueryData(STATUS_QUERY_KEY, data);
            setKillNotice({
                ok: true,
                text: `Kill switch is now ${data.kill_switch.enabled ? "ENABLED" : "DISABLED"} (confirmed by the server).`,
            });
            setKillTarget(null);
            setKillReason("");
        },
        onSettled: () => {
            queryClient.invalidateQueries({ queryKey: STATUS_QUERY_KEY });
        },
    });

    const flattenMutation = useMutation({
        mutationFn: (reason: string) => flattenAllPositions(reason),
        onSuccess: (response: FlattenResponse) => {
            setFlattenOutcome({ kind: "response", response, at: new Date().toISOString() });
        },
        onError: (error: Error) => {
            setFlattenOutcome({
                kind: "error",
                message: errorText(error),
                results: error instanceof EmergencyApiError ? error.results : [],
                at: new Date().toISOString(),
            });
        },
        onSettled: () => {
            setFlattenOpen(false);
            setFlattenPhrase("");
            setFlattenReason("");
            queryClient.invalidateQueries({ queryKey: STATUS_QUERY_KEY });
        },
    });

    const openKillDialog = (target: boolean) => {
        killMutation.reset();
        setKillNotice(null);
        setKillReason("");
        setKillTarget(target);
    };

    const openFlattenDialog = () => {
        setFlattenPhrase("");
        setFlattenReason("");
        setFlattenOpen(true);
    };

    const killReasonReady = killReason.trim().length >= MIN_REASON_LENGTH;
    const flattenReady = flattenPhrase === FLATTEN_PHRASE && flattenReason.trim().length >= MIN_REASON_LENGTH;

    const killEnabled = status ? status.kill_switch.enabled : null;
    const openPositions = status ? status.open_positions : null;
    const flattenFailed = flattenOutcome !== null
        && (flattenOutcome.kind === "error" || flattenOutcome.response.ok !== true);

    return (
        <div className="admin-card" style={{ borderColor: killEnabled ? "var(--admin-red)" : undefined }}>
            <div className="flex flex-wrap items-center justify-between gap-4 mb-4">
                <h2 className="text-xl font-semibold flex items-center gap-2" style={{ color: "var(--admin-text-primary)" }}>
                    <TriangleAlert className="w-5 h-5" style={{ color: "var(--admin-red)" }} />
                    Emergency controls
                </h2>
                <button
                    type="button"
                    className="admin-btn admin-btn-secondary text-sm flex items-center gap-2"
                    onClick={() => statusQuery.refetch()}
                    disabled={statusQuery.isFetching}
                >
                    <RefreshCw className={`w-4 h-4 ${statusQuery.isFetching ? "animate-spin" : ""}`} />
                    Refresh status
                </button>
            </div>

            {statusQuery.isError && (
                <div role="alert" className="mb-4 rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                    <div className="flex items-start gap-2">
                        <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
                        <div>
                            <div className="font-semibold">Emergency status is UNAVAILABLE — the state below is unknown.</div>
                            <div className="mt-1">{errorText(statusQuery.error)}</div>
                            <div className="mt-1 text-xs opacity-80">Route: {EMERGENCY_API_ROUTE}</div>
                        </div>
                    </div>
                </div>
            )}

            <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                <Flag
                    label="Kill switch"
                    text={killEnabled === null ? "UNKNOWN" : killEnabled ? "ENABLED" : "Disabled"}
                    tone={killEnabled === null ? "warning" : killEnabled ? "danger" : "info"}
                />
                <Flag
                    label="Live order submission"
                    text={!status ? "UNKNOWN" : status.live_order_submission_enabled ? "ENABLED — real money" : "Disabled"}
                    tone={!status ? "warning" : status.live_order_submission_enabled ? "danger" : "info"}
                />
                <Flag
                    label="Demo order submission"
                    text={!status ? "UNKNOWN" : status.demo_order_submission_enabled ? "Enabled" : "Disabled"}
                    tone={!status ? "warning" : status.demo_order_submission_enabled ? "success" : "info"}
                />
                <div>
                    <div className="admin-metric-label mb-2">Open positions</div>
                    <div className="text-2xl font-bold" style={{ color: "var(--admin-text-primary)" }}>
                        {openPositions === null ? "Unavailable" : openPositions.length}
                    </div>
                </div>
            </div>

            {status && (
                <div className="mt-4 text-xs" style={{ color: "var(--admin-text-secondary)" }}>
                    {status.kill_switch.enabled && (
                        <div>
                            Kill switch reason: <span style={{ color: "var(--admin-text-primary)" }}>{status.kill_switch.reason || "none given"}</span>
                            {" · "}set {formatDateTime(status.kill_switch.set_at)} by {status.kill_switch.set_by || "unknown"}
                        </div>
                    )}
                    <div>Status generated {formatDateTime(status.generated_at)}</div>
                </div>
            )}

            {openPositions !== null && openPositions.length > 0 && (
                <div className="mt-4 max-h-56 overflow-y-auto">
                    <table className="admin-table">
                        <thead>
                            <tr>
                                <th>Account</th>
                                <th>User</th>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Qty</th>
                            </tr>
                        </thead>
                        <tbody>
                            {openPositions.map((position, index) => (
                                <tr key={`${position.account_id}-${position.symbol}-${index}`}>
                                    <td className="font-mono text-xs">{position.account_id}</td>
                                    <td className="font-mono text-xs">{position.user_id || "—"}</td>
                                    <td className="font-mono text-xs">{position.symbol}</td>
                                    <td>{position.side}</td>
                                    <td>{position.qty}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}

            <div className="mt-5 flex flex-wrap items-center gap-3">
                {killEnabled !== true && (
                    <button type="button" className="admin-btn admin-btn-danger flex items-center gap-2" onClick={() => openKillDialog(true)}>
                        <Square className="w-4 h-4" />
                        Enable kill switch
                    </button>
                )}
                {killEnabled !== false && (
                    <button type="button" className="admin-btn admin-btn-secondary flex items-center gap-2" onClick={() => openKillDialog(false)}>
                        Disable kill switch
                    </button>
                )}
                <button type="button" className="admin-btn admin-btn-danger flex items-center gap-2" onClick={openFlattenDialog}>
                    <TriangleAlert className="w-4 h-4" />
                    Flatten all positions
                </button>
            </div>

            {killNotice && (
                <div role="status" className="mt-4 rounded-lg border border-green-500/30 bg-green-500/10 px-4 py-3 text-sm text-green-200 flex items-start justify-between gap-3">
                    <span>{killNotice.text}</span>
                    <button type="button" onClick={() => setKillNotice(null)} aria-label="Dismiss">
                        <X className="w-4 h-4" />
                    </button>
                </div>
            )}

            {flattenOutcome && (
                <div
                    role={flattenFailed ? "alert" : "status"}
                    className={`mt-4 rounded-lg border px-4 py-3 text-sm ${flattenFailed ? "border-red-500/50 bg-red-500/10 text-red-100" : "border-green-500/30 bg-green-500/10 text-green-100"}`}
                >
                    <div className="flex items-start justify-between gap-3">
                        <div>
                            <div className="text-base font-bold">
                                {flattenOutcome.kind === "error"
                                    ? "FLATTEN FAILED"
                                    : flattenOutcome.response.ok === true
                                        ? "Flatten completed"
                                        : "FLATTEN DID NOT COMPLETE — the server reported failures"}
                            </div>
                            <div className="mt-1 text-xs opacity-80">Reported {formatDateTime(flattenOutcome.at)}</div>
                        </div>
                        <button type="button" onClick={() => setFlattenOutcome(null)} aria-label="Dismiss">
                            <X className="w-4 h-4" />
                        </button>
                    </div>

                    {flattenOutcome.kind === "error" ? (
                        <div className="mt-2 space-y-2">
                            <div>{flattenOutcome.message}</div>
                            <div className="font-semibold">
                                Positions may still be open. Check the open positions above and on the exchange before doing anything else.
                            </div>
                            {flattenOutcome.results.length > 0 && <ResultsTable results={flattenOutcome.results} />}
                        </div>
                    ) : (
                        <div className="mt-2 space-y-2">
                            <div>
                                Kill switch after flatten: {flattenOutcome.response.kill_switch_enabled ? "ENABLED" : "DISABLED"}
                            </div>
                            {flattenOutcome.response.ok !== true && (
                                <div className="font-semibold">
                                    Some positions were not closed. Review each row and verify on the exchange.
                                </div>
                            )}
                            <ResultsTable results={flattenOutcome.response.results} />
                        </div>
                    )}
                </div>
            )}

            {/* Kill switch dialog */}
            <Modal isOpen={killTarget !== null} onClose={() => setKillTarget(null)} className="max-w-xl">
                <div className="flex items-center justify-between border-b border-border px-6 py-4">
                    <div className="text-lg font-semibold text-foreground">
                        {killTarget ? "Enable kill switch" : "Disable kill switch"}
                    </div>
                    <button
                        type="button"
                        className="text-muted-foreground transition hover:text-foreground"
                        onClick={() => setKillTarget(null)}
                        disabled={killMutation.isPending}
                        aria-label="Close"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>
                <div className="space-y-5 px-6 py-6">
                    <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 text-sm text-foreground">
                        {killTarget
                            ? "Enabling the kill switch blocks new order submission platform-wide. It does not close positions that are already open — use Flatten for that."
                            : "Disabling the kill switch allows order submission again, subject to the live/demo submission flags."}
                    </div>
                    <div className="space-y-2">
                        <label htmlFor="kill-switch-reason" className="block text-sm font-medium text-foreground">
                            Reason (required, recorded in the audit trail)
                        </label>
                        <textarea
                            id="kill-switch-reason"
                            value={killReason}
                            onChange={(event) => setKillReason(event.target.value)}
                            className="min-h-[96px] w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                            placeholder="Why is the kill switch being changed?"
                        />
                    </div>
                    {killMutation.isError && (
                        <div role="alert" className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                            {errorText(killMutation.error)}
                        </div>
                    )}
                </div>
                <div className="flex items-center justify-end gap-3 border-t border-border px-6 py-4">
                    <button type="button" className="admin-btn admin-btn-secondary" onClick={() => setKillTarget(null)} disabled={killMutation.isPending}>
                        Cancel
                    </button>
                    <button
                        type="button"
                        className={`admin-btn ${killTarget ? "admin-btn-danger" : "admin-btn-primary"} flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed`}
                        disabled={!killReasonReady || killMutation.isPending || killTarget === null}
                        onClick={() => {
                            if (killTarget !== null) {
                                killMutation.mutate({ enabled: killTarget, reason: killReason.trim() });
                            }
                        }}
                    >
                        {killMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                        {killTarget ? "Enable kill switch" : "Disable kill switch"}
                    </button>
                </div>
            </Modal>

            {/* Flatten dialog */}
            <Modal isOpen={flattenOpen} onClose={() => setFlattenOpen(false)} className="max-w-xl">
                <div className="flex items-center justify-between border-b border-border px-6 py-4">
                    <div className="text-lg font-semibold text-foreground">Flatten all positions</div>
                    <button
                        type="button"
                        className="text-muted-foreground transition hover:text-foreground"
                        onClick={() => setFlattenOpen(false)}
                        disabled={flattenMutation.isPending}
                        aria-label="Close"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>
                <div className="space-y-5 px-6 py-6">
                    <div className="rounded-xl border border-red-500/40 bg-red-500/10 p-4 text-sm text-foreground">
                        <div className="flex items-start gap-3">
                            <TriangleAlert className="mt-0.5 h-5 w-5 shrink-0 text-red-400" />
                            <div className="space-y-1">
                                <div className="font-semibold">
                                    This sends market close orders for every open position on every account.
                                </div>
                                <div>
                                    It cannot be undone. Open positions right now:{" "}
                                    <span className="font-semibold">
                                        {openPositions === null ? "unknown (status unavailable)" : openPositions.length}
                                    </span>
                                    .
                                </div>
                            </div>
                        </div>
                    </div>
                    <div className="space-y-2">
                        <label htmlFor="flatten-phrase" className="block text-sm font-medium text-foreground">
                            Type <span className="font-semibold">{FLATTEN_PHRASE}</span> to continue
                        </label>
                        <input
                            id="flatten-phrase"
                            value={flattenPhrase}
                            onChange={(event) => setFlattenPhrase(event.target.value)}
                            className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                            placeholder={FLATTEN_PHRASE}
                            autoComplete="off"
                            spellCheck={false}
                        />
                    </div>
                    <div className="space-y-2">
                        <label htmlFor="flatten-reason" className="block text-sm font-medium text-foreground">
                            Reason (required, recorded in the audit trail)
                        </label>
                        <textarea
                            id="flatten-reason"
                            value={flattenReason}
                            onChange={(event) => setFlattenReason(event.target.value)}
                            className="min-h-[96px] w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                            placeholder="Why are all positions being flattened?"
                        />
                    </div>
                    {flattenMutation.isPending && (
                        <div role="status" className="text-sm text-muted-foreground">
                            Flatten in progress. Do not close this page; this can take up to two minutes.
                        </div>
                    )}
                </div>
                <div className="flex items-center justify-end gap-3 border-t border-border px-6 py-4">
                    <button type="button" className="admin-btn admin-btn-secondary" onClick={() => setFlattenOpen(false)} disabled={flattenMutation.isPending}>
                        Cancel
                    </button>
                    <button
                        type="button"
                        className="admin-btn admin-btn-danger flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                        disabled={!flattenReady || flattenMutation.isPending}
                        onClick={() => flattenMutation.mutate(flattenReason.trim())}
                    >
                        {flattenMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                        Flatten all positions
                    </button>
                </div>
            </Modal>
        </div>
    );
}
