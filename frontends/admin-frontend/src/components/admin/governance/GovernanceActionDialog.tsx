import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Loader2, TriangleAlert, X } from "lucide-react";
import Modal from "@/components/UI/Modal";
import { submitGovernanceAction } from "@/api/governance";
import {
    GovernanceApiError,
    MAX_REASON_LENGTH,
    MIN_REASON_LENGTH,
    actionProblem,
    describeAction,
    reasonReady,
} from "@/utils/familyGovernance";
import type { ActionOutcome, GovernanceAction } from "@/utils/familyGovernance";

interface GovernanceActionDialogProps {
    /** The action to confirm; null closes the dialog. */
    action: GovernanceAction | null;
    onClose: () => void;
    /** The server returned the record it wrote. */
    onRecorded: (outcome: ActionOutcome) => void;
    /** An attempt ended, whatever its outcome: the page reloads what it shows. */
    onSettled: () => void;
}

/** The API's answer to a refused or failed action, in its own words. */
function Refusal({ error }: { error: unknown }) {
    const known = error instanceof GovernanceApiError ? error : null;
    const message = error instanceof Error && error.message ? error.message : "Request failed";

    if (known && known.outcomeUnknown) {
        return (
            <div role="alert" className="rounded-lg border border-amber-500/60 bg-amber-500/15 px-4 py-3 text-sm text-foreground">
                <div className="font-bold text-amber-700">
                    OUTCOME UNKNOWN: the record may or may not have been written. Reload the registry and the history
                    before trying again.
                </div>
                <div className="mt-1 break-words">{message}</div>
            </div>
        );
    }
    if (known && known.status !== null) {
        return (
            <div role="alert" className="rounded-lg border border-red-500/50 bg-red-500/10 px-4 py-3 text-sm text-foreground">
                <div className="font-bold text-red-700">
                    {known.status >= 500 ? "FAILED" : "REFUSED"} by the governance API (HTTP {known.status}). Nothing on this
                    page reports it as done.
                </div>
                {known.reason && (
                    <div className="mt-2">
                        Reason: <code className="font-mono font-semibold">{known.reason}</code>
                    </div>
                )}
                {known.detail && <div className="mt-1 whitespace-pre-wrap break-words font-mono text-xs">{known.detail}</div>}
                {!known.reason && !known.detail && <div className="mt-1 break-words">{message}</div>}
            </div>
        );
    }
    return (
        <div role="alert" className="rounded-lg border border-red-500/50 bg-red-500/10 px-4 py-3 text-sm text-foreground">
            <div className="break-words font-semibold text-red-700">{message}</div>
        </div>
    );
}

function DialogBody({ action, onClose, onRecorded, onSettled }: Omit<GovernanceActionDialogProps, "action"> & { action: GovernanceAction }) {
    const [reason, setReason] = useState("");

    // networkMode "always": an action clicked while offline must fail at once,
    // never sit paused and fire later when the network returns.
    const mutation = useMutation({
        networkMode: "always",
        mutationFn: (typedReason: string) => submitGovernanceAction(action, typedReason),
        onSuccess: (outcome: ActionOutcome) => onRecorded(outcome),
        onSettled: () => onSettled(),
    });

    const problem = actionProblem(action);
    const description = problem === null ? describeAction(action, reason) : null;
    const ready = description !== null && reasonReady(reason) && !mutation.isPending;

    return (
        <>
            <div className="flex items-center justify-between border-b border-border px-6 py-4">
                <div className="text-lg font-semibold text-foreground">{description ? description.title : "This action cannot be sent"}</div>
                <button
                    type="button"
                    className="text-muted-foreground transition hover:text-foreground"
                    onClick={onClose}
                    disabled={mutation.isPending}
                    aria-label="Close"
                >
                    <X className="h-5 w-5" />
                </button>
            </div>

            <div className="space-y-5 overflow-y-auto px-6 py-6">
                {problem !== null && (
                    <div role="alert" className="rounded-lg border border-red-500/50 bg-red-500/10 px-4 py-3 text-sm font-semibold text-red-700">
                        {problem} Nothing was sent.
                    </div>
                )}

                {description && (
                    <>
                        <div className={`rounded-xl border p-4 text-sm text-foreground ${description.tone === "danger" ? "border-red-500/40 bg-red-500/10" : "border-amber-500/30 bg-amber-500/10"}`}>
                            <div className="flex items-start gap-3">
                                <TriangleAlert className={`mt-0.5 h-5 w-5 shrink-0 ${description.tone === "danger" ? "text-red-600" : "text-amber-600"}`} />
                                <div className="space-y-1">
                                    {description.effects.map((effect) => <div key={effect}>{effect}</div>)}
                                </div>
                            </div>
                        </div>

                        <div className="space-y-2 text-sm text-foreground">
                            <div className="font-semibold">What will be recorded</div>
                            <ul className="list-disc space-y-1 pl-5">
                                {description.records.map((record) => <li key={record}>{record}</li>)}
                                <li>
                                    One row in the administrative audit trail (auth_audit_log): your administrator id, this
                                    action, the identifiers above, the reason and the outcome, including a refusal.
                                </li>
                            </ul>
                        </div>

                        <div className="space-y-2 text-sm text-foreground">
                            <div className="font-semibold">The request, exactly as it will be sent</div>
                            <div className="rounded-lg bg-muted/30 px-4 py-3">
                                <div className="break-all font-mono text-xs">{description.method} {description.path}</div>
                                <pre className="mt-2 whitespace-pre-wrap break-all font-mono text-xs">{JSON.stringify(description.body, null, 2)}</pre>
                            </div>
                        </div>

                        <div className="space-y-2">
                            <label htmlFor="governance-action-reason" className="block text-sm font-medium text-foreground">
                                Reason (required, {MIN_REASON_LENGTH} to {MAX_REASON_LENGTH} characters)
                            </label>
                            <textarea
                                id="governance-action-reason"
                                value={reason}
                                onChange={(event) => setReason(event.target.value)}
                                maxLength={MAX_REASON_LENGTH}
                                className="min-h-[96px] w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                                placeholder="Why is this being done, and on what evidence?"
                            />
                            <div className="text-xs text-muted-foreground">{description.reasonNote}</div>
                        </div>
                    </>
                )}
            </div>

            {/* Outside the scrolling part: an answer from the API is never below the fold. */}
            {mutation.isError && (
                <div className="max-h-48 shrink-0 overflow-y-auto border-t border-border px-6 py-4">
                    <Refusal error={mutation.error} />
                </div>
            )}

            <div className="flex shrink-0 items-center justify-end gap-3 border-t border-border px-6 py-4">
                {/* Not admin-btn-secondary: its light text is for the dark admin surface and cannot be read on the modal. */}
                <button
                    type="button"
                    className="rounded-lg border border-border px-4 py-2 text-sm font-semibold text-foreground transition hover:bg-muted/50 disabled:opacity-50"
                    onClick={onClose}
                    disabled={mutation.isPending}
                >
                    {mutation.isError ? "Close" : "Cancel"}
                </button>
                {description && (
                    <button
                        type="button"
                        className={`admin-btn ${description.tone === "danger" ? "admin-btn-danger" : "admin-btn-primary"} flex items-center gap-2 disabled:cursor-not-allowed disabled:opacity-50`}
                        disabled={!ready}
                        onClick={() => mutation.mutate(reason)}
                    >
                        {mutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                        {description.confirmLabel}
                    </button>
                )}
            </div>
        </>
    );
}

/**
 * The confirmation every governance action goes through: what it does, what it
 * records, the request as it will be sent, and a typed reason. A refusal is
 * shown with the API's own reason and detail and leaves the dialog open.
 */
export function GovernanceActionDialog({ action, onClose, onRecorded, onSettled }: GovernanceActionDialogProps) {
    // Every opening gets its own body, even for the same action reopened while the
    // previous dialog is still fading out: a typed reason, an answer or a refusal
    // is never carried from one confirmation into the next.
    const [opened, setOpened] = useState<{ action: GovernanceAction | null; count: number }>({ action: null, count: 0 });
    if (opened.action !== action) {
        setOpened({ action, count: action === null ? opened.count : opened.count + 1 });
    }
    return (
        <Modal isOpen={action !== null} onClose={onClose} className="max-w-2xl">
            {action !== null && (
                <DialogBody key={opened.count} action={action} onClose={onClose} onRecorded={onRecorded} onSettled={onSettled} />
            )}
        </Modal>
    );
}
