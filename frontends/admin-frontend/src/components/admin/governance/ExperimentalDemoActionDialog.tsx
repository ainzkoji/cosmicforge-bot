import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Loader2, TriangleAlert, X } from "lucide-react";
import Modal from "@/components/UI/Modal";
import { Refusal } from "@/components/admin/governance/GovernanceActionDialog";
import { submitDemoAction } from "@/api/experimentalDemo";
import { MAX_TEXT_LENGTH, MIN_TEXT_LENGTH, describeDemoAction, revokeProblem } from "@/utils/experimentalDemo";
import type { DemoAction, DemoActionOutcome } from "@/utils/experimentalDemo";

interface DialogProps {
    /** The action to confirm; null closes the dialog. */
    action: DemoAction | null;
    onClose: () => void;
    /** The server returned the record it wrote. */
    onRecorded: (outcome: DemoActionOutcome) => void;
    /** An attempt ended, whatever its outcome: the page reloads what it shows. */
    onSettled: () => void;
}

function reasonOk(reason: string): boolean {
    const length = reason.trim().length;
    return length >= MIN_TEXT_LENGTH && length <= MAX_TEXT_LENGTH;
}

function DialogBody({ action, onClose, onRecorded, onSettled }: Omit<DialogProps, "action"> & { action: DemoAction }) {
    const [reason, setReason] = useState("");

    // networkMode "always": an action clicked while offline must fail at once,
    // never sit paused and fire later when the network returns.
    const mutation = useMutation({
        networkMode: "always",
        mutationFn: (typedReason: string) => submitDemoAction(action, typedReason),
        onSuccess: (outcome: DemoActionOutcome) => onRecorded(outcome),
        onSettled: () => onSettled(),
    });

    const problem = revokeProblem(action);
    const description = problem === null ? describeDemoAction(action, reason) : null;
    const ready = description !== null && (!description.asksReason || reasonOk(reason)) && !mutation.isPending;

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
                        <div className="space-y-2 text-sm text-foreground">
                            <div className="font-semibold">{description.asksReason ? "What will be revoked" : "Exactly what will be authorized"}</div>
                            <table className="w-full text-sm">
                                <tbody>
                                    {description.summary.map(([label, value]) => (
                                        <tr key={label} className="align-top">
                                            <td className="w-1/3 py-0.5 pr-3 text-muted-foreground">{label}</td>
                                            <td className="break-words py-0.5 font-mono text-xs">{value}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>

                        <div className="rounded-xl border border-red-500/40 bg-red-500/10 p-4 text-sm text-foreground">
                            <div className="flex items-start gap-3">
                                <TriangleAlert className="mt-0.5 h-5 w-5 shrink-0 text-red-600" />
                                <ul className="list-disc space-y-1 pl-4">
                                    {description.effects.map((effect) => <li key={effect}>{effect}</li>)}
                                </ul>
                            </div>
                        </div>

                        <div className="space-y-2 text-sm text-foreground">
                            <div className="font-semibold">The request, exactly as it will be sent</div>
                            <div className="rounded-lg bg-muted/30 px-4 py-3">
                                <div className="break-all font-mono text-xs">{description.method} {description.path}</div>
                                <pre className="mt-2 whitespace-pre-wrap break-all font-mono text-xs">{JSON.stringify(description.body, null, 2)}</pre>
                            </div>
                            <div className="text-xs text-muted-foreground">
                                The actor on the record is you: the engine takes the administrator id from your session, never from this page.
                            </div>
                        </div>

                        {description.asksReason && (
                            <div className="space-y-2">
                                <label htmlFor="experimental-demo-action-reason" className="block text-sm font-medium text-foreground">
                                    Reason (required, {MIN_TEXT_LENGTH} to {MAX_TEXT_LENGTH} characters)
                                </label>
                                <textarea
                                    id="experimental-demo-action-reason"
                                    value={reason}
                                    onChange={(event) => setReason(event.target.value)}
                                    maxLength={MAX_TEXT_LENGTH}
                                    className="min-h-[96px] w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                                    placeholder="Why is this being revoked?"
                                />
                            </div>
                        )}
                    </>
                )}
            </div>

            {/* Outside the scrolling part: an answer from the API is never below the fold. */}
            {mutation.isError && (
                <div className="max-h-48 shrink-0 overflow-y-auto border-t border-border px-6 py-4">
                    <Refusal error={mutation.error} apiName={description?.apiName} />
                </div>
            )}

            <div className="flex shrink-0 items-center justify-end gap-3 border-t border-border px-6 py-4">
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
 * The confirmation every experimental-DEMO and market-entry action goes
 * through: exactly what is authorized or revoked, what it does and does not do,
 * and the request as it will be sent. A refusal is shown with the API's own
 * reason code and detail and leaves the dialog open.
 */
export function ExperimentalDemoActionDialog({ action, onClose, onRecorded, onSettled }: DialogProps) {
    // Every opening gets its own body: a typed reason or a refusal is never carried into the next confirmation.
    const [opened, setOpened] = useState<{ action: DemoAction | null; count: number }>({ action: null, count: 0 });
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
