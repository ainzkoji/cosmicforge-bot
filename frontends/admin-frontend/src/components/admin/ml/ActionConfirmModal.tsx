import { Loader2, TriangleAlert, X } from "lucide-react";
import Modal from "@/components/UI/Modal";
import { StatusBadge } from "@/components/admin/ml/StatusBadge";
import type { MLActionDefinition } from "@/api/admin";

interface ActionConfirmModalProps {
    action: MLActionDefinition | null;
    open: boolean;
    confirmationValue: string;
    note: string;
    submitting?: boolean;
    error?: string | null;
    onChangeConfirmation: (value: string) => void;
    onChangeNote: (value: string) => void;
    onClose: () => void;
    onConfirm: () => void;
}

export function ActionConfirmModal({
    action,
    open,
    confirmationValue,
    note,
    submitting = false,
    error,
    onChangeConfirmation,
    onChangeNote,
    onClose,
    onConfirm,
}: ActionConfirmModalProps) {
    if (!action) {
        return null;
    }

    const ready = confirmationValue.trim().toUpperCase() === action.confirmation_phrase;

    return (
        <Modal isOpen={open} onClose={onClose} className="max-w-2xl">
            <div className="flex items-center justify-between border-b border-border px-6 py-4">
                <div>
                    <div className="text-lg font-semibold text-foreground">{action.label}</div>
                    <div className="mt-1 text-sm text-muted-foreground">
                        Confirm this protected ML admin action before it is queued.
                    </div>
                </div>
                <button type="button" className="text-muted-foreground transition hover:text-foreground" onClick={onClose}>
                    <X className="h-5 w-5" />
                </button>
            </div>

            <div className="space-y-5 px-6 py-6">
                <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4">
                    <div className="flex items-start gap-3">
                        <TriangleAlert className="mt-0.5 h-5 w-5 text-amber-400" />
                        <div className="space-y-2 text-sm">
                            <div className="text-foreground">
                                Protected actions are fail-closed. Unsafe or unsupported actions will still be refused by the backend.
                            </div>
                            <div className="flex flex-wrap items-center gap-2">
                                <StatusBadge status={action.supported ? "healthy" : "unsupported"} label={action.supported ? "Supported" : "Restricted"} />
                                <StatusBadge status={action.allowed ? "healthy" : "blocked"} label={action.allowed ? "Allowed Now" : "Blocked"} />
                                {action.dangerous ? <StatusBadge status="warning" label="Dangerous" /> : <StatusBadge status="info" label="Safe Read" />}
                            </div>
                            {action.blocked_reason ? (
                                <div className="text-amber-100/90">{action.blocked_reason}</div>
                            ) : null}
                        </div>
                    </div>
                </div>

                <div className="grid gap-4 md:grid-cols-2">
                    <div className="rounded-lg bg-muted/30 px-4 py-3">
                        <div className="text-xs uppercase tracking-wide text-muted-foreground">Dataset</div>
                        <div className="mt-1 break-all text-sm text-foreground">{action.dataset_path || "No dataset path selected"}</div>
                    </div>
                    <div className="rounded-lg bg-muted/30 px-4 py-3">
                        <div className="text-xs uppercase tracking-wide text-muted-foreground">Target Model</div>
                        <div className="mt-1 text-sm text-foreground">{action.target_model_version || "No target model version"}</div>
                    </div>
                </div>

                <div className="space-y-2">
                    <label className="block text-sm font-medium text-foreground">
                        Type <span className="font-semibold">{action.confirmation_phrase}</span> to continue
                    </label>
                    <input
                        value={confirmationValue}
                        onChange={(event) => onChangeConfirmation(event.target.value)}
                        className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                        placeholder={action.confirmation_phrase}
                    />
                </div>

                <div className="space-y-2">
                    <label className="block text-sm font-medium text-foreground">Operator note</label>
                    <textarea
                        value={note}
                        onChange={(event) => onChangeNote(event.target.value)}
                        className="min-h-[96px] w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary"
                        placeholder="Why is this action being run now?"
                    />
                </div>

                {error ? (
                    <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                        {error}
                    </div>
                ) : null}
            </div>

            <div className="flex items-center justify-end gap-3 border-t border-border px-6 py-4">
                <button type="button" className="admin-btn admin-btn-secondary" onClick={onClose} disabled={submitting}>
                    Cancel
                </button>
                <button
                    type="button"
                    className="admin-btn admin-btn-primary"
                    onClick={onConfirm}
                    disabled={!ready || submitting || !action.allowed || !action.supported}
                >
                    {submitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    Queue Action
                </button>
            </div>
        </Modal>
    );
}
