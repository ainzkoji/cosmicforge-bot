import { useCallback, useEffect, useState } from "react";
import { acceptDocuments, getLegalStatus } from "@/api/legal";
import {
    acceptancePayload, allAccepted, categoryLabel, isDraft, pendingDocuments, reconsentReasonLabel,
    type PendingItem,
} from "@/utils/legalDocuments";
import { LegalBody } from "./LegalDocumentView";

/**
 * Asks a signed-in user to accept documents that are new to them or that
 * changed materially since they last accepted. Each acceptance is bound to the
 * exact version (content hash) shown here.
 */
export function ReconsentPrompt() {
    const [pending, setPending] = useState<PendingItem[]>([]);
    const [checked, setChecked] = useState<Record<string, boolean>>({});
    const [expanded, setExpanded] = useState<string | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [saving, setSaving] = useState(false);
    const [dismissed, setDismissed] = useState(false);

    const load = useCallback(() => {
        getLegalStatus()
            .then((status) => setPending(pendingDocuments(status)))
            .catch(() => setPending([])); // the prompt never blocks the app on a status error
    }, []);

    useEffect(() => {
        load();
    }, [load]);

    if (dismissed || pending.length === 0) return null;
    const docs = pending.map((p) => p.document);

    const submit = async () => {
        setSaving(true);
        setError(null);
        try {
            await acceptDocuments(acceptancePayload(docs));
            setChecked({});
            load();
        } catch (err) {
            setError(err instanceof Error ? err.message : "Could not record your acceptance.");
        } finally {
            setSaving(false);
        }
    };

    return (
        <div role="dialog" aria-modal="true" aria-labelledby="reconsent-title"
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
            <div className="w-full max-w-2xl max-h-[90vh] overflow-y-auto rounded-2xl border border-border bg-card p-6 space-y-5">
                <div>
                    <h2 id="reconsent-title" className="text-xl font-bold">Please review our documents</h2>
                    <p className="text-sm text-muted-foreground mt-1">
                        These documents are new or have changed since you last accepted them.
                    </p>
                </div>
                <ul className="space-y-3">
                    {pending.map(({ document: doc, reason }) => (
                        <li key={doc.doc_id} className="rounded-xl border border-border p-4 space-y-2">
                            <div className="flex items-start justify-between gap-3">
                                <div>
                                    <p className="font-semibold">{categoryLabel(doc.category)} — version {doc.version}</p>
                                    <p className="text-xs text-muted-foreground">{reconsentReasonLabel(reason)}</p>
                                    {isDraft(doc) && (
                                        <p className="text-xs text-amber-500 mt-1">Draft: not yet reviewed by legal counsel.</p>
                                    )}
                                    {doc.change_summary && <p className="text-xs mt-1">{doc.change_summary}</p>}
                                </div>
                                <button type="button" className="text-sm underline shrink-0"
                                    onClick={() => setExpanded(expanded === doc.doc_id ? null : doc.doc_id)}>
                                    {expanded === doc.doc_id ? "Hide" : "Read"}
                                </button>
                            </div>
                            {expanded === doc.doc_id && (
                                <div className="max-h-64 overflow-y-auto text-sm space-y-3 border-t border-border pt-3">
                                    <LegalBody body={doc.body} />
                                </div>
                            )}
                            <label className="flex items-center gap-2 text-sm">
                                <input type="checkbox" checked={checked[doc.doc_id] === true}
                                    onChange={(e) => setChecked({ ...checked, [doc.doc_id]: e.target.checked })} />
                                I have read and accept the {categoryLabel(doc.category)}.
                            </label>
                        </li>
                    ))}
                </ul>
                {error && <p className="text-sm text-red-500">{error}</p>}
                <div className="flex justify-end gap-3">
                    <button type="button" onClick={() => setDismissed(true)}
                        className="px-4 py-2 rounded-lg border border-border text-sm">
                        Later
                    </button>
                    <button type="button" disabled={!allAccepted(docs, checked) || saving} onClick={submit}
                        className="px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium disabled:opacity-50">
                        {saving ? "Saving…" : "Accept"}
                    </button>
                </div>
            </div>
        </div>
    );
}
