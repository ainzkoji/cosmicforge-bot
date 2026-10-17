import { useEffect, useState, type ReactNode } from "react";
import { getCurrentDocuments } from "@/api/legal";
import { draftBannerText, isDraft, parseLegalBody, type LegalDocument } from "@/utils/legalDocuments";

/** Renders the stored body blocks of a legal document. */
export function LegalBody({ body }: { body: string }) {
    return (
        <>
            {parseLegalBody(body).map((block, index) => {
                if (block.kind === "heading") {
                    return <h2 key={index} className="text-2xl font-bold text-[#1E1B4B]">{block.text}</h2>;
                }
                if (block.kind === "list") {
                    return (
                        <ul key={index} className="list-disc ml-6 space-y-2">
                            {block.items.map((item, i) => <li key={i}>{item}</li>)}
                        </ul>
                    );
                }
                return <p key={index}>{block.text}</p>;
            })}
        </>
    );
}

export function DraftBanner({ doc }: { doc: LegalDocument }) {
    if (!isDraft(doc)) return null;
    return (
        <div role="status" className="rounded-xl border border-amber-300 bg-amber-50 px-5 py-4 text-amber-900">
            <p className="font-semibold">Draft document</p>
            <p className="mt-1">{draftBannerText(doc)}</p>
        </div>
    );
}

/**
 * The current published version of a legal document category, with a DRAFT
 * banner while counsel approval has not been recorded. ``fallback`` renders
 * when nothing is published (or the server cannot be reached).
 */
export function LegalDocumentView({ category, fallback }: { category: string; fallback: ReactNode }) {
    const [doc, setDoc] = useState<LegalDocument | null>(null);
    const [state, setState] = useState<"loading" | "ready" | "missing">("loading");

    useEffect(() => {
        let active = true;
        getCurrentDocuments({ categories: [category] })
            .then((res) => {
                if (!active) return;
                const found = res.documents.find((d) => d.category === category) ?? null;
                setDoc(found);
                setState(found ? "ready" : "missing");
            })
            .catch(() => {
                if (active) setState("missing");
            });
        return () => {
            active = false;
        };
    }, [category]);

    if (state === "loading") {
        return <div className="bg-white min-h-[50vh]" aria-busy="true" />;
    }
    if (state === "missing" || !doc) return <>{fallback}</>;

    return (
        <div className="bg-white">
            <article className="max-w-3xl mx-auto px-6 pt-32 pb-24 space-y-6 text-gray-700 leading-relaxed">
                <header className="space-y-3">
                    <h1 className="text-4xl font-bold text-[#1E1B4B]">{doc.title}</h1>
                    <p className="text-sm text-gray-500">
                        Version {doc.version} · published {new Date(doc.published_at).toLocaleDateString()}
                    </p>
                </header>
                <DraftBanner doc={doc} />
                <LegalBody body={doc.body} />
            </article>
        </div>
    );
}
