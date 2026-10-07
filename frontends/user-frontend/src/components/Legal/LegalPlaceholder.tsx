import { Link } from "react-router-dom";

const SUPPORT_EMAIL = "support@cosmicforge.com";

/**
 * Honest placeholder for a legal document that has not been published yet.
 * It deliberately contains no legal terms.
 */
export function LegalPlaceholder({ title }: { title: string }) {
    return (
        <div className="bg-white">
            <article className="max-w-3xl mx-auto px-6 pt-32 pb-24 space-y-6 text-gray-700 leading-relaxed">
                <h1 className="text-4xl font-bold text-[#1E1B4B]">{title}</h1>
                <div role="status" className="rounded-xl border border-amber-300 bg-amber-50 px-5 py-4 text-amber-900">
                    <p className="font-semibold">This document is being finalised.</p>
                    <p className="mt-1">
                        The {title} has not been published yet. This page will be updated when it is available.
                    </p>
                </div>
                <p>
                    If you have a question about it in the meantime, contact us at{" "}
                    <a href={`mailto:${SUPPORT_EMAIL}`} className="underline">{SUPPORT_EMAIL}</a>.
                </p>
                <p>
                    The <Link to="/risk-disclosure" className="underline">Risk Disclosure</Link> is available now and
                    describes the risks of using the product.
                </p>
            </article>
        </div>
    );
}
