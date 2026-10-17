import test from "node:test";
import assert from "node:assert/strict";
import {
    acceptancePayload, allAccepted, categoryLabel, draftBannerText, isDraft, parseLegalBody,
    pendingDocuments, reconsentReasonLabel, type LegalDocument,
} from "../utils/legalDocuments.ts";

function doc(overrides: Partial<LegalDocument> = {}): LegalDocument {
    return {
        doc_id: "legal_terms_v1", category: "TERMS_OF_SERVICE", version: 1, locale: "en", title: "Terms",
        body: "Body", content_sha256: "a".repeat(64), published_at: "2026-10-01T00:00:00Z", material_change: true,
        review_status: "DRAFT_PENDING_COUNSEL", ...overrides,
    };
}

test("a document is a draft unless the server reports counsel approval", () => {
    assert.equal(isDraft(doc()), true);
    assert.equal(isDraft(doc({ review_status: "COUNSEL_APPROVED" })), false);
    // The client-side flag cannot make a document final.
    assert.equal(isDraft(doc({ is_draft: false })), true);
    assert.equal(isDraft(null), true);
    assert.match(draftBannerText(doc({ version: 3 })), /version 3.*not been reviewed by legal counsel/);
});

test("body parsing: headings, lists, paragraphs", () => {
    const blocks = parseLegalBody("DRAFT notice.\n\n## Risks\n\n- Loss of capital.\n- Leverage.\n\nLine one\nline two\r\n# Other");
    assert.deepEqual(blocks, [
        { kind: "paragraph", text: "DRAFT notice." },
        { kind: "heading", text: "Risks" },
        { kind: "list", items: ["Loss of capital.", "Leverage."] },
        { kind: "paragraph", text: "Line one line two" },
        { kind: "heading", text: "Other" },
    ]);
    assert.deepEqual(parseLegalBody(""), []);
});

test("acceptance payload carries exactly the shown versions and hashes", () => {
    const docs = [doc(), doc({ doc_id: "legal_risk_v2", category: "TRADING_RISK_DISCLOSURE", content_sha256: "b".repeat(64) })];
    assert.deepEqual(acceptancePayload(docs), [
        { doc_id: "legal_terms_v1", content_sha256: "a".repeat(64) },
        { doc_id: "legal_risk_v2", content_sha256: "b".repeat(64) },
    ]);
    assert.equal(allAccepted(docs, { legal_terms_v1: true }), false);
    assert.equal(allAccepted(docs, { legal_terms_v1: true, legal_risk_v2: true }), true);
    assert.equal(allAccepted([], {}), true);
});

test("pending documents: one per category, malformed entries dropped", () => {
    const status = {
        pending: [
            { reason: "MATERIAL_CHANGE", accepted_version: 1, document: doc({ version: 2 }) },
            { reason: "NOT_ACCEPTED", accepted_version: null, document: doc({ version: 2 }) },
            { reason: "NOT_ACCEPTED", accepted_version: null, document: null as never },
        ],
        current: [], unpublished: [], reconsent_required: true,
    };
    const pending = pendingDocuments(status);
    assert.equal(pending.length, 1);
    assert.equal(pending[0].reason, "MATERIAL_CHANGE");
    assert.deepEqual(pendingDocuments(null), []);
});

test("labels", () => {
    assert.equal(categoryLabel("TRADING_RISK_DISCLOSURE"), "Risk Disclosure");
    assert.equal(categoryLabel("UNKNOWN_THING"), "UNKNOWN_THING");
    assert.match(reconsentReasonLabel("MATERIAL_CHANGE"), /Updated/);
    assert.match(reconsentReasonLabel("WITHDRAWN"), /withdrew/);
    assert.match(reconsentReasonLabel("x"), /required/);
});
