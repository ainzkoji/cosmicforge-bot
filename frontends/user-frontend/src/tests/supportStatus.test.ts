/** Step 8.5 -- help desk form rules (secret refusal mirrors the backend) and public status parsing. */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
    SECRET_WARNING, canReply, looksLikeSecret, parseTicket, secretWarning, supportErrorMessage, validateReply,
    validateTicketForm,
} from "../lib/support.ts";
import { activeIncidents, headline, parsePublicStatus, unreachableStatus } from "../lib/statusPage.ts";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

test("secret-looking text is flagged like the backend does", () => {
    for (const s of [
        "my api key: AbCdEfGhIjKlMnOpQrStUvWxYz0123456789AbCdEfGhIjKlMnOpQrStUvWx",
        "secret=Zx81kLmQ09vbNmasdq",
        "password is hunter2hunter2",
        "-----BEGIN RSA PRIVATE KEY-----",
        "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
        "AKIAABCDEFGHIJKLMNOP",
        "123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawq",
        "postgresql://app:hunter2@db/prod",
    ]) assert.equal(looksLikeSecret(s), true, s);
    for (const s of [
        "I forgot my password and the reset email never arrived.",
        "Order 1234567890 on BTCUSDT was not filled; tx 0x" + "ab12".repeat(16),
        "My api key permissions: trading enabled, withdrawals disabled.",
        "uuid 3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ]) assert.equal(looksLikeSecret(s), false, s);
});

test("ticket form validation blocks secrets and empty fields", () => {
    assert.deepEqual(validateTicketForm({ category: "TRADING", subject: "Order stuck", message: "It has not filled." }), []);
    const bad = validateTicketForm({ category: "NOPE", subject: " ", message: "" });
    assert.equal(bad.length, 3);
    const secret = validateTicketForm({ category: "TRADING", subject: "Help", message: "api_secret=Q2xvdWRTZWNyZXQxMjM0" });
    assert.ok(secret.includes(SECRET_WARNING));
    assert.equal(secretWarning("hello"), null);
    assert.deepEqual(validateReply(""), ["Write a reply."]);
    assert.ok(validateReply("token: ghp_abcdefghijklmnopqrstuvwxyz0123")[0] === SECRET_WARNING);
});

test("server refusal maps to the secret warning, other errors keep the server message", () => {
    assert.equal(supportErrorMessage({ response: { data: { detail: { error_code: "SECRET_DETECTED", message: "x" } } } }),
        SECRET_WARNING);
    assert.equal(supportErrorMessage({ response: { data: { detail: { error_code: "TICKET_CLOSED", message: "closed" } } } }),
        "closed");
    assert.equal(supportErrorMessage(new Error("boom")), "boom");
});

test("ticket parsing keeps only the user view and refuses unknown states", () => {
    const t = parseTicket({ ticket_id: "t1", category: "BILLING", subject: "s", status: "WAITING_ON_CUSTOMER",
                            created_at: "a", updated_at: "b", closed_at: null, assigned: true,
                            messages: [{ message_id: "m", from: "SUPPORT", body: "hi", created_at: "c", author_id: "admin-9" }] });
    assert.equal(t.messages?.[0].from, "SUPPORT");
    assert.equal(JSON.stringify(t).includes("admin-9"), false);
    assert.equal(canReply(t), true);
    assert.equal(canReply({ ...t, status: "CLOSED" }), false);
    assert.throws(() => parseTicket({ ...t, status: "LOST" }));
});

test("public status parsing drops unknown fields and never upgrades an unknown state to operational", () => {
    const s = parsePublicStatus({
        generated_at: "2026-10-17T10:00:00Z", overall: "WEIRD", data_available: true, notice: null, internal: "x",
        components: [{ key: "PORTAL", name: "Portal", status: "OPERATIONAL", host: "10.0.0.1" },
                     { key: "TRADING_ENGINE", name: "Trading engine", status: "???" }],
        incidents: [{ id: "i", title: "Delay", status: "INVESTIGATING", impact: "OUTAGE", components: ["Trading engine"],
                      created_by: "admin-1", updates: [{ status: "INVESTIGATING", message: "Looking", at: null, actor: "a" }] }],
    });
    assert.equal(s.overall, "DEGRADED");
    assert.equal(s.components[1].status, "DEGRADED");
    const text = JSON.stringify(s);
    for (const leak of ["10.0.0.1", "admin-1", "internal", "\"actor\""]) assert.equal(text.includes(leak), false, leak);
    assert.equal(activeIncidents(s).length, 1);
    assert.equal(headline(unreachableStatus()).length > 0, true);
    assert.equal(unreachableStatus().overall, "DEGRADED");
});

test("routes: /status is public, support page uses the ticket API", () => {
    const app = src("App.tsx");
    assert.match(app, /<Route path="\/status" element={<Status \/>} \/>/);
    assert.ok(app.indexOf('path="/status"') < app.indexOf("<ProtectedRoute"), "status page must be public");
    const support = src("pages/Support.tsx");
    assert.match(support, /createTicket/);
    assert.match(support, /secretWarning/);
});
