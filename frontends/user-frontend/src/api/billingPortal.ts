/** Provider customer portal (payment method, cancellation, receipts), Section H Step 8.1. */
import { API_BASE, apiFetch, responseErrorMessage } from "./http";

/** Asks the server for a portal session; the URL comes from the payment provider. */
export async function openBillingPortal(): Promise<string> {
    const res = await apiFetch(`${API_BASE}/api/billing/subscription/manage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "portal" }),
    });
    if (!res.ok) throw new Error(await responseErrorMessage(res, "Could not open the billing portal"));
    const body: unknown = await res.json();
    const url = typeof body === "object" && body !== null ? (body as Record<string, unknown>).portal_url : null;
    if (typeof url !== "string" || !/^https:\/\//.test(url)) throw new Error("The billing portal is not available.");
    return url;
}
