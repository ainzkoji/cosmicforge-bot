/**
 * Subscription page view model (Section H Step 8.1).
 *
 * Derived only from GET /api/billing/subscription, which reports what the
 * signed provider webhooks recorded. Nothing here reads the checkout redirect
 * or any other client-side signal of a "successful" payment.
 */

export type Tone = "ok" | "warning" | "error" | "neutral";

export interface ServerNotice {
    code: string;
    severity?: string;
    message?: string;
    until?: string | null;
}

export interface SubscriptionLike {
    plan?: { id?: string; name?: string; price?: number; interval?: string } | null;
    status?: string | null;
    provider_status?: string | null;
    current_period_end?: string | null;
    cancel_at_period_end?: boolean | null;
    grace_period_end?: string | null;
    billing_hold?: boolean | null;
    notices?: ServerNotice[] | null;
    portal_available?: boolean | null;
    source?: string | null;
}

export interface BillingNotice {
    code: string;
    tone: Tone;
    message: string;
    until: string | null;
}

export interface SubscriptionView {
    planId: string;
    planName: string;
    isPaid: boolean;
    statusLabel: string;
    tone: Tone;
    renewalLabel: string;
    notices: BillingNotice[];
    canCancel: boolean;
    canResume: boolean;
    canOpenPortal: boolean;
}

export const FREE_PLAN_ID = "plan_free";

const DEFAULT_MESSAGES: Record<string, { tone: Tone; message: string }> = {
    PAYMENT_UNDER_REVIEW: {
        tone: "error",
        message: "A refund or payment dispute on your account is being reviewed. Paid features are paused until the review is finished.",
    },
    PAYMENT_FAILED: {
        tone: "error",
        message: "Your last payment failed. Update your payment method to keep your plan.",
    },
    CANCELLATION_SCHEDULED: {
        tone: "warning",
        message: "Your subscription is set to end at the end of the current period.",
    },
    SUBSCRIPTION_ENDED: {
        tone: "neutral",
        message: "Your paid subscription has ended. You are on the free plan.",
    },
};

const NOTICE_ORDER = ["PAYMENT_UNDER_REVIEW", "PAYMENT_FAILED", "CANCELLATION_SCHEDULED", "SUBSCRIPTION_ENDED"];

function toneFromSeverity(severity: string | undefined, fallback: Tone): Tone {
    if (severity === "error") return "error";
    if (severity === "warning") return "warning";
    if (severity === "info") return fallback === "error" ? "warning" : fallback;
    return fallback;
}

function lower(value: unknown): string {
    return typeof value === "string" ? value.trim().toLowerCase() : "";
}

/** Notices to show: the server's list, plus ones implied by the status fields (older servers). */
export function deriveNotices(sub: SubscriptionLike | null | undefined): BillingNotice[] {
    if (!sub) return [];
    const byCode = new Map<string, BillingNotice>();
    for (const notice of sub.notices ?? []) {
        if (!notice || typeof notice.code !== "string" || !notice.code) continue;
        const known = DEFAULT_MESSAGES[notice.code];
        byCode.set(notice.code, {
            code: notice.code,
            tone: toneFromSeverity(notice.severity, known?.tone ?? "neutral"),
            message: (typeof notice.message === "string" && notice.message.trim()) || known?.message || notice.code,
            until: notice.until ?? null,
        });
    }
    const paid = isPaidPlan(sub);
    const implied: Array<[string, boolean, string | null]> = [
        ["PAYMENT_UNDER_REVIEW", sub.billing_hold === true, null],
        ["PAYMENT_FAILED", paid && lower(sub.status) === "past_due", sub.grace_period_end ?? null],
        ["CANCELLATION_SCHEDULED", paid && sub.cancel_at_period_end === true, sub.current_period_end ?? null],
    ];
    for (const [code, applies, until] of implied) {
        if (applies && !byCode.has(code)) {
            byCode.set(code, { code, tone: DEFAULT_MESSAGES[code].tone, message: DEFAULT_MESSAGES[code].message, until });
        }
    }
    const rank = (code: string) => {
        const index = NOTICE_ORDER.indexOf(code);
        return index === -1 ? NOTICE_ORDER.length : index;
    };
    return [...byCode.values()].sort((a, b) => rank(a.code) - rank(b.code));
}

export function isPaidPlan(sub: SubscriptionLike | null | undefined): boolean {
    const planId = sub?.plan?.id;
    return typeof planId === "string" && planId !== "" && planId !== FREE_PLAN_ID;
}

export function formatBillingDate(value: string | null | undefined): string {
    if (!value) return "—";
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? "—" : parsed.toLocaleDateString();
}

export function deriveSubscriptionView(
    sub: SubscriptionLike | null | undefined,
    formatDate: (value: string | null | undefined) => string = formatBillingDate,
): SubscriptionView {
    const paid = isPaidPlan(sub);
    const status = lower(sub?.status);
    const held = sub?.billing_hold === true;
    const cancelling = paid && sub?.cancel_at_period_end === true;

    let statusLabel = "Free plan";
    let tone: Tone = "neutral";
    if (held) {
        statusLabel = "Under review";
        tone = "error";
    } else if (paid && status === "past_due") {
        statusLabel = "Payment failed";
        tone = "error";
    } else if (cancelling) {
        statusLabel = "Cancels at period end";
        tone = "warning";
    } else if (paid && status === "trialing") {
        statusLabel = "Trial";
        tone = "ok";
    } else if (paid && status === "active") {
        statusLabel = "Active";
        tone = "ok";
    } else if (paid) {
        statusLabel = status || "Unknown";
        tone = "warning";
    }

    let renewalLabel = "No billing scheduled.";
    if (paid && status === "past_due") {
        renewalLabel = `Access continues until ${formatDate(sub?.grace_period_end)} unless the payment is updated.`;
    } else if (cancelling) {
        renewalLabel = `Access continues until ${formatDate(sub?.current_period_end)}.`;
    } else if (paid && sub?.current_period_end) {
        renewalLabel = `Renews on ${formatDate(sub.current_period_end)}.`;
    }

    return {
        planId: sub?.plan?.id || FREE_PLAN_ID,
        planName: sub?.plan?.name || "Free",
        isPaid: paid,
        statusLabel,
        tone,
        renewalLabel,
        notices: deriveNotices(sub),
        canCancel: paid && !held && !cancelling && ["active", "trialing", "past_due"].includes(status),
        canResume: paid && !held && cancelling,
        canOpenPortal: sub?.portal_available === true,
    };
}
