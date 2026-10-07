import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { AlertCircle, ArrowRight, CheckCircle, Clock, Loader2 } from "lucide-react";
import { motion } from "framer-motion";
import { api } from "@/api/client";

// The plan is activated server-side when the payment provider's webhook
// arrives, which is usually seconds after the redirect back to this page.
const POLL_INTERVAL_MS = 2000;
const MAX_ATTEMPTS = 30; // ~1 minute

type Phase =
    | "checking"        // asking the backend whether the payment was confirmed
    | "confirmed"       // backend reports the subscription is active for this session
    | "pending"         // not confirmed within the polling window
    | "not_found"       // this session does not belong to the logged-in account
    | "unauthenticated" // no session to ask with
    | "missing";        // no session_id in the URL: nothing to verify

export default function PaymentSuccess() {
    const [searchParams] = useSearchParams();
    const sessionId = searchParams.get("session_id");
    const [phase, setPhase] = useState<Phase>(sessionId ? "checking" : "missing");
    const [round, setRound] = useState(0);
    // Identifies the current polling run; an older run stops as soon as it changes
    // (unmount, "Check Again", or React re-running the effect).
    const activeRun = useRef(0);

    const poll = useCallback(async (id: string, run: number) => {
        const stale = () => activeRun.current !== run;
        for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            if (stale()) return;
            try {
                const result = await api.getCheckoutStatus(id);
                if (stale()) return;
                if (result.status === "active") {
                    setPhase("confirmed");
                    return;
                }
            } catch (err: any) {
                if (stale()) return;
                if (err?.status === 401) {
                    setPhase("unauthenticated");
                    return;
                }
                if (err?.status === 404) {
                    setPhase("not_found");
                    return;
                }
                // Anything else (network blip, 5xx) is retried until the window closes.
            }
            await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
        }
        if (!stale()) setPhase("pending");
    }, []);

    useEffect(() => {
        const run = ++activeRun.current;
        if (!sessionId) {
            setPhase("missing");
            return;
        }
        if (!localStorage.getItem("access_token")) {
            setPhase("unauthenticated");
            return;
        }
        setPhase("checking");
        poll(sessionId, run);
        return () => {
            // Invalidate this run so its loop exits.
            if (activeRun.current === run) activeRun.current++;
        };
    }, [sessionId, round, poll]);

    const card = (() => {
        switch (phase) {
            case "checking":
                return {
                    icon: <Loader2 className="w-10 h-10 text-primary animate-spin" />,
                    tone: "bg-primary/10",
                    title: "Confirming your payment...",
                    body: "We are waiting for confirmation from the payment provider. This usually takes a few seconds. Please keep this page open.",
                };
            case "confirmed":
                return {
                    icon: <CheckCircle className="w-10 h-10 text-green-500" />,
                    tone: "bg-green-500/10",
                    title: "Payment confirmed",
                    body: "Your subscription is active and your new plan limits apply now.",
                };
            case "pending":
                return {
                    icon: <Clock className="w-10 h-10 text-yellow-500" />,
                    tone: "bg-yellow-500/10",
                    title: "Payment not confirmed yet",
                    body: "We have not received confirmation from the payment provider, so your plan has not changed yet. If you completed the payment it will activate automatically as soon as the confirmation arrives. If it still has not after a few minutes, contact support.",
                };
            case "not_found":
                return {
                    icon: <AlertCircle className="w-10 h-10 text-red-500" />,
                    tone: "bg-red-500/10",
                    title: "We could not verify this payment",
                    body: "This checkout does not belong to the account you are logged in with, so nothing was changed. Check your subscription page, or log in with the account you used to pay.",
                };
            case "unauthenticated":
                return {
                    icon: <AlertCircle className="w-10 h-10 text-yellow-500" />,
                    tone: "bg-yellow-500/10",
                    title: "Log in to see your payment status",
                    body: "Your session has ended. Log in again and open your subscription page to see whether the payment was confirmed.",
                };
            default:
                return {
                    icon: <AlertCircle className="w-10 h-10 text-muted-foreground" />,
                    tone: "bg-muted",
                    title: "No payment to verify",
                    body: "This page was opened without a checkout reference. Your current plan is shown on the subscription page.",
                };
        }
    })();

    return (
        <div className="min-h-screen flex items-center justify-center bg-background p-4">
            <motion.div
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                className="max-w-md w-full bg-card border border-border rounded-2xl p-8 text-center shadow-2xl"
            >
                <div className={`w-20 h-20 ${card.tone} rounded-full flex items-center justify-center mx-auto mb-6`}>
                    {card.icon}
                </div>

                <h1 className="text-3xl font-bold mb-4">{card.title}</h1>
                <p className="text-muted-foreground mb-8" role="status" aria-live="polite">
                    {card.body}
                </p>

                <div className="space-y-3">
                    {phase === "confirmed" && (
                        <Link
                            to="/dashboard"
                            className="flex items-center justify-center gap-2 w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-colors"
                        >
                            Go to Dashboard <ArrowRight className="w-4 h-4" />
                        </Link>
                    )}
                    {phase === "pending" && (
                        <button
                            onClick={() => setRound((value) => value + 1)}
                            className="block w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-colors"
                        >
                            Check Again
                        </button>
                    )}
                    {phase === "unauthenticated" && (
                        <Link
                            to="/login"
                            className="block w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-colors"
                        >
                            Log In
                        </Link>
                    )}
                    {phase !== "checking" && phase !== "unauthenticated" && (
                        <Link
                            to="/dashboard/subscription"
                            className="block w-full py-3 border border-border text-foreground rounded-lg font-medium hover:bg-muted transition-colors"
                        >
                            {phase === "confirmed" ? "View Subscription & Receipts" : "View Subscription"}
                        </Link>
                    )}
                </div>
            </motion.div>
        </div>
    );
}
