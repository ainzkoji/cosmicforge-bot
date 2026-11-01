import { useState } from "react";
import type { ReactNode } from "react";
import { ConfirmationDialog } from "@/components/UI/ConfirmationDialog";
import { isDemoEnvironment } from "@/utils/tradingMode";

export interface LiveTradingConfirmDialogProps {
    isOpen: boolean;
    onClose: () => void;
    onConfirm: () => void;
    /** e.g. "Start live bot?" */
    title: string;
    /** Label of the confirm button, e.g. "Start live bot". */
    confirmLabel: string;
    /** Broker / exchange name, e.g. "binance". */
    broker: string;
    /** Account label or id the bot trades on. */
    account: string;
    /**
     * Environment of the broker account as reported by the backend. Only a
     * positively DEMO/testnet value switches the dialog to its demo wording;
     * a missing, unloaded or unrecognised environment is treated as real money.
     */
    environment?: string | null;
    /** Title used when the account is a DEMO/testnet account, e.g. "Start bot on demo account?". */
    demoTitle?: string;
    /** Confirm label used when the account is a DEMO/testnet account. */
    demoConfirmLabel?: string;
    /** Additional rows shown in the summary (allocation, risk mode, ...). */
    details?: { label: string; value: ReactNode }[];
    isLoading?: boolean;
    /** Server error from the last attempt, shown inside the dialog. */
    error?: string | null;
}

/**
 * Confirmation required before any action that starts or deploys a bot in
 * LIVE mode (the bot places orders at a broker). The confirm button stays
 * disabled until the user ticks the acknowledgement. Paper actions do not use
 * this dialog.
 *
 * Whether the money is real depends on the broker account: on a positively
 * DEMO/testnet account the dialog says so (no real money) instead of claiming
 * real funds are at risk. Any other environment -- LIVE, unknown or not
 * loaded -- keeps the real-money wording.
 */
export function LiveTradingConfirmDialog(props: LiveTradingConfirmDialogProps) {
    // Mount the body only while open so the acknowledgement resets every time.
    if (!props.isOpen) return null;
    return <LiveTradingConfirmBody {...props} />;
}

function LiveTradingConfirmBody({
    onClose,
    onConfirm,
    title,
    confirmLabel,
    broker,
    account,
    environment,
    demoTitle,
    demoConfirmLabel,
    details = [],
    isLoading = false,
    error = null,
}: LiveTradingConfirmDialogProps) {
    const [acknowledged, setAcknowledged] = useState(false);
    const demo = isDemoEnvironment(environment);

    const rows: { label: string; value: ReactNode }[] = [
        { label: "Broker", value: <span className="capitalize">{broker}</span> },
        { label: "Account", value: account },
        {
            label: "Account environment",
            value: environment
                ? <span className="uppercase">{environment}</span>
                : <span>Unknown (treated as real money)</span>,
        },
        {
            label: "Money at risk",
            value: demo
                ? <span className="font-bold text-blue-500">DEMO exchange — no real money</span>
                : <span className="font-bold text-red-500">LIVE — real money</span>,
        },
        ...details,
    ];

    return (
        <ConfirmationDialog
            isOpen
            onClose={onClose}
            onConfirm={onConfirm}
            title={demo ? (demoTitle ?? title) : title}
            message={demo
                ? "This bot will place orders on your DEMO exchange account. Demo funds are not real money, and demo results do not predict results with real money."
                : "This bot will place real orders on your exchange account. Real funds are at risk and you can lose some or all of the capital you allocate."}
            confirmLabel={demo ? (demoConfirmLabel ?? confirmLabel) : confirmLabel}
            variant={demo ? "warning" : "danger"}
            isLoading={isLoading}
            confirmDisabled={!acknowledged}
        >
            <dl className="rounded-lg border border-border divide-y divide-border text-sm">
                {rows.map((row) => (
                    <div key={row.label} className="flex items-center justify-between gap-4 px-3 py-2">
                        <dt className="text-muted-foreground">{row.label}</dt>
                        <dd className="font-medium text-right break-all">{row.value}</dd>
                    </div>
                ))}
            </dl>

            <label className="mt-4 flex items-start gap-3 text-sm cursor-pointer">
                <input
                    type="checkbox"
                    checked={acknowledged}
                    onChange={(e) => setAcknowledged(e.target.checked)}
                    className="mt-0.5 h-4 w-4 shrink-0"
                />
                <span>
                    {demo
                        ? "I understand that this bot will place orders on my demo exchange account and that demo results do not predict results with real money."
                        : "I understand that this uses real money, that losses are possible, and that past or simulated results do not predict live results."}
                </span>
            </label>

            {error && (
                <p role="alert" className="mt-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-500">
                    {error}
                </p>
            )}
        </ConfirmationDialog>
    );
}
