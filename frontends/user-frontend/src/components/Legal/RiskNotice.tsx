import { Link } from "react-router-dom";

/**
 * Short risk warning shown next to calls-to-action and in the public footer.
 * Keep the wording factual; it is not a substitute for the Risk Disclosure page.
 */
export function RiskNotice({ className = "" }: { className?: string }) {
    return (
        <p role="note" className={`text-sm leading-relaxed ${className}`}>
            <strong>Risk warning:</strong> trading leveraged products carries a high risk of loss, and you can lose
            some or all of your capital. Past or simulated performance does not indicate future results. Nothing on
            this site is financial advice.{" "}
            <Link to="/risk-disclosure" className="underline hover:no-underline">
                Read the Risk Disclosure
            </Link>
            .
        </p>
    );
}
