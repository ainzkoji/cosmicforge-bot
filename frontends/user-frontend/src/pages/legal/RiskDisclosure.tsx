import type { ReactNode } from "react";
import { Link } from "react-router-dom";

const SUPPORT_EMAIL = "support@cosmicforge.com";

function Section({ title, children }: { title: string; children: ReactNode }) {
    return (
        <section className="space-y-3">
            <h2 className="text-2xl font-bold text-[#1E1B4B]">{title}</h2>
            {children}
        </section>
    );
}

export default function RiskDisclosure() {
    return (
        <div className="bg-white">
            <article className="max-w-3xl mx-auto px-6 pt-32 pb-24 space-y-10 text-gray-700 leading-relaxed">
                <header className="space-y-4">
                    <h1 className="text-4xl font-bold text-[#1E1B4B]">Risk Disclosure</h1>
                    <p className="text-lg">
                        Please read this page before you use CosmicForge. It explains what the product does and the
                        risks you take when you use it. If you do not understand or accept these risks, do not use
                        the product with real money.
                    </p>
                </header>

                <Section title="What the product does">
                    <ul className="list-disc ml-6 space-y-2">
                        <li>
                            CosmicForge is software that automates order placement on <strong>your own exchange
                            account</strong>. It connects through API keys that you create at your exchange and
                            provide to us.
                        </li>
                        <li>
                            We do not hold customer funds. Your money stays in your account at your exchange, and
                            deposits and withdrawals are made by you at the exchange.
                        </li>
                        <li>
                            The software follows rules and the settings you choose (for example capital budget,
                            position size and daily loss limit). It does not know your personal circumstances.
                        </li>
                    </ul>
                </Section>

                <Section title="Risks">
                    <ul className="list-disc ml-6 space-y-2">
                        <li>
                            <strong>Loss of capital.</strong> You can lose some or all of the money you trade with.
                        </li>
                        <li>
                            <strong>Leverage and liquidation.</strong> Futures and other leveraged products multiply
                            losses as well as gains. A small price move against a leveraged position can lead to
                            liquidation by the exchange and the loss of your margin.
                        </li>
                        <li>
                            <strong>Software, connectivity and exchange failures.</strong> Our software, your
                            connection, third-party services or the exchange can fail, be slow or be unavailable.
                            Orders may be delayed, rejected, partly filled or filled at a different price than
                            expected, and protective orders may not execute. Positions can remain open while the
                            software is not running.
                        </li>
                        <li>
                            <strong>The strategy may lose money.</strong> No trading strategy works in all market
                            conditions. A strategy that performed well in the past can lose money in the future.
                        </li>
                        <li>
                            <strong>No guarantee of profit.</strong> We do not promise or guarantee any profit,
                            return, win rate or maximum loss. Risk settings and loss limits reduce exposure but do
                            not cap your loss.
                        </li>
                        <li>
                            <strong>Simulated results differ from live results.</strong> Paper trading, demo
                            accounts and backtests do not reflect real fills, fees, slippage, liquidity or your own
                            behaviour under pressure. Past or simulated performance does not indicate future
                            results.
                        </li>
                    </ul>
                </Section>

                <Section title="Your responsibilities">
                    <ul className="list-disc ml-6 space-y-2">
                        <li>
                            <strong>API keys.</strong> Create keys with trading permission only. Never enable
                            withdrawal permission on a key you give to us or to anyone else. Restrict keys by IP
                            address where your exchange allows it, and revoke a key at the exchange if you think it
                            has been exposed.
                        </li>
                        <li>
                            <strong>Monitoring.</strong> Check your bots, open positions and exchange account
                            regularly. Stopping a bot does not close positions that are already open; close them at
                            your exchange if you want them closed.
                        </li>
                        <li>
                            <strong>Your own decisions.</strong> You decide whether to trade, how much capital to
                            use and which settings to choose. Only use money you can afford to lose.
                        </li>
                        <li>
                            <strong>Taxes and rules.</strong> You are responsible for your own tax reporting and for
                            checking that using this product and trading these instruments is permitted where you
                            live.
                        </li>
                    </ul>
                </Section>

                <Section title="Not investment advice">
                    <p>
                        Nothing in the product or on this website is investment, financial, legal or tax advice, or
                        a recommendation to buy or sell any instrument. Signals, statistics and educational content
                        are provided for information only. If you are unsure, speak to an independent, qualified
                        adviser.
                    </p>
                </Section>

                <footer className="border-t border-gray-200 pt-6 text-sm text-gray-500 space-y-2">
                    <p>
                        Questions about this page: <a href={`mailto:${SUPPORT_EMAIL}`} className="underline">{SUPPORT_EMAIL}</a>
                    </p>
                    <p>
                        See also <Link to="/terms" className="underline">Terms of Service</Link> and{" "}
                        <Link to="/privacy" className="underline">Privacy Policy</Link>.
                    </p>
                </footer>
            </article>
        </div>
    );
}
