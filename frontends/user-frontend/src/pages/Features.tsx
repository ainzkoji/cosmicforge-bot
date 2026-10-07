import { Link } from "react-router-dom";
import { ArrowLeft, Globe, Shield, BarChart3, Lock, TestTube, Layers } from "lucide-react";
import { useMarketing } from "@/context/MarketingContext";
import { RiskNotice } from "@/components/Legal/RiskNotice";

interface Feature {
    icon: typeof Globe;
    color: string;
    title: string;
    description: string;
    /** Shown as a badge. Leave unset for features that are available today. */
    badge?: string;
}

// This list is written here on purpose and describes only what the product does
// today. Do not add performance figures, profit claims or features that are not
// released; mark anything that is not available yet as "Planned".
const FEATURES: Feature[] = [
    {
        icon: Layers,
        color: "bg-purple-100 text-purple-600",
        title: "Rules-based trading engine",
        description: "Entries and exits follow a defined set of rules. It is software, not a promise: a strategy can lose money.",
    },
    {
        icon: Globe,
        color: "bg-indigo-100 text-indigo-600",
        title: "Binance Futures demo trading",
        description: "Automated execution runs against Binance Futures demo accounts today, on an exchange account that you connect.",
    },
    {
        icon: Lock,
        color: "bg-red-100 text-red-600",
        title: "Your own API key",
        description: "You connect the exchange with an API key you create. Use a trade-only key with withdrawals disabled.",
    },
    {
        icon: Shield,
        color: "bg-red-100 text-red-600",
        title: "Risk controls you set",
        description: "Choose a Conservative, Balanced or Aggressive risk profile, then set a capital budget, the amount per position and a daily loss limit that blocks new entries once it is reached.",
    },
    {
        icon: TestTube,
        color: "bg-orange-100 text-orange-600",
        title: "Paper and demo first",
        description: "Run with virtual funds before you consider real money. Simulated results differ from live results.",
    },
    {
        icon: BarChart3,
        color: "bg-cyan-100 text-cyan-600",
        title: "Monitor and stop at any time",
        description: "Watch what your bot does, and pause or stop it whenever you want.",
    },
    {
        icon: Globe,
        color: "bg-gray-100 text-gray-600",
        title: "Other brokers and markets",
        description: "Support for other brokers and markets is planned. It is not available yet.",
        badge: "Planned",
    },
];

export default function Features() {
    const { trackEvent } = useMarketing();

    return (
        <div className="bg-white">
            {/* Hero */}
            <section className="pt-32 pb-16 px-6 bg-gradient-to-b from-gray-50 to-white">
                <div className="max-w-4xl mx-auto text-center">
                    <Link to="/" className="inline-flex items-center gap-2 text-gray-600 hover:text-[#1E1B4B] mb-6 transition-colors">
                        <ArrowLeft className="w-4 h-4" /> Back to Home
                    </Link>
                    <h1 className="text-4xl md:text-5xl font-bold text-[#1E1B4B] mb-6">
                        Features
                    </h1>
                    <p className="text-xl text-gray-600 max-w-2xl mx-auto">
                        What CosmicForge does today: rules-based trading automation on your own Binance Futures demo
                        account, with risk limits you choose. Trading involves risk, including the loss of your capital.
                    </p>
                </div>
            </section>

            {/* Features Grid */}
            <section className="py-16 px-6">
                <div className="max-w-7xl mx-auto">
                    <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
                        {FEATURES.map((feature) => {
                            const Icon = feature.icon;

                            return (
                                <div key={feature.title} className="bg-white rounded-2xl p-8 border border-gray-200 hover:shadow-xl transition-all hover:-translate-y-1 relative overflow-hidden">
                                    {feature.badge && (
                                        <div className="absolute top-4 right-4 px-3 py-1 bg-yellow-100 text-yellow-700 text-xs font-bold rounded-full uppercase tracking-wide">
                                            {feature.badge}
                                        </div>
                                    )}
                                    <div className={`w-14 h-14 rounded-xl ${feature.color} flex items-center justify-center mb-6`}>
                                        <Icon className="w-7 h-7" />
                                    </div>
                                    <h3 className="text-xl font-semibold text-[#1E1B4B] mb-3">{feature.title}</h3>
                                    <p className="text-gray-600 leading-relaxed">{feature.description}</p>
                                </div>
                            );
                        })}
                    </div>
                </div>
            </section>

            {/* CTA */}
            <section className="py-20 px-6 bg-[#1E1B4B]">
                <div className="max-w-4xl mx-auto text-center">
                    <h2 className="text-3xl md:text-4xl font-bold text-white mb-4">
                        Try it with virtual funds first
                    </h2>
                    <p className="text-gray-300 mb-8">
                        Create an account, connect a demo exchange account and see how the bot behaves before you
                        consider using real money.
                    </p>
                    <Link
                        to="/register"
                        onClick={() => trackEvent("cta_click", "/features", { label: "bottom_cta" })}
                        className="inline-flex items-center gap-2 px-8 py-4 bg-white text-[#1E1B4B] font-semibold rounded-lg hover:bg-gray-100 transition-colors text-lg"
                    >
                        Create account
                    </Link>
                    <RiskNotice className="mt-10 max-w-2xl mx-auto text-gray-300" />
                </div>
            </section>
        </div>
    );
}
