import { Link } from "react-router-dom";
import { ArrowRight, Globe, Shield, Layers, Bot, Check } from "lucide-react";
import { useMarketing } from "@/context/MarketingContext";
import { motion } from "framer-motion";
import { RiskNotice } from "@/components/Legal/RiskNotice";

const STEPS = [
    {
        icon: Globe,
        title: "1. Connect your exchange",
        desc: "Link a Binance Futures demo account with an API key you create. Use a trade-only key with withdrawals disabled.",
    },
    {
        icon: Layers,
        title: "2. Choose a risk profile",
        desc: "Pick Conservative, Balanced or Aggressive settings for the built-in trading engine.",
    },
    {
        icon: Shield,
        title: "3. Set your limits",
        desc: "Set a capital budget, the amount per position and a daily loss limit.",
    },
    {
        icon: Bot,
        title: "4. Run and monitor",
        desc: "Start in paper or demo mode, watch what the bot does, and pause or stop it whenever you want.",
    },
];

const FEATURES = [
    {
        title: "Binance Futures demo trading",
        desc: "Automated execution runs against Binance Futures demo accounts today. Other brokers and markets are planned and not available yet.",
    },
    {
        title: "Rules-based engine",
        desc: "Entries and exits follow a defined set of rules. It is software, not a promise: a strategy can lose money.",
    },
    {
        title: "Risk controls you set",
        desc: "Per-bot capital budget, position sizing and a daily loss limit that blocks new entries once it is reached.",
    },
    {
        title: "Paper and demo first",
        desc: "Run with virtual funds before you consider real money. Simulated results differ from live results.",
    },
];

export default function LandingPage() {
    const { trackEvent } = useMarketing();

    return (
        <div className="bg-background text-foreground overflow-x-hidden">
            {/* 1. Hero Section */}
            <section className="relative pt-32 pb-24 px-6 overflow-hidden">
                {/* Background Blobs */}
                <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-primary/10 rounded-full blur-[100px] -z-10" />
                <div className="absolute bottom-0 left-0 w-[500px] h-[500px] bg-purple-500/10 rounded-full blur-[100px] -z-10" />

                <div className="max-w-7xl mx-auto flex flex-col items-center text-center">
                    <motion.div
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.5 }}
                    >
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-sm font-semibold mb-6">
                            <Bot className="w-4 h-4" /> Automated trading tools
                        </div>
                        <h1 className="text-5xl md:text-7xl font-bold tracking-tight mb-6 max-w-4xl">
                            Rules-based trading automation on <span className="text-primary">your own exchange account</span>
                        </h1>
                        <p className="text-xl text-muted-foreground mb-10 max-w-2xl mx-auto leading-relaxed">
                            CosmicForge places and manages orders automatically on your own exchange account, using
                            rules and risk limits you choose. You stay in control and can stop your bots at any time.
                            Trading involves risk, including the loss of your capital.
                        </p>

                        <div className="flex flex-col sm:flex-row gap-4 justify-center">
                            <Link
                                to="/register"
                                onClick={() => trackEvent("cta_click", "/", { label: "hero_primary" })}
                                className="inline-flex items-center justify-center gap-2 px-8 py-4 bg-primary text-primary-foreground font-bold rounded-xl text-lg hover:bg-primary/90 hover:scale-105 transition-all shadow-lg hover:shadow-primary/25"
                            >
                                Create account <ArrowRight className="w-5 h-5" />
                            </Link>
                            <Link
                                to="/how-it-works"
                                className="inline-flex items-center justify-center gap-2 px-8 py-4 bg-card border border-border font-bold rounded-xl text-lg hover:bg-muted transition-all"
                            >
                                How it works
                            </Link>
                        </div>

                        <RiskNotice className="mt-8 max-w-2xl mx-auto text-muted-foreground border border-border rounded-xl px-4 py-3 bg-card/60" />
                    </motion.div>

                    {/* Illustrative interface mock-up: deliberately contains no account or profit figures */}
                    <motion.div
                        initial={{ opacity: 0, y: 40 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.7, delay: 0.2 }}
                        className="mt-16 w-full max-w-5xl bg-card border border-gray-200 dark:border-gray-800 rounded-2xl shadow-2xl overflow-hidden"
                        aria-hidden="true"
                    >
                        <div className="bg-muted/50 p-2 border-b border-border flex gap-2">
                            <div className="w-3 h-3 rounded-full bg-red-400" />
                            <div className="w-3 h-3 rounded-full bg-amber-400" />
                            <div className="w-3 h-3 rounded-full bg-green-400" />
                        </div>
                        <div className="aspect-[16/9] bg-gradient-to-br from-gray-900 to-black p-8 flex items-center justify-center relative">
                            <div className="absolute inset-0 bg-grid-white/5 bg-[size:30px_30px]" />
                            <div className="relative z-10 text-center space-y-4">
                                <div className="px-4 py-2 bg-white/10 text-gray-200 rounded-lg inline-block border border-white/20 backdrop-blur-md">
                                    <span className="font-mono text-sm uppercase tracking-wider">Illustrative interface — not real account data</span>
                                </div>
                                <div className="flex gap-4 opacity-75 justify-center">
                                    <div className="w-32 h-20 bg-gray-800 rounded-lg" />
                                    <div className="w-32 h-20 bg-gray-800 rounded-lg" />
                                    <div className="w-32 h-20 bg-gray-800 rounded-lg" />
                                </div>
                            </div>
                        </div>
                    </motion.div>
                </div>
            </section>

            {/* 2. How It Works */}
            <section className="py-24 bg-muted/30">
                <div className="max-w-7xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <h2 className="text-3xl md:text-4xl font-bold mb-4">How It Works</h2>
                        <p className="text-muted-foreground text-lg max-w-2xl mx-auto">
                            Set up a bot on a demo account without writing code.
                        </p>
                    </div>

                    <div className="grid md:grid-cols-4 gap-8">
                        {STEPS.map((step, i) => (
                            <div key={step.title} className="relative group">
                                <div className="bg-card border border-border p-6 rounded-2xl h-full hover:-translate-y-2 transition-transform duration-300 shadow-sm hover:shadow-xl">
                                    <div className="w-12 h-12 bg-primary/10 rounded-xl flex items-center justify-center text-primary mb-4 group-hover:bg-primary group-hover:text-white transition-colors">
                                        <step.icon className="w-6 h-6" />
                                    </div>
                                    <h3 className="font-bold text-xl mb-2">{step.title}</h3>
                                    <p className="text-muted-foreground text-sm leading-relaxed">{step.desc}</p>
                                </div>
                                {i < STEPS.length - 1 && (
                                    <div className="hidden md:block absolute top-1/2 -right-4 translate-x-1/2 -translate-y-1/2 z-10 text-muted-foreground/30">
                                        <ArrowRight className="w-6 h-6" />
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* 3. What is available today */}
            <section className="py-24 px-6">
                <div className="max-w-7xl mx-auto">
                    <div className="grid md:grid-cols-2 gap-16 items-center">
                        <div className="space-y-8">
                            <div>
                                <h2 className="text-3xl md:text-4xl font-bold mb-4">What you get today</h2>
                                <p className="text-muted-foreground text-lg">
                                    CosmicForge is at an early stage. This is what is available now.
                                </p>
                            </div>

                            <div className="space-y-6">
                                {FEATURES.map((feat) => (
                                    <div key={feat.title} className="flex gap-4">
                                        <div className="mt-1">
                                            <div className="w-6 h-6 rounded-full bg-green-500/20 flex items-center justify-center text-green-600">
                                                <Check className="w-3.5 h-3.5 stroke-[3px]" />
                                            </div>
                                        </div>
                                        <div>
                                            <h3 className="font-bold text-lg">{feat.title}</h3>
                                            <p className="text-muted-foreground">{feat.desc}</p>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                        <div className="bg-gradient-to-br from-primary/5 to-purple-500/5 rounded-3xl p-8 border border-primary/10 relative" aria-hidden="true">
                            {/* Illustration only: no performance or confidence figures */}
                            <div className="space-y-4">
                                <div className="bg-card rounded-xl p-4 shadow-lg border border-border">
                                    <div className="flex justify-between items-center mb-3">
                                        <div className="flex items-center gap-2">
                                            <Bot className="w-5 h-5 text-purple-500" />
                                            <span className="font-bold">Example bot</span>
                                        </div>
                                        <span className="text-xs bg-blue-500/10 text-blue-600 px-2 py-1 rounded font-bold">DEMO</span>
                                    </div>
                                    <div className="flex justify-between text-xs text-muted-foreground">
                                        <span>Mode: paper</span>
                                        <span>Daily loss limit: set by you</span>
                                    </div>
                                </div>
                                <p className="text-xs text-muted-foreground text-center">Illustration only — not real account data.</p>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            {/* 4. Pricing: plans are loaded from the backend on the pricing page, never duplicated here */}
            <section className="py-24 bg-gray-50 dark:bg-gray-900/50">
                <div className="max-w-3xl mx-auto px-6 text-center">
                    <h2 className="text-3xl md:text-4xl font-bold mb-4">Plans and pricing</h2>
                    <p className="text-muted-foreground mb-8">
                        Current plans, limits and prices are listed on the pricing page.
                    </p>
                    <Link
                        to="/pricing"
                        onClick={() => trackEvent("cta_click", "/", { label: "pricing_link" })}
                        className="inline-flex items-center justify-center gap-2 px-8 py-3 border border-primary text-primary font-bold rounded-lg hover:bg-primary/5 transition-colors"
                    >
                        View plans and pricing <ArrowRight className="w-5 h-5" />
                    </Link>
                </div>
            </section>

            {/* 5. Final CTA */}
            <section className="py-24 px-6 bg-[#0F172A] text-white">
                <div className="max-w-4xl mx-auto text-center">
                    <h2 className="text-4xl md:text-5xl font-bold mb-6">Try it with virtual funds first</h2>
                    <p className="text-xl text-gray-400 mb-10">
                        Create an account, connect a demo exchange account and see how the bot behaves before you
                        consider using real money.
                    </p>
                    <Link
                        to="/register"
                        onClick={() => trackEvent("cta_click", "/", { label: "bottom_cta" })}
                        className="inline-flex items-center gap-2 px-10 py-5 bg-primary text-primary-foreground font-bold rounded-full text-xl hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/50"
                    >
                        Create account <ArrowRight className="w-6 h-6" />
                    </Link>
                    <RiskNotice className="mt-10 max-w-2xl mx-auto text-gray-400" />
                </div>
            </section>
        </div>
    );
}
