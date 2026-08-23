import { Link } from "react-router-dom";
import { ArrowRight, Zap, Globe, Shield, BarChart3, Lock, TestTube, PlayCircle, Layers, Settings, Bot, Check, Star } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { useMarketing } from "@/context/MarketingContext";
import { motion } from "framer-motion";

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
                            <Zap className="w-4 h-4" /> AI-Powered Trading for Everyone
                        </div>
                        <h1 className="text-5xl md:text-7xl font-bold tracking-tight mb-6 max-w-4xl">
                            Automate <span className="text-primary">Crypto, Forex & Stock</span> Trading with AI
                        </h1>
                        <p className="text-xl text-muted-foreground mb-10 max-w-2xl mx-auto leading-relaxed">
                            Stop staring at charts. Let our institutional-grade AI analyze markets 24/7 and execute meaningful trades while you sleep.
                        </p>

                        <div className="flex flex-col sm:flex-row gap-4 justify-center">
                            <Link
                                to="/register"
                                onClick={() => trackEvent("cta_click", "/", { label: "hero_primary" })}
                                className="inline-flex items-center justify-center gap-2 px-8 py-4 bg-primary text-primary-foreground font-bold rounded-xl text-lg hover:bg-primary/90 hover:scale-105 transition-all shadow-lg hover:shadow-primary/25"
                            >
                                Get Started Free <ArrowRight className="w-5 h-5" />
                            </Link>
                            <Link
                                to="/how-it-works"
                                className="inline-flex items-center justify-center gap-2 px-8 py-4 bg-card border border-border font-bold rounded-xl text-lg hover:bg-muted transition-all"
                            >
                                <PlayCircle className="w-5 h-5" /> Watch Demo
                            </Link>
                        </div>
                    </motion.div>

                    {/* Hero Image/Mockup */}
                    <motion.div
                        initial={{ opacity: 0, y: 40 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.7, delay: 0.2 }}
                        className="mt-16 w-full max-w-5xl bg-card border border-gray-200 dark:border-gray-800 rounded-2xl shadow-2xl overflow-hidden"
                    >
                        <div className="bg-muted/50 p-2 border-b border-border flex gap-2">
                            <div className="w-3 h-3 rounded-full bg-red-400" />
                            <div className="w-3 h-3 rounded-full bg-amber-400" />
                            <div className="w-3 h-3 rounded-full bg-green-400" />
                        </div>
                        <div className="aspect-[16/9] bg-gradient-to-br from-gray-900 to-black p-8 flex items-center justify-center relative">
                            {/* Abstract UI Representation */}
                            <div className="absolute inset-0 bg-grid-white/5 bg-[size:30px_30px]" />
                            <div className="relative z-10 text-center space-y-4">
                                <div className="p-4 bg-green-500/20 text-green-500 rounded-lg inline-block border border-green-500/30 backdrop-blur-md">
                                    <span className="font-mono font-bold text-2xl">+$2,453.80 (24h)</span>
                                </div>
                                <div className="flex gap-4 opacity-75">
                                    <div className="w-32 h-20 bg-gray-800 rounded-lg animate-pulse" />
                                    <div className="w-32 h-20 bg-gray-800 rounded-lg animate-pulse delay-75" />
                                    <div className="w-32 h-20 bg-gray-800 rounded-lg animate-pulse delay-150" />
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
                            Launch your first automated strategy in minutes. No coding required.
                        </p>
                    </div>

                    <div className="grid md:grid-cols-4 gap-8">
                        {[
                            { icon: Globe, title: "1. Connect Broker", desc: "Securely link your Binance, Coinbase, or Forex accounts." },
                            { icon: Layers, title: "2. Choose Strategy", desc: "Select from our marketplace of backtested AI strategies." },
                            { icon: Shield, title: "3. Set Risk", desc: "Define your stop-loss and maximum daily drawdown limits." },
                            { icon: Bot, title: "4. Auto-Trade", desc: "Activate your bot and let it execute trades 24/7." },
                        ].map((step, i) => (
                            <div key={i} className="relative group">
                                <div className="bg-card border border-border p-6 rounded-2xl h-full hover:-translate-y-2 transition-transform duration-300 shadow-sm hover:shadow-xl">
                                    <div className="w-12 h-12 bg-primary/10 rounded-xl flex items-center justify-center text-primary mb-4 group-hover:bg-primary group-hover:text-white transition-colors">
                                        <step.icon className="w-6 h-6" />
                                    </div>
                                    <h3 className="font-bold text-xl mb-2">{step.title}</h3>
                                    <p className="text-muted-foreground text-sm leading-relaxed">{step.desc}</p>
                                </div>
                                {i < 3 && (
                                    <div className="hidden md:block absolute top-1/2 -right-4 translate-x-1/2 -translate-y-1/2 z-10 text-muted-foreground/30">
                                        <ArrowRight className="w-6 h-6" />
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* 3. Key Features */}
            <section className="py-24 px-6">
                <div className="max-w-7xl mx-auto">
                    <div className="grid md:grid-cols-2 gap-16 items-center">
                        <div className="space-y-8">
                            <div>
                                <h2 className="text-3xl md:text-4xl font-bold mb-4">Institutional-Grade Features</h2>
                                <p className="text-muted-foreground text-lg">
                                    We provide the tools normally reserved for hedge funds, accessible to everyone.
                                </p>
                            </div>

                            <div className="space-y-6">
                                {[
                                    { title: "Multi-Asset Support", desc: "Trade Crypto, Forex, and Stocks from one unified dashboard." },
                                    { title: "AI Signals", desc: "Proprietary machine learning models analyze market sentiment and regime." },
                                    { title: "Advanced Risk Management", desc: "Server-side protection against flash crashes and slippage." },
                                    { title: "Visual Backtesting", desc: "Validate strategies against 5 years of historical data before deploying." },
                                ].map((feat, i) => (
                                    <div key={i} className="flex gap-4">
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
                        <div className="bg-gradient-to-br from-primary/5 to-purple-500/5 rounded-3xl p-8 border border-primary/10 relative">
                            {/* Feature Visual */}
                            <div className="space-y-4">
                                <div className="bg-card rounded-xl p-4 shadow-lg border border-border">
                                    <div className="flex justify-between items-center mb-2">
                                        <div className="flex items-center gap-2">
                                            <Bot className="w-5 h-5 text-purple-500" />
                                            <span className="font-bold">Alpha-Zero Bot</span>
                                        </div>
                                        <span className="text-xs bg-green-500/10 text-green-600 px-2 py-1 rounded font-bold">ACTIVE</span>
                                    </div>
                                    <div className="h-2 bg-muted rounded-full overflow-hidden">
                                        <div className="h-full w-[70%] bg-purple-500" />
                                    </div>
                                    <div className="flex justify-between text-xs mt-2 text-muted-foreground">
                                        <span>AI Confidence: 92%</span>
                                        <span>Risk: Low</span>
                                    </div>
                                </div>

                                <div className="bg-card rounded-xl p-4 shadow-lg border border-border opacity-80 scale-95">
                                    <div className="flex justify-between items-center mb-2">
                                        <div className="flex items-center gap-2">
                                            <Globe className="w-5 h-5 text-blue-500" />
                                            <span className="font-bold">Forex Trend</span>
                                        </div>
                                        <span className="text-xs bg-amber-500/10 text-amber-600 px-2 py-1 rounded font-bold">PAUSED</span>
                                    </div>
                                    <div className="h-2 bg-muted rounded-full overflow-hidden">
                                        <div className="h-full w-[0%] bg-blue-500" />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            {/* 4. Pricing Preview */}
            <section className="py-24 bg-gray-50 dark:bg-gray-900/50">
                <div className="max-w-7xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <h2 className="text-3xl md:text-4xl font-bold mb-4">Transparent Pricing</h2>
                        <p className="text-muted-foreground">Start for free, scale as you grow.</p>
                    </div>

                    <div className="grid md:grid-cols-3 gap-8 max-w-5xl mx-auto">
                        {/* Free Tier */}
                        <div className="bg-card border border-border rounded-2xl p-8 flex flex-col">
                            <h3 className="text-xl font-bold mb-2">Free</h3>
                            <div className="text-3xl font-bold mb-6">$0<span className="text-sm font-normal text-muted-foreground">/mo</span></div>
                            <ul className="space-y-4 mb-8 flex-1">
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> 1 Active Bot</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> $1,000 Volume Limit</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> Basic Strategies</li>
                            </ul>
                            <Link to="/register" className="w-full py-3 border border-primary text-primary font-bold rounded-lg text-center hover:bg-primary/5 transition-colors">
                                Get Started
                            </Link>
                        </div>

                        {/* Pro Tier */}
                        <div className="bg-card border-2 border-primary rounded-2xl p-8 flex flex-col relative shadow-2xl">
                            <div className="absolute top-0 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-primary text-primary-foreground text-xs font-bold px-3 py-1 rounded-full uppercase">Most Popular</div>
                            <h3 className="text-xl font-bold mb-2">Pro</h3>
                            <div className="text-3xl font-bold mb-6">$49<span className="text-sm font-normal text-muted-foreground">/mo</span></div>
                            <ul className="space-y-4 mb-8 flex-1">
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> 10 Active Bots</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> Unlimited Volume</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> AI Market Compass</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> Priority Support</li>
                            </ul>
                            <Link to="/register?plan=pro" className="w-full py-3 bg-primary text-primary-foreground font-bold rounded-lg text-center hover:bg-primary/90 transition-colors">
                                Start Pro Trial
                            </Link>
                        </div>

                        {/* Elite Tier */}
                        <div className="bg-card border border-border rounded-2xl p-8 flex flex-col">
                            <h3 className="text-xl font-bold mb-2">Elite</h3>
                            <div className="text-3xl font-bold mb-6">$99<span className="text-sm font-normal text-muted-foreground">/mo</span></div>
                            <ul className="space-y-4 mb-8 flex-1">
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> Unlimited Bots</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> Custom Strategy Builder</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> API Access</li>
                                <li className="flex gap-2 text-sm"><Check className="w-5 h-5 text-green-500" /> 1-on-1 Consultation</li>
                            </ul>
                            <Link to="/register?plan=elite" className="w-full py-3 border border-border font-bold rounded-lg text-center hover:bg-muted transition-colors">
                                Contact Sales
                            </Link>
                        </div>
                    </div>
                </div>
            </section>

            {/* 5. Social Proof */}
            <section className="py-24 border-b border-border">
                <div className="max-w-7xl mx-auto px-6 text-center">
                    <h2 className="text-3xl font-bold mb-12">Trusted by 10,000+ Traders</h2>
                    <div className="grid md:grid-cols-3 gap-8">
                        {[
                            { quote: "CosmicForge completely changed how I manage my portfolio. The AI insights are spot on.", author: "Alex Chen", role: "Crypto Trader" },
                            { quote: "Finally, a platform that supports Stocks and Forex with the same level of automation.", author: "Sarah Jenkins", role: "Day Trader" },
                            { quote: "The risk management features saved me during the last market crash. Highly recommended.", author: "Michael Ross", role: "Investor" },
                        ].map((testi, i) => (
                            <div key={i} className="bg-card p-6 rounded-xl border border-border hover:border-primary/50 transition-colors">
                                <div className="flex justify-center mb-4">
                                    {[1, 2, 3, 4, 5].map(s => <Star key={s} className="w-4 h-4 text-amber-500 fill-amber-500" />)}
                                </div>
                                <p className="text-lg italic text-muted-foreground mb-6">"{testi.quote}"</p>
                                <div>
                                    <div className="font-bold">{testi.author}</div>
                                    <div className="text-sm text-muted-foreground">{testi.role}</div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* 6. Final CTA */}
            <section className="py-24 px-6 bg-[#0F172A] text-white">
                <div className="max-w-4xl mx-auto text-center">
                    <h2 className="text-4xl md:text-5xl font-bold mb-6">Start Trading Smarter Today</h2>
                    <p className="text-xl text-gray-400 mb-10">
                        Join the revolution of AI-powered automation. No credit card required for the free tier.
                    </p>
                    <Link
                        to="/register"
                        onClick={() => trackEvent("cta_click", "/", { label: "bottom_cta" })}
                        className="inline-flex items-center gap-2 px-10 py-5 bg-primary text-primary-foreground font-bold rounded-full text-xl hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/50"
                    >
                        Create Free Account <ArrowRight className="w-6 h-6" />
                    </Link>
                </div>
            </section>
        </div>
    );
}
