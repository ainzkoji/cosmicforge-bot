import { Link } from "react-router-dom";
import {
    BookOpen, PlayCircle, Rocket, TrendingUp, ShieldCheck,
    ArrowRight, CheckCircle2, Award
} from "lucide-react";
import { motion } from "framer-motion";

export function BeginnerDashboard() {
    return (
        <div className="max-w-[1200px] mx-auto space-y-8 animate-in fade-in duration-500">
            {/* Welcome Banner */}
            <div className="bg-gradient-to-r from-primary/20 to-purple-500/10 border border-primary/20 rounded-3xl p-8 relative overflow-hidden">
                <div className="absolute top-0 right-0 w-64 h-64 bg-primary/10 rounded-full blur-3xl -mr-16 -mt-16" />

                <div className="relative z-10 max-w-2xl">
                    <h1 className="text-3xl font-bold mb-4">Welcome back, Trader! 👋</h1>
                    <p className="text-lg text-muted-foreground mb-6">
                        Ready to start your automated trading journey? We've curated a simple path for you to succeed.
                    </p>
                    <div className="flex flex-wrap gap-4">
                        <Link
                            to="/dashboard/academy"
                            className="bg-primary text-primary-foreground px-6 py-3 rounded-xl font-bold shadow-lg hover:shadow-primary/25 transition-all flex items-center gap-2 hover:-translate-y-0.5"
                        >
                            <BookOpen className="w-5 h-5" /> Continue Learning
                        </Link>
                        <Link
                            to="/dashboard/strategies"
                            className="bg-card border border-border px-6 py-3 rounded-xl font-bold hover:bg-muted transition-all flex items-center gap-2"
                        >
                            <Rocket className="w-5 h-5" /> Browse Presets
                        </Link>
                    </div>
                </div>
            </div>

            {/* Quick Stats (Simplified) */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-card border border-border p-6 rounded-2xl">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="p-2 bg-green-500/10 rounded-lg text-green-500">
                            <ShieldCheck className="w-5 h-5" />
                        </div>
                        <span className="font-semibold text-muted-foreground">Account Status</span>
                    </div>
                    <div className="text-2xl font-bold">Verified & Secure</div>
                    <div className="text-sm text-green-500 flex items-center gap-1 mt-1">
                        <CheckCircle2 className="w-3 h-3" /> Ready to trade
                    </div>
                </div>

                <div className="bg-card border border-border p-6 rounded-2xl">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="p-2 bg-blue-500/10 rounded-lg text-blue-500">
                            <TrendingUp className="w-5 h-5" />
                        </div>
                        <span className="font-semibold text-muted-foreground">Total Balance</span>
                    </div>
                    <div className="text-2xl font-bold">$0.00</div>
                    <div className="text-sm text-muted-foreground mt-1">
                        Connect a broker to deposit funds
                    </div>
                </div>

                <div className="bg-card border border-border p-6 rounded-2xl">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="p-2 bg-purple-500/10 rounded-lg text-purple-500">
                            <Award className="w-5 h-5" />
                        </div>
                        <span className="font-semibold text-muted-foreground">Academy Level</span>
                    </div>
                    <div className="text-2xl font-bold">Level 1</div>
                    <div className="w-full bg-muted rounded-full h-2 mt-3 overflow-hidden">
                        <div className="bg-purple-500 w-[15%] h-full" />
                    </div>
                </div>
            </div>

            {/* Recommended Steps */}
            <div>
                <h2 className="text-xl font-bold mb-4">Recommended Next Steps</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {/* Step 1 */}
                    <div className="bg-card border border-border rounded-xl p-6 hover:border-primary/50 transition-colors cursor-pointer group">
                        <div className="flex justify-between items-start mb-4">
                            <div className="p-3 bg-indigo-500/10 rounded-xl text-indigo-500 group-hover:bg-indigo-500 group-hover:text-white transition-colors">
                                <PlayCircle className="w-6 h-6" />
                            </div>
                            <span className="px-2 py-1 bg-muted rounded text-xs font-bold text-muted-foreground">5 min</span>
                        </div>
                        <h3 className="font-bold text-lg mb-2">Watch: How Bots Work</h3>
                        <p className="text-sm text-muted-foreground mb-4">
                            Understand the basics of automated trading and risk management.
                        </p>
                        <div className="flex items-center text-sm font-bold text-primary">
                            Start Lesson <ArrowRight className="w-4 h-4 ml-1" />
                        </div>
                    </div>

                    {/* Step 2 */}
                    <div className="bg-card border border-border rounded-xl p-6 hover:border-primary/50 transition-colors cursor-pointer group">
                        <div className="flex justify-between items-start mb-4">
                            <div className="p-3 bg-amber-500/10 rounded-xl text-amber-500 group-hover:bg-amber-500 group-hover:text-white transition-colors">
                                <Rocket className="w-6 h-6" />
                            </div>
                            <span className="px-2 py-1 bg-muted rounded text-xs font-bold text-muted-foreground">Action</span>
                        </div>
                        <h3 className="font-bold text-lg mb-2">Deploy Your First Bot</h3>
                        <p className="text-sm text-muted-foreground mb-4">
                            Use our "Safe Starter" preset to dip your toes in with minimal risk.
                        </p>
                        <div className="flex items-center text-sm font-bold text-primary">
                            Go to Gallery <ArrowRight className="w-4 h-4 ml-1" />
                        </div>
                    </div>

                    {/* Step 3 */}
                    <div className="bg-card border border-border rounded-xl p-6 hover:border-primary/50 transition-colors cursor-pointer group">
                        <div className="flex justify-between items-start mb-4">
                            <div className="p-3 bg-blue-500/10 rounded-xl text-blue-500 group-hover:bg-blue-500 group-hover:text-white transition-colors">
                                <ShieldCheck className="w-6 h-6" />
                            </div>
                            <span className="px-2 py-1 bg-muted rounded text-xs font-bold text-muted-foreground">Setup</span>
                        </div>
                        <h3 className="font-bold text-lg mb-2">Connect Another Broker</h3>
                        <p className="text-sm text-muted-foreground mb-4">
                            Link your existing exchange accounts (Binance, Coinbase, Kraken).
                        </p>
                        <div className="flex items-center text-sm font-bold text-primary">
                            Connect Now <ArrowRight className="w-4 h-4 ml-1" />
                        </div>
                    </div>
                </div>
            </div>

            {/* Featured Beginner Strategy */}
            <div className="bg-card border border-border rounded-2xl p-8 flex flex-col md:flex-row items-center gap-8">
                <div className="flex-1">
                    <div className="inline-block px-3 py-1 bg-green-500/10 text-green-500 rounded-full text-xs font-bold mb-3">BEGINNER FAVORITE</div>
                    <h2 className="text-2xl font-bold mb-2">The "Steady Saver" Bot</h2>
                    <p className="text-muted-foreground mb-6">
                        A low-volatility Dollar Cost Averaging (DCA) strategy designed for long-term growth. Perfect for your first deployment.
                    </p>
                    <div className="flex gap-4">
                        <button className="bg-primary text-primary-foreground px-6 py-3 rounded-lg font-bold hover:bg-primary/90 transition-colors">
                            View Strategy Details
                        </button>
                    </div>
                </div>
                <div className="w-full md:w-1/3 bg-muted/50 rounded-xl h-48 flex items-center justify-center border border-dashed border-border">
                    <span className="text-muted-foreground">Performance Chart Preview</span>
                </div>
            </div>
        </div>
    );
}
