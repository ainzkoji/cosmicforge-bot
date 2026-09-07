import { Link } from "react-router-dom";
import { ArrowLeft, Zap, Globe, Shield, BarChart3, Lock, TestTube, TrendingUp, Bell, Layers, Sparkles, Layout } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { useMarketing } from "@/context/MarketingContext";

const ICON_MAP: Record<string, any> = {
    // Core
    "ai_analysis": Zap,
    "multi_exchange": Globe,
    "copy_trading": TrendingUp,
    // Risk
    "risk_management": Shield,
    "secure_api": Lock,
    // Monitoring
    "realtime_monitoring": BarChart3,
    // Backtesting
    "backtesting": TestTube,
    // Integrations
    "tradingview": Layout,

    // Fallbacks
    "default": Sparkles
};

const COLOR_MAP: Record<string, string> = {
    "core": "bg-purple-100 text-purple-600",
    "risk": "bg-red-100 text-red-600",
    "monitoring": "bg-cyan-100 text-cyan-600",
    "backtesting": "bg-orange-100 text-orange-600",
    "integrations": "bg-indigo-100 text-indigo-600",
    "default": "bg-gray-100 text-gray-600"
};

export default function Features() {
    const { trackEvent } = useMarketing();

    const { data: content, isLoading } = useQuery({
        queryKey: ["public_features"],
        queryFn: api.getPublicFeatures
    });

    if (isLoading) return <div className="min-h-screen pt-32 text-center">Loading features...</div>;

    const featureList: any[] = content?.["features.list"] || [];
    const categories: any[] = content?.["features.categories"] || [];

    // Group features by category if we wanted, but flat grid is fine for now as per design
    // We'll just map them.

    return (
        <div className="bg-white">
            {/* Hero */}
            <section className="pt-32 pb-16 px-6 bg-gradient-to-b from-gray-50 to-white">
                <div className="max-w-4xl mx-auto text-center">
                    <Link to="/" className="inline-flex items-center gap-2 text-gray-600 hover:text-[#1E1B4B] mb-6 transition-colors">
                        <ArrowLeft className="w-4 h-4" /> Back to Home
                    </Link>
                    <h1 className="text-4xl md:text-5xl font-bold text-[#1E1B4B] mb-6">
                        Our Advanced Features
                    </h1>
                    <p className="text-xl text-gray-600 max-w-2xl mx-auto">
                        Everything you need to trade cryptocurrency with confidence, precision, and automation.
                    </p>
                </div>
            </section>

            {/* Features Grid */}
            <section className="py-16 px-6">
                <div className="max-w-7xl mx-auto">
                    <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
                        {featureList.map((feature, i) => {
                            const Icon = ICON_MAP[feature.id] || ICON_MAP.default;
                            const colorClass = COLOR_MAP[feature.category] || COLOR_MAP.default;

                            return (
                                <div key={i} className="bg-white rounded-2xl p-8 border border-gray-200 hover:shadow-xl transition-all hover:-translate-y-1 relative overflow-hidden">
                                    {feature.status !== 'live' && (
                                        <div className="absolute top-4 right-4 px-3 py-1 bg-yellow-100 text-yellow-700 text-xs font-bold rounded-full uppercase tracking-wide">
                                            {feature.status.replace('_', ' ')}
                                        </div>
                                    )}
                                    <div className={`w-14 h-14 rounded-xl ${colorClass} flex items-center justify-center mb-6`}>
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
                        Ready to Experience These Features?
                    </h2>
                    <p className="text-gray-300 mb-8">
                        Start your free trial today and see how CosmicForge Stratos can transform your trading.
                    </p>
                    <Link
                        to="/register"
                        onClick={() => trackEvent("cta_click", "/features", { label: "bottom_cta" })}
                        className="inline-flex items-center gap-2 px-8 py-4 bg-white text-[#1E1B4B] font-semibold rounded-lg hover:bg-gray-100 transition-colors text-lg"
                    >
                        Start Your Free Trial
                    </Link>
                </div>
            </section>
        </div>
    );
}
