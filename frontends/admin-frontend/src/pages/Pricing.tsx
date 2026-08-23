import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { ArrowLeft, Check, Sparkles } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api, Plan } from "@/api/client";
import { useMarketing } from "@/context/MarketingContext";

export default function Pricing() {
    const { trackEvent, createPricingIntent } = useMarketing();
    const navigate = useNavigate();
    const [interval, setInterval] = useState<"month" | "year">("month");

    const { data, isLoading } = useQuery({
        queryKey: ["public_pricing"],
        queryFn: api.getPublicPricing
    });

    const handlePlanSelect = async (plan: Plan) => {
        trackEvent("pricing_plan_select", "/pricing", { plan_id: plan.id, plan_name: plan.name });

        // Create intent backend-side
        await createPricingIntent(plan.id);

        // Navigate to register with selected plan
        navigate("/register");
    };

    if (isLoading) return <div className="min-h-screen pt-32 text-center">Loading plans...</div>;

    const plans = data?.plans || [];
    const filteredPlans = plans.filter(p => {
        if (p.price === 0) return true; // Show free plan always (or handle duplication if needed)

        // If it's a paid plan, match interval
        // Note: Free plan in backend has 'month' interval. 
        // We want to show "Star Gazer" for both? 
        // Backend returns all plans. Let's filter by interval.
        // Free plan is usually shown in both views.
        if (p.price === 0) return true;
        return p.interval === interval;
    });

    // Deduplicate free plan if it appears multiple times (it won't with current backend logic, but good safety)
    // Actually, backend sends "plan_free" once with interval="month". 
    // If we filter p.interval === interval, free plan (month) won't show on year tab.
    // Let's explicitly include free plan on year tab too.
    const displayPlans = plans.filter(p => p.price === 0 || p.interval === interval);

    // Helper to format entitlements for display
    const getFeaturesList = (plan: Plan) => {
        const feats = [];
        const limits = plan.entitlements || {};

        if (limits.max_bots === 'unlimited') feats.push("Unlimited Trading Bots");
        else feats.push(`${limits.max_bots || '0'} Trading Bot`);

        if (limits.max_accounts === 'unlimited') feats.push("Unlimited Exchange Accounts");
        else feats.push(`${limits.max_accounts || '0'} Exchange Connection${parseInt(limits.max_accounts || '0') > 1 ? 's' : ''}`);

        if (limits.live_trading === 'true') feats.push("Live Trading Enabled");

        if (limits.backtesting === 'advanced') feats.push("Advanced Backtesting Engine");
        else feats.push("Basic Backtesting");

        if (limits.copy_trading === 'true') feats.push("Social & Copy Trading");
        if (limits.api_access === 'true') feats.push("Full API Access");
        if (limits.advanced_reports === 'true') feats.push("Advanced Reporting & Analytics");
        if (limits.dedicated_support === 'true') feats.push("Dedicated Account Manager");

        return feats;
    };

    return (
        <div className="bg-white">
            {/* Hero */}
            <section className="pt-32 pb-16 px-6 bg-gradient-to-b from-gray-50 to-white">
                <div className="max-w-4xl mx-auto text-center">
                    <Link to="/" className="inline-flex items-center gap-2 text-gray-600 hover:text-[#1E1B4B] mb-6 transition-colors">
                        <ArrowLeft className="w-4 h-4" /> Back to Home
                    </Link>
                    <h1 className="text-4xl md:text-5xl font-bold text-[#1E1B4B] mb-6">
                        Choose Your Plan
                    </h1>
                    <p className="text-xl text-gray-600 max-w-2xl mx-auto mb-8">
                        Simple, transparent pricing that grows with you. Try any plan free for 14 days.
                    </p>

                    {/* Toggle */}
                    <div className="flex justify-center items-center gap-4">
                        <span className={`text-sm font-semibold ${interval === 'month' ? 'text-[#1E1B4B]' : 'text-gray-500'}`}>Monthly</span>
                        <button
                            onClick={() => setInterval(interval === 'month' ? 'year' : 'month')}
                            className="relative w-14 h-8 bg-[#1E1B4B] rounded-full p-1 transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-[#1E1B4B]"
                        >
                            <div
                                className={`w-6 h-6 bg-white rounded-full shadow-sm transition-transform duration-200 ease-in-out ${interval === 'year' ? 'translate-x-6' : 'translate-x-0'}`}
                            />
                        </button>
                        <span className={`text-sm font-semibold ${interval === 'year' ? 'text-[#1E1B4B]' : 'text-gray-500'}`}>
                            Yearly <span className="text-xs text-green-600 bg-green-100 px-2 py-0.5 rounded-full ml-1">Save 20%</span>
                        </span>
                    </div>
                </div>
            </section>

            {/* Pricing Cards */}
            <section className="py-16 px-6">
                <div className="max-w-6xl mx-auto">
                    <div className="grid md:grid-cols-3 gap-8">
                        {displayPlans.map((plan) => {
                            const isHighlighted = (plan.is_popular || plan.name.includes("Pro") || plan.name.includes("Voyager"));

                            return (
                                <div
                                    key={plan.id}
                                    className={`relative rounded-2xl p-8 flex flex-col ${isHighlighted
                                        ? 'bg-[#1E1B4B] text-white ring-4 ring-[#1E1B4B]/20 scale-105 shadow-xl z-10'
                                        : 'bg-white border-2 border-gray-200 hover:border-[#1E1B4B]/30 transition-colors'
                                        }`}
                                >
                                    {isHighlighted && (
                                        <div className="absolute -top-4 left-1/2 -translate-x-1/2 px-4 py-1 bg-cyan-400 text-[#1E1B4B] text-sm font-semibold rounded-full flex items-center gap-1 shadow-sm">
                                            <Sparkles className="w-3 h-3" /> Most Popular
                                        </div>
                                    )}
                                    <div className="text-center mb-8">
                                        <h3 className={`text-xl font-semibold mb-2 ${isHighlighted ? 'text-white' : 'text-[#1E1B4B]'}`}>
                                            {plan.name.replace(" (Yearly)", "")}
                                        </h3>
                                        <div className="flex items-baseline justify-center gap-1">
                                            {plan.price === 0 && plan.name !== "Enterprise" ? (
                                                <span className={`text-4xl font-bold ${isHighlighted ? 'text-white' : 'text-[#1E1B4B]'}`}>$0</span>
                                            ) : plan.price === 0 ? (
                                                <span className={`text-4xl font-bold ${isHighlighted ? 'text-white' : 'text-[#1E1B4B]'}`}>Custom</span>
                                            ) : (
                                                <span className={`text-4xl font-bold ${isHighlighted ? 'text-white' : 'text-[#1E1B4B]'}`}>${plan.price}</span>
                                            )}

                                            {plan.price > 0 && (
                                                <span className={isHighlighted ? 'text-gray-300' : 'text-gray-500'}>
                                                    /{interval === 'month' ? 'mo' : 'yr'}
                                                </span>
                                            )}
                                        </div>
                                        <p className={`text-sm mt-3 ${isHighlighted ? 'text-gray-300' : 'text-gray-500'}`}>
                                            {interval === 'year' && plan.price > 0
                                                ? "Billed annually"
                                                : plan.name === "Star Gazer" ? "Free forever" : "Billed monthly"}
                                        </p>
                                    </div>

                                    <ul className="space-y-4 mb-8 flex-1">
                                        {getFeaturesList(plan).map((feature, j) => (
                                            <li key={j} className="flex items-start gap-3">
                                                <Check className={`w-5 h-5 flex-shrink-0 mt-0.5 ${isHighlighted ? 'text-cyan-400' : 'text-green-500'}`} />
                                                <span className={`text-sm ${isHighlighted ? 'text-gray-200' : 'text-gray-600'}`}>
                                                    {feature}
                                                </span>
                                            </li>
                                        ))}
                                    </ul>

                                    <button
                                        onClick={() => handlePlanSelect(plan)}
                                        className={`w-full py-3.5 rounded-lg font-semibold text-center transition-all active:scale-95 ${isHighlighted
                                            ? 'bg-white text-[#1E1B4B] hover:bg-gray-100 shadow-lg'
                                            : 'bg-[#1E1B4B] text-white hover:bg-[#2D2A5B]'
                                            }`}
                                    >
                                        {plan.price === 0 && plan.name === "Enterprise" ? "Contact Sales" : (plan.price === 0 ? "Get Started Free" : "Start 14-Day Free Trial")}
                                    </button>
                                </div>
                            );
                        })}
                    </div>
                </div>
            </section>

            {/* FAQ */}
            <section className="py-16 px-6 bg-gray-50">
                <div className="max-w-3xl mx-auto">
                    <h2 className="text-3xl font-bold text-center text-[#1E1B4B] mb-12">
                        Pricing FAQ
                    </h2>
                    <div className="space-y-4">
                        {[
                            { q: "Can I switch plans anytime?", a: "Yes! You can upgrade or downgrade your plan at any time. Changes take effect immediately." },
                            { q: "Is there a free trial?", a: "Yes, all paid plans come with a 14-day free trial. No credit card required to start." },
                            { q: "What payment methods do you accept?", a: "We accept all major credit cards, PayPal, and cryptocurrency payments." },
                            { q: "Can I cancel anytime?", a: "Absolutely. No long-term contracts. Cancel anytime with no questions asked." },
                        ].map((faq, i) => (
                            <div key={i} className="bg-white rounded-xl p-6 border border-gray-200 shadow-sm">
                                <h3 className="font-semibold text-[#1E1B4B] mb-2">{faq.q}</h3>
                                <p className="text-gray-600 text-sm">{faq.a}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>
        </div>
    );
}
