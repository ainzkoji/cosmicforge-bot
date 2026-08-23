import { Link } from "react-router-dom";
import { ArrowLeft, ArrowRight, UserPlus, Link2, Settings, TrendingUp, Sparkles, HelpCircle } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { useMarketing } from "@/context/MarketingContext";

const ICON_MAP: Record<string, any> = {
    "user-plus": UserPlus,
    "link": Link2,
    "settings": Settings,
    "trending-up": TrendingUp,
    "default": Sparkles
};

export default function HowItWorks() {
    const { trackEvent } = useMarketing();

    const { data: content, isLoading } = useQuery({
        queryKey: ["public_how_it_works"],
        queryFn: api.getPublicHowItWorks
    });

    if (isLoading) return <div className="min-h-screen pt-32 text-center">Loading...</div>;

    const stepsRaw: any[] = content?.["how_it_works.steps"] || [];
    // Sort steps just in case
    const steps = [...stepsRaw].sort((a, b) => a.step - b.step);

    const faqs: any[] = content?.["faq.items"] || [];

    return (
        <div className="bg-white">
            {/* Hero */}
            <section className="pt-32 pb-16 px-6 bg-gradient-to-b from-gray-50 to-white">
                <div className="max-w-4xl mx-auto text-center">
                    <Link to="/" className="inline-flex items-center gap-2 text-gray-600 hover:text-[#1E1B4B] mb-6 transition-colors">
                        <ArrowLeft className="w-4 h-4" /> Back to Home
                    </Link>
                    <h1 className="text-4xl md:text-5xl font-bold text-[#1E1B4B] mb-6">
                        How It Works
                    </h1>
                    <p className="text-xl text-gray-600 max-w-2xl mx-auto">
                        Get started with automated crypto trading in just four simple steps.
                    </p>
                </div>
            </section>

            {/* Steps - Horizontal Timeline */}
            <section className="py-16 px-6">
                <div className="max-w-6xl mx-auto">
                    {/* Desktop Timeline */}
                    <div className="hidden md:block">
                        {/* Connection Line */}
                        <div className="relative mb-8">
                            <div className="absolute top-8 left-[12.5%] right-[12.5%] h-1 bg-[#1E1B4B]/20 rounded-full" />
                            <div className="grid grid-cols-4 gap-4 relative">
                                {steps.map((step) => (
                                    <div key={step.step} className="flex flex-col items-center">
                                        <div className="w-16 h-16 rounded-full bg-[#1E1B4B] text-white flex items-center justify-center text-2xl font-bold z-10 border-4 border-white">
                                            {step.step}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>

                        {/* Step Content */}
                        <div className="grid grid-cols-4 gap-8">
                            {steps.map((step) => {
                                const Icon = ICON_MAP[step.icon] || ICON_MAP.default;
                                return (
                                    <div key={step.step} className="text-center">
                                        <div className="w-14 h-14 rounded-xl bg-[#1E1B4B]/10 flex items-center justify-center mx-auto mb-4">
                                            <Icon className="w-7 h-7 text-[#1E1B4B]" />
                                        </div>
                                        <h3 className="text-xl font-semibold text-[#1E1B4B] mb-3">{step.title}</h3>
                                        <p className="text-gray-600 text-sm leading-relaxed">{step.description}</p>
                                    </div>
                                );
                            })}
                        </div>
                    </div>

                    {/* Mobile Steps */}
                    <div className="md:hidden space-y-8">
                        {steps.map((step, i) => {
                            const Icon = ICON_MAP[step.icon] || ICON_MAP.default;
                            return (
                                <div key={step.step} className="flex gap-4">
                                    <div className="flex flex-col items-center">
                                        <div className="w-12 h-12 rounded-full bg-[#1E1B4B] text-white flex items-center justify-center text-xl font-bold border-4 border-white shadow-sm">
                                            {step.step}
                                        </div>
                                        {i < steps.length - 1 && <div className="w-0.5 flex-1 bg-[#1E1B4B]/20 mt-2" />}
                                    </div>
                                    <div className="flex-1 pb-8">
                                        <div className="w-12 h-12 rounded-xl bg-[#1E1B4B]/10 flex items-center justify-center mb-3">
                                            <Icon className="w-6 h-6 text-[#1E1B4B]" />
                                        </div>
                                        <h3 className="text-lg font-semibold text-[#1E1B4B] mb-2">{step.title}</h3>
                                        <p className="text-gray-600 text-sm">{step.description}</p>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            </section>

            {/* FAQ Section */}
            <section className="py-16 px-6 bg-gray-50">
                <div className="max-w-3xl mx-auto">
                    <h2 className="text-3xl font-bold text-center text-[#1E1B4B] mb-12">
                        Frequently Asked Questions
                    </h2>
                    <div className="space-y-4">
                        {faqs.map((faq, i) => (
                            <div key={i} className="bg-white rounded-xl p-6 border border-gray-200 shadow-sm">
                                <h3 className="font-semibold text-[#1E1B4B] mb-2 flex items-start gap-2">
                                    <HelpCircle className="w-5 h-5 text-cyan-600 flex-shrink-0 mt-0.5" />
                                    {faq.q}
                                </h3>
                                <p className="text-gray-600 text-sm ml-7">{faq.a}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* CTA */}
            <section className="py-20 px-6">
                <div className="max-w-4xl mx-auto text-center">
                    <h2 className="text-3xl md:text-4xl font-bold text-[#1E1B4B] mb-4">
                        Ready to Get Started?
                    </h2>
                    <p className="text-gray-600 mb-8">
                        Create your account now and start trading in minutes.
                    </p>
                    <Link
                        to="/register"
                        onClick={() => trackEvent("cta_click", "/how-it-works", { label: "bottom_cta" })}
                        className="inline-flex items-center gap-2 px-8 py-4 bg-[#1E1B4B] text-white font-semibold rounded-lg hover:bg-[#2D2A5B] transition-colors text-lg"
                    >
                        Create Free Account <ArrowRight className="w-5 h-5" />
                    </Link>
                </div>
            </section>
        </div>
    );
}
