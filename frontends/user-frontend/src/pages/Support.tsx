import { useState } from "react";
import { Mail, ChevronDown, ChevronUp } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

export default function Support() {
    const [faqOpen, setFaqOpen] = useState<number | null>(null);

    const faqs = [
        { q: "How do I connect my exchange API keys?", a: "Go to the Broker Connection page, select your exchange, and paste your API Key and Secret. Ensure you have enabled 'Trading' permissions but disabled 'Withdrawal' permissions for security." },
        { q: "What happens if my bot hits a stop loss?", a: "The bot will automatically close the position to prevent further losses. You will receive a notification via your configured channels (Email, Push, Telegram)." },
        { q: "Can I run multiple bots simultaneously?", a: "Yes! Depending on your subscription plan, you can run multiple bots on different pairs or strategies at the same time." },
        { q: "How is the 'Profit Factor' calculated?", a: "Profit Factor is the ratio of gross profit to gross loss. A value greater than 1.5 is generally considered good." },
    ];

    return (
        <div className="max-w-4xl mx-auto space-y-12 animate-in fade-in">
            {/* Header */}
            <div className="text-center space-y-4">
                <h1 className="text-4xl font-bold">How can we help you?</h1>
                <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
                    Read the FAQs below, or email our support team.
                </p>
            </div>

            {/* Contact: support is handled by email only (no live chat, help center or ticket system). */}
            <div className="max-w-md mx-auto">
                <div className="bg-card border border-border rounded-xl p-6 text-center hover:border-primary/50 transition-colors group">
                    <div className="w-12 h-12 bg-primary/10 rounded-full flex items-center justify-center mx-auto mb-4 text-primary group-hover:scale-110 transition-transform">
                        <Mail className="w-6 h-6" />
                    </div>
                    <h3 className="font-bold text-lg mb-2">Email Support</h3>
                    <p className="text-sm text-muted-foreground mb-4">
                        Support is provided by email. Send us a detailed message about your issue, including the
                        bot or broker account it concerns.
                    </p>
                    <a href="mailto:support@cosmicforge.com" className="text-primary font-bold text-sm hover:underline">support@cosmicforge.com</a>
                </div>
            </div>

            {/* FAQ */}
            <div>
                <h2 className="text-2xl font-bold mb-6">Frequently Asked Questions</h2>
                <div className="space-y-4">
                    {faqs.map((faq, i) => (
                        <div key={i} className="bg-card border border-border rounded-xl overflow-hidden">
                            <button
                                onClick={() => setFaqOpen(faqOpen === i ? null : i)}
                                className="w-full text-left p-4 flex justify-between items-center hover:bg-muted/50 transition-colors"
                            >
                                <span className="font-medium">{faq.q}</span>
                                {faqOpen === i ? <ChevronUp className="w-4 h-4 text-muted-foreground" /> : <ChevronDown className="w-4 h-4 text-muted-foreground" />}
                            </button>
                            <AnimatePresence>
                                {faqOpen === i && (
                                    <motion.div
                                        initial={{ height: 0 }}
                                        animate={{ height: "auto" }}
                                        exit={{ height: 0 }}
                                        className="overflow-hidden"
                                    >
                                        <div className="p-4 pt-0 text-muted-foreground text-sm border-t border-border/50 bg-muted/20">
                                            {faq.a}
                                        </div>
                                    </motion.div>
                                )}
                            </AnimatePresence>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
