import { useState } from "react";
import { LifeBuoy, MessageSquare, Mail, Phone, ExternalLink, ChevronDown, ChevronUp } from "lucide-react";
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
                    Search our knowledge base, read FAQs, or contact our support team directly.
                </p>
            </div>

            {/* Contact Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-card border border-border rounded-xl p-6 text-center hover:border-primary/50 transition-colors group">
                    <div className="w-12 h-12 bg-primary/10 rounded-full flex items-center justify-center mx-auto mb-4 text-primary group-hover:scale-110 transition-transform">
                        <MessageSquare className="w-6 h-6" />
                    </div>
                    <h3 className="font-bold text-lg mb-2">Live Chat</h3>
                    <p className="text-sm text-muted-foreground mb-4">Chat with our AI assistant or a support agent.</p>
                    <button className="text-primary font-bold text-sm hover:underline">Start Chat</button>
                    <span className="block text-xs text-muted-foreground mt-2">Available 24/7</span>
                </div>

                <div className="bg-card border border-border rounded-xl p-6 text-center hover:border-primary/50 transition-colors group">
                    <div className="w-12 h-12 bg-primary/10 rounded-full flex items-center justify-center mx-auto mb-4 text-primary group-hover:scale-110 transition-transform">
                        <Mail className="w-6 h-6" />
                    </div>
                    <h3 className="font-bold text-lg mb-2">Email Support</h3>
                    <p className="text-sm text-muted-foreground mb-4">Send us a detailed message about your issue.</p>
                    <a href="mailto:support@cosmicforge.com" className="text-primary font-bold text-sm hover:underline">support@cosmicforge.com</a>
                    <span className="block text-xs text-muted-foreground mt-2">Response time: ~24h</span>
                </div>

                <div className="bg-card border border-border rounded-xl p-6 text-center hover:border-primary/50 transition-colors group">
                    <div className="w-12 h-12 bg-primary/10 rounded-full flex items-center justify-center mx-auto mb-4 text-primary group-hover:scale-110 transition-transform">
                        <LifeBuoy className="w-6 h-6" />
                    </div>
                    <h3 className="font-bold text-lg mb-2">Help Center</h3>
                    <p className="text-sm text-muted-foreground mb-4">Browse documentation and tutorials.</p>
                    <button className="text-primary font-bold text-sm hover:underline flex items-center justify-center gap-1">
                        Visit Help Center <ExternalLink className="w-3 h-3" />
                    </button>
                </div>
            </div>

            {/* Ticket Form */}
            <div className="bg-card border border-border rounded-2xl p-8">
                <h2 className="text-2xl font-bold mb-6">Submit a Ticket</h2>
                <form className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Name</label>
                            <input type="text" className="w-full bg-background border border-border rounded-lg p-2 focus:ring-2 focus:ring-primary/50 outline-none" placeholder="Your Name" />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Email</label>
                            <input type="email" className="w-full bg-background border border-border rounded-lg p-2 focus:ring-2 focus:ring-primary/50 outline-none" placeholder="john@example.com" />
                        </div>
                    </div>
                    <div className="space-y-2">
                        <label className="text-sm font-medium">Subject</label>
                        <input type="text" className="w-full bg-background border border-border rounded-lg p-2 focus:ring-2 focus:ring-primary/50 outline-none" placeholder="Brief description of the issue" />
                    </div>
                    <div className="space-y-2">
                        <label className="text-sm font-medium">Message</label>
                        <textarea className="w-full bg-background border border-border rounded-lg p-2 focus:ring-2 focus:ring-primary/50 outline-none min-h-[120px]" placeholder="Please provide as much detail as possible..." />
                    </div>
                    <button className="px-6 py-2 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-colors">
                        Submit Ticket
                    </button>
                </form>
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
