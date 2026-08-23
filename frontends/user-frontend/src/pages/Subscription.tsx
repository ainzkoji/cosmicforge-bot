import { useState, useEffect } from "react";
import { Check, CreditCard, Calendar, Zap, Shield, TrendingUp, Download, AlertCircle, Loader2 } from "lucide-react";
import { motion } from "framer-motion";
import { api } from "../api/client";

// --- Components ---

const PricingCard = ({ plan, billingCycle, onSelect, currentPlanId }: { plan: any, billingCycle: "monthly" | "yearly", onSelect: () => void, currentPlanId?: string }) => {
    // Backend plans are monthly by default in this MVP
    const price = plan.price;
    const isCurrent = currentPlanId === plan.id;

    return (
        <motion.div
            whileHover={{ y: -5 }}
            className={`relative p-6 rounded-2xl border ${plan.is_popular ? 'border-primary shadow-xl bg-primary/5' : 'border-border bg-card'} flex flex-col`}
        >
            {plan.is_popular && (
                <div className="absolute -top-3 left-1/2 -translate-x-1/2 bg-primary text-primary-foreground px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wide">
                    Most Popular
                </div>
            )}

            <div className="mb-6">
                <h3 className="text-xl font-bold mb-2">{plan.name}</h3>
                <p className="text-muted-foreground text-sm h-10">{plan.name} Plan</p>
            </div>

            <div className="mb-6">
                <div className="flex items-baseline gap-1">
                    <span className="text-4xl font-extrabold">${price}</span>
                    <span className="text-muted-foreground">/{plan.interval}</span>
                </div>
            </div>

            <ul className="space-y-3 mb-8 flex-1">
                {plan.features.map((feature: any, i: number) => (
                    <li key={i} className={`flex items-start gap-3 text-sm ${feature.included ? '' : 'text-muted-foreground line-through opacity-50'}`}>
                        <Check className="w-4 h-4 text-green-500 shrink-0 mt-0.5" />
                        <span>{feature.name} {feature.limit ? `(${feature.limit})` : ''}</span>
                    </li>
                ))}
            </ul>

            <button
                onClick={onSelect}
                disabled={isCurrent}
                className={`w-full py-2.5 rounded-lg font-medium transition-all ${isCurrent
                    ? 'bg-green-500/20 text-green-500 cursor-default'
                    : plan.is_popular
                        ? 'bg-primary text-primary-foreground hover:bg-primary/90 shadow-lg hover:shadow-primary/25'
                        : 'bg-muted text-foreground hover:bg-muted/80'
                    }`}
            >
                {isCurrent ? "Current Plan" : `Choose ${plan.name}`}
            </button>
        </motion.div>
    );
};

const UsageBar = ({ label, current, max, icon: Icon }: { label: string, current: number, max: number, icon: any }) => {
    const percentage = Math.min((current / max) * 100, 100);
    const isUnlimited = max > 100;

    return (
        <div className="bg-card/50 p-4 rounded-xl border border-border/50">
            <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                    <div className="p-1.5 bg-primary/10 rounded-md">
                        <Icon className="w-4 h-4 text-primary" />
                    </div>
                    <span className="font-medium text-sm">{label}</span>
                </div>
                <span className="text-sm text-muted-foreground font-mono">
                    {current} / {isUnlimited ? "∞" : max}
                </span>
            </div>
            <div className="h-2 bg-muted rounded-full overflow-hidden">
                <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: `${percentage}%` }}
                    transition={{ duration: 1, ease: "easeOut" }}
                    className={`h-full ${percentage > 90 ? 'bg-red-500' : 'bg-primary'}`}
                />
            </div>
        </div>
    );
};

export default function Subscription() {
    // State
    const [view, setView] = useState<"pricing" | "dashboard">("pricing");
    const [billingCycle, setBillingCycle] = useState<"monthly" | "yearly">("monthly");
    const [isProcessing, setIsProcessing] = useState(false);

    // Data State
    const [plans, setPlans] = useState<any[]>([]);
    const [subscription, setSubscription] = useState<any | null>(null);
    const [invoices, setInvoices] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        loadData();
    }, []);

    const loadData = async () => {
        try {
            const [plansRes, subRes, historyRes] = await Promise.all([
                api.getPlans().catch(() => ({ plans: [] })),
                api.getSubscription().catch(() => null),
                api.getBillingHistory().catch(() => ({ invoices: [] }))
            ]);

            if (plansRes && plansRes.plans) setPlans(plansRes.plans);

            if (subRes) {
                setSubscription(subRes);
                // If user has an active plan (that is not free), show dashboard by default
                if (subRes.plan && subRes.plan.id !== 'plan_free') {
                    setView("dashboard");
                }
            }

            if (historyRes && historyRes.invoices) setInvoices(historyRes.invoices);
        } catch (err) {
            console.error("Failed to load billing data", err);
        } finally {
            setLoading(false);
        }
    };

    const handleSubscribe = async (planId: string) => {
        setIsProcessing(true);
        try {
            const result = await api.createCheckoutSession(planId, window.location.href, window.location.href);
            // Redirect to stripe/mock url
            window.location.href = result.checkout_url;
        } catch (err: any) {
            alert("Failed to start checkout: " + err.message);
            setIsProcessing(false);
        }
    };

    const handleCancel = async () => {
        if (confirm("Are you sure? You will lose access to premium features at the end of the billing period.")) {
            try {
                await api.manageSubscription('cancel');
                alert("Subscription cancelled. access remains until end of period.");
                loadData(); // refresh
            } catch (err: any) {
                alert("Failed to cancel: " + err.message);
            }
        }
    };

    if (loading) {
        return <div className="flex h-96 items-center justify-center"><Loader2 className="w-8 h-8 animate-spin text-primary" /></div>;
    }

    const currentPlanId = subscription?.plan?.id;

    // --- Views ---

    if (view === "dashboard" && subscription) {
        const currentPlan = subscription.plan || { name: "Free", price: 0 };
        const entitlements = subscription.entitlements || {};

        return (
            <div className="max-w-6xl mx-auto space-y-8 animate-in fade-in duration-500">

                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold">Subscription & Billing</h1>
                        <p className="text-muted-foreground">Manage your plan, payment methods, and usage.</p>
                    </div>
                    <button onClick={() => setView("pricing")} className="text-sm text-muted-foreground hover:text-primary">
                        View Plans
                    </button>
                </div>

                {/* Main Grid */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">

                    {/* Left Column: Status & Usage */}
                    <div className="lg:col-span-2 space-y-6">

                        {/* Status Card */}
                        <div className="bg-card border border-border rounded-xl p-6 shadow-sm relative overflow-hidden">
                            <div className="absolute top-0 right-0 p-32 bg-primary/5 rounded-full blur-3xl -translate-y-1/2 translate-x-1/2 pointer-events-none" />

                            <div className="flex items-start justify-between mb-6 relative">
                                <div>
                                    <div className="flex items-center gap-3 mb-1">
                                        <h2 className="text-2xl font-bold">{currentPlan.name} Plan</h2>
                                        <span className={`px-2 py-0.5 rounded-full text-xs font-bold uppercase tracking-wide border ${subscription.status === 'active' ? 'bg-green-500/10 text-green-500 border-green-500/20' :
                                            subscription.status === 'canceled' ? 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20' :
                                                'bg-red-500/10 text-red-500 border-red-500/20'
                                            }`}>
                                            {subscription.status}
                                        </span>
                                    </div>
                                    <p className="text-muted-foreground text-sm">
                                        {subscription.cancel_at_period_end
                                            ? `Access continues until ${new Date(subscription.current_period_end).toLocaleDateString()}`
                                            : `Next billing date: ${new Date(subscription.current_period_end).toLocaleDateString()}`}
                                    </p>
                                </div>
                                <div className="text-right">
                                    <div className="text-xl font-bold">${currentPlan.price}<span className="text-sm font-normal text-muted-foreground">/{currentPlan.interval}</span></div>
                                    <div className="text-xs text-muted-foreground flex items-center justify-end gap-1">
                                        <CreditCard className="w-3 h-3" /> ••••
                                    </div>
                                </div>
                            </div>

                            <div className="flex gap-3 relative">
                                <button onClick={() => setView('pricing')} className="px-4 py-2 bg-primary text-primary-foreground rounded-lg text-sm font-medium hover:bg-primary/90 transition-colors">
                                    Change Plan
                                </button>
                                {subscription.status === 'active' && !subscription.cancel_at_period_end && currentPlan.id !== 'plan_free' && (
                                    <button
                                        onClick={handleCancel}
                                        className="px-4 py-2 border border-border bg-background hover:bg-muted text-foreground rounded-lg text-sm font-medium transition-colors"
                                    >
                                        Cancel Renewal
                                    </button>
                                )}
                            </div>
                        </div>

                        {/* Usage Metrics */}
                        <div>
                            <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
                                <TrendingUp className="w-5 h-5 text-primary" /> Resource Usage
                            </h3>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <UsageBar
                                    label="Active Bots"
                                    current={0} // TODO: fetch real usage
                                    max={entitlements.max_bots || 1}
                                    icon={Zap}
                                />
                                <UsageBar
                                    label="Connected Brokers"
                                    current={0} // TODO: fetch real usage
                                    max={entitlements.max_brokers || 1}
                                    icon={Shield}
                                />
                            </div>
                        </div>

                        {/* Recent Invoices */}
                        <div className="pt-4">
                            <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
                                <Calendar className="w-5 h-5 text-primary" /> Billing History
                            </h3>
                            <div className="bg-card border border-border rounded-xl overflow-hidden">
                                {invoices.length === 0 ? (
                                    <div className="p-4 text-center text-muted-foreground text-sm">No invoices found.</div>
                                ) : (
                                    <table className="w-full text-sm">
                                        <thead>
                                            <tr className="bg-muted/50 border-b border-border">
                                                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Date</th>
                                                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Invoice ID</th>
                                                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Amount</th>
                                                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Status</th>
                                                <th className="text-right py-3 px-4 font-medium text-muted-foreground">Receipt</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {invoices.map((inv) => (
                                                <tr key={inv.id} className="border-b border-border/50 last:border-0 hover:bg-muted/30 transition-colors">
                                                    <td className="py-3 px-4">{new Date(inv.date).toLocaleDateString()}</td>
                                                    <td className="py-3 px-4 font-mono text-xs">{inv.id}</td>
                                                    <td className="py-3 px-4 font-medium">${inv.amount.toFixed(2)}</td>
                                                    <td className="py-3 px-4">
                                                        <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-500/10 text-green-500 capitalize">
                                                            {inv.status}
                                                        </span>
                                                    </td>
                                                    <td className="py-3 px-4 text-right">
                                                        {inv.pdf_url && (
                                                            <a href={inv.pdf_url} target="_blank" rel="noreferrer" className="p-1 hover:bg-muted rounded text-muted-foreground hover:text-foreground transition-colors inline-block">
                                                                <Download className="w-4 h-4" />
                                                            </a>
                                                        )}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                )}
                            </div>
                        </div>

                    </div>

                    {/* Right Column: Upgrades & Upsell */}
                    <div className="space-y-6">
                        {currentPlan.id !== 'plan_whale' && (
                            <div className="bg-gradient-to-br from-primary/10 to-purple-500/10 border border-primary/20 rounded-xl p-6">
                                <div className="w-12 h-12 bg-primary/20 rounded-lg flex items-center justify-center mb-4 text-primary">
                                    <Zap className="w-6 h-6" />
                                </div>
                                <h3 className="text-lg font-bold mb-2">Upgrade to Tycoon</h3>
                                <p className="text-sm text-muted-foreground mb-4">
                                    Unlock unlimited bots, lowest latency execution, and AI strategy optimization.
                                </p>
                                <button onClick={() => setView("pricing")} className="w-full py-2 bg-primary text-primary-foreground rounded-lg font-medium shadow-lg hover:bg-primary/90 transition-all">
                                    View Upgrade Options
                                </button>
                            </div>
                        )}

                        <div className="bg-card border border-border rounded-xl p-6">
                            <h3 className="font-semibold mb-4 flex items-center gap-2">
                                <AlertCircle className="w-4 h-4 text-muted-foreground" />
                                Need Higher Limits?
                            </h3>
                            <p className="text-sm text-muted-foreground mb-4">
                                Running a large operation? Contact sales for a custom enterprise plan with higher limits and dedicated infrastructure.
                            </p>
                            <button className="text-sm text-primary font-medium hover:underline">Contact Sales &rarr;</button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }

    // Default: Pricing View
    return (
        <div className="max-w-6xl mx-auto py-8 space-y-12 animate-in fade-in duration-500">
            {isProcessing && (
                <div className="fixed inset-0 z-50 bg-background/80 backdrop-blur-sm flex items-center justify-center">
                    <div className="flex flex-col items-center gap-4">
                        <Loader2 className="w-10 h-10 animate-spin text-primary" />
                        <p className="text-lg font-medium">Processing Payment...</p>
                    </div>
                </div>
            )}

            <div className="text-center max-w-2xl mx-auto space-y-4">
                <h1 className="text-4xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-foreground to-foreground/70">
                    Simple, transparent pricing
                </h1>
                <p className="text-lg text-muted-foreground">
                    Choose the plan that fits your trading needs. Upgrade or cancel anytime.
                </p>

                {/* Billing Cycle Toggle */}
                <div className="flex items-center justify-center gap-4 mt-8">
                    <span className={`text-sm font-medium ${billingCycle === 'monthly' ? 'text-foreground' : 'text-muted-foreground'}`}>Monthly</span>
                    <button
                        onClick={() => setBillingCycle(prev => prev === 'monthly' ? 'yearly' : 'monthly')}
                        className={`w-12 h-6 rounded-full relative transition-colors ${billingCycle === 'yearly' ? 'bg-primary' : 'bg-muted'}`}
                    >
                        <motion.div
                            layout
                            className="w-4 h-4 bg-white rounded-full absolute top-1 left-1"
                            animate={{ x: billingCycle === 'yearly' ? 24 : 0 }}
                        />
                    </button>
                    <span className={`text-sm font-medium ${billingCycle === 'yearly' ? 'text-foreground' : 'text-muted-foreground'}`}>
                        Yearly <span className="text-green-500 text-xs ml-1 font-bold">-20%</span>
                    </span>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-8 px-4 md:px-0">
                {plans.filter(p => p.price === 0 || p.interval === (billingCycle === 'monthly' ? 'month' : 'year')).map((plan) => (
                    <PricingCard
                        key={plan.id}
                        plan={plan}
                        currentPlanId={currentPlanId}
                        billingCycle={billingCycle}
                        onSelect={() => handleSubscribe(plan.id)}
                    />
                ))}
            </div>

            <div className="bg-muted/30 rounded-2xl p-8 text-center border border-border/50">
                <h3 className="text-lg font-semibold mb-2">Enterprise & Institutional</h3>
                <p className="text-muted-foreground max-w-xl mx-auto mb-6">
                    Need custom API limits, dedicated nodes, or on-premise deployment?
                    We offer tailored solutions for funds and high-volume traders.
                </p>
                <button className="text-primary font-medium hover:underline">Contact Enterprise Sales &rarr;</button>
            </div>
        </div>
    );
}
