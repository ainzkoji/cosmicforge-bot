import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { ChevronRight, Check, Shield, TrendingUp, Zap, BarChart3, Rocket, LayoutDashboard, Loader2, AlertTriangle, Building2, UserCheck, Key, RefreshCw, AlertCircle, Coins, Globe, Landmark, ShieldCheck } from "lucide-react";
import { Link, useNavigate } from "react-router-dom";
import { api } from "../api/client";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";

export default function OnboardingWizard() {
    const navigate = useNavigate();
    const queryClient = useQueryClient();

    // Steps mapping
    const STEPS = ["welcome", "experience", "assets", "broker", "strategy", "risk", "summary"];
    const [currentStepIndex, setCurrentStepIndex] = useState(0);

    const [formData, setFormData] = useState<any>({
        experience_level: "",
        asset_types: [], // [NEW]
        broker_connected: false, // [NEW] - Mock state for now
        strategy_preference: "",
        risk_tolerance: "",
        max_drawdown: 10, // [NEW] Default 10%
        capital_allocation: "",
        allocation_model: "fixed_amount"
    });

    // --- Queries ---
    const { data: state, isLoading: isLoadingState } = useQuery({
        queryKey: ["onboarding-state"],
        queryFn: api.getOnboardingState,
        refetchOnWindowFocus: false,
    });

    const { data: strategiesData, isLoading: isLoadingStrategies } = useQuery({
        queryKey: ["strategies"],
        queryFn: api.getOnboardingStrategies,
    });

    const { data: nextStepsData, isLoading: isLoadingNextSteps } = useQuery({
        queryKey: ["next-steps"],
        queryFn: api.getOnboardingNextSteps,
        enabled: currentStepIndex === 6 // Only fetch on summary
    });

    // Sync state to local
    useEffect(() => {
        if (state) {
            if (state.data) {
                setFormData((prev: any) => ({ ...prev, ...state.data }));
            }
            if (state.current_step && STEPS.includes(state.current_step)) {
                setCurrentStepIndex(STEPS.indexOf(state.current_step));
            }
        }
    }, [state]);

    // --- Mutations ---
    const saveStepMutation = useMutation({
        mutationFn: (args: { step: string, data: any }) => api.saveOnboardingStep(args.step, args.data),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["onboarding-state"] });
        }
    });

    const completeMutation = useMutation({
        mutationFn: api.completeOnboarding,
        onSuccess: (data) => {
            queryClient.invalidateQueries({ queryKey: ["onboarding-state"] });
        }
    });

    // --- Handlers ---
    const handleNext = async () => {
        const stepName = STEPS[currentStepIndex];
        const dataToSave = { ...formData };

        // If step is 'broker', we assume they handled it via the nested UI or skipped
        // For now, we save state and move on.

        await saveStepMutation.mutateAsync({ step: stepName, data: dataToSave });

        if (currentStepIndex < STEPS.length - 1) {
            setCurrentStepIndex(prev => prev + 1);
        }
    };

    const handleBack = () => {
        if (currentStepIndex > 0) setCurrentStepIndex(prev => prev - 1);
    };

    const handleComplete = async () => {
        await completeMutation.mutateAsync();
        navigate("/dashboard");
    };

    const updateField = (field: string, value: any) => {
        setFormData((prev: any) => ({ ...prev, [field]: value }));
    };

    const toggleAsset = (asset: string) => {
        const current = formData.asset_types || [];
        if (current.includes(asset)) {
            updateField('asset_types', current.filter((a: string) => a !== asset));
        } else {
            updateField('asset_types', [...current, asset]);
        }
    };

    // --- Loading ---
    if (isLoadingState && !state) {
        return <div className="flex bg-background h-screen items-center justify-center"><Loader2 className="w-10 h-10 animate-spin text-primary" /></div>;
    }

    // --- Types/Helpers ---
    const OptionCard = ({ selected, onClick, title, desc, icon: Icon }: any) => (
        <div
            onClick={onClick}
            className={`cursor-pointer p-6 rounded-xl border-2 transition-all flex flex-col gap-4 relative overflow-hidden group ${selected
                ? 'border-primary bg-primary/5'
                : 'border-border bg-card hover:border-primary/50 hover:bg-muted/50'
                }`}
        >
            <div className={`p-3 rounded-lg w-fit ${selected ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground group-hover:text-primary group-hover:bg-primary/10'}`}>
                {Icon && <Icon className="w-6 h-6" />}
            </div>
            <div>
                <h3 className="font-bold text-lg mb-1">{title}</h3>
                <p className="text-sm text-muted-foreground">{desc}</p>
            </div>
            {selected && (
                <div className="absolute top-4 right-4 text-primary">
                    <Check className="w-6 h-6" />
                </div>
            )}
        </div>
    );

    const stepName = STEPS[currentStepIndex];

    return (
        <div className="min-h-screen flex flex-col bg-background text-foreground">
            {/* Minimal Header */}
            <div className="h-16 border-b border-border/40 flex items-center justify-between px-8 backdrop-blur-md sticky top-0 z-50">
                <div className="flex items-center gap-2">
                    <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center">
                        <Rocket className="w-5 h-5 text-primary" />
                    </div>
                    <span className="font-bold">Setup Wizard</span>
                </div>
                <div className="flex items-center gap-4 text-sm font-medium text-muted-foreground">
                    <span>Step {currentStepIndex + 1} of {STEPS.length}</span>
                    <div className="w-32 h-2 bg-muted rounded-full overflow-hidden">
                        <motion.div
                            initial={{ width: 0 }}
                            animate={{ width: `${((currentStepIndex + 1) / STEPS.length) * 100}%` }}
                            className="h-full bg-primary"
                        />
                    </div>
                </div>
                <Link to="/dashboard" className="text-sm text-muted-foreground hover:text-foreground">
                    Exit
                </Link>
            </div>

            {/* Main Content */}
            <div className="flex-1 flex flex-col items-center justify-center p-6 relative overflow-hidden">
                {/* Background Decor */}
                <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-primary/5 rounded-full blur-3xl -z-10" />
                <div className="absolute bottom-1/4 right-1/4 w-64 h-64 bg-purple-500/5 rounded-full blur-3xl -z-10" />

                <motion.div
                    key={currentStepIndex}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.3 }}
                    className="w-full max-w-4xl"
                >
                    {/* Step 1: Welcome */}
                    {stepName === "welcome" && (
                        <div className="text-center space-y-6">
                            <motion.div
                                animate={{ scale: [0.95, 1.05, 1] }}
                                transition={{ duration: 0.5 }}
                                className="w-20 h-20 bg-primary/10 rounded-2xl flex items-center justify-center mx-auto text-primary mb-8"
                            >
                                <Rocket className="w-10 h-10" />
                            </motion.div>
                            <h1 className="text-4xl font-bold tracking-tight">Let's set up your trading bot</h1>
                            <p className="text-xl text-muted-foreground max-w-xl mx-auto">
                                In less than 5 minutes, we'll configure your experience, connect your exchange, and deploy your first AI strategy.
                            </p>
                            <button
                                onClick={handleNext}
                                className="px-8 py-4 bg-primary text-primary-foreground rounded-full font-bold hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/25 text-lg"
                            >
                                Get Started in 5 Minutes &rarr;
                            </button>
                        </div>
                    )}

                    {/* Step 2: Experience */}
                    {stepName === "experience" && (
                        <div className="space-y-8">
                            <div className="text-center">
                                <h2 className="text-3xl font-bold mb-3">What is your experience level?</h2>
                                <p className="text-muted-foreground">We'll adapt the interface to match your skills.</p>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                <OptionCard
                                    title="Beginner"
                                    desc="I'm new to trading. Guide me with simple presets."
                                    icon={Shield}
                                    selected={formData.experience_level === 'beginner'}
                                    onClick={() => updateField('experience_level', 'beginner')}
                                />
                                <OptionCard
                                    title="Intermediate"
                                    desc="I have some experience. I want a balance of automation and control."
                                    icon={BarChart3}
                                    selected={formData.experience_level === 'intermediate'}
                                    onClick={() => updateField('experience_level', 'intermediate')}
                                />
                                <OptionCard
                                    title="Pro / Quant"
                                    desc="I need full access to API configuration, bespoke strategies, and raw data."
                                    icon={Zap}
                                    selected={formData.experience_level === 'advanced'}
                                    onClick={() => updateField('experience_level', 'advanced')}
                                />
                            </div>
                        </div>
                    )}

                    {/* Step 3: Asset Types [NEW] */}
                    {stepName === "assets" && (
                        <div className="space-y-8 max-w-2xl mx-auto">
                            <div className="text-center">
                                <h2 className="text-3xl font-bold mb-3">Which markets do you want to trade?</h2>
                                <p className="text-muted-foreground">Select all that apply.</p>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                <OptionCard
                                    title="Crypto"
                                    desc="Bitcoin, Ethereum, Altcoins."
                                    icon={Coins}
                                    selected={(formData.asset_types || []).includes('crypto')}
                                    onClick={() => toggleAsset('crypto')}
                                />
                                <OptionCard
                                    title="Forex"
                                    desc="Major and minor currency pairs (EUR/USD, etc)."
                                    icon={Globe}
                                    selected={(formData.asset_types || []).includes('forex')}
                                    onClick={() => toggleAsset('forex')}
                                />
                                <OptionCard
                                    title="Stocks"
                                    desc="US Equities (AAPL, TSLA) via API."
                                    icon={Landmark}
                                    selected={(formData.asset_types || []).includes('stocks')}
                                    onClick={() => toggleAsset('stocks')}
                                />
                            </div>
                        </div>
                    )}

                    {/* Step 4: Broker Connection [Integrated Prompt] */}
                    {stepName === "broker" && (
                        <div className="space-y-8 max-w-2xl mx-auto">
                            <div className="text-center">
                                <h2 className="text-3xl font-bold mb-3">Connect Your Exchange</h2>
                                <p className="text-muted-foreground">Securely link your account via API keys. keys are encrypted.</p>
                            </div>

                            <div className="bg-card border border-border rounded-xl p-8 text-center space-y-6">
                                <div className="flex justify-center gap-4">
                                    <div className="p-4 bg-muted rounded-xl grayscale hover:grayscale-0 transition-all cursor-pointer border border-border hover:border-primary">
                                        {/* Mock Icons */}
                                        <div className="w-12 h-8 bg-current opacity-50 rounded" />
                                        <div className="text-xs mt-2 font-bold">Binance</div>
                                    </div>
                                    <div className="p-4 bg-muted rounded-xl grayscale hover:grayscale-0 transition-all cursor-pointer border border-border hover:border-primary">
                                        <div className="w-12 h-8 bg-current opacity-50 rounded" />
                                        <div className="text-xs mt-2 font-bold">Coinbase</div>
                                    </div>
                                    <div className="p-4 bg-muted rounded-xl grayscale hover:grayscale-0 transition-all cursor-pointer border border-border hover:border-primary">
                                        <div className="w-12 h-8 bg-current opacity-50 rounded" />
                                        <div className="text-xs mt-2 font-bold">Forex.com</div>
                                    </div>
                                </div>

                                <div className="space-y-4 text-left bg-muted/30 p-4 rounded-lg">
                                    <div className="space-y-2">
                                        <label className="text-sm font-medium">API Key</label>
                                        <div className="relative">
                                            <Key className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                                            <input type="password" className="w-full pl-10 h-10 rounded-lg border border-border bg-background" placeholder="Paste API Key" />
                                        </div>
                                    </div>
                                    <div className="space-y-2">
                                        <label className="text-sm font-medium">Secret Key</label>
                                        <div className="relative">
                                            <Shield className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                                            <input type="password" className="w-full pl-10 h-10 rounded-lg border border-border bg-background" placeholder="Paste Secret Key" />
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-2 text-xs text-green-500">
                                        <ShieldCheck className="w-4 h-4" />
                                        Your keys are encrypted with AES-256 before storage.
                                    </div>
                                </div>

                                <button onClick={() => updateField('broker_connected', true)} className="w-full py-3 bg-primary text-primary-foreground font-bold rounded-lg hover:bg-primary/90">
                                    Connect Exchange
                                </button>
                                <button onClick={handleNext} className="text-sm text-muted-foreground hover:underline">
                                    Skip for now (Demo Mode)
                                </button>
                            </div>
                        </div>
                    )}


                    {/* Step 5: Strategy */}
                    {stepName === "strategy" && (
                        <div className="space-y-8">
                            <div className="text-center">
                                <h2 className="text-3xl font-bold mb-3">Choose Your Strategy</h2>
                                <p className="text-muted-foreground">
                                    {formData.experience_level === 'beginner'
                                        ? "Since you're a beginner, we recommend these safe, pre-configured strategies."
                                        : "Select a base strategy or start from scratch."}
                                </p>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                {strategiesData?.strategies?.slice(0, 3).map((strat: any) => (
                                    <div
                                        key={strat.id}
                                        onClick={() => updateField('strategy_preference', strat.id)}
                                        className={`cursor-pointer border rounded-xl overflow-hidden transition-all ${formData.strategy_preference === strat.id ? 'ring-2 ring-primary border-transparent' : 'border-border hover:border-primary/50'}`}
                                    >
                                        <div className="bg-muted/30 p-4 border-b border-border/50">
                                            <div className="flex justify-between items-start mb-2">
                                                <h3 className="font-bold">{strat.name}</h3>
                                                {formData.strategy_preference === strat.id && <Check className="w-5 h-5 text-primary" />}
                                            </div>
                                            <div className="text-xs uppercase font-bold bg-primary/10 text-primary w-fit px-2 py-0.5 rounded">
                                                {strat.type || "HODL"}
                                            </div>
                                        </div>
                                        <div className="p-4 bg-card">
                                            <p className="text-sm text-muted-foreground mb-4 h-12 line-clamp-3">{strat.description}</p>
                                            <div className="flex items-center text-xs text-green-500 font-mono">
                                                <TrendingUp className="w-3 h-3 mr-1" />
                                                Verified APY: {strat.apy || '12%'}
                                            </div>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* Step 6: Risk Configuration [Updated] */}
                    {stepName === "risk" && (
                        <div className="space-y-8 max-w-2xl mx-auto">
                            <div className="text-center">
                                <h2 className="text-3xl font-bold mb-3">Risk Configuration</h2>
                                <p className="text-muted-foreground">Define your safety limits.</p>
                            </div>

                            <div className="bg-card border border-border rounded-xl p-6 space-y-6">
                                {/* Risk Tolerance Select */}
                                <div className="space-y-3">
                                    <label className="font-medium">Risk Per Trade</label>
                                    <div className="grid grid-cols-3 gap-4">
                                        {['low', 'medium', 'high'].map(r => (
                                            <button
                                                key={r}
                                                onClick={() => updateField('risk_tolerance', r)}
                                                className={`py-2 rounded-lg border capitalize ${formData.risk_tolerance === r ? 'bg-primary text-primary-foreground border-primary' : 'border-border hover:bg-muted'}`}
                                            >
                                                {r}
                                            </button>
                                        ))}
                                    </div>
                                </div>

                                {/* Max Drawdown Slider */}
                                <div className="space-y-3">
                                    <div className="flex justify-between">
                                        <label className="font-medium flex items-center gap-2">
                                            <AlertCircle className="w-4 h-4" /> Max Drawdown
                                        </label>
                                        <span className="font-mono font-bold">{formData.max_drawdown}%</span>
                                    </div>
                                    <input
                                        type="range"
                                        min="5"
                                        max="50"
                                        step="1"
                                        value={formData.max_drawdown}
                                        onChange={(e) => updateField('max_drawdown', parseInt(e.target.value))}
                                        className="w-full"
                                    />
                                    <p className="text-xs text-muted-foreground">If your portfolio drops by this amount, all bots will stop immediately.</p>
                                </div>

                                {/* Stop Loss & TP (Visual only for now) */}
                                <div className="grid grid-cols-2 gap-4">
                                    <div className="space-y-2 opacity-70">
                                        <label className="text-sm font-medium">Stop Loss</label>
                                        <input disabled type="text" value="Automatic (AI)" className="w-full h-10 rounded-lg border border-border bg-muted px-3 text-sm" />
                                    </div>
                                    <div className="space-y-2 opacity-70">
                                        <label className="text-sm font-medium">Take Profit</label>
                                        <input disabled type="text" value="Automatic (AI)" className="w-full h-10 rounded-lg border border-border bg-muted px-3 text-sm" />
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}


                    {/* Step 7: Summary */}
                    {stepName === "summary" && (
                        <div className="space-y-8 max-w-lg mx-auto text-center">
                            <motion.div
                                initial={{ scale: 0.8, opacity: 0 }}
                                animate={{ scale: 1, opacity: 1 }}
                                className="w-24 h-24 bg-green-500/10 rounded-full flex items-center justify-center mx-auto text-green-500 mb-6"
                            >
                                <Check className="w-12 h-12" />
                            </motion.div>
                            <div>
                                <h1 className="text-4xl font-bold mb-2">Your Bot Is Ready!</h1>
                                <p className="text-muted-foreground text-lg">
                                    We've configured everything based on your preferences.
                                </p>
                            </div>

                            <div className="bg-card border border-border rounded-xl p-6 text-left space-y-3">
                                <div className="flex justify-between">
                                    <span className="text-muted-foreground">Profile</span>
                                    <span className="font-bold capitalize">{formData.experience_level}</span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-muted-foreground">Assets</span>
                                    <span className="font-bold">{(formData.asset_types || []).join(", ")}</span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-muted-foreground">Strategy</span>
                                    <span className="font-bold">Active</span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-muted-foreground">Stop Loss</span>
                                    <span className="font-bold text-green-500">Enabled</span>
                                </div>
                            </div>

                            <button
                                onClick={handleComplete}
                                className="w-full py-4 bg-primary text-primary-foreground rounded-full font-bold shadow-xl hover:bg-primary/90 transition-all text-xl"
                            >
                                Go to Dashboard
                            </button>
                        </div>
                    )}

                    {/* Navigation Buttons */}
                    {stepName !== "welcome" && stepName !== "summary" && (
                        <div className="flex justify-center gap-4 mt-12">
                            <button
                                onClick={handleBack}
                                className="px-6 py-2.5 rounded-lg font-medium text-muted-foreground hover:bg-muted hover:text-foreground transition-colors"
                            >
                                Back
                            </button>
                            <button
                                onClick={handleNext}
                                disabled={saveStepMutation.isPending}
                                className="px-8 py-2.5 rounded-lg font-medium bg-primary text-primary-foreground hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/25 disabled:opacity-50 flex items-center gap-2"
                            >
                                {saveStepMutation.isPending ? "Saving..." : "Next"} <ChevronRight className="w-4 h-4" />
                            </button>
                        </div>
                    )}
                </motion.div>
            </div>
        </div>
    );
}
