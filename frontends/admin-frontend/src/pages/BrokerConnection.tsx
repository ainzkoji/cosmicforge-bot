import { useState, useEffect } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { motion, AnimatePresence } from "framer-motion";
import {
    Shield, Zap, Search, ChevronRight, Check, AlertTriangle, RefreshCw,
    Trash2, Edit2, Globe, Clock, Lock, X, Building2, ShieldCheck,
    Plus, MoreVertical, CheckCircle2, ExternalLink, AlertCircle
} from "lucide-react";
import { api } from "../api/client";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";

// --- Types ---
interface Broker {
    id: string;
    name: string;
    market_types: string[];
    logo: string;
    auth_fields: {
        name: string;
        label: string;
        type: string;
        required: boolean;
        options?: string[];
    }[];
    features: string[];
    required_permissions: string[];
    is_available: boolean;
    unavailable_reason?: string;
    signup_url?: string;
}

interface BrokerAccount {
    id: string;
    broker_id: string;
    label: string;
    status: "draft" | "validating" | "connected" | "restricted" | "disconnected" | "disabled";
    masked_key: string;
    last_validated_at: string;
    capabilities: string[];
    market_type: string;
    environment?: "live" | "demo";
}

// Mock 2FA Modal
const TwoFactorModal = ({ isOpen, onClose, onVerify }: { isOpen: boolean, onClose: () => void, onVerify: () => void }) => {
    const [code, setCode] = useState("");

    if (!isOpen) return null;

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        if (code.length === 6) onVerify();
    };

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
            <motion.div
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                className="bg-card w-full max-w-md p-6 rounded-2xl border border-border/50 shadow-2xl relative overflow-hidden"
            >
                <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-primary/50 to-purple-500/50" />
                <div className="flex items-center gap-3 mb-6">
                    <div className="p-3 bg-primary/10 rounded-xl ring-1 ring-primary/20">
                        <ShieldCheck className="w-6 h-6 text-primary" />
                    </div>
                    <div>
                        <h3 className="text-xl font-bold">Security Verification</h3>
                        <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold">Two-Factor Authentication</p>
                    </div>
                </div>
                <p className="text-muted-foreground mb-8 text-sm leading-relaxed">
                    To ensure the security of your trading account, please enter the 6-digit code from your authenticator app.
                </p>
                <form onSubmit={handleSubmit} className="space-y-6">
                    <div className="relative">
                        <input
                            type="text"
                            maxLength={6}
                            value={code}
                            onChange={(e) => setCode(e.target.value.replace(/\D/g, ''))}
                            className="w-full text-center text-3xl tracking-[0.5em] font-mono py-4 rounded-xl border border-input bg-muted/30 focus:bg-background focus:ring-2 focus:ring-primary focus:border-primary transition-all outline-none"
                            placeholder="000000"
                            autoFocus
                        />
                    </div>
                    <div className="flex gap-3">
                        <button type="button" onClick={onClose} className="flex-1 px-4 py-3 hover:bg-muted rounded-xl font-medium transition-colors text-sm">Cancel</button>
                        <button type="submit" disabled={code.length !== 6} className="flex-1 px-4 py-3 bg-primary text-primary-foreground rounded-xl font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-all shadow-lg shadow-primary/20 text-sm">Verify & Proceed</button>
                    </div>
                </form>
            </motion.div>
        </div>
    );
};

export default function BrokerConnection() {
    const navigate = useNavigate();
    const queryClient = useQueryClient();

    const [view, setView] = useState<"list" | "connect">("list");
    const [step, setStep] = useState<"selection" | "input" | "permissions" | "success">("selection");

    // Connection State
    const [selectedBrokerId, setSelectedBrokerId] = useState<string | null>(null);
    const [environment, setEnvironment] = useState<"live" | "demo">("live");

    const [accountId, setAccountId] = useState<string | null>(null);
    const [credentials, setCredentials] = useState<Record<string, string>>({});

    // UI State
    const [is2FAOpen, setIs2FAOpen] = useState(false);
    const [pendingAction, setPendingAction] = useState<(() => void) | null>(null);
    const [activeMenuId, setActiveMenuId] = useState<string | null>(null);

    // --- Queries ---
    const catalogQuery = useQuery({
        queryKey: ["broker-catalog"],
        queryFn: api.getBrokerCatalog,
    });

    const accountsQuery = useQuery({
        queryKey: ["broker-accounts"],
        queryFn: api.getBrokerAccounts,
    });

    const [searchParams] = useSearchParams();
    const returnUrl = searchParams.get("return_url");

    // --- Mutations ---
    const connectMutation = useMutation({
        mutationFn: api.startBrokerConnection,
        onSuccess: (data: { account_id: string }) => {
            setAccountId(data.account_id);
            setStep("input");
        }
    });

    const submitCredsMutation = useMutation({
        mutationFn: (creds: any) => api.submitBrokerCredentials(accountId!, { ...creds, environment }),
        onSuccess: () => {
            if (returnUrl) {
                setTimeout(() => {
                    navigate(decodeURIComponent(returnUrl));
                }, 1500);
            } else {
                setStep("success");
            }
            queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
        }
    });

    const validateMutation = useMutation({
        mutationFn: () => api.validateBrokerConnection(accountId!),
        onSuccess: (data: { success: boolean }) => {
            if (data.success) {
                setStep("success");
                queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
            }
        }
    });

    const disconnectMutation = useMutation({
        mutationFn: api.disconnectBrokerAccount,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
        }
    });

    // --- Helpers ---
    const selectedBroker = catalogQuery.data?.brokers.find((b: Broker) => b.id === selectedBrokerId);

    const handleSelectBroker = (brokerId: string, marketType: string) => {
        setSelectedBrokerId(brokerId);
        connectMutation.mutate({ broker_id: brokerId, market_type: marketType });
    };

    const handleCredentialInput = (field: string, value: string) => {
        setCredentials(prev => ({ ...prev, [field]: value }));
    };

    const handleSubmitCredentials = () => {
        submitCredsMutation.mutate(credentials);
    };

    // 2FA Protected Actions
    const executeProtectedAction = (action: () => void) => {
        setPendingAction(() => action);
        setIs2FAOpen(true);
    };

    const handleDisconnect = (id: string) => {
        if (confirm("Are you sure you want to disconnect this broker?")) {
            executeProtectedAction(() => disconnectMutation.mutate(id));
        }
    };

    const handleRevalidate = (id: string) => {
        api.validateBrokerConnection(id).then(() => {
            queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
            alert("Re-validation triggered.");
        });
    };

    const resetFlow = () => {
        setView("list");
        setStep("selection");
        setSelectedBrokerId(null);
        setAccountId(null);
        setCredentials({});
        setEnvironment("live");
    }

    // --- Render Scenarios ---

    if (view === "list") {
        const hasAccounts = accountsQuery.data?.accounts && accountsQuery.data.accounts.length > 0;

        return (
            <div className="max-w-7xl mx-auto space-y-10 p-6" onClick={() => setActiveMenuId(null)}>
                <TwoFactorModal
                    isOpen={is2FAOpen}
                    onClose={() => { setIs2FAOpen(false); setPendingAction(null); }}
                    onVerify={() => { if (pendingAction) pendingAction(); setIs2FAOpen(false); setPendingAction(null); }}
                />

                {/* Header Section */}
                <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 pb-6 border-b border-border/40">
                    <div>
                        <h1 className="text-4xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-primary via-purple-500 to-blue-600">
                            Exchange Connections
                        </h1>
                        <p className="text-muted-foreground mt-2 text-lg max-w-2xl">
                            Securely connect your exchange accounts to enable automated trading. Your keys are encrypted and stored in a secure vault.
                        </p>
                    </div>
                    <button
                        onClick={() => setView("connect")}
                        className="group flex items-center gap-3 bg-primary text-primary-foreground px-6 py-3 rounded-full font-semibold shadow-xl shadow-primary/25 hover:shadow-2xl hover:shadow-primary/40 hover:-translate-y-0.5 transition-all text-sm"
                    >
                        <Plus className="w-5 h-5 group-hover:rotate-90 transition-transform" />
                        Connect New Broker
                    </button>
                </div>

                {/* Status Cards */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <div className="bg-card border border-border/50 rounded-2xl p-6 backdrop-blur-sm shadow-sm relative overflow-hidden group">
                        <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                            <ShieldCheck className="w-24 h-24 text-green-500" />
                        </div>
                        <div className="flex items-center gap-3 mb-2 relative z-10">
                            <div className="p-2 bg-green-500/10 rounded-lg">
                                <ShieldCheck className="w-5 h-5 text-green-500" />
                            </div>
                            <span className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">Active</span>
                        </div>
                        <div className="text-4xl font-bold relative z-10">
                            {accountsQuery.data?.accounts.filter((a: BrokerAccount) => a.status === 'connected').length || 0}
                        </div>
                    </div>
                    <div className="bg-card border border-border/50 rounded-2xl p-6 backdrop-blur-sm shadow-sm relative overflow-hidden group">
                        <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                            <Building2 className="w-24 h-24 text-blue-500" />
                        </div>
                        <div className="flex items-center gap-3 mb-2 relative z-10">
                            <div className="p-2 bg-blue-500/10 rounded-lg">
                                <Building2 className="w-5 h-5 text-blue-500" />
                            </div>
                            <span className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">Supported</span>
                        </div>
                        <div className="text-4xl font-bold relative z-10">
                            {catalogQuery.data?.brokers.length || 0}
                        </div>
                    </div>
                </div>

                {/* Accounts List */}
                <div className="space-y-6">
                    <h2 className="text-xl font-bold flex items-center gap-2">
                        <Globe className="w-5 h-5 text-primary" /> Your Connections
                    </h2>

                    {accountsQuery.isLoading ? (
                        <div className="space-y-4">
                            {[1, 2].map(i => (
                                <div key={`skeleton-${i}`} className="h-24 bg-muted/40 animate-pulse rounded-2xl" />
                            ))}
                        </div>
                    ) : !hasAccounts ? (
                        <div className="text-center py-20 border-2 border-dashed border-border/40 rounded-3xl bg-muted/5 flex flex-col items-center justify-center group hover:bg-muted/10 transition-colors cursor-pointer" onClick={() => setView("connect")}>
                            <div className="w-20 h-20 bg-muted/50 rounded-full flex items-center justify-center mb-6 group-hover:scale-110 transition-transform">
                                <Plus className="w-10 h-10 text-muted-foreground/50" />
                            </div>
                            <h3 className="text-xl font-semibold mb-2">No brokers connected yet</h3>
                            <p className="text-muted-foreground max-w-sm mx-auto mb-8">
                                Connect an exchange account to get started with automated trading strategies.
                            </p>
                            <button className="text-primary font-bold hover:underline flex items-center gap-2">
                                Connect now <ChevronRight className="w-4 h-4" />
                            </button>
                        </div>
                    ) : (
                        <div className="grid grid-cols-1 gap-4">
                            {accountsQuery.data?.accounts.map((account: BrokerAccount) => (
                                <motion.div
                                    initial={{ opacity: 0, y: 10 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    key={account.id}
                                    className="group bg-card hover:bg-accent/5 border border-border/50 rounded-2xl p-5 flex flex-col md:flex-row items-center justify-between gap-6 hover:shadow-lg hover:border-primary/20 transition-all cursor-default"
                                >
                                    <div className="flex items-center gap-5 w-full md:w-auto">
                                        <div className="w-16 h-16 rounded-2xl bg-white shadow-sm flex items-center justify-center shrink-0 p-3 relative overflow-hidden">
                                            {/* We could use real logos here if available */}
                                            {catalogQuery.data?.brokers.find((b: Broker) => b.id === account.broker_id)?.logo ? (
                                                <img src={catalogQuery.data?.brokers.find((b: Broker) => b.id === account.broker_id)?.logo} alt={account.broker_id} className="w-full h-full object-contain" />
                                            ) : (
                                                <Building2 className="w-8 h-8 text-black/20" />
                                            )}

                                            <div className={`absolute bottom-0 left-0 right-0 h-1 ${account.status === 'connected' ? 'bg-green-500' : 'bg-yellow-500'}`} />
                                        </div>
                                        <div>
                                            <div className="flex items-center gap-3 mb-1">
                                                <h3 className="font-bold text-xl capitalize">{account.label || account.broker_id}</h3>
                                                {account.environment === 'demo' && (
                                                    <span className="px-2 py-0.5 rounded-md text-[10px] uppercase font-bold tracking-wider bg-purple-500/10 text-purple-600 border border-purple-500/20">
                                                        Testnet
                                                    </span>
                                                )}
                                            </div>

                                            <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-sm text-muted-foreground">
                                                <div className="flex items-center gap-1.5">
                                                    <div className={`w-2 h-2 rounded-full ${account.status === 'connected' ? 'bg-green-500 animate-pulse' : 'bg-yellow-500'}`} />
                                                    <span className="capitalize font-medium text-foreground">{account.status}</span>
                                                </div>
                                                <span className="text-border">|</span>
                                                <span className="font-mono bg-muted/50 px-2 py-0.5 rounded text-xs border border-border/50">{account.masked_key}</span>
                                                <span className="text-border">|</span>
                                                <span className="capitalize">{account.market_type}</span>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="flex items-center gap-3 w-full md:w-auto justify-end relative">
                                        {/* Quick Actions Dropdown */}
                                        <div className="relative">
                                            <button
                                                onClick={(e) => { e.stopPropagation(); setActiveMenuId(activeMenuId === account.id ? null : account.id); }}
                                                className="p-2 hover:bg-muted rounded-full transition-colors relative"
                                            >
                                                <MoreVertical className="w-5 h-5 text-muted-foreground" />
                                            </button>

                                            <AnimatePresence>
                                                {activeMenuId === account.id && (
                                                    <motion.div
                                                        initial={{ opacity: 0, scale: 0.95, y: 5 }}
                                                        animate={{ opacity: 1, scale: 1, y: 0 }}
                                                        exit={{ opacity: 0, scale: 0.95, y: 5 }}
                                                        className="absolute right-0 mt-2 w-52 bg-card border border-border rounded-xl shadow-xl z-20 overflow-hidden"
                                                    >
                                                        <button
                                                            onClick={() => handleRevalidate(account.id)}
                                                            className="w-full text-left px-4 py-3 text-sm hover:bg-muted/50 transition-colors flex items-center gap-3 font-medium"
                                                        >
                                                            <RefreshCw className="w-4 h-4 text-primary" /> Check Connection
                                                        </button>
                                                        <div className="h-px bg-border/50 mx-2" />
                                                        <button
                                                            onClick={() => handleDisconnect(account.id)}
                                                            className="w-full text-left px-4 py-3 text-sm text-destructive hover:bg-destructive/5 transition-colors flex items-center gap-3 font-medium"
                                                        >
                                                            <Trash2 className="w-4 h-4" /> Disconnect
                                                        </button>
                                                    </motion.div>
                                                )}
                                            </AnimatePresence>
                                        </div>
                                    </div>
                                </motion.div>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        );
    }

    // --- Connect Container ---

    // Step Content Renderer
    const renderStepContent = () => {
        if (step === "selection") {
            return (
                <motion.div
                    key="selection"
                    initial={{ opacity: 0, x: 20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: -20 }}
                    className="space-y-6"
                >
                    <div className="space-y-4">
                        {catalogQuery.data?.brokers?.map((broker: Broker) => (
                            <div
                                key={broker.id}
                                className={`relative rounded-xl border transition-all ${broker.is_available
                                        ? "border-border/60 hover:border-primary/50 hover:shadow-lg bg-card hover:bg-accent/5 cursor-pointer group"
                                        : "border-border/30 bg-muted/20 opacity-70 cursor-not-allowed"
                                    }`}
                                onClick={() => {
                                    if (broker.is_available) {
                                        handleSelectBroker(broker.id, broker.market_types[0]);
                                    }
                                }}
                            >
                                <div className="p-5 flex items-center gap-5">
                                    <div className={`w-14 h-14 rounded-xl flex items-center justify-center shrink-0 border ${broker.is_available ? 'bg-white border-border/20 shadow-sm' : 'bg-muted border-transparent'}`}>
                                        {broker.logo ? (
                                            <img src={broker.logo} alt={broker.name} className="w-10 h-10 object-contain" />
                                        ) : (
                                            <Globe className="w-8 h-8 text-black/20" />
                                        )}
                                    </div>

                                    <div className="flex-1">
                                        <div className="flex items-center gap-2">
                                            <h3 className="font-bold text-lg">{broker.name}</h3>
                                            {!broker.is_available && (
                                                <span className="px-2 py-0.5 bg-muted text-muted-foreground text-[10px] font-bold uppercase rounded-md">
                                                    Coming Soon
                                                </span>
                                            )}
                                            {broker.is_available && (
                                                <span className="px-2 py-0.5 bg-green-500/10 text-green-600 text-[10px] font-bold uppercase rounded-md border border-green-500/20">
                                                    Available
                                                </span>
                                            )}
                                        </div>
                                        <p className="text-sm text-muted-foreground mt-0.5">
                                            {broker.market_types.join(", ")} trading
                                            {broker.features.includes("futures") && " (Futures Only)"}
                                        </p>
                                    </div>

                                    <div className="shrink-0 flex items-center">
                                        {broker.is_available ? (
                                            <div className="w-8 h-8 rounded-full bg-primary/10 flex items-center justify-center group-hover:bg-primary group-hover:text-primary-foreground transition-colors">
                                                <ChevronRight className="w-5 h-5" />
                                            </div>
                                        ) : (
                                            <Lock className="w-5 h-5 text-muted-foreground/30" />
                                        )}
                                    </div>
                                </div>

                                {/* Signup Link Footer */}
                                <div className="px-5 py-3 border-t border-border/30 bg-muted/10 flex justify-between items-center text-xs">
                                    <span className="text-muted-foreground font-medium">features: {broker.features.slice(0, 3).join(", ")}</span>
                                    {broker.signup_url && (
                                        <a
                                            href={broker.signup_url}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            onClick={(e) => { e.stopPropagation(); }}
                                            className="flex items-center gap-1 text-primary hover:underline font-semibold"
                                        >
                                            Create Account <ExternalLink className="w-3 h-3" />
                                        </a>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>
                    <div className="text-center text-xs text-muted-foreground">
                        More exchanges are added regularly. Check back soon.
                    </div>
                </motion.div>
            );
        }

        if (step === "input" && selectedBroker) {
            return (
                <motion.div
                    key="input"
                    initial={{ opacity: 0, x: 20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: -20 }}
                    className="space-y-6"
                >
                    <div className="flex items-center gap-4 pb-6 border-b border-border/40">
                        <div className="w-12 h-12 rounded-xl bg-white border border-border/20 shadow-sm flex items-center justify-center shrink-0">
                            {selectedBroker.logo ? (
                                <img src={selectedBroker.logo} alt={selectedBroker.name} className="w-8 h-8 object-contain" />
                            ) : (
                                <Globe className="w-6 h-6 text-black/20" />
                            )}
                        </div>
                        <div>
                            <h2 className="text-xl font-bold">{selectedBroker.name}</h2>
                            <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                <span>Connecting to</span>
                                <span className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono uppercase text-foreground">{environment}</span>
                                <span>environment</span>
                            </div>
                        </div>
                    </div>

                    {/* Signup Prompt */}
                    {selectedBroker.signup_url && (
                        <div className="bg-blue-500/5 border border-blue-500/10 rounded-xl p-4 flex gap-3 text-sm">
                            <AlertCircle className="w-5 h-5 text-blue-500 shrink-0" />
                            <div className="text-blue-600/80">
                                Don't have a <b>{selectedBroker.name}</b> account yet?{' '}
                                <a href={selectedBroker.signup_url} target="_blank" rel="noopener noreferrer" className="font-bold hover:underline">
                                    Sign up here &rarr;
                                </a>
                            </div>
                        </div>
                    )}

                    {/* Environment Toggle */}
                    <div className="flex p-1 bg-muted/40 rounded-lg">
                        <button
                            onClick={() => setEnvironment("live")}
                            className={`flex-1 py-2 text-sm font-semibold rounded-md transition-all ${environment === 'live' ? 'bg-background shadow-sm text-foreground ring-1 ring-border/50' : 'text-muted-foreground hover:bg-muted/50'}`}
                        >
                            Live Trading
                        </button>
                        <button
                            onClick={() => setEnvironment("demo")}
                            className={`flex-1 py-2 text-sm font-semibold rounded-md transition-all ${environment === 'demo' ? 'bg-background shadow-sm text-foreground ring-1 ring-border/50' : 'text-muted-foreground hover:bg-muted/50'}`}
                        >
                            Testnet / Demo
                        </button>
                    </div>

                    <div className="space-y-5">
                        {selectedBroker.auth_fields.map((field: { name: string, label: string, type: string, required: boolean }) => (
                            <div key={field.name} className="space-y-2">
                                <div className="flex justify-between items-center">
                                    <label className="text-sm font-semibold text-foreground/80">{field.label}</label>
                                    {field.required && <span className="text-[10px] font-bold text-primary uppercase bg-primary/10 px-1.5 py-0.5 rounded">Required</span>}
                                </div>
                                <input
                                    type={field.type}
                                    value={credentials[field.name] || ''}
                                    onChange={(e) => handleCredentialInput(field.name, e.target.value)}
                                    className="flex h-12 w-full rounded-xl border border-input bg-card/50 px-4 py-3 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 transition-all font-mono"
                                    placeholder={`Paste your ${field.label} here`}
                                    spellCheck={false}
                                />
                            </div>
                        ))}
                    </div>

                    <div className="pt-2">
                        <div className="flex items-center gap-2 mb-6 text-xs text-muted-foreground bg-muted/30 p-3 rounded-lg border border-border/40">
                            <Lock className="w-3 h-3" />
                            Your credentials are encrypted with AES-256 before being stored.
                        </div>

                        <div className="flex gap-3">
                            <button
                                onClick={() => setStep("selection")}
                                className="px-6 h-12 rounded-xl text-sm font-semibold hover:bg-muted transition-colors text-muted-foreground"
                            >
                                Back
                            </button>
                            <button
                                onClick={handleSubmitCredentials}
                                disabled={submitCredsMutation.isPending}
                                className="flex-1 h-12 bg-primary text-primary-foreground rounded-xl font-semibold hover:bg-primary/90 transition-all shadow-lg shadow-primary/20 disabled:opacity-70 flex items-center justify-center gap-2"
                            >
                                {submitCredsMutation.isPending ? "Securing Keys..." : "Continue to Verification"}
                                <ChevronRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                </motion.div>
            );
        }

        if (step === "permissions" && selectedBroker) {
            return (
                <motion.div
                    key="permissions"
                    initial={{ opacity: 0, x: 20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: -20 }}
                    className="space-y-8"
                >
                    <div className="text-center space-y-2">
                        <div className="w-16 h-16 bg-primary/10 rounded-full flex items-center justify-center mx-auto mb-4">
                            <RefreshCw className="w-8 h-8 text-primary" />
                        </div>
                        <h2 className="text-xl font-bold">Verifying Connection</h2>
                        <p className="text-sm text-muted-foreground max-w-xs mx-auto">
                            We're checking your API keys and verifying permissions for <b>{environment.toUpperCase()}</b> trading.
                        </p>
                    </div>

                    <div className="bg-muted/20 border border-border/40 rounded-xl p-5 space-y-3">
                        <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-2">Required Permissions</h3>
                        {selectedBroker.required_permissions.map((perm: string) => (
                            <div key={perm} className="flex items-center gap-3">
                                <div className="w-6 h-6 rounded-full bg-green-500/10 flex items-center justify-center shrink-0">
                                    <Check className="w-3.5 h-3.5 text-green-500" />
                                </div>
                                <span className="font-medium text-sm">{perm}</span>
                            </div>
                        ))}
                    </div>

                    <button
                        onClick={() => validateMutation.mutate()}
                        disabled={validateMutation.isPending}
                        className="w-full h-12 bg-primary text-primary-foreground rounded-xl font-semibold hover:bg-primary/90 transition-all shadow-lg shadow-primary/20 disabled:opacity-70 flex items-center justify-center gap-2"
                    >
                        {validateMutation.isPending ? (
                            <>
                                <RefreshCw className="w-5 h-5 animate-spin" /> Verifying...
                            </>
                        ) : "Confirm & Connect"}
                    </button>
                </motion.div>
            );
        }

        if (step === "success") {
            return (
                <motion.div
                    key="success"
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    className="text-center py-8 space-y-6"
                >
                    <div className="w-24 h-24 bg-green-500 rounded-full flex items-center justify-center mx-auto shadow-2xl shadow-green-500/30">
                        <CheckCircle2 className="w-12 h-12 text-white" />
                    </div>

                    <div>
                        <h2 className="text-2xl font-bold">Connection Successful!</h2>
                        <p className="text-muted-foreground mt-2 max-w-sm mx-auto">
                            Your <b>{selectedBroker?.name}</b> account is now linked. You can start deploying bots immediately.
                        </p>
                    </div>

                    <div className="pt-4">
                        <button
                            onClick={resetFlow}
                            className="bg-card hover:bg-accent border border-border text-foreground px-8 py-3 rounded-xl font-medium transition-all"
                        >
                            Return to Dashboard
                        </button>
                    </div>
                </motion.div>
            );
        }

        return null; // Fallback
    };

    return (
        <div className="min-h-[80vh] flex flex-col items-center justify-center p-6">
            <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                className="w-full max-w-2xl"
            >
                {/* Header / Back */}
                <div className="mb-8">
                    <button
                        onClick={resetFlow}
                        className="mb-6 flex items-center gap-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors"
                    >
                        <X className="w-4 h-4" /> Cancel Connection
                    </button>

                    <h1 className="text-3xl font-extrabold tracking-tight mb-2">Connect Broker</h1>
                    <div className="flex items-center gap-2 text-muted-foreground">
                        <span className={`px-2 py-0.5 rounded text-xs font-bold uppercase ${step === 'selection' ? 'bg-primary/20 text-primary' : 'bg-muted'}`}>Step 1: Select</span>
                        <ChevronRight className="w-3 h-3" />
                        <span className={`px-2 py-0.5 rounded text-xs font-bold uppercase ${step === 'input' ? 'bg-primary/20 text-primary' : 'bg-muted'}`}>Step 2: Authenticate</span>
                        <ChevronRight className="w-3 h-3" />
                        <span className={`px-2 py-0.5 rounded text-xs font-bold uppercase ${step === 'permissions' ? 'bg-primary/20 text-primary' : 'bg-muted'}`}>Step 3: Verify</span>
                    </div>
                </div>

                <div className="bg-card border border-border/50 rounded-2xl shadow-xl overflow-hidden relative">
                    <div className="h-1 bg-muted w-full absolute top-0 left-0">
                        <motion.div
                            className="h-full bg-gradient-to-r from-primary to-blue-500"
                            initial={{ width: "25%" }}
                            animate={{ width: step === 'selection' ? "33%" : step === 'input' ? "66%" : step === 'permissions' ? "90%" : "100%" }}
                        />
                    </div>

                    <div className="p-8">
                        <AnimatePresence mode="wait">
                            {renderStepContent()}
                        </AnimatePresence>
                    </div>
                </div>
            </motion.div>
        </div>
    );
}
