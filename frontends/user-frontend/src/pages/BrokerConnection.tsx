import { useState, useEffect } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { motion, AnimatePresence } from "framer-motion";
import {
    Shield, Zap, Search, ChevronRight, Check, AlertTriangle, RefreshCw,
    Trash2, Edit2, Globe, Clock, Lock, X, Building2, ShieldCheck,
    Plus, MoreVertical, CheckCircle2, ExternalLink, AlertCircle, Wallet
} from "lucide-react";
import { api } from "../api/client";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";

// --- Components ---
function CapitalSummary({ accountId }: { accountId: string }) {
    const { data: summary, isLoading, refetch } = useQuery({
        queryKey: ["broker-capital", accountId],
        queryFn: () => api.getBrokerSummary(accountId),
        refetchInterval: 30000,
        retry: false
    });

    if (isLoading) return <div className="h-4 w-24 bg-muted animate-pulse rounded" />;

    if (!summary || !summary.capital) {
        if (summary?.error) return <div className="text-xs text-red-400 max-w-[150px] truncate" title={summary.error}>Error: {summary.error}</div>;
        return <div className="text-xs text-muted-foreground">-</div>;
    }

    const cap = summary.capital;

    return (
        <div className="flex flex-col gap-1 text-right">
            <div className="flex items-center justify-end gap-2 text-sm font-bold">
                <Wallet className="w-3.5 h-3.5 text-muted-foreground" />
                <span>{cap.total_equity.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} <span className="text-xs text-muted-foreground font-normal">{cap.currency}</span></span>
            </div>
            <div className="text-xs text-muted-foreground flex items-center justify-end gap-2">
                <span title="Available Balance">Avail: {cap.available_balance.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                <span className="text-border">|</span>
                <span className={`${cap.unrealized_pnl >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                    PnL: {cap.unrealized_pnl > 0 ? '+' : ''}{cap.unrealized_pnl.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                </span>
            </div>
        </div>
    );
}

// --- Types ---
interface Broker {
    id: string;
    name: string;
    market_types: string[];
    logo: string;
    connect_type?: string; // Added for different connection flows
    auth_fields: {
        name: string;
        label: string;
        type: string;
        required: boolean;
        options?: any[];
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
    status: "draft" | "validating" | "connected" | "restricted" | "disconnected" | "disabled" | "error";
    masked_key: string;
    last_validated_at: string;
    capabilities: string[];
    market_type: string;
    environment?: "live" | "demo" | "testnet" | "practice";
}

export default function BrokerConnection() {
    const navigate = useNavigate();
    const queryClient = useQueryClient();

    const [view, setView] = useState<"list" | "connect">("list");
    const [step, setStep] = useState<"selection" | "input" | "permissions" | "success" | "pairing">("selection");

    // Connection State
    const [selectedMarket, setSelectedMarket] = useState<"crypto" | "forex">("crypto");
    const [selectedBrokerId, setSelectedBrokerId] = useState<string | null>(null);
    const [environment, setEnvironment] = useState<"live" | "demo">("live");

    const [accountId, setAccountId] = useState<string | null>(null);
    const [credentials, setCredentials] = useState<Record<string, string>>({});

    // MT Pairing State (Magic Link Flow)
    const [setupLinkToken, setSetupLinkToken] = useState<string | null>(null);
    const [pairingSessionId, setPairingSessionId] = useState<string | null>(null);
    const [pairingExpiresAt, setPairingExpiresAt] = useState<string | null>(null);
    const [pairingStatus, setPairingStatus] = useState<"pending" | "paired" | "expired">("pending");
    const [connectorState, setConnectorState] = useState<"not_installed" | "waiting" | "connected">("not_installed");

    // UI State
    const [activeMenuId, setActiveMenuId] = useState<string | null>(null);
    const [testResult, setTestResult] = useState<any>(null); // Store test result for modal/alert

    // --- Queries ---
    const catalogQuery = useQuery({
        queryKey: ["broker-catalog"],
        queryFn: api.getBrokerCatalog,
    });

    // Filter brokers by market
    const filteredBrokers = catalogQuery.data?.brokers.filter((b: Broker) =>
        b.market_types.includes(selectedMarket)
    ) || [];

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
                setStep("permissions");
            }
            queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
        },
        onError: (error: any) => {
            alert(`Failed to submit credentials: ${error.message}`);
        }
    });

    const validateMutation = useMutation({
        mutationFn: () => api.validateBrokerConnection(accountId!),
        onSuccess: (data: { success: boolean; error?: string }) => {
            if (data.success) {
                setStep("success");
                queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
            } else {
                alert(`Validation failed: ${data.error || "Unknown error"}`);
            }
        },
        onError: (error: any) => {
            alert(`Validation failed: ${error.message}`);
        }
    });

    // Added for Test Connection Button (Phase 6 requirement)
    const testConnectionMutation = useMutation({
        mutationFn: () => api.testBrokerConnection({
            broker_id: selectedBrokerId!,
            credentials,
            environment
        }),
        onSuccess: (data: any) => {
            if (data.success || data.ok) {
                // If it's MT bridge, show details
                if (data.platform) {
                    const msg = `Connection Successful!\n\nPlatform: ${data.platform}\nAccount: ${data.account}\nBal: ${data.balance} ${data.currency}\nEquity: ${data.details?.equity}`;
                    alert(msg);
                } else {
                    alert("Connection Successful! You can now proceed to save.");
                }
            } else {
                alert(`Test failed: ${data.error}`);
            }
        },
        onError: (e: any) => alert(`Test Error: ${e.message}`)
    });

    const disconnectMutation = useMutation({
        mutationFn: api.disconnectBrokerAccount,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
        }
    });

    // --- MT Pairing Logic (Magic Link) ---
    const createPairingMutation = useMutation({
        mutationFn: api.createMTPairingSession,
        onSuccess: (data: { connector_link_token: string; setup_link: string; expires_at: string; session_id: string }) => {
            setSetupLinkToken(data.connector_link_token);
            setPairingSessionId(data.session_id);
            setPairingExpiresAt(data.expires_at);
            setPairingStatus("pending");
            setConnectorState("not_installed");
            setStep("pairing");
        },
        onError: (err: any) => {
            alert(err.message || "Failed to create pairing session");
        }
    });

    // Polling for pairing status (using session ID)
    const pairingQuery = useQuery({
        queryKey: ["mt-pairing", pairingSessionId],
        queryFn: () => api.getMTPairingStatus(pairingSessionId!),
        enabled: !!pairingSessionId && pairingStatus === "pending" && step === "pairing",
        refetchInterval: 2000,
        retry: false
    });



    useEffect(() => {
        if (pairingQuery.data) {
            if (pairingQuery.data.status === "paired") {
                setPairingStatus("paired");
                setConnectorState("connected"); // Show success state first
                setTimeout(() => {
                    setStep("success");
                    queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
                }, 2000); // Show connected animation for 2 seconds
            } else if (pairingQuery.data.status === "expired") {
                setPairingStatus("expired");
            }
        }
    }, [pairingQuery.data]);

    // Countdown effect
    const [timeLeft, setTimeLeft] = useState(0);
    useEffect(() => {
        if (step === "pairing" && pairingExpiresAt) {
            const interval = setInterval(() => {
                const now = new Date().getTime();
                const exp = new Date(pairingExpiresAt).getTime();
                setTimeLeft(Math.max(0, exp - now));
            }, 1000);
            return () => clearInterval(interval);
        }
    }, [step, pairingExpiresAt]);

    // --- Helpers ---
    const selectedBroker = catalogQuery.data?.brokers.find((b: Broker) => b.id === selectedBrokerId);

    const handleSelectBroker = (brokerId: string, marketType: string) => {
        // Force the market type to selectedMarket just in case, but usually follows broker
        const existingDraft = accountsQuery.data?.accounts.find(
            (a: BrokerAccount) => a.broker_id === brokerId && a.status === 'draft'
        );

        setSelectedBrokerId(brokerId);

        // MT Pairing Flow
        if (brokerId === "mt4" || brokerId === "mt5") {
            // Start pairing flow (Magic Link)
            createPairingMutation.mutate({ brokerId });
            return;
        }

        if (existingDraft) {
            setAccountId(existingDraft.id);
            setStep("input");
        } else {
            connectMutation.mutate({ broker_id: brokerId, market_type: selectedMarket });
        }
    };


    const handleCredentialInput = (field: string, value: string) => {
        setCredentials(prev => ({ ...prev, [field]: value }));
    };

    const handleSubmitCredentials = () => {
        submitCredsMutation.mutate(credentials);
    };


    const handleDisconnect = (id: string) => {
        if (confirm("Are you sure you want to disconnect this broker?")) {
            disconnectMutation.mutate(id);
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
                                        <div className="flex-1 min-w-0">
                                            <div className="flex items-center justify-between mb-1">
                                                <h3 className="font-bold text-lg truncate text-accent-foreground">{account.label || "Broker Account"}</h3>
                                                <div className="flex items-center gap-2">
                                                    {account.environment === 'testnet' || account.environment === 'demo' ? (
                                                        <span className="bg-purple-500/10 text-purple-400 px-2 py-0.5 rounded text-[10px] uppercase font-bold border border-purple-500/20">TESTNET</span>
                                                    ) : null}
                                                </div>
                                            </div>
                                            <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-sm text-muted-foreground">
                                                <div className="flex items-center gap-1.5">
                                                    <div className={`w-2 h-2 rounded-full ${account.status === 'connected' ? 'bg-green-500 animate-pulse' : account.status === 'restricted' ? 'bg-yellow-500' : 'bg-red-500'}`} />
                                                    <span className="capitalize font-medium text-foreground">{account.status}</span>
                                                </div>
                                                <span className="text-border">|</span>
                                                <span className="font-mono text-xs bg-muted/50 px-1.5 py-0.5 rounded">{account.masked_key?.substring(0, 8)}...</span>
                                                <span className="text-border">|</span>
                                                <span>{account.market_type === 'crypto' ? 'Crypto' : 'Stocks'}</span>
                                            </div>

                                            {(account.status === 'restricted' || account.status === 'error') && (account as any).last_error_message && (
                                                <div className="mt-3 text-xs bg-red-500/10 border border-red-500/20 text-red-400 p-2 rounded-lg flex items-start gap-2">
                                                    <span className="shrink-0 mt-0.5">⚠️</span>
                                                    <span>{(account as any).last_error_message}</span>
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                    {/* Capital Section */}
                                    {account.status === 'connected' && (
                                        <div className="hidden md:block px-6 py-2 border-l border-border/50">
                                            <CapitalSummary accountId={account.id} />
                                        </div>
                                    )}

                                    <div className="flex items-center gap-3 w-full md:w-auto justify-end relative">
                                        {/* Resume Setup Button for Drafts */}
                                        {account.status === 'draft' && (
                                            <button
                                                onClick={() => {
                                                    setSelectedBrokerId(account.broker_id);
                                                    setAccountId(account.id);
                                                    setStep("input");
                                                    setView("connect");
                                                }}
                                                className="px-4 py-2 bg-primary text-primary-foreground text-sm font-semibold rounded-lg shadow-md hover:bg-primary/90 transition-colors mr-2"
                                            >
                                                Resume Setup
                                            </button>
                                        )}

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
                                                        {account.status !== 'disconnected' && (
                                                            <button
                                                                onClick={() => handleDisconnect(account.id)}
                                                                className="w-full text-left px-4 py-3 text-sm text-yellow-600 hover:bg-yellow-500/10 transition-colors flex items-center gap-3 font-medium"
                                                            >
                                                                <Zap className="w-4 h-4" /> Disconnect
                                                            </button>
                                                        )}

                                                        {(account.status === 'disconnected' || account.status === 'draft' || account.status === 'restricted') && (
                                                            <button
                                                                onClick={async () => {
                                                                    if (confirm("Are you sure you want to permanently delete this account? This cannot be undone.")) {
                                                                        await api.deleteBrokerAccount(account.id);
                                                                        queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
                                                                    }
                                                                }}
                                                                className="w-full text-left px-4 py-3 text-sm text-destructive hover:bg-destructive/5 transition-colors flex items-center gap-3 font-medium"
                                                            >
                                                                <Trash2 className="w-4 h-4" /> Remove
                                                            </button>
                                                        )}
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
                    {/* Market Toggle */}
                    <div className="flex p-1 bg-muted/40 rounded-xl mb-4">
                        <button
                            onClick={() => setSelectedMarket("crypto")}
                            className={`flex-1 py-3 text-sm font-bold rounded-lg transition-all ${selectedMarket === 'crypto' ? 'bg-background shadow-sm text-foreground ring-1 ring-border/50' : 'text-muted-foreground hover:bg-muted/50'}`}
                        >
                            Crypto
                        </button>
                        <button
                            onClick={() => setSelectedMarket("forex")}
                            className={`flex-1 py-3 text-sm font-bold rounded-lg transition-all ${selectedMarket === 'forex' ? 'bg-background shadow-sm text-foreground ring-1 ring-border/50' : 'text-muted-foreground hover:bg-muted/50'}`}
                        >
                            Forex
                        </button>
                    </div>

                    <div className="space-y-4">
                        {filteredBrokers.length === 0 && (
                            <div className="text-center py-10 text-muted-foreground">
                                No brokers available for {selectedMarket}.
                            </div>
                        )}
                        {filteredBrokers.map((broker: Broker) => (
                            <div
                                key={broker.id}
                                className={`relative rounded-xl border transition-all ${broker.is_available
                                    ? "border-border/60 hover:border-primary/50 hover:shadow-lg bg-card hover:bg-accent/5 cursor-pointer group"
                                    : "border-border/30 bg-muted/20 opacity-70 cursor-not-allowed"
                                    }`}
                                onClick={() => {
                                    if (broker.is_available) {
                                        handleSelectBroker(broker.id, selectedMarket);
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
                                            {broker.market_types.filter(m => m === selectedMarket).join(", ")} trading
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

        if (step === "pairing" && selectedBroker) {
            const minutes = Math.floor(timeLeft / 60000);
            const seconds = Math.floor((timeLeft % 60000) / 1000);
            const setupLink = `cosmicforge://mt-connect?token=${setupLinkToken}`;

            const copySetupLink = () => {
                navigator.clipboard.writeText(setupLink);
                alert("Setup link copied! Paste it in the connector.");
            };

            return (
                <motion.div
                    key="pairing"
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
                            <h2 className="text-xl font-bold">Connect {selectedBroker.name}</h2>
                            <div className="text-sm text-muted-foreground">Two simple steps to connect your trading platform</div>
                        </div>
                    </div>

                    {/* 3-State Wizard */}
                    <div className="bg-card border border-border/50 rounded-xl p-8 space-y-6">

                        {/* State 1: Not Installed */}
                        {connectorState === "not_installed" && (
                            <>
                                <div className="text-center space-y-6">
                                    <div className="w-20 h-20 mx-auto bg-blue-500/10 rounded-full flex items-center justify-center">
                                        <Shield className="w-10 h-10 text-blue-500" />
                                    </div>
                                    <div>
                                        <h3 className="text-2xl font-bold mb-2">Download Windows Connector</h3>
                                        <p className="text-muted-foreground max-w-lg mx-auto">
                                            Install the secure connector on your Windows machine running MetaTrader. One-time setup, works forever.
                                        </p>
                                    </div>
                                    <a
                                        href="/connector/mt-bridge-connector.zip"
                                        onClick={() => setConnectorState("waiting")}
                                        className="inline-flex items-center gap-3 px-8 py-4 bg-primary text-primary-foreground rounded-xl text-lg font-bold shadow-lg hover:shadow-xl hover:-translate-y-0.5 transition-all"
                                    >
                                        <ExternalLink className="w-5 h-5" />
                                        Download Connector
                                    </a>
                                </div>
                            </>
                        )}

                        {/* State 2: Waiting for Connection */}
                        {connectorState === "waiting" && (
                            <>
                                <div className="text-center space-y-6">
                                    <div className="w-20 h-20 mx-auto bg-green-500/10 rounded-full flex items-center justify-center">
                                        <RefreshCw className="w-10 h-10 text-green-500 animate-spin" />
                                    </div>
                                    <div>
                                        <h3 className="text-2xl font-bold mb-2">Run Connector & Paste Token</h3>
                                        <p className="text-muted-foreground max-w-lg mx-auto mb-4">
                                            Open the connector on your Windows machine and paste the token below when prompted.
                                        </p>
                                    </div>

                                    {/* Setup Link Token Display */}
                                    <div className="max-w-2xl mx-auto">
                                        <div className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-3">Your Setup Token</div>
                                        <div className="bg-muted/50 border-2 border-primary/20 rounded-xl p-4 flex items-center gap-3">
                                            <code className="flex-1 text-sm font-mono text-left break-all">{setupLink}</code>
                                            <button
                                                onClick={copySetupLink}
                                                className="px-4 py-2 bg-primary text-primary-foreground rounded-lg text-sm font-bold hover:bg-primary/90 transition-colors shrink-0"
                                            >
                                                Copy
                                            </button>
                                        </div>
                                        <div className="mt-3 flex items-center justify-center gap-2 text-sm text-muted-foreground">
                                            <Clock className="w-4 h-4 text-orange-500" />
                                            <span className={timeLeft < 60000 ? "text-orange-500 animate-pulse font-bold" : ""}>
                                                Expires in {minutes}:{seconds.toString().padStart(2, '0')}
                                            </span>
                                        </div>
                                    </div>

                                    <div className="pt-4 flex justify-center">
                                        <div className="flex items-center gap-3 px-4 py-2 bg-green-500/5 text-green-600 rounded-full text-sm font-medium animate-pulse border border-green-500/20">
                                            <RefreshCw className="w-4 h-4 animate-spin" />
                                            Waiting for connector...
                                        </div>
                                    </div>

                                    {/* Simplified Instructions */}
                                    <details className="text-left max-w-2xl mx-auto">
                                        <summary className="cursor-pointer text-sm text-muted-foreground hover:text-foreground font-medium">Need help? Click for instructions</summary>
                                        <div className="mt-4 space-y-2 text-sm text-muted-foreground pl-4 border-l-2 border-border">
                                            <p>1. Extract the downloaded ZIP file</p>
                                            <p>2. Double-click <code className="bg-muted px-1.5 py-0.5 rounded text-xs">pair.exe</code> (or run <code className="bg-muted px-1.5 py-0.5 rounded text-xs">python pair.py</code>)</p>
                                            <p>3. Paste the token above when prompted</p>
                                            <p>4. Wait for "Connected ✅" message</p>
                                        </div>
                                    </details>

                                    {/* Advanced Section for Support */}
                                    <details className="text-left max-w-2xl mx-auto">
                                        <summary className="cursor-pointer text-xs text-muted-foreground/70 hover:text-muted-foreground font-medium">Advanced (Support Only)</summary>
                                        <div className="mt-3 p-4 bg-muted/20 rounded-lg space-y-2 text-xs font-mono">
                                            <div><span className="text-muted-foreground">Session ID:</span> {pairingSessionId}</div>
                                            <div><span className="text-muted-foreground">Status:</span> {pairingStatus}</div>
                                            <div><span className="text-muted-foreground">Last Checked:</span> {new Date().toLocaleTimeString()}</div>
                                        </div>
                                    </details>
                                </div>
                            </>
                        )}

                        {/* State 3: Connected */}
                        {connectorState === "connected" && (
                            <div className="text-center space-y-6">
                                <div className="w-20 h-20 mx-auto bg-green-500/10 rounded-full flex items-center justify-center">
                                    <CheckCircle2 className="w-10 h-10 text-green-500" />
                                </div>
                                <div>
                                    <h3 className="text-2xl font-bold mb-2 text-green-600">✅ Connected!</h3>
                                    <p className="text-muted-foreground">
                                        Your MetaTrader account is now linked and ready for trading.
                                    </p>
                                </div>
                            </div>
                        )}
                    </div>

                    <div className="flex justify-start pt-4">
                        <button
                            onClick={() => {
                                setStep("selection");
                                setSetupLinkToken(null);
                                setConnectorState("not_installed");
                            }}
                            className="px-6 h-12 rounded-xl text-sm font-semibold hover:bg-destructive/10 hover:text-destructive transition-colors text-muted-foreground"
                        >
                            Cancel
                        </button>
                    </div>
                </motion.div>
            );
        }

        if (step === "input" && selectedBroker) {

            // IBKR Configured Flow
            if (selectedBroker.id === "ibkr") {
                const updateIbkrConfig = (key: string, value: string) => {
                    setCredentials(prev => ({ ...prev, [key]: value }));
                };

                // Initialize defaults if empty
                useEffect(() => {
                    if (!credentials.host) updateIbkrConfig("host", "127.0.0.1");
                    if (!credentials.client_id) updateIbkrConfig("client_id", "1");
                    if (!credentials.bridge_mode) updateIbkrConfig("bridge_mode", "tws");

                    // Port logic based on mode/env
                    const mode = credentials.bridge_mode || "tws";
                    const isPaper = environment === "demo";
                    let defaultPort = "7496";
                    if (mode === "tws") defaultPort = isPaper ? "7497" : "7496";
                    if (mode === "gateway") defaultPort = isPaper ? "4002" : "4001";

                    // Only set if not already set (or if we want to auto-switch, which is complex. 
                    // Let's just set it if empty, or force update if it matches a "known default" of another mode? 
                    // Simpler: Just set it if empty.)
                    if (!credentials.port) updateIbkrConfig("port", defaultPort);
                }, [environment, credentials.bridge_mode]);

                const handleTestIBKR = async () => {
                    try {
                        const config = {
                            host: credentials.host || "127.0.0.1",
                            port: parseInt(credentials.port || "7496"),
                            client_id: parseInt(credentials.client_id || "1"),
                            bridge_mode: credentials.bridge_mode || "tws",
                            test: true
                        };
                        // We use linkBroker but maybe we need a distinct "test" flag? 
                        // The current backend connects immediately. If it fails, it returns status="unreachable".
                        // So calling linkBroker IS testing the connection.

                        const data = await api.linkBroker("ibkr", config);
                        if (data.status === "connected") {
                            alert("Connection Successful! TWS is reachable and accounts were found.");
                        } else {
                            alert(`Connection Failed: ${data.message}`);
                        }
                    } catch (e: any) {
                        alert(`Test Error: ${e.message}`);
                    }
                };

                const handleConnectIBKR = async () => {
                    try {
                        const config = {
                            host: credentials.host || "127.0.0.1",
                            port: parseInt(credentials.port || "7496"),
                            client_id: parseInt(credentials.client_id || "1"),
                            bridge_mode: credentials.bridge_mode || "tws"
                        };

                        const data = await api.linkBroker("ibkr", config);

                        if (data.status === "connected") {
                            queryClient.invalidateQueries({ queryKey: ["broker-accounts"] });
                            setStep("success");
                        } else {
                            alert(`Connection Failed: ${data.message || "Unknown error"}`);
                        }
                    } catch (e: any) {
                        alert(`Error: ${e.message}`);
                    }
                };

                return (
                    <motion.div
                        key="ibkr-connect"
                        initial={{ opacity: 0, x: 20 }}
                        animate={{ opacity: 1, x: 0 }}
                        exit={{ opacity: 0, x: -20 }}
                        className="space-y-6"
                    >
                        <div className="flex items-center gap-4 pb-6 border-b border-border/40">
                            <div className="w-12 h-12 rounded-xl bg-white border border-border/20 shadow-sm flex items-center justify-center shrink-0">
                                <img src={selectedBroker.logo} alt="IBKR" className="w-8 h-8 object-contain" />
                            </div>
                            <div>
                                <h2 className="text-xl font-bold">Connect Interactive Brokers</h2>
                                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                    <span>Target Environment:</span>
                                    <div className="flex p-0.5 bg-muted/40 rounded-lg">
                                        <button
                                            onClick={() => setEnvironment("live")}
                                            className={`px-3 py-1 text-xs font-bold rounded-md transition-all ${environment === 'live' ? 'bg-background shadow-sm text-foreground' : 'text-muted-foreground hover:bg-muted/50'}`}
                                        >
                                            LIVE
                                        </button>
                                        <button
                                            onClick={() => setEnvironment("demo")}
                                            className={`px-3 py-1 text-xs font-bold rounded-md transition-all ${environment === 'demo' ? 'bg-background shadow-sm text-foreground' : 'text-muted-foreground hover:bg-muted/50'}`}
                                        >
                                            PAPER
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div className="bg-card border border-border/50 rounded-xl p-6 space-y-6">

                            {/* Bridge Mode */}
                            <div className="space-y-3">
                                <label className="text-sm font-semibold text-foreground/80">Connection Mode</label>
                                <div className="grid grid-cols-2 gap-4">
                                    <button
                                        onClick={() => {
                                            updateIbkrConfig("bridge_mode", "tws");
                                            // Auto-update port
                                            updateIbkrConfig("port", environment === "demo" ? "7497" : "7496");
                                        }}
                                        className={`p-4 rounded-xl border-2 text-left transition-all ${credentials.bridge_mode !== 'gateway' // Default TWS
                                            ? 'border-primary bg-primary/5 ring-1 ring-primary/20'
                                            : 'border-border/50 hover:border-border hover:bg-muted/5'}`}
                                    >
                                        <div className="font-bold mb-1">Trader Workstation (TWS)</div>
                                        <div className="text-xs text-muted-foreground">Desktop App. Requires API enabled.</div>
                                    </button>

                                    <button
                                        onClick={() => {
                                            updateIbkrConfig("bridge_mode", "gateway");
                                            updateIbkrConfig("port", environment === "demo" ? "4002" : "4001");
                                        }}
                                        className={`p-4 rounded-xl border-2 text-left transition-all ${credentials.bridge_mode === 'gateway'
                                            ? 'border-primary bg-primary/5 ring-1 ring-primary/20'
                                            : 'border-border/50 hover:border-border hover:bg-muted/5'}`}
                                    >
                                        <div className="font-bold mb-1">IB Gateway</div>
                                        <div className="text-xs text-muted-foreground">Lightweight API Gateway.</div>
                                    </button>
                                </div>
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                                {/* Host */}
                                <div className="space-y-2">
                                    <label className="text-sm font-semibold text-foreground/80">Host IP</label>
                                    <input
                                        type="text"
                                        value={credentials.host || "127.0.0.1"}
                                        onChange={(e) => updateIbkrConfig("host", e.target.value)}
                                        className="w-full h-11 rounded-lg border border-input bg-background/50 px-3 text-sm font-mono"
                                    />
                                </div>

                                {/* Port */}
                                <div className="space-y-2">
                                    <label className="text-sm font-semibold text-foreground/80">Port</label>
                                    <input
                                        type="number"
                                        value={credentials.port || (environment === "demo" ? "7497" : "7496")}
                                        onChange={(e) => updateIbkrConfig("port", e.target.value)}
                                        className="w-full h-11 rounded-lg border border-input bg-background/50 px-3 text-sm font-mono"
                                    />
                                    <div className="text-[10px] text-muted-foreground">
                                        Defaults: TWS(7496/7497), Gateway(4001/4002)
                                    </div>
                                </div>

                                {/* Client ID */}
                                <div className="space-y-2">
                                    <label className="text-sm font-semibold text-foreground/80">Client ID</label>
                                    <input
                                        type="number"
                                        value={credentials.client_id || "1"}
                                        onChange={(e) => updateIbkrConfig("client_id", e.target.value)}
                                        className="w-full h-11 rounded-lg border border-input bg-background/50 px-3 text-sm font-mono"
                                    />
                                    <div className="text-[10px] text-muted-foreground">
                                        Must be unique per connection. 0 is reserved.
                                    </div>
                                </div>
                            </div>

                            <div className="bg-blue-500/5 border border-blue-500/10 rounded-xl p-4 flex gap-3 text-sm">
                                <Search className="w-5 h-5 text-blue-500 shrink-0" />
                                <div className="text-blue-900 dark:text-blue-100 opacity-90">
                                    Ensure <b>ActiveX and Socket Clients</b> are enabled in TWS/Gateway API Settings.
                                    Add <b>127.0.0.1</b> to the Trusted IPs list if connecting locally.
                                </div>
                            </div>

                        </div>

                        <div className="flex gap-3 pt-4">
                            <button
                                onClick={() => setStep("selection")}
                                className="px-6 h-12 rounded-xl text-sm font-semibold hover:bg-muted transition-colors text-muted-foreground"
                            >
                                Back
                            </button>

                            <button
                                onClick={handleTestIBKR}
                                className="px-6 h-12 rounded-xl text-sm font-semibold bg-blue-500/10 text-blue-500 hover:bg-blue-500/20 transition-all border border-blue-500/20"
                            >
                                Test Connection
                            </button>

                            <button
                                onClick={handleConnectIBKR}
                                className="flex-1 h-12 bg-[#D13838] hover:bg-[#b02e2e] text-white rounded-xl font-bold shadow-lg shadow-red-900/20 transition-all flex items-center justify-center gap-2"
                            >
                                Save & Connect <ChevronRight className="w-4 h-4" />
                            </button>
                        </div>
                    </motion.div>
                );
            }

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
                        {selectedBroker.auth_fields.map((field: { name: string, label: string, type: string, required: boolean, options?: any[] }) => (
                            <div key={field.name} className="space-y-2">
                                <div className="flex justify-between items-center">
                                    <label className="text-sm font-semibold text-foreground/80">{field.label}</label>
                                    {field.required && <span className="text-[10px] font-bold text-primary uppercase bg-primary/10 px-1.5 py-0.5 rounded">Required</span>}
                                </div>
                                {field.type === 'select' && field.options ? (
                                    <div className="relative">
                                        <select
                                            value={credentials[field.name] || (field.options[0] as any).value || ''}
                                            onChange={(e) => handleCredentialInput(field.name, e.target.value)}
                                            className="flex h-12 w-full rounded-xl border border-input bg-card/50 px-4 py-3 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 transition-all font-mono appearance-none"
                                        >
                                            {field.options.map((opt: any) => (
                                                <option key={opt.value || opt} value={opt.value || opt}>
                                                    {opt.label || opt}
                                                </option>
                                            ))}
                                        </select>
                                        <div className="absolute right-4 top-1/2 -translate-y-1/2 pointer-events-none text-muted-foreground">
                                            <ChevronRight className="w-4 h-4 rotate-90" />
                                        </div>
                                    </div>
                                ) : (
                                    <input
                                        type={field.type}
                                        value={credentials[field.name] || ''}
                                        onChange={(e) => handleCredentialInput(field.name, e.target.value)}
                                        className="flex h-12 w-full rounded-xl border border-input bg-card/50 px-4 py-3 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 transition-all font-mono"
                                        placeholder={`Paste your ${field.label} here`}
                                        spellCheck={false}
                                    />
                                )}
                                {(field as any).help && (
                                    <p className="text-[11px] text-muted-foreground px-1">{(field as any).help}</p>
                                )}
                            </div>
                        ))}
                    </div>

                    {/* KYC Warning for OANDA/Forex Live */}
                    {selectedBroker.id === 'oanda' && environment === 'live' && (
                        <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-xl p-4 flex gap-3 text-sm mt-4">
                            <Shield className="w-5 h-5 text-yellow-600 shrink-0" />
                            <div className="text-yellow-700 dark:text-yellow-400">
                                <p className="font-bold">KYC Verification Required</p>
                                <p className="mt-1 opacity-90">
                                    Live Forex trading requires identity verification. Ensure your OANDA account is fully verified before connecting, otherwise orders may be rejected.
                                </p>
                            </div>
                        </div>
                    )}

                    {/* MT4/MT5 Bridge Info Box */}
                    {['mt4', 'mt5'].includes(selectedBroker.id) && (
                        <div className="bg-blue-500/10 border border-blue-500/20 rounded-xl p-4 flex gap-3 text-sm mt-4">
                            <AlertTriangle className="w-5 h-5 text-blue-600 shrink-0" />
                            <div className="text-blue-700 dark:text-blue-400">
                                <p className="font-bold">Bridge Requirement</p>
                                <ul className="list-disc ml-4 mt-1 opacity-90 space-y-1">
                                    <li>You must run MT4/MT5 Bridge on a Windows VPS or a publicly reachable machine.</li>
                                    <li>If you run locally, you need port forwarding (e.g. ngrok) or a tunnel to make the bridge accessible to the bot.</li>
                                </ul>
                            </div>
                        </div>
                    )}

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

                            {/* Test Connection Button */}
                            <button
                                onClick={() => testConnectionMutation.mutate()}
                                disabled={testConnectionMutation.isPending || !Object.keys(credentials).length}
                                className="px-6 h-12 rounded-xl text-sm font-semibold bg-blue-500/10 text-blue-500 hover:bg-blue-500/20 transition-all border border-blue-500/20 disabled:opacity-50"
                            >
                                {testConnectionMutation.isPending ? "Testing..." : "Test Connection"}
                            </button>

                            <button
                                onClick={handleSubmitCredentials}
                                disabled={submitCredsMutation.isPending}
                                className="flex-1 h-12 bg-primary text-primary-foreground rounded-xl font-semibold hover:bg-primary/90 transition-all shadow-lg shadow-primary/20 disabled:opacity-70 flex items-center justify-center gap-2"
                            >
                                {submitCredsMutation.isPending ? "Securing Keys..." : "Save & Verify"}
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
