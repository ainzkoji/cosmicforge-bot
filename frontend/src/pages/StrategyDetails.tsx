import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { ChevronLeft, Info, Copy, Calendar, Terminal, Play } from "lucide-react";

export default function StrategyDetails() {
    const { id } = useParams();
    const [activeTab, setActiveTab] = useState("overview");

    const { data: strategy, isLoading, error } = useQuery({
        queryKey: ["strategy", id],
        queryFn: () => api.getStrategyDetails(id || ""),
        enabled: !!id
    });

    // --- Prerequisites for "Use in Bot" ---
    const { data: brokerAccounts, isLoading: isLoadingBrokers } = useQuery({
        queryKey: ["broker-accounts"],
        queryFn: api.getBrokerAccounts,
    });

    const { data: kycStatus, isLoading: isLoadingKYC } = useQuery({
        queryKey: ["kyc-status"],
        queryFn: api.kycGetStatus,
    });

    const handleUseInBot = () => {
        // 1. Check Broker
        if (!brokerAccounts?.accounts || brokerAccounts.accounts.length === 0) {
            // Redirect with return URL
            const returnUrl = encodeURIComponent(`/dashboard/strategies/${id}`);
            window.location.href = `/dashboard/brokers?return_url=${returnUrl}`;
            return;
        }

        // 2. Check KYC
        if (kycStatus?.status !== 'approved') {
            const returnUrl = encodeURIComponent(`/dashboard/strategies/${id}`);
            window.location.href = `/dashboard/kyc?return_url=${returnUrl}`;
            return;
        }

        // 3. Success (Mock Bot Creation for now)
        alert("Requirements met! Opening Bot Creation Wizard... (Coming Soon)");
    };

    if (isLoading || isLoadingBrokers || isLoadingKYC) return <div className="p-8 text-center text-muted-foreground">Loading strategy details...</div>;
    if (error || !strategy) return <div className="p-8 text-center text-red-500">Failed to load strategy.</div>;

    const latestVersion = strategy.versions?.[0];

    return (
        <div className="container mx-auto max-w-5xl py-8 space-y-8 animate-in fade-in">
            {/* Header */}
            <div>
                <Link to="/dashboard/strategies" className="flex items-center text-sm text-muted-foreground mb-4 hover:text-foreground">
                    <ChevronLeft className="w-4 h-4 mr-1" /> Back to Marketplace
                </Link>
                <div className="flex flex-col md:flex-row justify-between items-start gap-6">
                    <div>
                        <div className="flex items-center gap-3 mb-2">
                            <h1 className="text-4xl font-bold">{strategy.name}</h1>
                            <span className={`text-xs px-2 py-1 rounded border uppercase font-bold tracking-wide ${strategy.visibility === 'official' ? 'bg-primary/10 text-primary border-primary/20' : 'bg-muted text-muted-foreground border-border'
                                }`}>
                                {strategy.visibility}
                            </span>
                        </div>
                        <p className="text-xl text-muted-foreground max-w-2xl">{strategy.description}</p>

                        <div className="flex gap-2 mt-4">
                            {strategy.market_types?.map((m: string) => (
                                <span key={m} className="px-2 py-0.5 bg-muted rounded text-xs font-medium text-muted-foreground capitalize">{m}</span>
                            ))}
                            {strategy.tags?.map((t: string) => (
                                <span key={t} className="px-2 py-0.5 border border-border rounded text-xs font-medium text-muted-foreground capitalize">{t}</span>
                            ))}
                        </div>
                    </div>

                    <div className="flex gap-3">
                        <button className="px-6 py-2.5 bg-secondary text-secondary-foreground rounded-lg font-medium hover:bg-muted transition-colors flex items-center gap-2">
                            <Copy className="w-4 h-4" /> Fork Strategy
                        </button>
                        <button
                            onClick={handleUseInBot}
                            disabled={isLoadingBrokers || isLoadingKYC}
                            className="bg-primary text-primary-foreground px-6 py-2 rounded-lg font-medium hover:bg-primary/90 transition-colors flex items-center gap-2"
                        >
                            <Play className="w-4 h-4" /> Use in Bot
                        </button>
                    </div>
                </div>
            </div>

            {/* Tabs */}
            <div className="border-b border-border">
                <nav className="flex gap-6">
                    {['overview', 'versions', 'source'].map(tab => (
                        <button
                            key={tab}
                            onClick={() => setActiveTab(tab)}
                            className={`pb-3 text-sm font-medium border-b-2 transition-colors capitalize ${activeTab === tab ? 'border-primary text-foreground' : 'border-transparent text-muted-foreground hover:text-foreground'
                                }`}
                        >
                            {tab}
                        </button>
                    ))}
                </nav>
            </div>

            {/* Content */}
            <div className="min-h-[400px]">
                {activeTab === 'overview' && (
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                        <div className="lg:col-span-2 space-y-6">
                            <div className="bg-card border border-border rounded-xl p-6">
                                <h3 className="text-lg font-semibold mb-4">Performance Summary</h3>
                                <div className="p-8 text-center border border-dashed border-border rounded-lg bg-muted/20">
                                    <BarChart2 className="w-8 h-8 mx-auto text-muted-foreground mb-2" />
                                    <p className="text-muted-foreground">No public backtest data available for this strategy yet.</p>
                                </div>
                            </div>

                            <div className="bg-card border border-border rounded-xl p-6">
                                <h3 className="text-lg font-semibold mb-4">About this Strategy</h3>
                                <div className="prose dark:prose-invert max-w-none text-sm text-muted-foreground">
                                    <p>
                                        This strategy is designed to operate on {strategy.market_types?.join(' and ')} markets.
                                        It utilizes a set of indicators to identify entry and exit points.
                                        Please review the parameters carefully before running on a live account.
                                    </p>
                                </div>
                            </div>
                        </div>

                        <div className="space-y-6">
                            <div className="bg-card border border-border rounded-xl p-6">
                                <h3 className="font-semibold mb-4 flex items-center gap-2">
                                    <Info className="w-4 h-4" /> Strategy Info
                                </h3>
                                <div className="space-y-3 text-sm">
                                    <div className="flex justify-between py-1 border-b border-border/50">
                                        <span className="text-muted-foreground">Version</span>
                                        <span>{latestVersion?.version_number || 1}</span>
                                    </div>
                                    <div className="flex justify-between py-1 border-b border-border/50">
                                        <span className="text-muted-foreground">Last Updated</span>
                                        <span>{new Date(strategy.updated_at).toLocaleDateString()}</span>
                                    </div>
                                    <div className="flex justify-between py-1 border-b border-border/50">
                                        <span className="text-muted-foreground">Tier</span>
                                        <span className="capitalize">{strategy.entitlement_tier}</span>
                                    </div>
                                    <div className="flex justify-between py-1 border-b border-border/50">
                                        <span className="text-muted-foreground">Risk Style</span>
                                        <span className="capitalize px-2 py-0.5 rounded-full bg-muted text-xs">
                                            {strategy.recommended_risk_style || 'Moderate'}
                                        </span>
                                    </div>
                                    {strategy.constraints_json?.min_capital && (
                                        <div className="flex justify-between py-1 border-b border-border/50">
                                            <span className="text-muted-foreground">Min Capital</span>
                                            <span>${strategy.constraints_json.min_capital}</span>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                )}

                {activeTab === 'versions' && (
                    <div className="space-y-4">
                        {strategy.versions?.map((v: any) => (
                            <div key={v.id} className="bg-card border border-border rounded-xl p-4 flex justify-between items-center bg-muted/20">
                                <div>
                                    <div className="font-bold flex items-center gap-2">
                                        v{v.version_number}
                                        {v.version_number === latestVersion?.version_number && <span className="bg-primary/20 text-primary text-[10px] px-2 py-0.5 rounded-full uppercase">Latest</span>}
                                    </div>
                                    <p className="text-sm text-muted-foreground mt-1">{v.changelog || "No changes documented"}</p>
                                </div>
                                <div className="text-right text-xs text-muted-foreground">
                                    <div className="flex items-center gap-1 mb-1 justify-end">
                                        <Calendar className="w-3 h-3" /> Released {new Date(v.created_at).toLocaleDateString()}
                                    </div>
                                    <button className="text-primary hover:underline">View Spec</button>
                                </div>
                            </div>
                        ))}
                    </div>
                )}

                {activeTab === 'source' && (
                    <div className="bg-black/80 rounded-xl p-6 border border-white/10 overflow-hidden font-mono text-xs">
                        <div className="flex items-center gap-2 mb-4 text-muted-foreground border-b border-white/10 pb-2">
                            <Terminal className="w-4 h-4" /> Strategy Logic Spec (JSON)
                        </div>
                        <pre className="overflow-auto max-h-[500px] text-green-400">
                            {JSON.stringify(latestVersion?.spec_json || {}, null, 2)}
                        </pre>
                    </div>
                )}
            </div>
        </div>
    );
}

function BarChart2(props: any) {
    return (
        <svg
            {...props}
            xmlns="http://www.w3.org/2000/svg"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
        >
            <line x1="18" x2="18" y1="20" y2="10" />
            <line x1="12" x2="12" y1="20" y2="4" />
            <line x1="6" x2="6" y1="20" y2="14" />
        </svg>
    )
}
