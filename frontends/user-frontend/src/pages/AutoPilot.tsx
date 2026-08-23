import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { Zap, Shield, TrendingUp, Activity, Check, AlertTriangle, Play, Smartphone } from "lucide-react";
import { api } from "../api/client";
import { useNavigate } from "react-router-dom";

export default function AutoPilot() {
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);
    const [brokers, setBrokers] = useState<any[]>([]);

    // Form State
    const [riskMode, setRiskMode] = useState<"conservative" | "balanced" | "aggressive">("balanced");
    const [allocationType, setAllocationType] = useState<"fixed_amount" | "percent_balance">("fixed_amount");
    const [allocationValue, setAllocationValue] = useState<number>(1000);
    const [selectedBroker, setSelectedBroker] = useState<string>("");
    const [mode, setMode] = useState<"paper" | "live">("paper");

    // Phase 7: Market & Forex
    const [marketType, setMarketType] = useState<"crypto" | "forex">("crypto");
    const [forexAllowlist, setForexAllowlist] = useState<string[]>([]);

    // Dynamic Forex Pairs from backend
    const [forexPairs, setForexPairs] = useState<string[]>([]);
    const [forexPairsLoading, setForexPairsLoading] = useState(false);
    const [forexPairsSource, setForexPairsSource] = useState<"broker" | "fallback" | "error" | null>(null);

    useEffect(() => {
        loadBrokers();
    }, []);

    const loadBrokers = async () => {
        try {
            const data = await api.getBrokerAccounts();
            setBrokers(data.accounts || []); // Ensure array
            // Don't auto-select yet, let user filter apply first
        } catch (e) {
            console.error("Failed to load brokers", e);
        }
    };

    // Filter brokers by market and connected status
    const filteredBrokers = brokers.filter(b => {
        if (b.status !== "connected") return false; // Only allow fully connected accounts

        if (marketType === "crypto") return b.market_type === "crypto" || !b.market_type; // Default/Crypto
        if (marketType === "forex") return b.market_type === "forex";
        return true;
    });

    // Load forex pairs when market type is forex and broker is selected
    useEffect(() => {
        if (marketType === "forex" && selectedBroker) {
            loadForexPairs(selectedBroker);
        }
    }, [marketType, selectedBroker]);

    const loadForexPairs = async (brokerAccountId: string) => {
        setForexPairsLoading(true);
        try {
            const selectedBrokerObj = brokers.find(b => b.id === brokerAccountId);
            const brokerId = selectedBrokerObj?.broker_id || "oanda";
            const environment = selectedBrokerObj?.environment || "practice";

            const data = await api.getForexInstruments(brokerId, brokerAccountId, environment);
            const symbols = data.instruments.map((inst: any) => inst.symbol);
            setForexPairs(symbols);
            setForexPairsSource(data.source);
            console.log(`Loaded ${symbols.length} forex pairs from ${data.source}`);
        } catch (e) {
            console.error("Failed to load forex instruments", e);
            // Strict requirement: No hardcoded fallback
            alert("Unable to load instruments from broker. Please ensure Gateway is running.");
            setForexPairs([]);
            setForexPairsSource("error");
        } finally {
            setForexPairsLoading(false);
        }
    };

    useEffect(() => {
        // Auto-select first compatible broker if current selection is invalid
        const current = brokers.find(b => b.id === selectedBroker);
        const isCompatible = current && (
            (marketType === "crypto" && current.market_type !== "forex") ||
            (marketType === "forex" && (current.market_type === "forex" || ["oanda", "ibkr"].includes(current.broker_id)))
        );

        if (!isCompatible && filteredBrokers.length > 0) {
            setSelectedBroker(filteredBrokers[0].id);
        } else if (!isCompatible) {
            setSelectedBroker("");
        }
    }, [marketType, brokers, filteredBrokers, selectedBroker]);

    const handleDeploy = async () => {
        if (!selectedBroker) {
            alert("Please select a broker account");
            return;
        }
        if (!allocationValue || isNaN(allocationValue) || allocationValue <= 0) {
            alert("Please enter a valid allocation amount");
            return;
        }
        if (marketType === "forex" && forexAllowlist.length === 0) {
            alert("Please select at least one currency pair for Forex allowlist.");
            return;
        }

        setLoading(true);
        try {
            // Map frontend risk mode to backend expected format
            const riskModeMap: Record<string, "conservative" | "medium" | "aggressive"> = {
                "conservative": "conservative",
                "balanced": "medium",
                "aggressive": "aggressive"
            };

            await api.deployAutoPilot({
                broker_account_ids: [selectedBroker],
                risk_mode: riskModeMap[riskMode],
                allocation: {
                    total_capital_budget: allocationType === "fixed_amount" ? allocationValue : allocationValue,
                    trade_amount_per_position: allocationType === "fixed_amount" ? allocationValue : allocationValue
                },
                execution_mode: mode,
                symbol_universe_mode: "auto",
                market_type: marketType,
                forex_config: marketType === "forex" ? { allowlist: forexAllowlist } : undefined
            });
            // Redirect to bots dashboard
            navigate("/dashboard/bots");
        } catch (e: any) {
            alert("Deployment failed: " + (e.response?.data?.detail || e.message));
        } finally {
            setLoading(false);
        }
    };

    const riskCards = [
        {
            id: "conservative",
            title: "Conservative",
            icon: Shield,
            color: "text-blue-400",
            bg: "bg-blue-400/10",
            border: "border-blue-400/20",
            activeBorder: "border-blue-400",
            desc: "Low risk, steady growth. Focus on capital preservation.",
            stats: { drawdown: "< 10%", return: "10-25% EST" }
        },
        {
            id: "balanced",
            title: "Balanced",
            icon: Activity,
            color: "text-primary",
            bg: "bg-primary/10",
            border: "border-primary/20",
            activeBorder: "border-primary",
            desc: "Moderate risk for optimal growth. Balanced exposure.",
            stats: { drawdown: "10-20%", return: "25-60% EST" }
        },
        {
            id: "aggressive",
            title: "Aggressive",
            icon: TrendingUp,
            color: "text-amber-400",
            bg: "bg-amber-400/10",
            border: "border-amber-400/20",
            activeBorder: "border-amber-400",
            desc: "High risk, maximum potential. For risk-tolerant capital.",
            stats: { drawdown: "20-35%", return: "60-150% EST" }
        }
    ];

    const togglePair = (pair: string) => {
        if (forexAllowlist.includes(pair)) {
            setForexAllowlist(forexAllowlist.filter(p => p !== pair));
        } else {
            setForexAllowlist([...forexAllowlist, pair]);
        }
    };

    return (
        <div className="max-w-5xl mx-auto space-y-8 pb-20">
            {/* Header */}
            <div className="space-y-2">
                <h1 className="text-3xl font-bold text-white flex items-center gap-3">
                    <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-primary to-purple-600 flex items-center justify-center shadow-lg shadow-primary/25">
                        <Zap className="text-white w-6 h-6" />
                    </div>
                    Auto Pilot
                </h1>
                <p className="text-gray-400 max-w-2xl">
                    Deploy our Master Ensemble strategy in one click. We automatically manage portfolio allocation, risk, and strategy selection based on your preference.
                </p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                {/* Left Column: Configuration */}
                <div className="lg:col-span-2 space-y-8">

                    {/* 0. Market Selection */}
                    <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                        <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                            <span className="w-6 h-6 rounded-full bg-white/5 flex items-center justify-center text-xs font-bold text-gray-400">1</span>
                            Select Market
                        </h2>
                        <div className="grid grid-cols-2 gap-4">
                            <div
                                onClick={() => setMarketType("crypto")}
                                className={`cursor-pointer p-4 rounded-xl border transition-all text-center ${marketType === "crypto" ? "bg-blue-500/10 border-blue-500 text-white" : "bg-[#0F1218] border-white/5 text-gray-400 hover:border-white/10"}`}
                            >
                                <div className="font-bold">Crypto</div>
                                <div className="text-xs mt-1 opacity-70">Binance, Bybit, etc.</div>
                            </div>
                            <div
                                onClick={() => setMarketType("forex")}
                                className={`cursor-pointer p-4 rounded-xl border transition-all text-center ${marketType === "forex" ? "bg-blue-500/10 border-blue-500 text-white" : "bg-[#0F1218] border-white/5 text-gray-400 hover:border-white/10"}`}
                            >
                                <div className="font-bold">Forex & CFDs</div>
                                <div className="text-xs mt-1 opacity-70">OANDA, IBKR, etc.</div>
                            </div>
                        </div>
                    </section>

                    {/* 1. Broker Selection */}
                    <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                        <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                            <span className="w-6 h-6 rounded-full bg-white/5 flex items-center justify-center text-xs font-bold text-gray-400">2</span>
                            Select Account
                        </h2>

                        {filteredBrokers.length === 0 ? (
                            <div className="p-4 bg-yellow-500/10 border border-yellow-500/20 rounded-xl text-yellow-500 text-sm flex items-center gap-3">
                                <AlertTriangle className="w-5 h-5 flex-shrink-0" />
                                <div>
                                    No {marketType} broker accounts connected. Please connect an exchange account first.
                                    <br />
                                    <a href="/dashboard/brokers" className="underline font-bold mt-1 inline-block">Connect Broker</a>
                                </div>
                            </div>
                        ) : (
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                {filteredBrokers.map(broker => (
                                    <div
                                        key={broker.id}
                                        onClick={() => setSelectedBroker(broker.id)}
                                        className={`cursor-pointer p-4 rounded-xl border transition-all ${selectedBroker === broker.id
                                            ? "bg-primary/10 border-primary shadow-lg shadow-primary/10"
                                            : "bg-[#0F1218] border-white/5 hover:border-white/10"
                                            }`}
                                    >
                                        <div className="flex justify-between items-start">
                                            <div className="font-bold text-white">{broker.name || broker.broker_id}</div>
                                            {selectedBroker === broker.id && <Check className="w-4 h-4 text-primary" />}
                                        </div>
                                        <div className="text-sm text-gray-500 mt-1">{broker.broker_id}</div>
                                        {broker.environment && <div className="text-xs text-gray-600 uppercase mt-2">{broker.environment}</div>}
                                    </div>
                                ))}
                            </div>
                        )}
                    </section>

                    {/* Forex Allowlist (Conditional) */}
                    {marketType === "forex" && (
                        <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                            <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                                <span className="w-6 h-6 rounded-full bg-white/5 flex items-center justify-center text-xs font-bold text-gray-400">3</span>
                                Trading Pairs (Allowlist)
                            </h2>
                            <p className="text-sm text-gray-400">Select the pairs you want the bot to trade. At least one required.</p>

                            {forexPairsLoading ? (
                                <div className="text-center py-8 text-gray-400">
                                    <div className="animate-spin h-8 w-8 border-2 border-primary border-t-transparent rounded-full mx-auto mb-2"></div>
                                    Loading available pairs...
                                </div>
                            ) : (
                                <>
                                    {forexPairsSource === "fallback" && (
                                        <div className="p-3 bg-yellow-500/10 border border-yellow-500/20 rounded-xl text-yellow-400 text-xs flex gap-2">
                                            <AlertTriangle className="w-4 h-4 flex-shrink-0" />
                                            <div>Using fallback pair list. Connect a broker account for full instrument listing.</div>
                                        </div>
                                    )}

                                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                        {forexPairs.map(pair => (
                                            <div
                                                key={pair}
                                                onClick={() => togglePair(pair)}
                                                className={`cursor-pointer px-3 py-2 rounded-lg border text-sm font-mono text-center transition-all ${forexAllowlist.includes(pair)
                                                    ? "bg-primary/20 border-primary text-primary"
                                                    : "bg-[#0F1218] border-white/5 text-gray-400 hover:border-white/20"
                                                    }`}
                                            >
                                                {pair}
                                            </div>
                                        ))}
                                    </div>

                                    <div className="p-3 bg-blue-500/10 border border-blue-500/20 rounded-xl text-blue-400 text-xs flex gap-2">
                                        <AlertTriangle className="w-4 h-4 flex-shrink-0" />
                                        <div>
                                            Forex markets are not 24/7. Missing market hours may result in skipped trades.
                                        </div>
                                    </div>
                                </>
                            )}
                        </section>
                    )}

                    {/* Risk Mode */}
                    <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                        <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                            <span className="w-6 h-6 rounded-full bg-white/5 flex items-center justify-center text-xs font-bold text-gray-400">{marketType === "forex" ? "4" : "3"}</span>
                            Risk Preference
                        </h2>

                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            {riskCards.map((card) => (
                                <motion.div
                                    key={card.id}
                                    whileHover={{ scale: 1.02 }}
                                    whileTap={{ scale: 0.98 }}
                                    onClick={() => setRiskMode(card.id as any)}
                                    className={`cursor-pointer relative overflow-hidden rounded-xl border p-4 transition-all ${riskMode === card.id ? `${card.bg} ${card.activeBorder}` : "bg-[#0F1218] border-white/5 hover:border-white/10"
                                        }`}
                                >
                                    <div className={`mb-3 ${card.color}`}>
                                        <card.icon className="w-6 h-6" />
                                    </div>
                                    <h3 className="font-bold text-white mb-1">{card.title}</h3>
                                    <p className="text-xs text-gray-400 mb-4 h-10">{card.desc}</p>

                                    <div className="space-y-1 pt-3 border-t border-white/5">
                                        <div className="flex justify-between text-xs">
                                            <span className="text-gray-500">Drawdown</span>
                                            <span className="text-gray-300 font-medium">{card.stats.drawdown}</span>
                                        </div>
                                        <div className="flex justify-between text-xs">
                                            <span className="text-gray-500">Target</span>
                                            <span className="text-primary font-bold">{card.stats.return}</span>
                                        </div>
                                    </div>
                                </motion.div>
                            ))}
                        </div>
                    </section>

                    {/* Allocation */}
                    <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                        <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                            <span className="w-6 h-6 rounded-full bg-white/5 flex items-center justify-center text-xs font-bold text-gray-400">{marketType === "forex" ? "5" : "4"}</span>
                            Capital Allocation
                        </h2>

                        <div className="flex flex-col md:flex-row gap-6">
                            <div className="flex-1">
                                <label className="text-sm text-gray-400 mb-2 block">Allocation Type</label>
                                <div className="flex bg-[#0F1218] p-1 rounded-lg border border-white/5">
                                    <button
                                        onClick={() => setAllocationType("fixed_amount")}
                                        className={`flex-1 py-2 text-sm font-medium rounded-md transition-all ${allocationType === "fixed_amount" ? "bg-white/10 text-white" : "text-gray-500 hover:text-gray-300"
                                            }`}
                                    >
                                        Fixed Amount ($)
                                    </button>
                                    <button
                                        onClick={() => setAllocationType("percent_balance")}
                                        className={`flex-1 py-2 text-sm font-medium rounded-md transition-all ${allocationType === "percent_balance" ? "bg-white/10 text-white" : "text-gray-500 hover:text-gray-300"
                                            }`}
                                    >
                                        % of Balance
                                    </button>
                                </div>
                            </div>

                            <div className="flex-1">
                                <label className="text-sm text-gray-400 mb-2 block">
                                    {allocationType === "fixed_amount" ? "Amount (USDT)" : "Percentage (%)"}
                                </label>
                                <div className="relative">
                                    <input
                                        type="number"
                                        value={allocationValue}
                                        onChange={(e) => {
                                            const val = parseFloat(e.target.value);
                                            if (!isNaN(val) && val > 0) {
                                                setAllocationValue(val);
                                            } else if (e.target.value === '') {
                                                setAllocationValue(0);
                                            }
                                        }}
                                        className="w-full bg-[#0F1218] border border-white/10 rounded-lg px-4 py-2 text-white focus:border-primary focus:outline-none transition-colors font-mono"
                                    />
                                    <div className="absolute right-4 top-2 text-gray-500 text-sm">
                                        {allocationType === "fixed_amount" ? "USDT" : "%"}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </section>
                </div>

                {/* Right Column: Deploy Card */}
                <div className="lg:col-span-1">
                    <div className="sticky top-24 bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-6">
                        <h3 className="font-bold text-white text-lg">Deployment Summary</h3>

                        <div className="space-y-4">
                            <div className="flex justify-between items-center py-2 border-b border-white/5">
                                <span className="text-gray-500 text-sm">Strategy</span>
                                <span className="text-white font-medium flex items-center gap-2">
                                    <Zap className="w-4 h-4 text-purple-500" />
                                    Master Ensemble
                                </span>
                            </div>

                            <div className="flex justify-between items-center py-2 border-b border-white/5">
                                <span className="text-gray-500 text-sm">Market</span>
                                <span className="text-white font-medium uppercase">{marketType}</span>
                            </div>

                            <div className="flex justify-between items-center py-2 border-b border-white/5">
                                <span className="text-gray-500 text-sm">Risk Mode</span>
                                <span className={`font-medium ${riskMode === 'conservative' ? 'text-blue-400' :
                                    riskMode === 'balanced' ? 'text-primary' : 'text-amber-400'
                                    } uppercase text-sm`}>{riskMode}</span>
                            </div>

                            <div className="flex justify-between items-center py-2 border-b border-white/5">
                                <span className="text-gray-500 text-sm">Allocation</span>
                                <span className="text-white font-medium">
                                    {allocationType === 'fixed_amount' ? `$${allocationValue}` : `${allocationValue}%`}
                                </span>
                            </div>

                            <div className="py-2">
                                <label className="text-sm text-gray-500 mb-2 block">Execution Mode</label>
                                <div className="flex bg-[#0F1218] p-1 rounded-lg border border-white/5">
                                    <button
                                        onClick={() => setMode("paper")}
                                        className={`flex-1 py-1.5 text-xs font-bold uppercase rounded-md transition-all ${mode === "paper" ? "bg-blue-500/20 text-blue-400 shadow-sm" : "text-gray-600 hover:text-gray-400"
                                            }`}
                                    >
                                        Paper
                                    </button>
                                    <button
                                        onClick={() => setMode("live")}
                                        className={`flex-1 py-1.5 text-xs font-bold uppercase rounded-md transition-all ${mode === "live" ? "bg-primary/20 text-primary shadow-sm" : "text-gray-600 hover:text-gray-400"
                                            }`}
                                    >
                                        Live
                                    </button>
                                </div>
                            </div>
                        </div>

                        <div className="pt-4">
                            <button
                                onClick={handleDeploy}
                                disabled={loading || !selectedBroker}
                                className={`w-full py-4 rounded-xl font-bold text-lg flex items-center justify-center gap-2 transition-all shadow-lg hover:shadow-primary/25 disabled:opacity-50 disabled:cursor-not-allowed ${mode === 'live'
                                    ? "bg-gradient-to-r from-primary to-purple-600 text-white"
                                    : "bg-blue-500/10 text-blue-400 border border-blue-500/20 hover:bg-blue-500/20"
                                    }`}
                            >
                                {loading ? (
                                    <span className="w-6 h-6 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                                ) : (
                                    <>
                                        <Play className="w-5 h-5 fill-current" />
                                        {mode === 'live' ? "Deploy Live Bot" : "Start Paper Trading"}
                                    </>
                                )}
                            </button>
                            <p className="text-center text-xs text-gray-500 mt-3">
                                {mode === 'live'
                                    ? "Real capital will be used. Trade responsibly."
                                    : "Zero risk simulation environment."}
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
