import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
    ChevronLeft, Save, Plus, Trash2, Settings, FileJson, Sparkles,
    Play, Activity, BarChart2, TrendingUp, AlertTriangle, Clock, Loader2
} from "lucide-react";
import { api } from "../api/client";
import { useMutation } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";

// --- Types ---
type InspectorType = "INDICATORS" | "ENTRY" | "EXIT" | "RISK";

// --- Mock Data for Indicators ---
const AVAILABLE_INDICATORS = [
    { id: "SMA", name: "Simple Moving Average", params: [{ name: "Period", default: 14 }, { name: "Source", default: "Close" }] },
    { id: "RSI", name: "Relative Strength Index", params: [{ name: "Period", default: 14 }] },
    { id: "MACD", name: "MACD", params: [{ name: "Fast", default: 12 }, { name: "Slow", default: 26 }, { name: "Signal", default: 9 }] },
    { id: "EMA", name: "Exponential Moving Average", params: [{ name: "Period", default: 20 }] },
    { id: "BB", name: "Bollinger Bands", params: [{ name: "Period", default: 20 }, { name: "StdDev", default: 2 }] },
];

export default function StrategyBuilder() {
    const navigate = useNavigate();
    const [activeTab, setActiveTab] = useState<InspectorType>("INDICATORS");
    const [isBacktesting, setIsBacktesting] = useState(false);
    const [showResults, setShowResults] = useState(false);

    // Strategy State
    const [meta, setMeta] = useState({ name: "Untitled Strategy", description: "", market: "crypto" });
    const [indicators, setIndicators] = useState<any[]>([
        { id: "ind_1", type: "SMA", params: { Period: 14, Source: "Close" } }
    ]);
    const [entryRules, setEntryRules] = useState<any[]>([
        { id: "rule_1", left: "Close", operator: ">", right: "SMA", value: "0" } // Price > SMA
    ]);
    const [exitRules, setExitRules] = useState<any[]>([]);
    const [risk, setRisk] = useState({ stopLoss: 2.0, takeProfit: 4.0, maxDrawdown: 10.0 });

    // --- Mutations ---
    const saveMutation = useMutation({
        mutationFn: api.createStrategyDraft,
        onSuccess: () => {
            navigate('/dashboard/strategies');
        },
        onError: (err) => {
            alert("Failed to save strategy: " + err);
        }
    });

    const handleBacktest = () => {
        setIsBacktesting(true);
        setShowResults(false);
        // Simulate calc
        setTimeout(() => {
            setIsBacktesting(false);
            setShowResults(true);
        }, 2000);
    };

    const handleSave = () => {
        const strategyData = {
            name: meta.name,
            description: meta.description,
            market_type: meta.market,
            visibility: 'private',
            spec_json: {
                indicators,
                entry_rules: entryRules,
                exit_rules: exitRules,
                risk_settings: risk
            }
        };
        saveMutation.mutate(strategyData);
    };

    return (
        <div className="container mx-auto max-w-[1400px] h-[calc(100vh-80px)] p-4 flex flex-col animate-in fade-in">
            {/* Header */}
            <header className="flex items-center justify-between mb-6 flex-none">
                <div className="flex items-center gap-4">
                    <Link to="/dashboard/strategies" className="p-2 hover:bg-white/5 rounded-lg transition-colors">
                        <ChevronLeft className="w-6 h-6" />
                    </Link>
                    <div>
                        <input
                            value={meta.name}
                            onChange={e => setMeta({ ...meta, name: e.target.value })}
                            className="bg-transparent text-2xl font-bold outline-none placeholder:text-muted-foreground w-64 md:w-96"
                            placeholder="Strategy Name"
                        />
                        <div className="flex items-center gap-2 text-sm text-muted-foreground">
                            <span className="bg-primary/10 text-primary px-2 py-0.5 rounded textxs font-bold uppercase">Builder Mode</span>
                        </div>
                    </div>
                </div>

                <div className="flex gap-3">
                    {/* Backtest Period Selector */}
                    <select className="bg-[#0F1218] border border-white/10 text-sm text-gray-300 rounded-xl px-4 outline-none focus:border-primary/50 transition-colors">
                        <option>Last 30 Days</option>
                        <option>Last 3 Months</option>
                        <option>Year to Date</option>
                        <option>Last 12 Months</option>
                    </select>

                    <button
                        onClick={handleBacktest}
                        disabled={isBacktesting}
                        className={`px-6 py-2.5 rounded-xl font-bold flex items-center gap-2 transition-all ${isBacktesting ? 'bg-muted text-muted-foreground cursor-wait' : 'bg-green-600 hover:bg-green-500 text-white shadow-lg shadow-green-900/20'
                            }`}
                    >
                        {isBacktesting ? (
                            <>Running...</>
                        ) : (
                            <><Play className="w-4 h-4 fill-current" /> Run Backtest</>
                        )}
                    </button>
                    <button
                        onClick={handleSave}
                        disabled={saveMutation.isPending}
                        className="px-6 py-2.5 bg-blue-600 hover:bg-blue-500 text-white rounded-xl font-bold flex items-center gap-2 shadow-lg shadow-blue-900/20 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {saveMutation.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                        {saveMutation.isPending ? 'Saving...' : 'Save Strategy'}
                    </button>
                </div>
            </header>

            {/* Main Workspace Grid */}
            <div className="flex-1 grid grid-cols-1 lg:grid-cols-12 gap-6 min-h-0">

                {/* LEFT COLUMN: Configuration Tabs */}
                <div className="lg:col-span-8 flex flex-col bg-[#0F1218] border border-white/5 rounded-2xl overflow-hidden shadow-xl">
                    {/* Tabs Navigation */}
                    <div className="flex border-b border-white/5 bg-[#0B0E14]">
                        {[
                            { id: "INDICATORS", label: "1. Indicators", icon: Activity },
                            { id: "ENTRY", label: "2. Entry Rules", icon: TrendingUp },
                            { id: "EXIT", label: "3. Exit Rules", icon: LogOutIcon }, // Custom Icon below
                            { id: "RISK", label: "4. Risk Mgmt", icon: AlertTriangle },
                        ].map(tab => (
                            <button
                                key={tab.id}
                                onClick={() => setActiveTab(tab.id as InspectorType)}
                                className={`flex-1 py-4 text-sm font-bold flex items-center justify-center gap-2 transition-colors relative ${activeTab === tab.id
                                    ? "text-primary bg-primary/5"
                                    : "text-muted-foreground hover:text-white hover:bg-white/5"
                                    }`}
                            >
                                <tab.icon className="w-4 h-4" />
                                {tab.label}
                                {activeTab === tab.id && (
                                    <motion.div layoutId="tab-indicator" className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary" />
                                )}
                            </button>
                        ))}
                    </div>

                    {/* Tab Content Area */}
                    <div className="flex-1 p-8 overflow-y-auto custom-scrollbar relative">
                        <div className="absolute inset-0 pointer-events-none bg-[radial-gradient(#ffffff05_1px,transparent_1px)] [background-size:16px_16px]" />

                        <div className="relative z-10 max-w-4xl mx-auto space-y-8">

                            {/* INDICATORS TAB */}
                            {activeTab === "INDICATORS" && (
                                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="space-y-6">
                                    <div className="flex justify-between items-center">
                                        <div>
                                            <h2 className="text-xl font-bold">Active Indicators</h2>
                                            <p className="text-muted-foreground">Define the tools your strategy uses to analyze the market.</p>
                                        </div>
                                        <button className="text-primary hover:text-primary/80 font-bold text-sm flex items-center gap-1">
                                            <Plus className="w-4 h-4" /> Add Indicator
                                        </button>
                                    </div>

                                    <div className="grid gap-4">
                                        {indicators.map((ind, i) => (
                                            <div key={ind.id} className="bg-white/5 border border-white/5 p-4 rounded-xl flex items-center justify-between group hover:border-white/10 transition-colors">
                                                <div className="flex items-center gap-4">
                                                    <div className="w-10 h-10 rounded-lg bg-blue-500/10 flex items-center justify-center text-blue-500 font-bold">
                                                        {ind.type.substring(0, 2)}
                                                    </div>
                                                    <div>
                                                        <div className="font-bold flex items-center gap-2">
                                                            {AVAILABLE_INDICATORS.find(ai => ai.id === ind.type)?.name || ind.type}
                                                            <span className="text-xs bg-white/10 px-1.5 py-0.5 rounded text-muted-foreground">Source: {ind.params.Source || 'Close'}</span>
                                                        </div>
                                                        <div className="text-xs text-muted-foreground flex gap-4 mt-1">
                                                            {Object.entries(ind.params).map(([key, val]) => (
                                                                <span key={key}>{key}: <span className="text-white">{val as string}</span></span>
                                                            ))}
                                                        </div>
                                                    </div>
                                                </div>
                                                <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                                                    <button className="p-2 hover:bg-white/10 rounded-lg"><Settings className="w-4 h-4" /></button>
                                                    <button onClick={() => setIndicators(prev => prev.filter(x => x.id !== ind.id))} className="p-2 hover:bg-red-500/10 text-red-500 rounded-lg"><Trash2 className="w-4 h-4" /></button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </motion.div>
                            )}

                            {/* ENTRY RULES TAB */}
                            {activeTab === "ENTRY" && (
                                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="space-y-6">
                                    <div className="flex justify-between items-center">
                                        <div>
                                            <h2 className="text-xl font-bold">Entry Conditions (Buy)</h2>
                                            <p className="text-muted-foreground">When all these conditions are met, a LONG position will be opened.</p>
                                        </div>
                                        <button className="text-green-500 hover:text-green-400 font-bold text-sm flex items-center gap-1">
                                            <Plus className="w-4 h-4" /> Add Condition
                                        </button>
                                    </div>

                                    <div className="space-y-4">
                                        {entryRules.map((rule, i) => (
                                            <div key={rule.id} className="bg-white/5 border border-white/5 p-4 rounded-xl flex items-center gap-4">
                                                <span className="text-xs font-bold text-muted-foreground uppercase w-8">IF</span>

                                                <div className="flex-1 bg-black/20 rounded-lg p-2 text-sm font-mono text-center border border-white/5 cursor-pointer hover:border-primary/50 transition-colors">
                                                    {rule.left}
                                                </div>

                                                <div className="w-12 h-8 flex items-center justify-center font-bold bg-white/5 rounded text-primary">
                                                    {rule.operator}
                                                </div>

                                                <div className="flex-1 bg-black/20 rounded-lg p-2 text-sm font-mono text-center border border-white/5 cursor-pointer hover:border-primary/50 transition-colors">
                                                    {rule.right}
                                                </div>

                                                <button onClick={() => setEntryRules(prev => prev.filter(r => r.id !== rule.id))} className="text-muted-foreground hover:text-red-500">
                                                    <XCircle className="w-5 h-5" />
                                                </button>
                                            </div>
                                        ))}
                                        {entryRules.length > 0 && (
                                            <div className="flex justify-center">
                                                <div className="bg-white/5 px-3 py-1 rounded-full text-xs font-bold text-muted-foreground">AND</div>
                                            </div>
                                        )}
                                        <div className="border-2 border-dashed border-white/10 rounded-xl p-4 flex items-center justify-center text-sm text-muted-foreground hover:border-white/20 hover:text-white cursor-pointer transition-all">
                                            + Add Another Condition
                                        </div>
                                    </div>
                                </motion.div>
                            )}

                            {/* EXIT RULES TAB */}
                            {activeTab === "EXIT" && (
                                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="space-y-6">
                                    <div className="flex justify-between items-center">
                                        <div>
                                            <h2 className="text-xl font-bold">Exit Conditions (Sell)</h2>
                                            <p className="text-muted-foreground">Trigger a sell when these conditions are met (in addition to Stop Loss/Take Profit).</p>
                                        </div>
                                        <button className="text-red-500 hover:text-red-400 font-bold text-sm flex items-center gap-1">
                                            <Plus className="w-4 h-4" /> Add Condition
                                        </button>
                                    </div>

                                    {exitRules.length === 0 ? (
                                        <div className="text-center py-12 border border-dashed border-white/10 rounded-xl bg-white/5">
                                            <p className="text-muted-foreground mb-4">No specific exit signal defined. Strategy will rely solely on Stop Loss / Take Profit.</p>
                                            <button onClick={() => setExitRules([...exitRules, { id: Date.now(), left: 'RSI', operator: '>', right: 'Value', value: '70' }])} className="text-primary font-bold hover:underline">
                                                Add RSI Overbought Exit
                                            </button>
                                        </div>
                                    ) : (
                                        <div className="space-y-4">
                                            {/* Similar map to Entry Rules */}
                                            {exitRules.map((rule, i) => (
                                                <div key={rule.id} className="bg-white/5 border border-white/5 p-4 rounded-xl flex items-center gap-4">
                                                    <span className="text-xs font-bold text-muted-foreground uppercase w-8">IF</span>
                                                    <div className="flex-1 bg-black/20 rounded-lg p-2 text-sm font-mono text-center">{rule.left}</div>
                                                    <div className="font-bold text-primary">{rule.operator}</div>
                                                    <div className="flex-1 bg-black/20 rounded-lg p-2 text-sm font-mono text-center">{rule.right}</div>
                                                    <button onClick={() => setExitRules(prev => prev.filter(r => r.id !== rule.id))}><XCircle className="w-5 h-5 text-muted-foreground" /></button>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </motion.div>
                            )}

                            {/* RISK TAB */}
                            {activeTab === "RISK" && (
                                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="space-y-6">
                                    <h2 className="text-xl font-bold">Risk Management</h2>

                                    <div className="grid grid-cols-2 gap-6">
                                        <div className="bg-white/5 border border-white/5 p-6 rounded-xl">
                                            <div className="flex justify-between mb-2">
                                                <label className="font-medium text-gray-300">Stop Loss</label>
                                                <span className="text-red-500 font-mono font-bold">{risk.stopLoss}%</span>
                                            </div>
                                            <input
                                                type="range" min="0.1" max="50" step="0.1"
                                                value={risk.stopLoss}
                                                onChange={e => setRisk({ ...risk, stopLoss: parseFloat(e.target.value) })}
                                                className="w-full h-2 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-red-500"
                                            />
                                            <p className="text-xs text-muted-foreground mt-2">Exits trade if price drops by this percentage.</p>
                                        </div>

                                        <div className="bg-white/5 border border-white/5 p-6 rounded-xl">
                                            <div className="flex justify-between mb-2">
                                                <label className="font-medium text-gray-300">Take Profit</label>
                                                <span className="text-green-500 font-mono font-bold">{risk.takeProfit}%</span>
                                            </div>
                                            <input
                                                type="range" min="0.5" max="100" step="0.5"
                                                value={risk.takeProfit}
                                                onChange={e => setRisk({ ...risk, takeProfit: parseFloat(e.target.value) })}
                                                className="w-full h-2 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-green-500"
                                            />
                                            <p className="text-xs text-muted-foreground mt-2">Secures profits if price rises by this percentage.</p>
                                        </div>
                                    </div>
                                </motion.div>
                            )}

                        </div>
                    </div>
                </div>

                {/* RIGHT COLUMN: Backtest Results / Preview */}
                <div className="lg:col-span-4 bg-[#0F1218] border border-white/5 rounded-2xl overflow-hidden flex flex-col shadow-xl">
                    <div className="p-4 border-b border-white/5 bg-[#0B0E14] flex justify-between items-center">
                        <h3 className="font-bold flex items-center gap-2">
                            <BarChart2 className="w-4 h-4 text-primary" /> Backtest Results
                        </h3>
                        <div className="text-xs text-muted-foreground">Last Run: {showResults ? 'Just now' : 'Never'}</div>
                    </div>

                    <div className="flex-1 relative">
                        {!showResults ? (
                            <div className="absolute inset-0 flex flex-col items-center justify-center p-8 text-center text-muted-foreground opacity-50">
                                <Activity className="w-16 h-16 mb-4 stroke-[1]" />
                                <p>Run a backtest to see how this strategy performs on historical data.</p>
                            </div>
                        ) : (
                            <div className="absolute inset-0 overflow-y-auto custom-scrollbar animate-in fade-in slide-in-from-bottom-4">
                                {/* Equity Curve Placeholder */}
                                <div className="h-48 bg-gradient-to-b from-green-500/10 to-transparent relative border-b border-white/5">
                                    <div className="absolute bottom-4 left-4 right-4 top-4">
                                        {/* Mock SVG Graph */}
                                        <svg viewBox="0 0 100 50" className="w-full h-full stroke-green-500 fill-none stroke-2 overflow-visible">
                                            <path d="M0,50 Q10,40 20,45 T40,30 T60,35 T80,10 T100,5" vectorEffect="non-scaling-stroke" />
                                        </svg>
                                    </div>
                                    <div className="absolute top-2 right-4 text-xs font-mono text-green-400">+124.5%</div>
                                </div>

                                {/* Metrics */}
                                <div className="p-6 space-y-6">
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="bg-white/5 p-3 rounded-lg">
                                            <div className="text-xs text-muted-foreground mb-1">Win Rate</div>
                                            <div className="text-xl font-bold text-white">65.4%</div>
                                        </div>
                                        <div className="bg-white/5 p-3 rounded-lg">
                                            <div className="text-xs text-muted-foreground mb-1">Profit Factor</div>
                                            <div className="text-xl font-bold text-green-400">1.85</div>
                                        </div>
                                        <div className="bg-white/5 p-3 rounded-lg">
                                            <div className="text-xs text-muted-foreground mb-1">Max Drawdown</div>
                                            <div className="text-xl font-bold text-red-500">-12.3%</div>
                                        </div>
                                        <div className="bg-white/5 p-3 rounded-lg">
                                            <div className="text-xs text-muted-foreground mb-1">Total Trades</div>
                                            <div className="text-xl font-bold text-white">342</div>
                                        </div>
                                    </div>

                                    {/* Trade List Mock */}
                                    <div>
                                        <h4 className="font-bold text-sm mb-3">Recent Signals</h4>
                                        <div className="space-y-2">
                                            {[1, 2, 3, 4, 5].map(i => (
                                                <div key={i} className="flex justify-between items-center text-xs p-2 hover:bg-white/5 rounded transition-colors">
                                                    <div className="flex items-center gap-2">
                                                        <span className={`w-1.5 h-1.5 rounded-full ${i % 3 === 0 ? 'bg-red-500' : 'bg-green-500'}`} />
                                                        <span className="font-mono text-gray-400">2023-11-{10 + i}</span>
                                                    </div>
                                                    <span className={`font-bold ${i % 3 === 0 ? 'text-red-500' : 'text-green-500'}`}>
                                                        {i % 3 === 0 ? 'SELL' : 'BUY'}
                                                    </span>
                                                    <span className="font-mono text-white text-right w-16">${40000 + (i * 100)}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

// Icon Helper
function LogOutIcon(props: any) {
    return (
        <svg  {...props} xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" /><polyline points="16 17 21 12 16 7" /><line x1="21" x2="9" y1="12" y2="12" /></svg>
    )
}

function XCircle(props: any) {
    return <svg {...props} xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><path d="m15 9-6 6" /><path d="m9 9 6 6" /></svg>
}
