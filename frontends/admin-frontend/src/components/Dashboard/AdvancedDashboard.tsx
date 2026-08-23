import { Link } from "react-router-dom";
import {
    TrendingUp, TrendingDown, Activity, Zap, DollarSign, BarChart3,
    Play, Pause, Settings, Plus, ArrowUpRight, ArrowDownRight,
    Bot, Wallet, Target, AlertCircle, Eye, EyeOff, RefreshCw, Sparkles, Brain,
    MoreHorizontal, Calendar, Filter, Download
} from "lucide-react";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { motion } from "framer-motion";

// Mock data to match the visual density of the screenshots
const mockActiveBots = [
    { id: "bot-1", name: "Momentum Rider", pair: "BTC/USDT", strategy: "Trend", status: "active", pnl: 2457.83, pnlPercent: 12.4, daily: 1.2 },
    { id: "bot-2", name: "Range Master", pair: "ETH/USDT", strategy: "Mean Rev", status: "active", pnl: -342.12, pnlPercent: -1.8, daily: -0.4 },
    { id: "bot-3", name: "Alpha Scalper", pair: "SOL/USD", strategy: "Scalp", status: "active", pnl: 892.45, pnlPercent: 5.1, daily: 3.2 },
];

const mockTrades = [
    { id: 1, pair: "BTC/USDT", side: "BUY", price: 42150.00, amount: 0.5, total: 21075.00, time: "10:42 AM", status: "market", pnl: null },
    { id: 2, pair: "ETH/USDT", side: "SELL", price: 2240.50, amount: 10.0, total: 22405.00, time: "09:15 AM", status: "limit", pnl: 450.20 },
    { id: 3, pair: "SOL/USD", side: "BUY", price: 98.50, amount: 100.0, total: 9850.00, time: "08:30 AM", status: "market", pnl: null },
    { id: 4, pair: "BTC/USDT", side: "SELL", price: 42890.00, amount: 0.5, total: 21445.00, time: "Yesterday", status: "limit", pnl: 370.00 },
    { id: 5, pair: "AVAX/USDT", side: "BUY", price: 35.20, amount: 200.0, total: 7040.00, time: "Yesterday", status: "market", pnl: null },
];

export function AdvancedDashboard() {
    const [hideBalances, setHideBalances] = useState(false);

    // --- Dark Theme Mock Data ---
    const totalEquity = 124532.87;
    const dayChange = 2354.12;
    const dayChangePercent = 2.45;

    return (
        <div className="space-y-6 animate-in fade-in duration-500 text-foreground">
            {/* Header Bar */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 py-2">
                <div>
                    <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
                        <LayoutDashboardIcon className="w-6 h-6 text-primary" />
                        Dashboard
                    </h1>
                    <p className="text-sm text-muted-foreground">Overview of your automated portfolio</p>
                </div>

                <div className="flex items-center gap-3 bg-card/50 backdrop-blur-md p-1.5 rounded-xl border border-border/50">
                    <button className="px-4 py-2 text-xs font-semibold bg-primary/10 text-primary rounded-lg border border-primary/20 hover:bg-primary/20 transition-colors">
                        Live
                    </button>
                    <div className="h-4 w-px bg-border mx-1" />
                    <button className="flex items-center gap-2 px-3 py-2 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors">
                        <Calendar className="w-3.5 h-3.5" /> Today
                    </button>
                    <button className="p-2 hover:bg-muted rounded-lg transition-colors text-muted-foreground hover:text-foreground">
                        <Filter className="w-4 h-4" />
                    </button>
                </div>
            </div>

            {/* Top Metrics Row */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {/* Main Balance Card */}
                <div className="col-span-1 md:col-span-2 bg-[#0F1218] border border-white/5 rounded-2xl p-6 relative overflow-hidden group shadow-xl shadow-black/20">
                    <div className="absolute top-0 right-0 p-6 opacity-30">
                        <div className="w-24 h-24 bg-primary/20 rounded-full blur-3xl -mr-10 -mt-10" />
                    </div>
                    <div className="relative z-10 flex flex-col justify-between h-full">
                        <div className="flex justify-between items-start mb-6">
                            <div>
                                <h3 className="text-sm font-medium text-muted-foreground mb-1">Total Portfolio Value</h3>
                                <div className="text-4xl font-mono font-bold text-white tracking-tight flex items-baseline gap-2">
                                    {hideBalances ? "•••••" : `$${totalEquity.toLocaleString()}`}
                                    <span className="text-lg font-sans font-semibold px-2 py-0.5 rounded-full bg-green-500/10 text-green-500 border border-green-500/20">
                                        +{dayChangePercent}%
                                    </span>
                                </div>
                            </div>
                            <button onClick={() => setHideBalances(!hideBalances)} className="text-muted-foreground hover:text-white transition-colors">
                                {hideBalances ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                            </button>
                        </div>

                        <div className="grid grid-cols-3 gap-8 pt-4 border-t border-white/5">
                            <div>
                                <div className="text-xs text-muted-foreground mb-1">Daily P&L</div>
                                <div className="font-mono font-semibold text-green-500">+$2,453.12</div>
                            </div>
                            <div>
                                <div className="text-xs text-muted-foreground mb-1">Invested</div>
                                <div className="font-mono font-semibold text-white">$84,320.00</div>
                            </div>
                            <div>
                                <div className="text-xs text-muted-foreground mb-1">Cash</div>
                                <div className="font-mono font-semibold text-white">$40,212.87</div>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Bot Performance Summary */}
                <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 flex flex-col justify-between hover:border-white/10 transition-colors shadow-lg">
                    <div className="flex justify-between items-center mb-4">
                        <span className="text-sm font-medium text-muted-foreground">Active Strategies</span>
                        <Activity className="w-5 h-5 text-purple-500" />
                    </div>
                    <div className="flex items-end gap-2 mb-2">
                        <div className="text-3xl font-bold text-white">3</div>
                        <span className="text-sm text-muted-foreground mb-1">running</span>
                    </div>
                    <div className="w-full bg-muted/20 rounded-full h-1.5 overflow-hidden">
                        <div className="bg-purple-500 h-full w-3/4 animate-pulse" />
                    </div>
                    <div className="mt-4 flex gap-2">
                        <div className="flex-1 bg-white/5 rounded-lg p-2 text-center">
                            <div className="text-xs text-muted-foreground">Win Rate</div>
                            <div className="text-sm font-bold text-white">68%</div>
                        </div>
                        <div className="flex-1 bg-white/5 rounded-lg p-2 text-center">
                            <div className="text-xs text-muted-foreground">Trades</div>
                            <div className="text-sm font-bold text-white">142</div>
                        </div>
                    </div>
                </div>

                {/* Market Compass / AI Insight */}
                <div className="bg-gradient-to-br from-indigo-500/10 to-purple-500/10 border border-indigo-500/20 rounded-2xl p-6 flex flex-col justify-between hover:border-indigo-500/40 transition-colors shadow-lg relative overflow-hidden">
                    <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(to_bottom,transparent,black)]" />
                    <div className="relative z-10">
                        <div className="flex items-center gap-2 mb-3">
                            <div className="p-1.5 bg-indigo-500/20 rounded-lg">
                                <Sparkles className="w-4 h-4 text-indigo-400" />
                            </div>
                            <span className="text-sm font-bold text-indigo-300">AI Market Insights</span>
                        </div>
                        <div className="text-lg font-semibold text-white mb-1">
                            High Volatility Detected
                        </div>
                        <p className="text-xs text-indigo-200/60 leading-relaxed mb-4">
                            BTC/USDT volatility &gt; 4%. "Momentum Rider" bot functionality optimized for current conditions.
                        </p>
                        <button className="text-xs font-bold text-indigo-400 flex items-center gap-1 hover:gap-2 transition-all">
                            View Analysis <ArrowUpRight className="w-3 h-3" />
                        </button>
                    </div>
                </div>
            </div>

            {/* Charts Section */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Main Equity Chart */}
                <div className="lg:col-span-2 bg-[#0F1218] border border-white/5 rounded-2xl p-6 shadow-xl">
                    <div className="flex justify-between items-center mb-6">
                        <div>
                            <h2 className="text-lg font-bold text-white">Performance Overview</h2>
                            <p className="text-xs text-muted-foreground">Net Liquidation Value (7D)</p>
                        </div>
                        <div className="flex bg-white/5 rounded-lg p-1">
                            {['1H', '24H', '7D', '30D', 'ALL'].map(tf => (
                                <button key={tf} className={`px-3 py-1 rounded text-xs font-medium transition-all ${tf === '7D' ? 'bg-white/10 text-white' : 'text-muted-foreground hover:text-white'}`}>
                                    {tf}
                                </button>
                            ))}
                        </div>
                    </div>

                    {/* CSS Chart Representation */}
                    <div className="relative h-64 w-full">
                        <div className="absolute inset-0 flex flex-col justify-between text-xs text-muted-foreground opacity-20">
                            {[125000, 122000, 119000, 116000, 113000].map(val => (
                                <div key={val} className="w-full border-t border-white/20 pt-1">${val.toLocaleString()}</div>
                            ))}
                        </div>
                        {/* The Line */}
                        <svg className="absolute inset-0 w-full h-full overflow-visible" preserveAspectRatio="none">
                            <defs>
                                <linearGradient id="chartGradient" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor="#3B82F6" stopOpacity="0.2" />
                                    <stop offset="100%" stopColor="#3B82F6" stopOpacity="0" />
                                </linearGradient>
                            </defs>
                            <path d="M0,180 C100,170 200,190 300,150 C400,110 500,130 600,80 C700,30 800,60 900,40"
                                fill="none"
                                stroke="#3B82F6"
                                strokeWidth="3"
                                filter="drop-shadow(0 4px 6px rgba(59,130,246,0.3))"
                            />
                            <path d="M0,180 C100,170 200,190 300,150 C400,110 500,130 600,80 C700,30 800,60 900,40 V256 H0 Z"
                                fill="url(#chartGradient)"
                                stroke="none"
                            />
                            {/* Hover dot mock */}
                            <circle cx="600" cy="80" r="4" fill="#60A5FA" stroke="white" strokeWidth="2" />
                            <circle cx="600" cy="80" r="12" fill="#60A5FA" opacity="0.2" className="animate-pulse" />
                        </svg>

                        {/* Hover Tooltip Mock */}
                        <div className="absolute top-[20%] right-[30%] bg-[#1A1F2C] border border-white/10 p-3 rounded-xl shadow-2xl backdrop-blur-md">
                            <div className="text-xs text-muted-foreground mb-1">Apr 14, 2024</div>
                            <div className="text-sm font-bold text-white">$123,450.20</div>
                            <div className="text-xs text-green-500 font-medium">+2.4%</div>
                        </div>
                    </div>
                </div>

                {/* Asset Allocation Donut */}
                <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 shadow-xl flex flex-col">
                    <div className="flex justify-between items-center mb-6">
                        <h2 className="text-lg font-bold text-white">Allocation</h2>
                        <button className="p-1 hover:bg-white/5 rounded"><MoreHorizontal className="w-4 h-4 text-muted-foreground" /></button>
                    </div>

                    <div className="flex-1 flex items-center justify-center relative">
                        {/* SVG Donut */}
                        <div className="relative w-48 h-48">
                            <svg viewBox="0 0 100 100" className="transform -rotate-90 w-full h-full">
                                <circle cx="50" cy="50" r="40" fill="none" stroke="#1F2937" strokeWidth="12" />
                                {/* Segments */}
                                <circle cx="50" cy="50" r="40" fill="none" stroke="#10B981" strokeWidth="12" strokeDasharray="60 251" />
                                <circle cx="50" cy="50" r="40" fill="none" stroke="#F59E0B" strokeWidth="12" strokeDasharray="40 251" strokeDashoffset="-60" />
                                <circle cx="50" cy="50" r="40" fill="none" stroke="#3B82F6" strokeWidth="12" strokeDasharray="100 251" strokeDashoffset="-100" />
                            </svg>
                            <div className="absolute inset-0 flex flex-col items-center justify-center">
                                <span className="text-2xl font-bold text-white">4</span>
                                <span className="text-xs text-muted-foreground uppercase tracking-wider">Assets</span>
                            </div>
                        </div>
                    </div>

                    <div className="mt-6 space-y-3">
                        <div className="flex justify-between items-center text-sm">
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 rounded-full bg-blue-500" />
                                <span className="text-gray-300">Bitcoin</span>
                            </div>
                            <span className="font-mono text-white">45%</span>
                        </div>
                        <div className="flex justify-between items-center text-sm">
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 rounded-full bg-green-500" />
                                <span className="text-gray-300">USDT</span>
                            </div>
                            <span className="font-mono text-white">35%</span>
                        </div>
                        <div className="flex justify-between items-center text-sm">
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 rounded-full bg-amber-500" />
                                <span className="text-gray-300">Solana</span>
                            </div>
                            <span className="font-mono text-white">20%</span>
                        </div>
                    </div>
                </div>
            </div>

            {/* Bottom Section - Tables */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Active Strategies Table */}
                <div className="lg:col-span-2 bg-[#0F1218] border border-white/5 rounded-2xl overflow-hidden shadow-xl">
                    <div className="p-6 border-b border-white/5 flex justify-between items-center">
                        <h2 className="text-lg font-bold text-white">Open Positions</h2>
                        <button className="text-xs text-primary hover:text-primary/80 transition-colors">View All</button>
                    </div>
                    <div className="overflow-x-auto">
                        <table className="w-full">
                            <thead className="bg-white/5 text-xs text-muted-foreground uppercase tracking-wider font-semibold">
                                <tr>
                                    <th className="px-6 py-4 text-left">Pair</th>
                                    <th className="px-6 py-4 text-left">Strategy</th>
                                    <th className="px-6 py-4 text-right">Entry Price</th>
                                    <th className="px-6 py-4 text-right">Size</th>
                                    <th className="px-6 py-4 text-right">P&L</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-white/5">
                                {mockActiveBots.map((bot) => (
                                    <tr key={bot.id} className="hover:bg-white/5 transition-colors group">
                                        <td className="px-6 py-4">
                                            <div className="flex items-center gap-3">
                                                <div className="w-8 h-8 rounded-full bg-white/10 flex items-center justify-center font-bold text-[10px] text-white">
                                                    {bot.pair.split('/')[0].substring(0, 3)}
                                                </div>
                                                <div>
                                                    <div className="font-bold text-white text-sm">{bot.pair}</div>
                                                    <div className="text-[10px] text-green-500">Long</div>
                                                </div>
                                            </div>
                                        </td>
                                        <td className="px-6 py-4">
                                            <div className="text-sm text-gray-300">{bot.name}</div>
                                            <div className="text-[10px] text-muted-foreground">{bot.strategy}</div>
                                        </td>
                                        <td className="px-6 py-4 text-right font-mono text-sm text-white">$41,200.50</td>
                                        <td className="px-6 py-4 text-right font-mono text-sm text-white">0.5 BTC</td>
                                        <td className="px-6 py-4 text-right">
                                            <div className={`font-mono text-sm font-bold ${bot.pnl >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                                {bot.pnl >= 0 ? '+' : ''}${Math.abs(bot.pnl).toLocaleString()}
                                            </div>
                                            <div className={`text-[10px] ${bot.pnlPercent >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                                {bot.pnlPercent >= 0 ? '+' : ''}{bot.pnlPercent}%
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>

                {/* Recent Activity List */}
                <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6 shadow-xl">
                    <h2 className="text-lg font-bold text-white mb-6">Recent Activity</h2>
                    <div className="space-y-6">
                        {mockTrades.map((trade, i) => (
                            <div key={i} className="flex gap-4 relative">
                                {i !== mockTrades.length - 1 && <div className="absolute top-8 left-[14px] w-px h-full bg-white/5" />}
                                <div className={`w-7 h-7 rounded-full flex items-center justify-center z-10 ${trade.side === 'BUY' ? 'bg-green-500/10 text-green-500' : 'bg-red-500/10 text-red-500'}`}>
                                    {trade.side === 'BUY' ? <ArrowUpRight className="w-4 h-4" /> : <ArrowDownRight className="w-4 h-4" />}
                                </div>
                                <div className="flex-1">
                                    <div className="flex justify-between items-start mb-1">
                                        <span className="text-sm font-bold text-white">{trade.side} {trade.pair}</span>
                                        <span className="text-xs text-muted-foreground">{trade.time}</span>
                                    </div>
                                    <div className="flex justify-between items-center text-xs">
                                        <span className="text-gray-400">
                                            {trade.amount} @ ${trade.price.toLocaleString()}
                                        </span>
                                        <span className="font-mono text-white/50">${trade.total.toLocaleString()}</span>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                    <button className="w-full mt-6 py-2 text-sm text-muted-foreground border border-white/10 rounded-lg hover:bg-white/5 hover:text-white transition-colors">
                        View All History
                    </button>
                </div>
            </div>
        </div>
    );
}

// Icon helper since I can't import layout-dashboard easily if it's not exported or if I want a specific one
function LayoutDashboardIcon(props: any) {
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
            <rect width="7" height="9" x="3" y="3" rx="1" />
            <rect width="7" height="5" x="14" y="3" rx="1" />
            <rect width="7" height="9" x="14" y="12" rx="1" />
            <rect width="7" height="5" x="3" y="16" rx="1" />
        </svg>
    )
}
