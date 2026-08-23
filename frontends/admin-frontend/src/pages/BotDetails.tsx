import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import {
    ChevronLeft, Settings, Play, Pause, Power, Activity,
    ArrowUpRight, ArrowDownRight, Clock, FileText, Terminal,
    Maximize2
} from "lucide-react";
import { motion } from "framer-motion";

export default function BotDetails() {
    const { id } = useParams();
    const [activeTab, setActiveTab] = useState("overview");

    // Mock Data
    const bot = {
        id, name: "Alpha Trend Follower", status: "running", pair: "BTC/USDT",
        pnl: 1245.50, pnlPercent: 5.2, uptime: "4d 12h", risk: "Medium"
    };

    const logs = [
        { time: "14:32:01", type: "info", msg: "Scanning market for entry signals..." },
        { time: "14:30:45", type: "success", msg: "Order filled: BUY 0.1 BTC @ 42,100" },
        { time: "14:30:44", type: "info", msg: "Placing limit buy order..." },
        { time: "14:15:00", type: "info", msg: "EMA Cross detected. Validating volume..." },
    ];

    return (
        <div className="container mx-auto max-w-[1600px] space-y-6 animate-in fade-in">
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div className="flex items-center gap-4">
                    <Link to="/dashboard/bots" className="p-2 hover:bg-white/5 rounded-lg transition-colors">
                        <ChevronLeft className="w-6 h-6" />
                    </Link>
                    <div>
                        <div className="flex items-center gap-3">
                            <h1 className="text-3xl font-bold">{bot.name}</h1>
                            <span className="bg-green-500/10 text-green-500 border border-green-500/20 px-2 py-0.5 rounded text-xs uppercase font-bold tracking-wider flex items-center gap-1">
                                <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse" /> Running
                            </span>
                        </div>
                        <div className="flex items-center gap-4 text-sm text-muted-foreground mt-1">
                            <span className="font-mono text-white">{bot.pair}</span>
                            <span>•</span>
                            <span>Started {bot.uptime} ago</span>
                        </div>
                    </div>
                </div>

                <div className="flex items-center gap-3">
                    <button className="px-4 py-2 border border-border rounded-lg hover:bg-white/5 transition-colors flex items-center gap-2">
                        <Pause className="w-4 h-4 text-amber-500" /> Pause
                    </button>
                    <Link to={`/dashboard/bots/${id}/edit`} className="px-4 py-2 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-colors flex items-center gap-2">
                        <Settings className="w-4 h-4" /> Configure
                    </Link>
                </div>
            </div>

            {/* Main Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[calc(100vh-200px)] min-h-[600px]">

                {/* Left Col: Chart & Positions */}
                <div className="lg:col-span-2 flex flex-col gap-6">
                    {/* Real-time Chart */}
                    <div className="flex-1 bg-card border border-border rounded-xl p-4 flex flex-col relative overflow-hidden group">
                        <div className="flex justify-between items-center mb-4">
                            <h3 className="font-bold flex items-center gap-2">
                                <Activity className="w-4 h-4 text-primary" /> Live Market Feed
                            </h3>
                            <div className="flex gap-2">
                                <span className="text-sm font-mono text-green-500 font-bold">42,150.00</span>
                                <span className="text-xs text-muted-foreground self-end">BTC/USDT</span>
                            </div>
                        </div>

                        {/* Mock Chart Visualization */}
                        <div className="flex-1 bg-[#0B0E14] border border-white/5 rounded-lg relative overflow-hidden">
                            {/* Grid Lines */}
                            <div className="absolute inset-0 bg-[linear-gradient(rgba(255,255,255,0.02)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.02)_1px,transparent_1px)] bg-[size:40px_40px]" />

                            {/* Candles (Static Mock) */}
                            <div className="absolute inset-0 flex items-center justify-center">
                                <svg viewBox="0 0 800 400" className="w-full h-full p-4">
                                    {/* Mock Price Line */}
                                    <path d="M0,300 L50,280 L100,290 L150,250 L200,260 L250,200 L300,210 L350,150 L400,160 L450,120 L500,130 L550,100 L600,110 L650,80 L700,90 L750,50 L800,60"
                                        fill="none" stroke="#22c55e" strokeWidth="2" />
                                    {/* Trade Marker */}
                                    <circle cx="350" cy="150" r="4" fill="#22c55e" />
                                    <text x="350" y="140" fill="#22c55e" fontSize="12" textAnchor="middle">BUY</text>
                                </svg>
                            </div>
                        </div>
                    </div>

                    {/* Open Positions */}
                    <div className="h-64 bg-card border border-border rounded-xl p-4 flex flex-col">
                        <h3 className="font-bold mb-4">Open Positions</h3>
                        <div className="flex-1 overflow-auto">
                            <table className="w-full text-sm text-left">
                                <thead className="text-xs text-muted-foreground uppercase bg-muted/50 sticky top-0">
                                    <tr>
                                        <th className="px-4 py-2 rounded-l-lg">Symbol</th>
                                        <th className="px-4 py-2">Side</th>
                                        <th className="px-4 py-2">Entry Price</th>
                                        <th className="px-4 py-2">Size</th>
                                        <th className="px-4 py-2">Unrealized P&L</th>
                                        <th className="px-4 py-2 rounded-r-lg">Action</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-white/5">
                                    <tr className="hover:bg-white/5 transition-colors">
                                        <td className="px-4 py-3 font-bold">BTC/USDT</td>
                                        <td className="px-4 py-3 text-green-500">LONG</td>
                                        <td className="px-4 py-3 font-mono">42,100.00</td>
                                        <td className="px-4 py-3">0.10 BTC</td>
                                        <td className="px-4 py-3 text-green-500 font-bold">+$50.00 (1.2%)</td>
                                        <td className="px-4 py-3">
                                            <button className="text-xs bg-red-500/10 text-red-500 border border-red-500/20 px-2 py-1 rounded hover:bg-red-500 hover:text-white transition-all">
                                                Close
                                            </button>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                {/* Right Col: Stats & Logs */}
                <div className="flex flex-col gap-6">
                    {/* Performance Card */}
                    <div className="bg-card border border-border rounded-xl p-6">
                        <h3 className="font-bold mb-4 text-muted-foreground uppercase text-xs tracking-wider">Session Performance</h3>
                        <div className="text-4xl font-mono font-bold text-green-500 mb-1">+$1,245.50</div>
                        <div className="items-center flex gap-2 text-sm text-green-400 mb-6">
                            <ArrowUpRight className="w-4 h-4" /> +5.2% today
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            <div className="bg-black/20 rounded-lg p-3">
                                <div className="text-xs text-muted-foreground mb-1">Total Trades</div>
                                <div className="text-xl font-bold">12</div>
                            </div>
                            <div className="bg-black/20 rounded-lg p-3">
                                <div className="text-xs text-muted-foreground mb-1">Win Rate</div>
                                <div className="text-xl font-bold text-blue-400">75%</div>
                            </div>
                        </div>
                    </div>

                    {/* Execution Logs */}
                    <div className="flex-1 bg-[#050505] border border-white/10 rounded-xl p-4 flex flex-col font-mono text-xs overflow-hidden shadow-inner">
                        <div className="flex justify-between items-center mb-3 pb-2 border-b border-white/10">
                            <h3 className="font-bold text-gray-400 flex items-center gap-2">
                                <Terminal className="w-3 h-3" /> Live Logs
                            </h3>
                            <div className="flex gap-2">
                                <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                                <span className="text-[10px] text-gray-500">Connected</span>
                            </div>
                        </div>

                        <div className="flex-1 overflow-y-auto space-y-2 custom-scrollbar pr-2">
                            {logs.map((log, i) => (
                                <div key={i} className="flex gap-3 text-gray-300">
                                    <span className="text-gray-600 shrink-0">[{log.time}]</span>
                                    <span className={`${log.type === 'info' ? 'text-blue-400' :
                                            log.type === 'success' ? 'text-green-400' : 'text-gray-300'
                                        }`}>
                                        {log.msg}
                                    </span>
                                </div>
                            ))}
                            {/* Fading effect at bottom to show history */}
                        </div>
                    </div>
                </div>

            </div>
        </div>
    );
}
