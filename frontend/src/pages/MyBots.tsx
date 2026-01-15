import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
    Play, Pause, Settings, Trash2, Plus,
    TrendingUp, Activity, AlertTriangle, MoreVertical
} from "lucide-react";
import { motion } from "framer-motion";

// Mock Data
const MOCK_BOTS = [
    {
        id: "bot-1",
        name: "BTC Momentum Alpha",
        strategy: "Trend Following",
        pair: "BTC/USDT",
        status: "running",
        pnl: 1250.40,
        pnlPercent: 12.5,
        uptime: "14d 2h",
        risk: "Medium"
    },
    {
        id: "bot-2",
        name: "ETH Mean Reversion",
        strategy: "Mean Reversion",
        pair: "ETH/USDT",
        status: "paused",
        pnl: -45.20,
        pnlPercent: -1.2,
        uptime: "5d 8h",
        risk: "Low"
    },
    {
        id: "bot-3",
        name: "SOL Scalper",
        strategy: "Scalping",
        pair: "SOL/USD",
        status: "stopped",
        pnl: 890.00,
        pnlPercent: 8.9,
        uptime: "2d 4h",
        risk: "High"
    }
];

export default function MyBots() {
    const navigate = useNavigate();
    const [bots, setBots] = useState(MOCK_BOTS);
    const [filter, setFilter] = useState("all");

    const handleStatusToggle = (id: string, currentStatus: string) => {
        setBots(prev => prev.map(bot => {
            if (bot.id === id) {
                return {
                    ...bot,
                    status: currentStatus === "running" ? "paused" : "running"
                };
            }
            return bot;
        }));
    };

    const handleDelete = (id: string) => {
        if (confirm("Are you sure you want to delete this bot?")) {
            setBots(prev => prev.filter(b => b.id !== id));
        }
    };

    return (
        <div className="space-y-6 text-foreground animate-in fade-in duration-500">
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">My Bots</h1>
                    <p className="text-muted-foreground">Manage and monitor your active trading instances.</p>
                </div>
                <Link
                    to="/dashboard/strategies"
                    className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/20"
                >
                    <Plus className="w-5 h-5" /> Create New Bot
                </Link>
            </div>

            {/* Stats Overview */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-[#0F1218] border border-white/5 p-6 rounded-2xl flex items-center justify-between">
                    <div>
                        <div className="text-muted-foreground text-sm font-medium mb-1">Total Active P&L</div>
                        <div className="text-2xl font-bold text-green-500">+$2,095.20</div>
                    </div>
                    <div className="w-10 h-10 rounded-full bg-green-500/10 flex items-center justify-center text-green-500">
                        <TrendingUp className="w-6 h-6" />
                    </div>
                </div>
                <div className="bg-[#0F1218] border border-white/5 p-6 rounded-2xl flex items-center justify-between">
                    <div>
                        <div className="text-muted-foreground text-sm font-medium mb-1">Active Bots</div>
                        <div className="text-2xl font-bold text-white">{bots.filter(b => b.status === 'running').length} / {bots.length}</div>
                    </div>
                    <div className="w-10 h-10 rounded-full bg-blue-500/10 flex items-center justify-center text-blue-500">
                        <Activity className="w-6 h-6" />
                    </div>
                </div>
                <div className="bg-[#0F1218] border border-white/5 p-6 rounded-2xl flex items-center justify-between">
                    <div>
                        <div className="text-muted-foreground text-sm font-medium mb-1">Risk Exposure</div>
                        <div className="text-2xl font-bold text-amber-500">Medium</div>
                    </div>
                    <div className="w-10 h-10 rounded-full bg-amber-500/10 flex items-center justify-center text-amber-500">
                        <AlertTriangle className="w-6 h-6" />
                    </div>
                </div>
            </div>

            {/* Bot List */}
            <div className="bg-[#0F1218] border border-white/5 rounded-2xl overflow-hidden shadow-xl">
                <div className="p-4 border-b border-white/5 flex gap-2">
                    {['all', 'running', 'paused', 'stopped'].map(status => (
                        <button
                            key={status}
                            onClick={() => setFilter(status)}
                            className={`px-3 py-1.5 rounded-lg text-xs font-bold uppercase tracking-wide transition-colors ${filter === status
                                ? 'bg-white/10 text-white'
                                : 'text-muted-foreground hover:bg-white/5'
                                }`}
                        >
                            {status}
                        </button>
                    ))}
                </div>

                <div className="divide-y divide-white/5">
                    {bots.filter(b => filter === 'all' || b.status === filter).map((bot) => (
                        <div key={bot.id} className="p-6 flex flex-col md:flex-row items-center justify-between gap-6 hover:bg-white/[0.02] transition-colors group">

                            {/* Bot Info */}
                            <div className="flex-1 min-w-0 flex items-center gap-4 w-full md:w-auto">
                                <Link to={`/dashboard/bots/${bot.id}`} className="flex items-center gap-4 group-hover:opacity-80 transition-opacity">
                                    <div className={`w-3 h-3 rounded-full flex-shrink-0 ${bot.status === 'running' ? 'bg-green-500 shadow-[0_0_10px_rgba(34,197,94,0.5)]' :
                                        bot.status === 'paused' ? 'bg-amber-500' : 'bg-red-500'
                                        }`} />
                                    <div>
                                        <h3 className="font-bold text-white text-lg truncate group-hover:text-primary transition-colors">{bot.name}</h3>
                                        <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                            <span className="px-1.5 py-0.5 rounded bg-white/5 font-mono text-xs text-white">{bot.pair}</span>
                                            <span>•</span>
                                            <span>{bot.strategy}</span>
                                        </div>
                                    </div>
                                </Link>
                            </div>

                            {/* Metrics */}
                            <div className="grid grid-cols-3 gap-8 w-full md:w-auto text-center md:text-left">
                                <div>
                                    <div className="text-xs text-muted-foreground mb-1">P&L</div>
                                    <div className={`font-mono font-bold ${bot.pnl >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                        {bot.pnl >= 0 ? '+' : ''}${Math.abs(bot.pnl).toLocaleString()}
                                    </div>
                                </div>
                                <div>
                                    <div className="text-xs text-muted-foreground mb-1">ROI</div>
                                    <div className={`font-mono font-bold ${bot.pnlPercent >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                        {bot.pnlPercent}%
                                    </div>
                                </div>
                                <div>
                                    <div className="text-xs text-muted-foreground mb-1">Uptime</div>
                                    <div className="font-mono text-white">{bot.uptime}</div>
                                </div>
                            </div>

                            {/* Actions */}
                            <div className="flex items-center gap-2 w-full md:w-auto justify-end">
                                <button
                                    onClick={() => handleStatusToggle(bot.id, bot.status)}
                                    className={`p-2 rounded-lg border transition-all ${bot.status === 'running'
                                        ? 'border-amber-500/20 text-amber-500 hover:bg-amber-500/10'
                                        : 'border-green-500/20 text-green-500 hover:bg-green-500/10'
                                        }`}
                                    title={bot.status === 'running' ? "Pause Bot" : "Resume Bot"}
                                >
                                    {bot.status === 'running' ? <Pause className="w-5 h-5" /> : <Play className="w-5 h-5" />}
                                </button>

                                <Link to={`/dashboard/bots/${bot.id}`} className="p-2 rounded-lg border border-white/10 text-gray-400 hover:text-white hover:bg-white/5 transition-colors" title="Monitor">
                                    <Activity className="w-5 h-5" />
                                </Link>

                                <Link
                                    to={`/dashboard/bots/${bot.id}/edit`}
                                    className="p-2 rounded-lg border border-white/10 text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
                                    title="Settings"
                                >
                                    <Settings className="w-5 h-5" />
                                </Link>

                                <button
                                    onClick={() => handleDelete(bot.id)}
                                    className="p-2 rounded-lg border border-red-500/20 text-red-500 hover:bg-red-500/10 transition-colors"
                                    title="Delete"
                                >
                                    <Trash2 className="w-5 h-5" />
                                </button>
                            </div>

                        </div>
                    ))}

                    {bots.length === 0 && (
                        <div className="p-12 text-center text-muted-foreground">
                            No bots found. Create one to get started!
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
