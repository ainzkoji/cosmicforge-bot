import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { Save, ArrowLeft, AlertTriangle } from "lucide-react";
import { api } from "../api/client";

export default function EditBot() {
    const { id } = useParams();
    const navigate = useNavigate();
    const [isLoading, setIsLoading] = useState(false);

    // In a real app, fetch this data based on ID
    const [config, setConfig] = useState({
        name: "BTC Momentum Alpha",
        status: "running",
        pair: "BTC/USDT",
        strategy: "Trend Following",
        amount_type: "percentage",
        amount_value: 10,
        leverage: 1,
        stop_loss: 2.5,
        take_profit: 5.0,
        trailing_stop: true,
        max_daily_loss: 100,
        broker_account: "Binance - Main"
    });

    const handleChange = (field: string, value: any) => {
        setConfig(prev => ({ ...prev, [field]: value }));
    };

    const handleSave = async () => {
        setIsLoading(true);
        // Simulate API
        setTimeout(() => {
            setIsLoading(false);
            navigate("/dashboard/bots");
        }, 1000);
    };

    return (
        <div className="max-w-4xl mx-auto space-y-6 animate-in fade-in duration-500 text-foreground">
            <div className="flex items-center gap-4 mb-6">
                <button onClick={() => navigate(-1)} className="p-2 hover:bg-white/5 rounded-full transition-colors">
                    <ArrowLeft className="w-6 h-6" />
                </button>
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Edit Bot Parameters</h1>
                    <p className="text-muted-foreground font-mono text-sm">{id}</p>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {/* Main Settings */}
                <div className="md:col-span-2 space-y-6">
                    {/* General Config */}
                    <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6">
                        <h2 className="text-lg font-bold text-white mb-4 border-b border-white/5 pb-2">General Configuration</h2>
                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-medium text-gray-400 mb-1">Bot Name</label>
                                <input
                                    type="text"
                                    value={config.name}
                                    onChange={(e) => handleChange("name", e.target.value)}
                                    className="w-full bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none"
                                />
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="block text-sm font-medium text-gray-400 mb-1">Trading Pair</label>
                                    <select
                                        value={config.pair}
                                        onChange={(e) => handleChange("pair", e.target.value)}
                                        className="w-full bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none appearance-none"
                                    >
                                        <option>BTC/USDT</option>
                                        <option>ETH/USDT</option>
                                        <option>SOL/USD</option>
                                    </select>
                                </div>
                                <div>
                                    <label className="block text-sm font-medium text-gray-400 mb-1">Strategy Type</label>
                                    <input
                                        type="text"
                                        value={config.strategy}
                                        disabled
                                        className="w-full bg-white/5 border border-white/5 rounded-lg p-3 text-gray-500 cursor-not-allowed"
                                        title="Strategy type cannot be changed after deployment"
                                    />
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Risk Management */}
                    <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6">
                        <h2 className="text-lg font-bold text-white mb-4 border-b border-white/5 pb-2">Risk Management</h2>
                        <div className="grid grid-cols-2 gap-6">
                            <div>
                                <label className="block text-sm font-medium text-gray-400 mb-1">Order Size</label>
                                <div className="flex gap-2">
                                    <input
                                        type="number"
                                        value={config.amount_value}
                                        onChange={(e) => handleChange("amount_value", parseFloat(e.target.value))}
                                        className="w-full bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none"
                                    />
                                    <select
                                        value={config.amount_type}
                                        onChange={(e) => handleChange("amount_type", e.target.value)}
                                        className="bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none"
                                    >
                                        <option value="percentage">%</option>
                                        <option value="fixed">USD</option>
                                    </select>
                                </div>
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-gray-400 mb-1">Stop Loss (%)</label>
                                <input
                                    type="number"
                                    step="0.1"
                                    value={config.stop_loss}
                                    onChange={(e) => handleChange("stop_loss", parseFloat(e.target.value))} // Fixed: was stop_loss_loss
                                    className="w-full bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-gray-400 mb-1">Take Profit (%)</label>
                                <input
                                    type="number"
                                    step="0.1"
                                    value={config.take_profit}
                                    onChange={(e) => handleChange("take_profit", parseFloat(e.target.value))}
                                    className="w-full bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-gray-400 mb-1">Max Daily Loss ($)</label>
                                <input
                                    type="number"
                                    value={config.max_daily_loss}
                                    onChange={(e) => handleChange("max_daily_loss", parseFloat(e.target.value))}
                                    className="w-full bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none"
                                />
                            </div>
                        </div>
                    </div>
                </div>

                {/* Sidebar Config */}
                <div className="space-y-6">
                    <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6">
                        <h2 className="text-lg font-bold text-white mb-4 border-b border-white/5 pb-2">Status</h2>
                        <div className="flex items-center gap-3 mb-6">
                            <div className={`w-3 h-3 rounded-full ${config.status === 'running' ? 'bg-green-500 animate-pulse' : 'bg-amber-500'}`} />
                            <span className="capitalize font-bold text-lg">{config.status}</span>
                        </div>

                        {config.status === 'running' && (
                            <div className="p-4 bg-amber-500/10 border border-amber-500/20 rounded-lg text-sm text-amber-200 mb-4">
                                <div className="flex items-center gap-2 font-bold mb-1">
                                    <AlertTriangle className="w-4 h-4" /> Warning
                                </div>
                                Changes to running bots apply to <b>new orders</b> only. Existing positions are unaffected.
                            </div>
                        )}

                        <button
                            onClick={handleSave}
                            disabled={isLoading}
                            className="w-full py-3 bg-primary text-primary-foreground font-bold rounded-xl flex items-center justify-center gap-2 hover:bg-primary/90 transition-all shadow-lg shadow-primary/20"
                        >
                            {isLoading ? "Saving..." : <><Save className="w-4 h-4" /> Save Changes</>}
                        </button>
                    </div>

                    <div className="bg-[#0F1218] border border-white/5 rounded-2xl p-6">
                        <label className="block text-sm font-medium text-gray-400 mb-2">Connected Broker</label>
                        <select
                            value={config.broker_account}
                            onChange={(e) => handleChange("broker_account", e.target.value)}
                            className="w-full bg-black/20 border border-white/10 rounded-lg p-3 text-white focus:border-primary focus:outline-none"
                        >
                            <option>Binance - Main</option>
                            <option>Coinbase Pro</option>
                            <option>Paper Trading</option>
                        </select>
                    </div>
                </div>
            </div>
        </div>
    );
}
