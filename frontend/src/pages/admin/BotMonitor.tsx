import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useEffect, useState } from "react";
import { Activity, TrendingUp, TrendingDown, Pause, Play, Square } from "lucide-react";

interface BotOverview {
    total_bots: number;
    active_bots: number;
    executions_24h: number;
    success_rate: number;
    total_pnl_24h: number;
}

interface BotExecution {
    id: string;
    bot_name: string;
    action: string;
    symbol: string;
    quantity: number;
    price: number;
    pnl: number;
    status: string;
    executed_at: string;
}

export default function BotMonitor() {
    const [overview, setOverview] = useState<BotOverview | null>(null);
    const [executions, setExecutions] = useState<BotExecution[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetchData();
        const interval = setInterval(fetchData, 10000); // Refresh every 10 seconds
        return () => clearInterval(interval);
    }, []);

    const fetchData = async () => {
        try {
            // Mock data - will be replaced with actual API calls
            setOverview({
                total_bots: 45,
                active_bots: 32,
                executions_24h: 1247,
                success_rate: 87.5,
                total_pnl_24h: 4532.80
            });

            setExecutions([
                {
                    id: "1",
                    bot_name: "BTC Momentum Pro",
                    action: "BUY",
                    symbol: "BTCUSDT",
                    quantity: 0.05,
                    price: 43250.50,
                    pnl: 125.30,
                    status: "success",
                    executed_at: new Date(Date.now() - 2 * 60000).toISOString()
                },
                {
                    id: "2",
                    bot_name: "ETH Scalper",
                    action: "SELL",
                    symbol: "ETHUSDT",
                    quantity: 2.5,
                    price: 2280.75,
                    pnl: -45.20,
                    status: "success",
                    executed_at: new Date(Date.now() - 5 * 60000).toISOString()
                },
                {
                    id: "3",
                    bot_name: "SOL Grid Bot",
                    action: "BUY",
                    symbol: "SOLUSDT",
                    quantity: 50,
                    price: 102.45,
                    pnl: 78.90,
                    status: "success",
                    executed_at: new Date(Date.now() - 8 * 60000).toISOString()
                }
            ]);
            setLoading(false);
        } catch (error) {
            console.error("Failed to fetch bot data:", error);
            setLoading(false);
        }
    };

    const handleEmergencyStop = async () => {
        if (!confirm("Are you sure you want to stop ALL bots? This will halt all trading activity.")) {
            return;
        }
        // API call to emergency stop
        alert("Emergency stop triggered!");
    };

    const formatTime = (isoString: string) => {
        const date = new Date(isoString);
        return date.toLocaleTimeString();
    };

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            Bot Activity Monitor
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Real-time tracking of all trading bot operations
                        </p>
                    </div>
                    <button
                        onClick={handleEmergencyStop}
                        className="admin-btn admin-btn-danger flex items-center gap-2"
                    >
                        <Square className="w-4 h-4" />
                        Emergency Stop All
                    </button>
                </div>

                {/* Overview Cards */}
                <div className="grid grid-cols-1 md:grid-cols-5 gap-6">
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Total Bots</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {overview?.total_bots || 0}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Active Bots</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-green)' }}>
                            {overview?.active_bots || 0}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Executions (24h)</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {overview?.executions_24h.toLocaleString() || 0}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Success Rate</div>
                        <div className="text-2xl font-bold" style={{ color: overview && overview.success_rate > 80 ? 'var(--admin-green)' : 'var(--admin-yellow)' }}>
                            {overview?.success_rate.toFixed(1)}%
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Total P&L (24h)</div>
                        <div className="text-2xl font-bold flex items-center gap-2" style={{ color: overview && overview.total_pnl_24h > 0 ? 'var(--admin-green)' : 'var(--admin-red)' }}>
                            {overview && overview.total_pnl_24h > 0 ? <TrendingUp className="w-5 h-5" /> : <TrendingDown className="w-5 h-5" />}
                            ${overview?.total_pnl_24h.toFixed(2) || 0}
                        </div>
                    </div>
                </div>

                {/* Live Executions Feed */}
                <div className="admin-card">
                    <div className="flex items-center justify-between mb-4">
                        <h2 className="text-xl font-semibold" style={{ color: 'var(--admin-text-primary)' }}>
                            Live Trade Executions
                        </h2>
                        <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                            <span className="text-sm" style={{ color: 'var(--admin-text-secondary)' }}>Live</span>
                        </div>
                    </div>

                    <table className="admin-table">
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>Bot Name</th>
                                <th>Action</th>
                                <th>Symbol</th>
                                <th>Quantity</th>
                                <th>Price</th>
                                <th>P&L</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
                            {executions.map((exec) => (
                                <tr key={exec.id}>
                                    <td style={{ fontSize: '0.85rem' }}>{formatTime(exec.executed_at)}</td>
                                    <td className="font-medium">{exec.bot_name}</td>
                                    <td>
                                        <span className={`admin-badge ${exec.action === 'BUY' ? 'admin-badge-success' : 'admin-badge-danger'}`}>
                                            {exec.action}
                                        </span>
                                    </td>
                                    <td className="font-mono">{exec.symbol}</td>
                                    <td>{exec.quantity}</td>
                                    <td>${exec.price.toLocaleString()}</td>
                                    <td style={{ color: exec.pnl >= 0 ? 'var(--admin-green)' : 'var(--admin-red)' }}>
                                        {exec.pnl >= 0 ? '+' : ''}{exec.pnl.toFixed(2)}
                                    </td>
                                    <td>
                                        <span className={`admin-badge ${exec.status === 'success' ? 'admin-badge-success' : 'admin-badge-danger'}`}>
                                            {exec.status}
                                        </span>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>

                {/* Strategy Performance would go here */}
                <div className="admin-card">
                    <h2 className="text-xl font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Strategy Performance Breakdown
                    </h2>
                    <div className="h-64 flex items-center justify-center" style={{ background: 'var(--admin-bg-primary)', borderRadius: 'var(--admin-radius-md)' }}>
                        <p style={{ color: 'var(--admin-text-muted)' }}>
                            Performance chart by strategy will be rendered here
                        </p>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
