import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useEffect, useState } from "react";
import { Activity, TrendingUp, TrendingDown, Square, Clock, AlertCircle, RefreshCw } from "lucide-react";
import { getBotOverview, getBotLiveTelemetry, getBotRuns } from "@/api/admin";
import { Link } from "react-router-dom";

interface BotOverview {
    status: string;
    uptime_seconds: number;
    active_run_id: string | null;
    active_positions: number;
    daily_pnl: number;
    daily_trades: number;
    recent_events_1h: number;
}

interface BotTelemetry {
    positions: any[];
    latest_decisions: any[];
    latest_events: any[];
}

interface BotRun {
    run_id: string;
    start_time: string | null;
    started_at?: string | null;
    end_time: string | null;
    stopped_at?: string | null;
    mode: string;
    status: string;
    pnl: number;
    realized_pnl?: number;
    trade_count: number;
    trades?: number;
}

export default function BotMonitor() {
    const [overview, setOverview] = useState<BotOverview | null>(null);
    const [telemetry, setTelemetry] = useState<BotTelemetry | null>(null);
    const [runs, setRuns] = useState<BotRun[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        fetchInitialData();
        const interval = setInterval(fetchLiveData, 5000); // 5 seconds refresh for live data
        return () => clearInterval(interval);
    }, []);

    const fetchInitialData = async () => {
        setLoading(true);
        setError(null);
        try {
            await Promise.all([fetchOverview(), fetchLiveData(), fetchRuns()]);
        } catch (error) {
            console.error("Failed to fetch initial bot data:", error);
            setError("Unable to load Bot Monitor data. Check that the selected backend is running.");
        } finally {
            setLoading(false);
        }
    };

    const fetchOverview = async () => {
        try {
            const data = await getBotOverview();
            setOverview(data);
        } catch (error) {
            console.error("Failed to fetch bot overview:", error);
            setError("Unable to load bot overview.");
        }
    };

    const fetchLiveData = async () => {
        try {
            const data = await getBotLiveTelemetry();
            setTelemetry(data);
            // Also refresh overview to keep PnL up to date
            const overviewData = await getBotOverview();
            setOverview(overviewData);
        } catch (error) {
            console.error("Failed to fetch live telemetry:", error);
            setError("Unable to load live bot telemetry.");
        }
    };

    const fetchRuns = async () => {
        try {
            const data = await getBotRuns(10);
            setRuns(data.runs || data); // Adjust based on actual response structure
        } catch (error) {
            console.error("Failed to fetch bot runs:", error);
            setError("Unable to load bot run history.");
        }
    };

    const handleEmergencyStop = async () => {
        if (!confirm("Are you sure you want to stop the bot? This will halt all trading activity.")) {
            return;
        }
        // TODO: Implement emergency stop API call
        alert("Emergency stop triggered!");
    };

    const formatTime = (isoString?: string | null) => {
        if (!isoString) return 'n/a';
        const date = new Date(isoString);
        return Number.isNaN(date.getTime()) ? 'n/a' : date.toLocaleTimeString();
    };

    const formatDateTime = (isoString?: string | null) => {
        if (!isoString) return 'n/a';
        const date = new Date(isoString);
        return Number.isNaN(date.getTime()) ? 'n/a' : date.toLocaleString();
    };

    const formatDuration = (seconds: number) => {
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        return `${h}h ${m}m`;
    };

    const formatPrice = (value: unknown) => {
        const parsed = Number(value);
        return Number.isFinite(parsed) && parsed !== 0 ? parsed.toString() : 'n/a';
    };

    if (loading) {
        return (
            <AdminLayout>
                <div className="flex items-center justify-center h-screen">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
                </div>
            </AdminLayout>
        );
    }

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
                            Real-time tracking of trading bot operations
                        </p>
                    </div>
                    <button
                        onClick={handleEmergencyStop}
                        className="admin-btn admin-btn-danger flex items-center gap-2"
                    >
                        <Square className="w-4 h-4" />
                        Emergency Stop
                    </button>
                </div>

                {/* Overview Cards */}
                {error && (
                    <div className="admin-card flex items-center justify-between gap-4 border-red-500/20 bg-red-500/5">
                        <div className="flex items-center gap-2 text-sm text-red-300">
                            <AlertCircle className="w-4 h-4" />
                            {error}
                        </div>
                        <button className="admin-btn admin-btn-secondary text-sm" onClick={fetchInitialData}>
                            <RefreshCw className="w-4 h-4" />
                            Retry
                        </button>
                    </div>
                )}

                {/* Overview Cards */}
                <div className="grid grid-cols-1 md:grid-cols-5 gap-6">
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Status</div>
                        <div className="text-2xl font-bold uppercase flex items-center gap-2" style={{ color: overview?.status === 'running' ? 'var(--admin-green)' : 'var(--admin-text-muted)' }}>
                            {overview?.status === 'running' && <span className="relative flex h-3 w-3 mr-1">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                                <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
                            </span>}
                            {overview?.status || 'Unknown'}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Uptime</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {overview ? formatDuration(overview.uptime_seconds) : '-'}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Active Positions</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {overview?.active_positions || 0}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Daily Trades</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {overview?.daily_trades || 0}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Daily P&L</div>
                        <div className="text-2xl font-bold flex items-center gap-2" style={{ color: (overview?.daily_pnl || 0) >= 0 ? 'var(--admin-green)' : 'var(--admin-red)' }}>
                            {(overview?.daily_pnl || 0) >= 0 ? <TrendingUp className="w-5 h-5" /> : <TrendingDown className="w-5 h-5" />}
                            ${Math.abs(overview?.daily_pnl || 0).toFixed(2)}
                        </div>
                    </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Live Activity Feed */}
                    <div className="admin-card">
                        <div className="flex items-center justify-between mb-4">
                            <h2 className="text-xl font-semibold flex items-center gap-2" style={{ color: 'var(--admin-text-primary)' }}>
                                <Activity className="w-5 h-5" />
                                Live Activity
                            </h2>
                            <div className="flex items-center gap-2">
                                <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                                <span className="text-xs" style={{ color: 'var(--admin-text-secondary)' }}>Live</span>
                            </div>
                        </div>

                        <div className="space-y-3 max-h-[400px] overflow-y-auto">
                            {telemetry?.latest_events && telemetry.latest_events.length > 0 ? (
                                telemetry.latest_events.map((event: any, i: number) => (
                                    <div key={i} className="flex justify-between items-start text-sm p-2 rounded hover:bg-white/5">
                                        <div>
                                            <span className="font-semibold text-gray-300 mr-2">{event.event_type}</span>
                                            <span className="text-gray-400">{event.details}</span>
                                        </div>
                                        <span className="text-xs text-gray-500 whitespace-nowrap ml-2">
                                            {formatTime(event.created_at)}
                                        </span>
                                    </div>
                                ))
                            ) : (
                                <div className="text-center py-8 text-gray-500">No recent activity</div>
                            )}
                        </div>
                    </div>

                    {/* Latest Decisions */}
                    <div className="admin-card">
                        <div className="flex items-center justify-between mb-4">
                            <h2 className="text-xl font-semibold flex items-center gap-2" style={{ color: 'var(--admin-text-primary)' }}>
                                <AlertCircle className="w-5 h-5" />
                                Strategy Decisions
                            </h2>
                        </div>
                        <div className="space-y-3 max-h-[400px] overflow-y-auto">
                            {telemetry?.latest_decisions && telemetry.latest_decisions.length > 0 ? (
                                telemetry.latest_decisions.map((decision: any, i: number) => (
                                    <div key={i} className="border-l-2 border-gray-600 pl-3 py-1">
                                        <div className="flex justify-between">
                                            <span className="font-mono text-xs text-blue-400">{decision.symbol}</span>
                                            <span className="text-xs text-gray-500">{formatTime(decision.created_at)}</span>
                                        </div>
                                        <div className="text-sm font-medium mt-1">
                                            {decision.action} <span className="text-gray-400">@ {formatPrice(decision.price)}</span>
                                        </div>
                                        <div className="text-xs text-gray-500 mt-1 truncate">
                                            {decision.reason}
                                        </div>
                                        {decision.sizing_cap_event?.admin_message && (
                                            <div className="mt-2 rounded border border-amber-500/20 bg-amber-500/10 p-2 text-xs text-amber-200">
                                                {decision.sizing_cap_event.admin_message}
                                            </div>
                                        )}
                                    </div>
                                ))
                            ) : (
                                <div className="text-center py-8 text-gray-500">No strategy decisions recorded</div>
                            )}
                        </div>
                    </div>
                </div>

                {/* Historical Runs Table */}
                <div className="admin-card">
                    <div className="flex items-center justify-between mb-4">
                        <h2 className="text-xl font-semibold flex items-center gap-2" style={{ color: 'var(--admin-text-primary)' }}>
                            <Clock className="w-5 h-5" />
                            Recent Runs
                        </h2>
                        <Link to="/admin/audit" className="text-sm text-blue-400 hover:text-blue-300">
                            View All Logs
                        </Link>
                    </div>

                    <table className="admin-table">
                        <thead>
                            <tr>
                                <th>Start Time</th>
                                <th>Mode</th>
                                <th>Trades</th>
                                <th>P&L</th>
                                <th>Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {runs.map((run) => (
                                <tr key={run.run_id}>
                                    <td style={{ fontSize: '0.9rem' }}>{formatDateTime(run.start_time)}</td>
                                    <td>
                                        <span className="px-2 py-1 rounded text-xs bg-gray-800 text-gray-300 border border-gray-700">
                                            {run.mode || 'unknown'}
                                        </span>
                                    </td>
                                    <td>{run.trade_count ?? 0}</td>
                                    <td style={{ color: (run.pnl || 0) >= 0 ? 'var(--admin-green)' : 'var(--admin-red)' }}>
                                        {(run.pnl || 0) >= 0 ? '+' : ''}{(run.pnl || 0).toFixed(2)}
                                    </td>
                                    <td>
                                        <span className={`admin-badge ${run.status === 'running' ? 'admin-badge-success' : 'admin-badge-secondary'}`}>
                                            {run.status || 'unknown'}
                                        </span>
                                    </td>
                                    <td>
                                        <Link
                                            to={`/admin/bot/runs/${run.run_id}`}
                                            className="text-xs bg-blue-500/10 text-blue-400 px-3 py-1.5 rounded hover:bg-blue-500/20 transition-colors"
                                        >
                                            View Details
                                        </Link>
                                    </td>
                                </tr>
                            ))}
                            {runs.length === 0 && (
                                <tr>
                                    <td colSpan={6} className="text-center py-6 text-gray-500">
                                        No recent runs found
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </AdminLayout>
    );
}
