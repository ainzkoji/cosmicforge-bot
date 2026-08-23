import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { ArrowLeft, Clock, TrendingUp, TrendingDown, Target, Zap, Activity } from "lucide-react";
import { getBotRunDetails } from "@/api/admin";

interface RunDetails {
    run: {
        run_id: string;
        start_time: string | null;
        started_at?: string | null;
        end_time: string | null;
        stopped_at?: string | null;
        mode: string;
        status: string;
        config: any;
    };
    summary: {
        total_trades: number;
        pnl: number;
        win_rate: number;
        volume: number;
    };
    events: {
        created_at: string;
        event_type: string;
        details: string;
    }[];
    traces: {
        created_at: string;
        symbol: string;
        action: string;
        price: number | null;
        reason: string;
        confidence: number;
    }[];
}

export default function BotRunDetails() {
    const { runId } = useParams<{ runId: string }>();
    const [data, setData] = useState<RunDetails | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        if (runId) {
            fetchDetails(runId);
        }
    }, [runId]);

    const fetchDetails = async (id: string) => {
        try {
            const details = await getBotRunDetails(id);
            setData(details);
        } catch (error) {
            console.error("Failed to fetch run details:", error);
        } finally {
            setLoading(false);
        }
    };

    const formatDateTime = (value?: string | null) => {
        if (!value) return "n/a";
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? "n/a" : date.toLocaleString();
    };

    const formatTime = (value?: string | null) => {
        if (!value) return "n/a";
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? "n/a" : date.toLocaleTimeString();
    };

    const formatConfidence = (value: unknown) => {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? `${(parsed * 100).toFixed(0)}%` : "n/a";
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

    if (!data) {
        return (
            <AdminLayout>
                <div className="text-center py-12">
                    <h2 className="text-xl text-gray-400">Run not found</h2>
                    <Link to="/admin/bot-monitor" className="text-primary hover:underline mt-4 inline-block">
                        Back to Monitor
                    </Link>
                </div>
            </AdminLayout>
        );
    }

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center gap-4">
                    <Link to="/admin/bot-monitor" className="p-2 rounded-lg bg-gray-800 hover:bg-gray-700 transition">
                        <ArrowLeft className="w-5 h-5 text-gray-400" />
                    </Link>
                    <div>
                        <h1 className="text-2xl font-bold flex items-center gap-3" style={{ color: 'var(--admin-text-primary)' }}>
                            Run Details
                            <span className="font-mono text-base font-normal text-gray-500 bg-gray-900 px-2 py-1 rounded">
                                {data.run.run_id}
                            </span>
                        </h1>
                        <div className="flex items-center gap-4 text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            <span className="flex items-center gap-1">
                                <Clock className="w-4 h-4" />
                                {formatDateTime(data.run.start_time)}
                            </span>
                            <span className={`px-2 py-0.5 rounded text-xs border ${data.run.status === 'running'
                                    ? 'bg-green-500/10 text-green-400 border-green-500/20'
                                    : 'bg-gray-800 text-gray-400 border-gray-700'
                                }`}>
                                {(data.run.status || "unknown").toUpperCase()}
                            </span>
                            <span className="px-2 py-0.5 rounded text-xs bg-blue-500/10 text-blue-400 border border-blue-500/20">
                                {data.run.mode || "unknown"}
                            </span>
                        </div>
                    </div>
                </div>

                {/* Performance Cards */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Total P&L</div>
                        <div className="text-2xl font-bold flex items-center gap-2" style={{ color: (data.summary.pnl || 0) >= 0 ? 'var(--admin-green)' : 'var(--admin-red)' }}>
                            {(data.summary.pnl || 0) >= 0 ? <TrendingUp className="w-5 h-5" /> : <TrendingDown className="w-5 h-5" />}
                            ${(data.summary.pnl || 0).toFixed(2)}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Total Trades</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {data.summary.total_trades || 0}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Win Rate</div>
                        <div className="text-2xl font-bold" style={{ color: (data.summary.win_rate || 0) >= 50 ? 'var(--admin-green)' : 'var(--admin-yellow)' }}>
                            {(data.summary.win_rate || 0).toFixed(1)}%
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Volume Traded</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            ${(data.summary.volume || 0).toLocaleString()}
                        </div>
                    </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Decision Trace */}
                    <div className="admin-card">
                        <h2 className="text-xl font-semibold mb-4 flex items-center gap-2" style={{ color: 'var(--admin-text-primary)' }}>
                            <Target className="w-5 h-5" />
                            Strategy Decisions
                        </h2>
                        <div className="overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>Time</th>
                                        <th>Symbol</th>
                                        <th>Action</th>
                                        <th>Reason (Confidence)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {data.traces.map((trace, i) => (
                                        <tr key={i}>
                                            <td className="whitespace-nowrap text-xs text-gray-500">
                                                {formatTime(trace.created_at)}
                                            </td>
                                            <td className="font-mono text-blue-400">{trace.symbol || "n/a"}</td>
                                            <td>
                                                <span className={`text-xs font-bold px-1.5 py-0.5 rounded ${trace.action === 'BUY' ? 'bg-green-500/20 text-green-400' :
                                                        trace.action === 'SELL' ? 'bg-red-500/20 text-red-400' :
                                                            'bg-gray-700 text-gray-400'
                                                    }`}>
                                                    {trace.action || "unknown"}
                                                </span>
                                            </td>
                                            <td className="max-w-[200px] truncate text-xs" title={trace.reason}>
                                                {trace.reason || "n/a"}
                                                <span className="ml-1 text-gray-500">({formatConfidence(trace.confidence)})</span>
                                            </td>
                                        </tr>
                                    ))}
                                    {data.traces.length === 0 && (
                                        <tr><td colSpan={4} className="text-center py-4 text-gray-500">No decisions recorded</td></tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>

                    {/* Events Timeline */}
                    <div className="admin-card">
                        <h2 className="text-xl font-semibold mb-4 flex items-center gap-2" style={{ color: 'var(--admin-text-primary)' }}>
                            <Activity className="w-5 h-5" />
                            System Events
                        </h2>
                        <div className="space-y-4 max-h-[500px] overflow-y-auto pr-2">
                            {data.events.map((event, i) => (
                                <div key={i} className="relative pl-6 border-l border-gray-700 pb-2 last:pb-0">
                                    <div className="absolute left-[-5px] top-1 w-2.5 h-2.5 rounded-full bg-gray-600 border-2 border-[#0B0E14]"></div>
                                    <div className="text-xs text-gray-500 mb-0.5">
                                        {formatTime(event.created_at)}
                                    </div>
                                    <div className="text-sm font-medium text-gray-300">
                                        {event.event_type || "unknown"}
                                    </div>
                                    <div className="text-sm text-gray-400 mt-0.5">
                                        {event.details || "n/a"}
                                    </div>
                                </div>
                            ))}
                            {data.events.length === 0 && (
                                <div className="text-center py-8 text-gray-500">No events recorded</div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
