import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useEffect, useState } from "react";
import { Activity, Cpu, HardDrive, Zap, AlertCircle } from "lucide-react";

interface SystemHealth {
    status: string;
    cpu_percent: number;
    memory_percent: number;
    disk_percent: number;
    avg_response_time_ms: number;
    recent_errors: number;
    timestamp: string;
}

export default function SystemHealth() {
    const [health, setHealth] = useState<SystemHealth | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetchHealth();
        const interval = setInterval(fetchHealth, 5000); // Refresh every 5 seconds
        return () => clearInterval(interval);
    }, []);

    const fetchHealth = async () => {
        try {
            // This will be replaced with actual API call
            setHealth({
                status: "healthy",
                cpu_percent: 45.2,
                memory_percent: 62.8,
                disk_percent: 38.5,
                avg_response_time_ms: 125,
                recent_errors: 2,
                timestamp: new Date().toISOString()
            });
            setLoading(false);
        } catch (error) {
            console.error("Failed to fetch system health:", error);
            setLoading(false);
        }
    };

    const getStatusColor = (status: string) => {
        switch (status) {
            case "healthy": return "var(--admin-green)";
            case "degraded": return "var(--admin-yellow)";
            case "critical": return "var(--admin-red)";
            default: return "var(--admin-text-secondary)";
        }
    };

    const getMetricColor = (value: number, thresholds: { warning: number, critical: number }) => {
        if (value >= thresholds.critical) return "var(--admin-red)";
        if (value >= thresholds.warning) return "var(--admin-yellow)";
        return "var(--admin-green)";
    };

    if (loading) {
        return (
            <AdminLayout>
                <div className="flex items-center justify-center h-64">
                    <p style={{ color: 'var(--admin-text-muted)' }}>Loading system health...</p>
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
                            System Health
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Real-time monitoring of platform infrastructure
                        </p>
                    </div>
                    <div className="flex items-center gap-3">
                        <div className="flex items-center gap-2 px-4 py-2 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                            <div
                                className="w-3 h-3 rounded-full"
                                style={{ background: health ? getStatusColor(health.status) : 'gray' }}
                            />
                            <span className="font-semibold capitalize" style={{ color: 'var(--admin-text-primary)' }}>
                                {health?.status || 'Unknown'}
                            </span>
                        </div>
                    </div>
                </div>

                {/* Status Cards */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    {/* CPU Usage */}
                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">CPU Usage</div>
                                <div className="text-3xl font-bold" style={{ color: health ? getMetricColor(health.cpu_percent, { warning: 70, critical: 85 }) : 'var(--admin-text-primary)' }}>
                                    {health?.cpu_percent.toFixed(1)}%
                                </div>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(59, 130, 246, 0.1)' }}>
                                <Cpu className="w-6 h-6" style={{ color: 'var(--admin-blue)' }} />
                            </div>
                        </div>
                        <div className="w-full h-2 rounded-full overflow-hidden" style={{ background: 'var(--admin-bg-primary)' }}>
                            <div
                                className="h-full rounded-full transition-all"
                                style={{
                                    width: `${health?.cpu_percent}%`,
                                    background: health ? getMetricColor(health.cpu_percent, { warning: 70, critical: 85 }) : 'gray'
                                }}
                            />
                        </div>
                    </div>

                    {/* Memory Usage */}
                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">Memory Usage</div>
                                <div className="text-3xl font-bold" style={{ color: health ? getMetricColor(health.memory_percent, { warning: 75, critical: 90 }) : 'var(--admin-text-primary)' }}>
                                    {health?.memory_percent.toFixed(1)}%
                                </div>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(139, 92, 246, 0.1)' }}>
                                <Activity className="w-6 h-6" style={{ color: 'var(--admin-purple)' }} />
                            </div>
                        </div>
                        <div className="w-full h-2 rounded-full overflow-hidden" style={{ background: 'var(--admin-bg-primary)' }}>
                            <div
                                className="h-full rounded-full transition-all"
                                style={{
                                    width: `${health?.memory_percent}%`,
                                    background: health ? getMetricColor(health.memory_percent, { warning: 75, critical: 90 }) : 'gray'
                                }}
                            />
                        </div>
                    </div>

                    {/* Disk Usage */}
                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">Disk Usage</div>
                                <div className="text-3xl font-bold" style={{ color: health ? getMetricColor(health.disk_percent, { warning: 80, critical: 95 }) : 'var(--admin-text-primary)' }}>
                                    {health?.disk_percent.toFixed(1)}%
                                </div>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(6, 182, 212, 0.1)' }}>
                                <HardDrive className="w-6 h-6" style={{ color: 'var(--admin-cyan)' }} />
                            </div>
                        </div>
                        <div className="w-full h-2 rounded-full overflow-hidden" style={{ background: 'var(--admin-bg-primary)' }}>
                            <div
                                className="h-full rounded-full transition-all"
                                style={{
                                    width: `${health?.disk_percent}%`,
                                    background: health ? getMetricColor(health.disk_percent, { warning: 80, critical: 95 }) : 'gray'
                                }}
                            />
                        </div>
                    </div>

                    {/* API Response Time */}
                    <div className="admin-card">
                        <div className="flex items-start justify-between mb-3">
                            <div>
                                <div className="admin-metric-label mb-2">API Response Time</div>
                                <div className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                                    {health?.avg_response_time_ms.toFixed(0)}ms
                                </div>
                                <p className="text-xs mt-1" style={{ color: 'var(--admin-text-muted)' }}>
                                    Average (last 5 min)
                                </p>
                            </div>
                            <div className="p-2 rounded-lg" style={{ background: 'rgba(16, 185, 129, 0.1)' }}>
                                <Zap className="w-6 h-6" style={{ color: 'var(--admin-green)' }} />
                            </div>
                        </div>
                    </div>
                </div>

                {/* Error Alert */}
                {health && health.recent_errors > 0 && (
                    <div className="admin-card" style={{ background: 'rgba(239, 68, 68, 0.1)', borderColor: 'var(--admin-red)' }}>
                        <div className="flex items-start gap-4">
                            <AlertCircle className="w-6 h-6 flex-shrink-0" style={{ color: 'var(--admin-red)' }} />
                            <div>
                                <p className="font-semibold" style={{ color: 'var(--admin-red)' }}>
                                    {health.recent_errors} Error{health.recent_errors > 1 ? 's' : ''} Detected
                                </p>
                                <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                                    Recent errors in the last 5 minutes. Check audit logs for details.
                                </p>
                            </div>
                        </div>
                    </div>
                )}

                {/* Performance Metrics Chart would go here */}
                <div className="admin-card">
                    <h2 className="text-xl font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Performance Metrics (Last Hour)
                    </h2>
                    <div className="h-80 flex items-center justify-center" style={{ background: 'var(--admin-bg-primary)', borderRadius: 'var(--admin-radius-md)' }}>
                        <p style={{ color: 'var(--admin-text-muted)' }}>
                            Real-time chart will be rendered here
                        </p>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
