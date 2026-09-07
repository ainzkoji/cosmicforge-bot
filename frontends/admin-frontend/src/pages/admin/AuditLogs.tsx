import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useState } from "react";
import { Filter, Shield, AlertCircle, CheckCircle, Info, DollarSign, Loader2 } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { getAuditLogs } from "@/api/admin";
import { ExportButton } from "@/components/admin/common/ExportButton";
import { DateRangePicker } from "@/components/admin/filters/DateRangePicker";

export default function AuditLogs() {
    const [eventTypeFilter, setEventTypeFilter] = useState<string | undefined>(undefined);
    const [searchQuery, setSearchQuery] = useState("");
    const [dateRange, setDateRange] = useState<{ start: string; end: string }>({ start: "", end: "" });

    const [source, setSource] = useState<string>("all");

    // Fetch audit logs with optional event type filter
    const { data: logsData, isLoading } = useQuery({
        queryKey: ["adminAuditLogs", eventTypeFilter, source],
        queryFn: () => getAuditLogs(eventTypeFilter, 100, source),
    });

    const logs = logsData?.logs || [];

    // Client-side search and date filtering
    const filteredLogs = logs.filter((log: any) => {
        // Search filter
        if (searchQuery) {
            const searchLower = searchQuery.toLowerCase();
            const matchesSearch = (
                log.user_id?.toLowerCase().includes(searchLower) ||
                log.email?.toLowerCase().includes(searchLower) ||
                log.event_type?.toLowerCase().includes(searchLower) ||
                log.details?.toLowerCase().includes(searchLower)
            );
            if (!matchesSearch) return false;
        }

        // Date range filter
        if (dateRange.start || dateRange.end) {
            const logDate = new Date(log.created_at);
            if (dateRange.start && logDate < new Date(dateRange.start)) return false;
            if (dateRange.end) {
                const endDate = new Date(dateRange.end);
                endDate.setHours(23, 59, 59, 999); // Include full end date
                if (logDate > endDate) return false;
            }
        }

        return true;
    });

    const getEventIcon = (type: string) => {
        const lowerType = type?.toLowerCase() || '';
        if (lowerType.includes('security') || lowerType.includes('2fa') || lowerType.includes('password')) {
            return <Shield className="w-4 h-4" style={{ color: 'var(--admin-red)' }} />;
        }
        if (lowerType.includes('payment') || lowerType.includes('subscription')) {
            return <DollarSign className="w-4 h-4" style={{ color: 'var(--admin-green)' }} />;
        }
        if (lowerType.includes('admin') || lowerType.includes('kyc')) {
            return <AlertCircle className="w-4 h-4" style={{ color: 'var(--admin-yellow)' }} />;
        }
        return <Info className="w-4 h-4" style={{ color: 'var(--admin-blue)' }} />;
    };

    const getStatusBadge = (eventType: string) => {
        const lowerType = eventType?.toLowerCase() || '';
        if (lowerType.includes('failed') || lowerType.includes('rejected') || lowerType.includes('suspended')) {
            return "admin-badge-danger";
        }
        if (lowerType.includes('warning') || lowerType.includes('disabled')) {
            return "admin-badge-warning";
        }
        if (lowerType.includes('success') || lowerType.includes('approved') || lowerType.includes('granted')) {
            return "admin-badge-success";
        }
        return "admin-badge-info";
    };

    const formatTimestamp = (timestamp: string) => {
        try {
            return new Date(timestamp).toLocaleString();
        } catch {
            return timestamp;
        }
    };

    // Calculate stats from fetched logs
    const totalEvents = logs.length;
    const criticalEvents = logs.filter((log: any) =>
        log.event_type?.toLowerCase().includes('failed') ||
        log.event_type?.toLowerCase().includes('suspended') ||
        log.event_type?.toLowerCase().includes('rejected')
    ).length;
    const securityEvents = logs.filter((log: any) =>
        log.event_type?.toLowerCase().includes('security') ||
        log.event_type?.toLowerCase().includes('2fa') ||
        log.event_type?.toLowerCase().includes('password')
    ).length;

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            Audit Logs & Activity
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Monitor all system events and user activities
                        </p>
                    </div>
                    <div className="flex gap-3">
                        <ExportButton
                            data={filteredLogs}
                            filename="audit_logs"
                            label="Export"
                        />
                    </div>
                </div>

                {/* Filter Bar */}
                <div className="flex flex-col gap-4">
                    {/* Date Range Picker */}
                    <DateRangePicker
                        onDateRangeChange={(start, end) => setDateRange({ start, end })}
                    />

                    {/* Other Filters */}
                    <div className="flex items-center gap-4">
                        <select
                            className="admin-input"
                            value={source}
                            onChange={(e) => setSource(e.target.value)}
                            style={{ width: '150px' }}
                        >
                            <option value="all">All Sources</option>
                            <option value="auth">Auth Only</option>
                            <option value="bot">Bot Only</option>
                        </select>
                        <select
                            className="admin-input"
                            value={eventTypeFilter || 'all'}
                            onChange={(e) => setEventTypeFilter(e.target.value === 'all' ? undefined : e.target.value)}
                            style={{ width: '200px' }}
                        >
                            <option value="all">All Events</option>
                            <option value="login">Login Events</option>
                            <option value="user_">User Actions</option>
                            <option value="admin_">Admin Actions</option>
                            <option value="kyc_">KYC Events</option>
                        </select>

                        <input
                            type="text"
                            placeholder="Search logs..."
                            className="admin-input flex-1"
                            style={{ maxWidth: '400px' }}
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                        />

                    </div>
                </div>

                {/* Stats Cards */}
                <div className="grid grid-cols-4 gap-4">
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Total Events</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            {totalEvents.toLocaleString()}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Critical/Failed</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-red)' }}>
                            {criticalEvents}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Security Events</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-yellow)' }}>
                            {securityEvents}
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">System Uptime</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-green)' }}>
                            99.9%
                        </div>
                    </div>
                </div>

                {/* Logs Table */}
                <div className="admin-card">
                    <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Recent Activity
                    </h3>
                    {isLoading ? (
                        <div className="flex items-center justify-center py-12">
                            <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                        </div>
                    ) : (
                        <div className="overflow-x-auto">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>Timestamp</th>
                                        <th>Event Type</th>
                                        <th>User/Email</th>
                                        <th>Details</th>
                                        <th>IP Address</th>
                                        <th>Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {filteredLogs.map((log: any) => (
                                        <tr key={log.id}>
                                            <td style={{ fontFamily: 'Monaco, monospace', fontSize: '0.8rem' }}>
                                                {formatTimestamp(log.created_at)}
                                            </td>
                                            <td>
                                                <div className="flex items-center gap-2">
                                                    {getEventIcon(log.event_type)}
                                                    <span className="capitalize text-sm">{log.event_type?.replace('_', ' ')}</span>
                                                </div>
                                            </td>
                                            <td>
                                                <div>
                                                    <div className="text-sm">{log.email || log.user_id || 'System'}</div>
                                                    {log.user_id && (
                                                        <div className="text-xs" style={{ color: 'var(--admin-text-muted)' }}>
                                                            {log.user_id.substring(0, 8)}...
                                                        </div>
                                                    )}
                                                </div>
                                            </td>
                                            <td className="max-w-md truncate text-sm">{log.details || '-'}</td>
                                            <td style={{ fontFamily: 'Monaco, monospace', fontSize: '0.8rem' }}>
                                                {log.ip || '-'}
                                            </td>
                                            <td>
                                                <span className={`admin-badge ${getStatusBadge(log.event_type)}`}>
                                                    {log.event_type?.includes('failed') ? 'Failed' :
                                                        log.event_type?.includes('success') ? 'Success' :
                                                            log.event_type?.includes('warning') ? 'Warning' : 'Info'}
                                                </span>
                                            </td>
                                        </tr>
                                    ))}
                                    {filteredLogs.length === 0 && (
                                        <tr>
                                            <td colSpan={6} className="text-center py-8" style={{ color: 'var(--admin-text-muted)' }}>
                                                No audit logs found
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>

                {/* Export Section */}
                <div className="admin-card">
                    <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--admin-text-primary)' }}>
                        Export Logs
                    </h3>
                    <div className="flex items-end gap-4">
                        <div className="flex-1">
                            <label className="block text-sm font-medium mb-2" style={{ color: 'var(--admin-text-secondary)' }}>
                                Format
                            </label>
                            <select className="admin-input">
                                <option>CSV</option>
                                <option>JSON</option>
                            </select>
                        </div>
                        <div className="flex-1">
                            <label className="block text-sm font-medium mb-2" style={{ color: 'var(--admin-text-secondary)' }}>
                                Date Range
                            </label>
                            <input type="text" className="admin-input" placeholder="Jan 7, 2024 - Jan 14, 2024" />
                        </div>
                        <button className="admin-btn admin-btn-primary">
                            Generate Report
                        </button>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
