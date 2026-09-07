import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useEffect, useState } from "react";
import { Activity, AlertCircle, CheckCircle, Info, Shield, DollarSign, Bot, User } from "lucide-react";

interface ActivityEvent {
    id: string;
    event_type: string;
    event_category: string;
    user_id?: string;
    bot_id?: string;
    description: string;
    severity: string;
    created_at: string;
}

export default function ActivityFeed() {
    const [events, setEvents] = useState<ActivityEvent[]>([]);
    const [filter, setFilter] = useState<string>("all");
    const [isLive, setIsLive] = useState(true);

    useEffect(() => {
        fetchEvents();

        // Simulate WebSocket updates
        const interval = setInterval(() => {
            if (isLive) {
                // Add new mock event
                const newEvent: ActivityEvent = {
                    id: Date.now().toString(),
                    event_type: ["login", "bot_execution", "transaction", "security_event"][Math.floor(Math.random() * 4)],
                    event_category: "user_action",
                    description: generateRandomEvent(),
                    severity: ["info", "success", "warning"][Math.floor(Math.random() * 3)],
                    created_at: new Date().toISOString()
                };
                setEvents(prev => [newEvent, ...prev].slice(0, 50));
            }
        }, 5000);

        return () => clearInterval(interval);
    }, [isLive]);

    const generateRandomEvent = () => {
        const events = [
            "User logged in from new device",
            "Bot executed trade on BTCUSDT",
            "Withdrawal request submitted",
            "2FA enabled on account",
            "API key generated",
            "Strategy deployed successfully",
            "Commission payment processed"
        ];
        return events[Math.floor(Math.random() * events.length)];
    };

    const fetchEvents = async () => {
        // Mock initial data
        setEvents([
            {
                id: "1",
                event_type: "login",
                event_category: "security",
                description: "User john@example.com logged in from Chrome/Windows",
                severity: "info",
                created_at: new Date(Date.now() - 2 * 60000).toISOString()
            },
            {
                id: "2",
                event_type: "bot_execution",
                event_category: "bot_control",
                bot_id: "bot123",
                description: "BTC Momentum Pro executed BUY order on BTCUSDT",
                severity: "success",
                created_at: new Date(Date.now() - 5 * 60000).toISOString()
            },
            {
                id: "3",
                event_type: "transaction",
                event_category: "finance",
                description: "Withdrawal request $2,500 from user sarah@example.com",
                severity: "warning",
                created_at: new Date(Date.now() - 8 * 60000).toISOString()
            }
        ]);
    };

    const getEventIcon = (eventType: string) => {
        switch (eventType) {
            case "login": return <User className="w-4 h-4" />;
            case "bot_execution": return <Bot className="w-4 h-4" />;
            case "transaction": return <DollarSign className="w-4 h-4" />;
            case "security_event": return <Shield className="w-4 h-4" />;
            default: return <Activity className="w-4 h-4" />;
        }
    };

    const getSeverityColor = (severity: string) => {
        switch (severity) {
            case "success": return "var(--admin-green)";
            case "warning": return "var(--admin-yellow)";
            case "error": return "var(--admin-red)";
            case "critical": return "var(--admin-red)";
            default: return "var(--admin-blue)";
        }
    };

    const getSeverityIcon = (severity: string) => {
        switch (severity) {
            case "success": return <CheckCircle className="w-4 h-4" />;
            case "warning": return <AlertCircle className="w-4 h-4" />;
            case "error": return <AlertCircle className="w-4 h-4" />;
            default: return <Info className="w-4 h-4" />;
        }
    };

    const getTimeAgo = (isoString: string) => {
        const seconds = Math.floor((new Date().getTime() - new Date(isoString).getTime()) / 1000);
        if (seconds < 60) return `${seconds}s ago`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
        if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
        return `${Math.floor(seconds / 86400)}d ago`;
    };

    const filteredEvents = filter === "all" ? events : events.filter(e => e.event_type === filter);

    return (
        <AdminLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            Live Activity Feed
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Real-time stream of all platform events
                        </p>
                    </div>
                    <div className="flex items-center gap-3">
                        <button
                            onClick={() => setIsLive(!isLive)}
                            className={`px-4 py-2 rounded-lg flex items-center gap-2 ${isLive ? 'bg-green-600' : 'bg-gray-600'}`}
                        >
                            <div className={`w-2 h-2 rounded-full ${isLive ? 'bg-white animate-pulse' : 'bg-gray-400'}`} />
                            <span className="text-white text-sm font-medium">{isLive ? 'Live' : 'Paused'}</span>
                        </button>
                    </div>
                </div>

                {/* Filters */}
                <div className="flex gap-2">
                    {["all", "login", "bot_execution", "transaction", "security_event"].map((type) => (
                        <button
                            key={type}
                            onClick={() => setFilter(type)}
                            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${filter === type
                                ? 'text-white'
                                : ''
                                }`}
                            style={{
                                background: filter === type ? 'var(--admin-blue)' : 'var(--admin-bg-hover)',
                                color: filter === type ? 'white' : 'var(--admin-text-secondary)'
                            }}
                        >
                            {type.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                        </button>
                    ))}
                </div>

                {/* Event Stream */}
                <div className="admin-card">
                    <div className="space-y-2 max-h-[600px] overflow-y-auto">
                        {filteredEvents.map((event) => (
                            <div
                                key={event.id}
                                className="flex items-start gap-4 p-4 rounded-lg transition-colors hover:bg-opacity-80"
                                style={{ background: 'var(--admin-bg-hover)' }}
                            >
                                <div
                                    className="p-2 rounded-lg flex-shrink-0 mt-1"
                                    style={{ background: `${getSeverityColor(event.severity)}20`, color: getSeverityColor(event.severity) }}
                                >
                                    {getEventIcon(event.event_type)}
                                </div>

                                <div className="flex-1 min-w-0">
                                    <div className="flex items-start justify-between gap-4">
                                        <div className="flex-1">
                                            <div className="flex items-center gap-2 mb-1">
                                                <span className="font-medium capitalize" style={{ color: 'var(--admin-text-primary)' }}>
                                                    {event.event_type.replace('_', ' ')}
                                                </span>
                                                <div className="flex items-center gap-1" style={{ color: getSeverityColor(event.severity) }}>
                                                    {getSeverityIcon(event.severity)}
                                                    <span className="text-xs font-medium capitalize">{event.severity}</span>
                                                </div>
                                            </div>
                                            <p className="text-sm" style={{ color: 'var(--admin-text-secondary)' }}>
                                                {event.description}
                                            </p>
                                        </div>
                                        <span className="text-xs flex-shrink-0" style={{ color: 'var(--admin-text-muted)' }}>
                                            {getTimeAgo(event.created_at)}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Event Stats */}
                <div className="grid grid-cols-4 gap-4">
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Events (1h)</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            1,247
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Security Events</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-yellow)' }}>
                            32
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Bot Actions</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-green)' }}>
                            856
                        </div>
                    </div>
                    <div className="admin-card">
                        <div className="admin-metric-label mb-2">Critical Alerts</div>
                        <div className="text-2xl font-bold" style={{ color: 'var(--admin-red)' }}>
                            3
                        </div>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
