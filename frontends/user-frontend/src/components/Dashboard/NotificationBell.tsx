import React, { useState, useEffect } from 'react';
import { Bell } from 'lucide-react';
import { apiClient } from '../../api/client';

interface Alert {
    id: number;
    ts: string;
    alert_type: string;
    severity: string;
    symbol?: string;
    message: string;
    acknowledged: boolean;
}

export const NotificationBell: React.FC = () => {
    const [alerts, setAlerts] = useState<Alert[]>([]);
    const [unreadCount, setUnreadCount] = useState(0);
    const [isOpen, setIsOpen] = useState(false);
    const [loading, setLoading] = useState(false);
    const [lastSeenId, setLastSeenId] = useState<number>(0);

    // Request permission on mount
    useEffect(() => {
        if ('Notification' in window && Notification.permission === 'default') {
            Notification.requestPermission();
        }
    }, []);

    const fetchAlerts = async () => {
        try {
            // Don't set loading on every poll to avoid UI flicker
            // only on initial load or manual refresh if we wanted
            // setLoading(true); 
            const response = await apiClient.get('/api/notifications/alerts', {
                params: { limit: 20, unread_only: false }
            });

            const newAlerts: Alert[] = response.data.alerts || [];
            setAlerts(newAlerts);
            setUnreadCount(newAlerts.filter((a) => !a.acknowledged).length || 0);

            // Check for new alerts to notify
            if (newAlerts.length > 0) {
                const latestAlert = newAlerts[0];
                // If we have a new alert ID that is greater than what we've seen
                // AND it's not acknowledged
                if (latestAlert.id > lastSeenId) {
                    setLastSeenId(latestAlert.id);
                    // Only notify if unread
                    if (!latestAlert.acknowledged) {
                        triggerBrowserNotification(latestAlert);
                    }
                }
            }

        } catch (error) {
            console.error('Failed to fetch alerts:', error);
        } finally {
            setLoading(false);
        }
    };

    const triggerBrowserNotification = (alert: Alert) => {
        if (!('Notification' in window)) return;

        if (Notification.permission === 'granted') {
            new Notification(`CosmicForge: ${alert.severity}`, {
                body: alert.message,
                icon: '/logo192.png', // Ensure this asset exists or use a default
                tag: `alert-${alert.id}` // Prevent duplicate notifications
            });
        }
    };

    useEffect(() => {
        setLoading(true);
        fetchAlerts().then(() => setLoading(false));
        const interval = setInterval(fetchAlerts, 30000); // Poll every 30s
        return () => clearInterval(interval);
    }, []);

    const handleAcknowledge = async (alertId: number) => {
        try {
            await apiClient.post(`/api/notifications/alerts/${alertId}/acknowledge`);
            // Optimistic update
            setAlerts(prev => prev.map(a => a.id === alertId ? { ...a, acknowledged: true } : a));
            setUnreadCount(prev => Math.max(0, prev - 1));
        } catch (error) {
            console.error('Failed to acknowledge alert:', error);
            fetchAlerts(); // Revert on error
        }
    };

    const handleAcknowledgeAll = async () => {
        try {
            await apiClient.post('/api/notifications/alerts/acknowledge-all');
            setAlerts(prev => prev.map(a => ({ ...a, acknowledged: true })));
            setUnreadCount(0);
        } catch (error) {
            console.error('Failed to acknowledge all:', error);
            fetchAlerts();
        }
    };

    const getSeverityColor = (severity: string) => {
        switch (severity?.toUpperCase()) {
            case 'CRITICAL': return 'text-red-500';
            case 'ERROR': return 'text-orange-500';
            case 'WARNING': return 'text-yellow-500';
            default: return 'text-blue-500';
        }
    };

    return (
        <div className="relative">
            <button
                onClick={() => setIsOpen(!isOpen)}
                className="p-2 rounded-full text-gray-400 hover:text-white hover:bg-white/5 relative transition-colors"
                title="Notifications"
            >
                <Bell size={20} />
                {unreadCount > 0 && (
                    <span className="absolute top-2 right-2 w-2 h-2 bg-red-500 rounded-full border-2 border-[#0F1218]" />
                )}
            </button>

            {isOpen && (
                <>
                    <div
                        className="fixed inset-0 z-40"
                        onClick={() => setIsOpen(false)}
                    />
                    <div className="absolute right-0 top-full mt-2 w-80 bg-[#0B0E14] border border-white/10 rounded-xl shadow-2xl z-50 overflow-hidden">
                        <div className="p-4 border-b border-white/5 flex justify-between items-center bg-[#0F1218]">
                            <h3 className="font-bold text-white">Notifications</h3>
                            {unreadCount > 0 && (
                                <button
                                    onClick={handleAcknowledgeAll}
                                    className="text-xs text-blue-500 hover:text-blue-400 transition-colors"
                                >
                                    Mark all read
                                </button>
                            )}
                        </div>

                        <div className="max-h-[400px] overflow-y-auto custom-scrollbar">
                            {loading && alerts.length === 0 ? (
                                <div className="p-8 text-center text-gray-500">Loading...</div>
                            ) : alerts.length === 0 ? (
                                <div className="p-8 text-center text-gray-500">No notifications</div>
                            ) : (
                                alerts.map((alert) => (
                                    <div
                                        key={alert.id}
                                        className={`p-4 border-b border-white/5 hover:bg-white/5 transition-colors ${!alert.acknowledged ? 'bg-white/[0.02]' : ''
                                            }`}
                                    >
                                        <div className="flex justify-between items-start mb-1">
                                            <span className={`font-bold text-sm ${getSeverityColor(alert.severity)}`}>
                                                {alert.severity}
                                            </span>
                                            <span className="text-[10px] text-gray-500">
                                                {new Date(alert.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                            </span>
                                        </div>
                                        <p className="text-sm text-gray-300 mb-2">{alert.message}</p>

                                        {!alert.acknowledged && (
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    handleAcknowledge(alert.id);
                                                }}
                                                className="text-xs text-blue-500/80 hover:text-blue-400"
                                            >
                                                Mark read
                                            </button>
                                        )}
                                    </div>
                                ))
                            )}
                        </div>

                        <div className="p-2 text-center border-t border-white/5 bg-[#0F1218]">
                            {/* Link to full settings or history if it existed */}
                            <span className="text-xs text-gray-600">
                                {Notification.permission === 'granted' ? 'Push Enabled' : 'Push Disabled'}
                            </span>
                        </div>
                    </div>
                </>
            )}
        </div>
    );
};
