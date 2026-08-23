import React, { useState, useEffect } from 'react';
import { Bell, Mail, Send, Save, Check, Smartphone, Trash2, Plus } from 'lucide-react';
import { apiClient } from '../api/client';
import {
    initializeFirebaseMessaging,
    registerFCMToken,
    getUserDevices,
    removeDevice,
    isPushNotificationSupported,
    getNotificationPermission
} from '../services/firebaseMessaging';

interface Preference {
    channel: string;
    category: string;
    is_enabled: boolean;
    min_severity: string;
}

interface Endpoint {
    channel: string;
    recipient?: string;
    status: string;
    verified_at?: string;
}

interface Device {
    deviceId: string;
    deviceName: string;
    token: string;
    status: string;
    registeredAt: string;
}

const NotificationSettings: React.FC = () => {
    const [preferences, setPreferences] = useState<Preference[]>([]);
    const [endpoints, setEndpoints] = useState<Endpoint[]>([]);
    const [devices, setDevices] = useState<Device[]>([]);
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [telegramLinkCode, setTelegramLinkCode] = useState('');
    const [telegramDeepLink, setTelegramDeepLink] = useState('');
    const [pushEnabled, setPushEnabled] = useState(false);
    const [registeringPush, setRegisteringPush] = useState(false);

    const channels = ['in_app', 'email', 'telegram', 'push'];
    const categories = ['trade', 'risk', 'system', 'marketing'];
    const severities = ['INFO', 'WARNING', 'ERROR', 'CRITICAL'];

    useEffect(() => {
        loadSettings();
    }, []);

    const loadSettings = async () => {
        try {
            setLoading(true);
            const [prefsRes, endpointsRes, devicesRes] = await Promise.all([
                apiClient.get('/api/notifications/preferences'),
                apiClient.get('/api/notifications/endpoints'),
                // Only try to fetch devices if user is logged in (implied by this page being protected)
                apiClient.get('/api/notifications/tokens').catch(() => ({ data: { devices: [] } }))
            ]);

            setPreferences(prefsRes.data.preferences || []);
            setEndpoints(endpointsRes.data.endpoints || []);
            setDevices(devicesRes.data.devices || []);
        } catch (error) {
            console.error('Failed to load settings:', error);
        } finally {
            setLoading(false);
        }
    };

    const togglePreference = (channel: string, category: string) => {
        setPreferences((prev) => {
            const existing = prev.find((p) => p.channel === channel && p.category === category);
            if (existing) {
                return prev.map((p) =>
                    p.channel === channel && p.category === category
                        ? { ...p, is_enabled: !p.is_enabled }
                        : p
                );
            } else {
                return [...prev, { channel, category, is_enabled: true, min_severity: 'INFO' }];
            }
        });
    };

    const savePreferences = async () => {
        try {
            setSaving(true);
            await apiClient.put('/api/notifications/preferences', preferences);
            alert('Settings saved successfully!');
        } catch (error) {
            console.error('Failed to save preferences:', error);
            alert('Failed to save settings');
        } finally {
            setSaving(false);
        }
    };

    const startTelegramLink = async () => {
        try {
            const response = await apiClient.post('/api/notifications/telegram/link/start');
            setTelegramLinkCode(response.data.code);
            setTelegramDeepLink(response.data.deep_link);
        } catch (error) {
            console.error('Failed to start Telegram link:', error);
        }
    };

    const getChannelIcon = (channel: string) => {
        switch (channel) {
            case 'in_app': return <Bell size={16} />;
            case 'email': return <Mail size={16} />;
            case 'telegram': return <Send size={16} />;
            default: return null;
        }
    };

    const getPreference = (channel: string, category: string): Preference | undefined => {
        return preferences.find((p) => p.channel === channel && p.category === category);
    };

    const getTelegramEndpoint = (): Endpoint | undefined => {
        return endpoints.find((e) => e.channel === 'telegram');
    };

    if (loading) {
        return <div className="p-8 text-center">Loading...</div>;
    }

    return (
        <div className="max-w-4xl mx-auto p-6">
            <h1 className="text-3xl font-bold mb-6">Notification Settings</h1>

            {/* Push Notification Configuration */}
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
                <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
                    <Smartphone size={20} /> Push Notifications
                </h2>

                <div className="flex items-center justify-between">
                    <div>
                        <p className="font-medium">Browser Notifications</p>
                        <p className="text-sm text-gray-500">
                            {isPushNotificationSupported()
                                ? getNotificationPermission() === 'granted'
                                    ? 'Enabled on this device'
                                    : 'Not enabled on this device'
                                : 'Not supported on this browser'
                            }
                        </p>
                    </div>

                    {isPushNotificationSupported() && (
                        <button
                            onClick={async () => {
                                setRegisteringPush(true);
                                try {
                                    const token = await initializeFirebaseMessaging();
                                    if (token) {
                                        // Assume user ID is available in context or storage, or pass it if needed.
                                        // For now we rely on the implementation where user ID might be extracted from token or context
                                        // Start simple
                                        alert('Push notifications enabled successfully!');
                                        loadSettings();
                                    } else {
                                        alert('Failed to enable push notifications. Check console for details.');
                                    }
                                } catch (e) {
                                    console.error(e);
                                    alert('Error enabling push: ' + e);
                                } finally {
                                    setRegisteringPush(false);
                                }
                            }}
                            disabled={getNotificationPermission() === 'granted' || registeringPush}
                            className={`px-4 py-2 rounded text-white ${getNotificationPermission() === 'granted'
                                ? 'bg-green-600 cursor-default'
                                : 'bg-blue-600 hover:bg-blue-700'
                                }`}
                        >
                            {registeringPush ? 'Enabling...' : getNotificationPermission() === 'granted' ? 'Enabled' : 'Enable Push'}
                        </button>
                    )}
                </div>

                {devices.length > 0 && (
                    <div className="mt-6">
                        <h3 className="text-lg font-medium mb-3">Active Devices</h3>
                        <div className="space-y-2">
                            {devices.map(device => (
                                <div key={device.deviceId} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-900 rounded">
                                    <div className="flex items-center gap-3">
                                        <Smartphone size={16} />
                                        <div>
                                            <p className="font-medium">{device.deviceName}</p>
                                            <p className="text-xs text-gray-500">
                                                Added: {new Date(device.registeredAt).toLocaleDateString()}
                                            </p>
                                        </div>
                                    </div>
                                    <button
                                        onClick={async () => {
                                            if (confirm('Remove this device?')) {
                                                await removeDevice(device.deviceId);
                                                loadSettings();
                                            }
                                        }}
                                        className="text-red-500 hover:text-red-700"
                                    >
                                        <Trash2 size={16} />
                                    </button>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>

            {/* Telegram Configuration */}
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
                <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
                    <Send size={20} /> Telegram
                </h2>

                {getTelegramEndpoint()?.verified_at ? (
                    <div className="flex items-center gap-2 text-green-600">
                        <Check size={20} />
                        <span>Connected since {new Date(getTelegramEndpoint()!.verified_at!).toLocaleString()}</span>
                    </div>
                ) : (
                    <div>
                        <button
                            onClick={startTelegramLink}
                            className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
                        >
                            Connect Telegram
                        </button>

                        {telegramLinkCode && (
                            <div className="mt-4 p-4 bg-blue-50 dark:bg-blue-900/20 rounded">
                                <p className="font-semibold mb-2">Follow these steps:</p>
                                <ol className="list-decimal list-inside space-y-2">
                                    <li>Click the link below or search for the bot on Telegram</li>
                                    <li>Send the command: <code className="bg-gray-200 dark:bg-gray-700 px-2 py-1 rounded">/start {telegramLinkCode}</code></li>
                                    <li>Wait for confirmation</li>
                                </ol>
                                <a
                                    href={telegramDeepLink}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="inline-block mt-4 bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
                                >
                                    Open Telegram
                                </a>
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* Preferences Grid */}
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
                <h2 className="text-xl font-semibold mb-4">Notification Preferences</h2>

                <div className="overflow-x-auto">
                    <table className="w-full">
                        <thead>
                            <tr className="border-b dark:border-gray-700">
                                <th className="text-left p-3">Category</th>
                                {channels.map((channel) => (
                                    <th key={channel} className="text-center p-3">
                                        <div className="flex items-center justify-center gap-2">
                                            {getChannelIcon(channel)}
                                            <span className="capitalize">{channel.replace('_', ' ')}</span>
                                        </div>
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {categories.map((category) => (
                                <tr key={category} className="border-b dark:border-gray-700">
                                    <td className="p-3 font-medium capitalize">{category}</td>
                                    {channels.map((channel) => {
                                        const pref = getPreference(channel, category);
                                        const isEnabled = pref?.is_enabled ?? false;
                                        return (
                                            <td key={`${channel}-${category}`} className="text-center p-3">
                                                <input
                                                    type="checkbox"
                                                    checked={isEnabled}
                                                    onChange={() => togglePreference(channel, category)}
                                                    className="w-5 h-5"
                                                />
                                            </td>
                                        );
                                    })}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>

                <div className="mt-6 flex justify-end">
                    <button
                        onClick={savePreferences}
                        disabled={saving}
                        className="bg-green-600 text-white px-6 py-2 rounded hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
                    >
                        <Save size={16} />
                        {saving ? 'Saving...' : 'Save Preferences'}
                    </button>
                </div>
            </div>
        </div>
    );
};

export default NotificationSettings;
