import { useState, useEffect } from "react";
import { api, Session } from "@/api/client";
import { Loader2, Laptop, Smartphone, Globe, Clock, Trash2 } from "lucide-react";

export function DeviceManagement() {
    const [sessions, setSessions] = useState<Session[]>([]);
    const [loading, setLoading] = useState(true);
    const [revokingId, setRevokingId] = useState<string | null>(null);

    useEffect(() => {
        loadSessions();
    }, []);

    const loadSessions = async () => {
        try {
            const data = await api.getSessions();
            setSessions(data.sessions);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    const handleRevoke = async (id: string) => {
        setRevokingId(id);
        try {
            await api.revokeSession(id);
            // Refresh list
            await loadSessions();
        } catch (err) {
            console.error("Failed to revoke session", err);
        } finally {
            setRevokingId(null);
        }
    };

    if (loading) return <div className="p-8 text-center"><Loader2 className="w-6 h-6 animate-spin mx-auto text-[#1E1B4B]" /></div>;

    return (
        <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden">
            <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50/50">
                <h3 className="font-semibold text-[#1E1B4B]">Active Sessions</h3>
                {/* <button className="text-red-500 text-sm font-medium hover:text-red-600">Log out all devices</button> */}
            </div>

            <div className="divide-y divide-gray-100">
                {sessions.length === 0 ? (
                    <div className="p-8 text-center text-gray-500 text-sm">No active sessions found.</div>
                ) : (
                    sessions.map((session) => {
                        const isCurrent = false; // Need logic to detect current session ID if we want to show "Current session"
                        const isMobile = session.device?.toLowerCase().includes("mobile") || session.device?.toLowerCase().includes("phone");

                        return (
                            <div key={session.id} className="p-4 flex items-center justify-between hover:bg-gray-50 transition-colors">
                                <div className="flex items-center gap-4">
                                    <div className="w-10 h-10 rounded-full bg-[#1E1B4B]/5 flex items-center justify-center">
                                        {isMobile ? <Smartphone className="w-5 h-5 text-[#1E1B4B]" /> : <Laptop className="w-5 h-5 text-[#1E1B4B]" />}
                                    </div>
                                    <div>
                                        <div className="flex items-center gap-2">
                                            <p className="font-medium text-[#1E1B4B] text-sm">
                                                {session.device || "Unknown Device"}
                                            </p>
                                            {isCurrent && (
                                                <span className="px-2 py-0.5 bg-green-100 text-green-700 text-xs rounded-full font-medium">Current</span>
                                            )}
                                        </div>
                                        <div className="flex items-center gap-4 text-xs text-gray-500 mt-1">
                                            <span className="flex items-center gap-1">
                                                <Globe className="w-3 h-3" /> {session.ip || "Unknown IP"}
                                            </span>
                                            <span className="flex items-center gap-1">
                                                <Clock className="w-3 h-3" /> {new Date(session.created_at).toLocaleDateString()}
                                            </span>
                                        </div>
                                    </div>
                                </div>

                                {!isCurrent && (
                                    <button
                                        onClick={() => handleRevoke(session.id)}
                                        disabled={revokingId === session.id}
                                        className="text-red-500 hover:text-red-700 p-2 rounded-lg hover:bg-red-50 transition-colors disabled:opacity-50"
                                    >
                                        {revokingId === session.id ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
                                    </button>
                                )}
                            </div>
                        );
                    })
                )}
            </div>
            <div className="px-6 py-4 bg-gray-50/50 text-xs text-center text-gray-500 border-t border-gray-100">
                <button className="text-[#06B6D4] hover:underline" onClick={loadSessions}>See all security activity</button>
            </div>
        </div>
    );
}
