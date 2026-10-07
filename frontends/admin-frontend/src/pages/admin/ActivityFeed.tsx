import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { Link } from "react-router-dom";
import { Activity } from "lucide-react";

/**
 * There is no live activity stream behind this page yet. It deliberately shows
 * no events until a real source is connected; recorded admin actions are in
 * the audit log and bot events are on the bot monitor.
 */
export default function ActivityFeed() {
    return (
        <AdminLayout>
            <div className="space-y-6">
                <div>
                    <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                        Activity Feed
                    </h1>
                    <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                        Platform activity
                    </p>
                </div>

                <div className="admin-card">
                    <div role="status" className="admin-empty-state" style={{ flexDirection: 'column', minHeight: 220 }}>
                        <Activity className="w-8 h-8" />
                        <div className="text-lg font-semibold" style={{ color: 'var(--admin-text-primary)' }}>
                            No live activity source connected
                        </div>
                        <div className="text-sm max-w-xl">
                            This page does not receive live events yet, so nothing is shown here.
                        </div>
                        <div className="flex flex-wrap items-center justify-center gap-4 text-sm">
                            <Link to="/admin/audit" className="text-blue-400 hover:text-blue-300">
                                Open Audit Logs
                            </Link>
                            <Link to="/admin/bot-monitor" className="text-blue-400 hover:text-blue-300">
                                Open Bot Monitor
                            </Link>
                        </div>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
