import { useState } from "react";
import { AlertBanner } from "@/components/admin/ml/AlertBanner";
import { StatusBadge } from "@/components/admin/ml/StatusBadge";
import type { MLAlertItem } from "@/api/admin";

interface MLAlertPanelProps {
    alerts: MLAlertItem[];
    defaultExpanded?: number;
}

const severityRank: Record<string, number> = {
    danger: 0,
    warning: 1,
    info: 2,
    success: 3,
};

function toneForLevel(level: string) {
    if (level === "danger") {
        return "danger" as const;
    }
    if (level === "warning") {
        return "warning" as const;
    }
    return "info" as const;
}

export function MLAlertPanel({ alerts, defaultExpanded = 3 }: MLAlertPanelProps) {
    const [showAll, setShowAll] = useState(false);
    const sortedAlerts = [...alerts].sort((left, right) => (severityRank[left.level] ?? 99) - (severityRank[right.level] ?? 99));
    const primaryAlert = sortedAlerts[0];
    const secondaryAlerts = sortedAlerts.slice(1, Math.max(defaultExpanded, 2));
    const additionalAlerts = sortedAlerts.slice(Math.max(defaultExpanded, 2));
    const criticalCount = sortedAlerts.filter((item) => item.level === "danger").length;
    const warningCount = sortedAlerts.filter((item) => item.level === "warning").length;
    const infoCount = sortedAlerts.filter((item) => item.level === "info" || item.level === "success").length;

    if (!sortedAlerts.length) {
        return (
            <div className="admin-ml-alert-panel">
                <div className="admin-ml-alert-header">
                    <div>
                        <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                            Critical Alerts
                        </div>
                        <div className="admin-ml-muted">No active ML alerts are firing right now.</div>
                    </div>
                    <StatusBadge status="healthy" label="Stable" />
                </div>
                <div className="admin-ml-alert-rail">
                    <AlertBanner
                        title="Monitoring signals look stable"
                        body="No major ML warnings are currently active. Continue watching readiness, linkage, and recent scoring activity."
                        tone="info"
                        compact
                    />
                </div>
            </div>
        );
    }

    return (
        <div className="admin-ml-alert-panel">
            <div className="admin-ml-alert-header">
                <div>
                    <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                        Critical Alerts
                    </div>
                    <div className="admin-ml-muted">The highest-priority ML signals are expanded first.</div>
                </div>
                <div className="flex flex-wrap gap-2">
                    <StatusBadge status={criticalCount > 0 ? "danger" : "healthy"} label={`Critical ${criticalCount}`} />
                    <StatusBadge status={warningCount > 0 ? "warning" : "healthy"} label={`Warning ${warningCount}`} />
                    <StatusBadge status="info" label={`Info ${infoCount}`} />
                </div>
            </div>

            <div className="admin-ml-alert-rail">
                {primaryAlert ? (
                    <AlertBanner
                        key={primaryAlert.code}
                        title={primaryAlert.title}
                        body={primaryAlert.body}
                        tone={toneForLevel(primaryAlert.level)}
                        compact
                    />
                ) : null}

                {secondaryAlerts.length ? (
                    <div className="admin-ml-alert-grid">
                        {secondaryAlerts.map((alert) => (
                            <AlertBanner
                                key={alert.code}
                                title={alert.title}
                                body={alert.body}
                                tone={toneForLevel(alert.level)}
                                compact
                            />
                        ))}
                    </div>
                ) : null}
            </div>

            {additionalAlerts.length ? (
                <div className="admin-ml-alert-more">
                    <button
                        type="button"
                        className="admin-btn admin-btn-secondary"
                        onClick={() => setShowAll((current) => !current)}
                    >
                        {showAll ? "Hide extra alerts" : `More alerts (${additionalAlerts.length})`}
                    </button>

                    {showAll ? (
                        <div className="admin-ml-alert-list admin-ml-alert-list-extra">
                            {additionalAlerts.map((alert) => (
                                <AlertBanner
                                    key={alert.code}
                                    title={alert.title}
                                    body={alert.body}
                                    tone={toneForLevel(alert.level)}
                                    compact
                                />
                            ))}
                        </div>
                    ) : null}
                </div>
            ) : null}
        </div>
    );
}
