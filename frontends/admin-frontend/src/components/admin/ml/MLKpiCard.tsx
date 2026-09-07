import type { ReactNode } from "react";
import { StatusBadge } from "@/components/admin/ml/StatusBadge";

interface MLKpiCardProps {
    title: string;
    value: string;
    helper?: string;
    icon?: ReactNode;
    status?: string | boolean | null;
    statusLabel?: string;
}

export function MLKpiCard({
    title,
    value,
    helper,
    icon,
    status,
    statusLabel,
}: MLKpiCardProps) {
    return (
        <div className="admin-ml-kpi-card">
            <div className="admin-ml-kpi-topline">
                <div className="admin-ml-kpi-title">{title}</div>
                {icon ? <div className="admin-ml-kpi-icon">{icon}</div> : null}
            </div>
            <div className="admin-ml-kpi-value">{value}</div>
            <div className="admin-ml-kpi-footer">
                {status !== undefined ? <StatusBadge status={status} label={statusLabel} /> : null}
                {helper ? <span className="admin-ml-kpi-helper">{helper}</span> : null}
            </div>
        </div>
    );
}
