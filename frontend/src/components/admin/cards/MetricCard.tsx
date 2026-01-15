import { ReactNode } from "react";
import { TrendingUp, TrendingDown } from "lucide-react";

interface MetricCardProps {
    title: string;
    value: string | number;
    change?: {
        value: string;
        positive: boolean;
    };
    icon?: ReactNode;
}

export function MetricCard({ title, value, change, icon }: MetricCardProps) {
    return (
        <div className="admin-metric-card">
            <div className="flex items-start justify-between mb-4">
                <div className="admin-metric-label">{title}</div>
                {icon && (
                    <div className="p-2 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                        {icon}
                    </div>
                )}
            </div>

            <div className="admin-metric-value mb-2">
                {value}
            </div>

            {change && (
                <div className={`admin-metric-change flex items-center gap-1 ${change.positive ? 'positive' : 'negative'}`}>
                    {change.positive ? (
                        <TrendingUp className="w-4 h-4" />
                    ) : (
                        <TrendingDown className="w-4 h-4" />
                    )}
                    <span>{change.value}</span>
                </div>
            )}
        </div>
    );
}
