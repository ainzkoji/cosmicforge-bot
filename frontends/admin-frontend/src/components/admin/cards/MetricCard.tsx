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
    accent?: string; // hex color — adds top border stripe + tinted icon bg
}

export function MetricCard({ title, value, change, icon, accent }: MetricCardProps) {
    return (
        <div
            className="admin-metric-card"
            style={accent ? { borderTop: `3px solid ${accent}` } : undefined}
        >
            <div className="flex items-start justify-between mb-3">
                <div className="admin-metric-label">{title}</div>
                {icon && (
                    <div
                        className="p-2 rounded-lg flex-shrink-0"
                        style={{
                            background: accent
                                ? `color-mix(in srgb, ${accent} 14%, var(--admin-bg-hover))`
                                : "var(--admin-bg-hover)",
                        }}
                    >
                        {icon}
                    </div>
                )}
            </div>

            <div className="admin-metric-value mb-2">{value}</div>

            {change && (
                <div className={`admin-metric-change flex items-center gap-1 ${change.positive ? "positive" : "negative"}`}>
                    {change.positive ? (
                        <TrendingUp className="w-3 h-3" />
                    ) : (
                        <TrendingDown className="w-3 h-3" />
                    )}
                    <span>{change.value} vs last period</span>
                </div>
            )}
        </div>
    );
}
