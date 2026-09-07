import React from "react";
import {
    ResponsiveContainer,
    AreaChart,
    Area,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
} from "recharts";
import { DollarSign, Loader2 } from "lucide-react";
import { RevenueOverviewItem } from "@/api/admin";

interface RevenueOverviewChartProps {
    data: RevenueOverviewItem[] | undefined;
    isLoading: boolean;
    timeframe: "30d" | "6m" | "12m";
    setTimeframe: (val: "30d" | "6m" | "12m") => void;
}

// Formatters for Currency
function formatChartCurrency(value: number) {
    return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
    }).format(value);
}

// Formatters for Dates on the X-Axis
function formatDateLabel(dateStr: string, timeframe: "30d" | "6m" | "12m") {
    if (!dateStr) return "";
    try {
        const date = new Date(dateStr);
        if (timeframe === "30d") {
            // "May 23"
            return date.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
        } else {
            // "May 2026" or "May"
            return date.toLocaleDateString("en-US", { month: "short", timeZone: "UTC" });
        }
    } catch {
        return dateStr;
    }
}

export const RevenueOverviewChart: React.FC<RevenueOverviewChartProps> = ({
    data,
    isLoading,
    timeframe,
    setTimeframe,
}) => {
    // Elegant Custom Tooltip matching the admin dashboard styling
    const CustomTooltip = ({ active, payload, label }: any) => {
        if (active && payload && payload.length) {
            const date = new Date(label);
            const formattedDate = date.toLocaleDateString("en-US", {
                year: "numeric",
                month: "long",
                day: timeframe === "30d" ? "numeric" : undefined,
                timeZone: "UTC"
            });
            return (
                <div style={{
                    background: "rgba(17, 24, 39, 0.90)",
                    backdropFilter: "blur(8px)",
                    border: "1px solid var(--admin-border-color)",
                    padding: "12px 16px",
                    borderRadius: "8px",
                    boxShadow: "0 10px 25px rgba(0, 0, 0, 0.5)",
                }}>
                    <p style={{ margin: "0 0 6px", fontSize: "11px", color: "var(--admin-text-muted)", fontWeight: 500 }}>
                        {formattedDate}
                    </p>
                    <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                        <div style={{ display: "flex", justifyContent: "space-between", gap: "24px", fontSize: "12px" }}>
                            <span style={{ color: "var(--admin-text-secondary)" }}>Subscriptions:</span>
                            <span style={{ fontWeight: 600, color: "var(--admin-text-primary)" }}>
                                {formatChartCurrency(payload[0].payload.subscription_revenue || 0)}
                            </span>
                        </div>
                        <div style={{ display: "flex", justifyContent: "space-between", gap: "24px", fontSize: "12px" }}>
                            <span style={{ color: "var(--admin-text-secondary)" }}>Commissions:</span>
                            <span style={{ fontWeight: 600, color: "var(--admin-text-primary)" }}>
                                {formatChartCurrency(payload[0].payload.commission_revenue || 0)}
                            </span>
                        </div>
                        <div style={{
                            display: "flex",
                            justifyContent: "space-between",
                            gap: "24px",
                            fontSize: "12px",
                            marginTop: "4px",
                            paddingTop: "4px",
                            borderTop: "1px solid rgba(255, 255, 255, 0.08)"
                        }}>
                            <span style={{ fontWeight: 700, color: "var(--admin-purple)" }}>Total:</span>
                            <span style={{ fontWeight: 700, color: "var(--admin-purple)" }}>
                                {formatChartCurrency(payload[0].payload.total_revenue || 0)}
                            </span>
                        </div>
                    </div>
                </div>
            );
        }
        return null;
    };

    const hasData = data && data.length > 0;

    return (
        <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
            {/* Header controls inside the component block for standalone utility */}
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: "12px" }}>
                <div>
                    <h3 style={{ margin: 0, fontSize: "13px", color: "var(--admin-text-secondary)", fontWeight: 600 }}>
                        Time-Series Financial Overview
                    </h3>
                </div>
                <div style={{ display: "flex", gap: "6px" }}>
                    {(["30d", "6m", "12m"] as const).map((t) => (
                        <button
                            key={t}
                            onClick={() => setTimeframe(t)}
                            style={{
                                background: timeframe === t ? "rgba(139, 92, 246, 0.15)" : "var(--admin-bg-secondary)",
                                border: `1px solid ${timeframe === t ? "var(--admin-purple)" : "var(--admin-border-color)"}`,
                                color: timeframe === t ? "var(--admin-purple)" : "var(--admin-text-secondary)",
                                padding: "4px 10px",
                                borderRadius: "6px",
                                fontSize: "11px",
                                fontWeight: 700,
                                cursor: "pointer",
                                transition: "all 0.15s ease",
                                textTransform: "uppercase",
                                letterSpacing: "0.02em"
                            }}
                        >
                            {t === "30d" ? "30 Days" : t === "6m" ? "6 Months" : "12 Months"}
                        </button>
                    ))}
                </div>
            </div>

            <div style={{
                position: "relative",
                height: 300,
                borderRadius: 10,
                background: "var(--admin-bg-primary)",
                border: "1px solid var(--admin-border-color)",
                overflow: "hidden",
            }}>
                {isLoading ? (
                    <div style={{
                        position: "absolute", inset: 0,
                        display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
                        gap: "12px", background: "rgba(15, 17, 23, 0.35)",
                    }}>
                        <Loader2 className="w-8 h-8 animate-spin" style={{ color: "var(--admin-purple)" }} />
                        <span style={{ fontSize: "12px", color: "var(--admin-text-muted)" }}>Fetching revenue records...</span>
                    </div>
                ) : !hasData ? (
                    <div style={{
                        position: "absolute", inset: 0,
                        display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
                        padding: "24px", textAlign: "center",
                        background: "rgba(15, 17, 23, 0.20)"
                    }}>
                        <div style={{
                            padding: "16px 24px", borderRadius: 12, maxWidth: 360,
                            background: "rgba(15, 17, 23, 0.65)",
                            border: "1px solid var(--admin-border-color)",
                            boxShadow: "0 8px 32px rgba(0, 0, 0, 0.3)",
                        }}>
                            <DollarSign className="h-8 w-8 mx-auto mb-2" style={{ color: "var(--admin-text-muted)" }} />
                            <div style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-secondary)" }}>
                                No Revenue Records Found
                            </div>
                            <div style={{ fontSize: 11, color: "var(--admin-text-muted)", marginTop: 5, lineHeight: 1.4 }}>
                                The platform currently has no paid subscription invoices or commission logs recorded. Actual transaction trends will populate automatically.
                            </div>
                        </div>
                    </div>
                ) : (
                    <div style={{ width: "100%", height: "100%", padding: "16px 12px 8px 4px" }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={data} margin={{ top: 10, right: 10, left: 10, bottom: 0 }}>
                                <defs>
                                    <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="5%" stopColor="#8B5CF6" stopOpacity={0.25} />
                                        <stop offset="95%" stopColor="#8B5CF6" stopOpacity={0.0} />
                                    </linearGradient>
                                </defs>
                                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                                <XAxis
                                    dataKey="date"
                                    tickFormatter={(val) => formatDateLabel(val, timeframe)}
                                    tick={{ fill: "var(--admin-text-muted)", fontSize: 9, fontFamily: "monospace" }}
                                    axisLine={{ stroke: "rgba(255,255,255,0.08)" }}
                                    tickLine={{ stroke: "rgba(255,255,255,0.08)" }}
                                />
                                <YAxis
                                    tickFormatter={formatChartCurrency}
                                    tick={{ fill: "var(--admin-text-muted)", fontSize: 9, fontFamily: "monospace" }}
                                    axisLine={{ stroke: "rgba(255,255,255,0.08)" }}
                                    tickLine={{ stroke: "rgba(255,255,255,0.08)" }}
                                />
                                <Tooltip content={<CustomTooltip />} />
                                <Area
                                    type="monotone"
                                    dataKey="total_revenue"
                                    stroke="#8B5CF6"
                                    strokeWidth={2}
                                    fillOpacity={1}
                                    fill="url(#colorRevenue)"
                                />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </div>
        </div>
    );
};
