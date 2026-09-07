import React from "react";
import {
    ResponsiveContainer,
    PieChart,
    Pie,
    Cell,
    Tooltip,
} from "recharts";
import { PieChart as PieIcon, Loader2 } from "lucide-react";
import { TradingPairDataPoint } from "@/api/admin";

interface TopTradingPairsChartProps {
    data: TradingPairDataPoint[] | undefined;
    isLoading: boolean;
}

// Sleek dark theme harmonic colors for the donut slices
const DONUT_COLORS = [
    "#3B82F6", // Electric Blue
    "#10B981", // Emerald Green
    "#F59E0B", // Amber Yellow
    "#8B5CF6", // Velvet Purple
    "#06B6D4", // Cyber Cyan
];

export const TopTradingPairsChart: React.FC<TopTradingPairsChartProps> = ({
    data,
    isLoading,
}) => {
    const hasData = data && data.length > 0;
    const totalTrades = data?.reduce((acc, curr) => acc + curr.trade_count, 0) || 0;

    // Custom Glassmorphic Tooltip
    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            const item = payload[0].payload;
            return (
                <div style={{
                    background: "rgba(17, 24, 39, 0.92)",
                    backdropFilter: "blur(6px)",
                    border: "1px solid var(--admin-border-color)",
                    padding: "8px 12px",
                    borderRadius: "6px",
                    boxShadow: "0 6px 20px rgba(0, 0, 0, 0.4)",
                    fontSize: "11px",
                }}>
                    <p style={{ margin: "0 0 4px", fontWeight: 700, color: payload[0].color }}>
                        {item.symbol}
                    </p>
                    <div style={{ display: "flex", flexDirection: "column", gap: "2px", color: "var(--admin-text-secondary)" }}>
                        <div>Executions: <strong style={{ color: "var(--admin-text-primary)" }}>{item.trade_count.toLocaleString()}</strong></div>
                        <div>Share: <strong style={{ color: "var(--admin-text-primary)" }}>{item.percentage}%</strong></div>
                    </div>
                </div>
            );
        }
        return null;
    };

    return (
        <div style={{ display: "flex", flexDirection: "column", gap: "12px", minHeight: "260px" }}>
            {isLoading ? (
                <div style={{
                    flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
                    gap: "8px", height: "240px",
                }}>
                    <Loader2 className="w-8 h-8 animate-spin" style={{ color: "var(--admin-blue)" }} />
                    <span style={{ fontSize: "11px", color: "var(--admin-text-muted)" }}>Analyzing trade fills...</span>
                </div>
            ) : !hasData ? (
                <div style={{
                    flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
                    padding: "16px 12px", textAlign: "center", height: "240px",
                }}>
                    <div style={{
                        width: 80, height: 80, borderRadius: "50%", background: "rgba(255,255,255,0.02)",
                        border: "1px solid var(--admin-border-color)", display: "flex", alignItems: "center", justifyContent: "center",
                        marginBottom: "12px"
                    }}>
                        <PieIcon className="h-6 w-6" style={{ color: "var(--admin-text-muted)" }} />
                    </div>
                    <div style={{ fontSize: "12px", fontWeight: 700, color: "var(--admin-text-secondary)" }}>
                        No Traded Fills Logged
                    </div>
                    <div style={{ fontSize: "11px", color: "var(--admin-text-muted)", marginTop: "4px", maxWidth: "260px", lineHeight: "1.4" }}>
                        Ensure there are active bot executions running and completing trades in the platform.
                    </div>
                </div>
            ) : (
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: "16px" }}>
                    {/* Donut Chart Ring */}
                    <div style={{ position: "relative", width: "160px", height: "160px" }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Tooltip content={<CustomTooltip />} />
                                <Pie
                                    data={data as any[]}
                                    cx="50%"
                                    cy="50%"
                                    innerRadius={50}
                                    outerRadius={68}
                                    paddingAngle={3}
                                    dataKey="trade_count"
                                    nameKey="symbol"
                                    animationDuration={600}
                                >
                                    {data.map((_, index) => (
                                        <Cell
                                            key={`cell-${index}`}
                                            fill={DONUT_COLORS[index % DONUT_COLORS.length]}
                                            style={{ outline: "none", cursor: "pointer" }}
                                        />
                                    ))}
                                </Pie>
                            </PieChart>
                        </ResponsiveContainer>

                        {/* Centered statistics overlay inside the donut hole */}
                        <div style={{
                            position: "absolute",
                            inset: 0,
                            display: "flex",
                            flexDirection: "column",
                            alignItems: "center",
                            justifyContent: "center",
                            pointerEvents: "none",
                        }}>
                            <span style={{ fontSize: "9px", textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--admin-text-muted)", fontWeight: 700 }}>
                                Total Fills
                            </span>
                            <span style={{ fontSize: "18px", fontWeight: 800, color: "var(--admin-text-primary)", fontFamily: "monospace", marginTop: "2px" }}>
                                {totalTrades.toLocaleString()}
                            </span>
                        </div>
                    </div>

                    {/* Premium Legend with colored markers and exact value distributions */}
                    <div style={{
                        display: "flex",
                        flexWrap: "wrap",
                        gap: "10px 14px",
                        justifyContent: "center",
                        maxWidth: "340px",
                        padding: "0 10px"
                    }}>
                        {data.map((item, index) => {
                            const color = DONUT_COLORS[index % DONUT_COLORS.length];
                            return (
                                <div key={item.symbol} style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                    <div style={{
                                        width: "8px",
                                        height: "8px",
                                        borderRadius: "50%",
                                        background: color,
                                        boxShadow: `0 0 6px ${color}66`
                                    }} />
                                    <span style={{ fontSize: "11px", fontWeight: 700, color: "var(--admin-text-secondary)", fontFamily: "monospace" }}>
                                        {item.symbol}
                                    </span>
                                    <span style={{ fontSize: "10px", color: "var(--admin-text-muted)", fontFamily: "monospace" }}>
                                        ({item.percentage}%)
                                    </span>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}
        </div>
    );
};
