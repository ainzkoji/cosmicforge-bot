import {
    ResponsiveContainer,
    BarChart,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Cell,
    ReferenceLine,
} from "recharts";
import type { ReactionOut } from "@/api/eventReactionApi";

interface ChartRow {
    symbol: string;
    atr_before: number;
    atr_after: number;
    expansion: number;
}

interface Props {
    reactions: ReactionOut[];
    volSpikeThreshold?: number;
}

export function VolatilityReactionChart({ reactions, volSpikeThreshold = 2.5 }: Props) {
    const rows: ChartRow[] = reactions
        .filter((r) => r.atr_before != null && r.atr_after != null)
        .map((r) => ({
            symbol: r.symbol,
            atr_before: r.atr_before!,
            atr_after: r.atr_after!,
            expansion: r.volatility_expansion_ratio ?? r.atr_after! / r.atr_before!,
        }))
        .sort((a, b) => b.expansion - a.expansion)
        .slice(0, 10);

    if (!rows.length) {
        return (
            <div className="flex items-center justify-center h-32 text-gray-500 text-sm">
                No ATR data available
            </div>
        );
    }

    return (
        <div className="space-y-1">
            <p className="text-xs text-gray-400 font-medium">ATR Expansion by Symbol</p>
            <ResponsiveContainer width="100%" height={200}>
                <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 40, bottom: 4, left: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
                    <XAxis
                        type="number"
                        tick={{ fill: "#9ca3af", fontSize: 10 }}
                        label={{ value: "Expansion ratio (x)", position: "insideBottom", fill: "#6b7280", fontSize: 10, offset: -2 }}
                    />
                    <YAxis dataKey="symbol" type="category" tick={{ fill: "#9ca3af", fontSize: 11 }} width={56} />
                    <Tooltip
                        contentStyle={{ background: "#1e2130", border: "1px solid #374151", fontSize: 12 }}
                        formatter={(value) => [
                            typeof value === "number" ? `${value.toFixed(2)}x` : "n/a",
                            "ATR expansion",
                        ]}
                    />
                    <ReferenceLine
                        x={volSpikeThreshold}
                        stroke="#f59e0b"
                        strokeDasharray="4 2"
                        label={{ value: `${volSpikeThreshold}x`, fill: "#f59e0b", fontSize: 10 }}
                    />
                    <Bar dataKey="expansion" radius={[0, 3, 3, 0]}>
                        {rows.map((r, i) => (
                            <Cell
                                key={i}
                                fill={r.expansion >= volSpikeThreshold ? "#ef4444" : "#3b82f6"}
                            />
                        ))}
                    </Bar>
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}
