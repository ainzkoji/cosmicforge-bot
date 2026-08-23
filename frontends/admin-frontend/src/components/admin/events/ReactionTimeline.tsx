import {
    ResponsiveContainer,
    ComposedChart,
    Line,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ReferenceLine,
} from "recharts";
import type { SnapshotOut } from "@/api/eventReactionApi";

const WINDOW_ORDER = [
    "PRE_60", "PRE_30", "EVENT",
    "POST_5", "POST_15", "POST_30", "POST_60", "POST_240",
];

const WINDOW_LABELS: Record<string, string> = {
    PRE_60:   "−60m",
    PRE_30:   "−30m",
    EVENT:    "Event",
    POST_5:   "+5m",
    POST_15:  "+15m",
    POST_30:  "+30m",
    POST_60:  "+60m",
    POST_240: "+4h",
};

interface ChartPoint {
    label: string;
    price?: number | null;
    volume?: number | null;
    atr?: number | null;
    isEvent: boolean;
}

interface Props {
    snapshots: SnapshotOut[];
    symbol: string;
}

function buildChartData(snapshots: SnapshotOut[]): ChartPoint[] {
    const byWindow: Record<string, SnapshotOut> = {};
    for (const s of snapshots) {
        if (!byWindow[s.window_label] || s.timestamp_utc > byWindow[s.window_label].timestamp_utc) {
            byWindow[s.window_label] = s;
        }
    }
    return WINDOW_ORDER.map(w => ({
        label: WINDOW_LABELS[w] ?? w,
        price: byWindow[w]?.price ?? null,
        volume: byWindow[w]?.volume ?? null,
        atr: byWindow[w]?.atr ?? null,
        isEvent: w === "EVENT",
    }));
}

export function ReactionTimeline({ snapshots, symbol }: Props) {
    if (!snapshots.length) {
        return (
            <div className="flex items-center justify-center h-32 text-gray-500 text-sm">
                No snapshot data available for {symbol}
            </div>
        );
    }

    const data = buildChartData(snapshots);

    return (
        <div className="space-y-1">
            <p className="text-xs text-gray-400 font-medium">{symbol} — Price / Volume / ATR timeline</p>
            <ResponsiveContainer width="100%" height={220}>
                <ComposedChart data={data} margin={{ top: 4, right: 16, bottom: 4, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="label" tick={{ fill: "#9ca3af", fontSize: 11 }} />
                    <YAxis yAxisId="price" orientation="left" tick={{ fill: "#9ca3af", fontSize: 11 }} />
                    <YAxis yAxisId="vol" orientation="right" tick={{ fill: "#6b7280", fontSize: 10 }} />
                    <Tooltip
                        contentStyle={{ background: "#1e2130", border: "1px solid #374151", fontSize: 12 }}
                        labelStyle={{ color: "#e5e7eb" }}
                    />
                    <Legend wrapperStyle={{ fontSize: 11, color: "#9ca3af" }} />
                    <ReferenceLine
                        yAxisId="price"
                        x="Event"
                        stroke="#f59e0b"
                        strokeDasharray="4 2"
                        label={{ value: "Event", fill: "#f59e0b", fontSize: 10 }}
                    />
                    <Bar
                        yAxisId="vol"
                        dataKey="volume"
                        name="Volume"
                        fill="#3b82f6"
                        opacity={0.4}
                        radius={[2, 2, 0, 0]}
                    />
                    <Line
                        yAxisId="price"
                        type="monotone"
                        dataKey="price"
                        name="Price"
                        stroke="#22c55e"
                        strokeWidth={2}
                        dot={{ r: 3, fill: "#22c55e" }}
                        connectNulls
                    />
                    <Line
                        yAxisId="price"
                        type="monotone"
                        dataKey="atr"
                        name="ATR"
                        stroke="#f97316"
                        strokeWidth={1.5}
                        strokeDasharray="4 2"
                        dot={false}
                        connectNulls
                    />
                </ComposedChart>
            </ResponsiveContainer>
        </div>
    );
}
