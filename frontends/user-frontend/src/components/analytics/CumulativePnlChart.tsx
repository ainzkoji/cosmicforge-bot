import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, type PositionRecord } from '@/api/client';
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
} from 'recharts';
import { TrendingUp } from 'lucide-react';

interface ChartPoint {
    date: string;
    cumPnl: number;
}

function buildChartData(items: PositionRecord[]): ChartPoint[] {
    let cumulative = 0;
    return items.map(pos => {
        cumulative = Math.round((cumulative + (pos.net_pnl ?? 0)) * 100) / 100;
        const raw = pos.closed_at ?? pos.opened_at ?? '';
        const date = raw.length >= 10 ? raw.slice(0, 10) : raw;
        return { date, cumPnl: cumulative };
    });
}

function formatTick(val: number): string {
    if (Math.abs(val) >= 1000) return `$${(val / 1000).toFixed(1)}k`;
    return `$${val}`;
}

interface Props {
    timeframe?: string;
    darkBackground?: boolean;
}

export function CumulativePnlChart({ timeframe = 'ALL', darkBackground = false }: Props) {
    const CHART_HEIGHT = 256;
    const containerRef = useRef<HTMLDivElement | null>(null);
    const [measuredWidth, setMeasuredWidth] = useState(0);

    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;

        let rafId: number | null = null;
        const update = () => {
            if (rafId != null) cancelAnimationFrame(rafId);
            rafId = requestAnimationFrame(() => {
                const rect = el.getBoundingClientRect();
                const next = Math.max(0, Math.floor(rect.width));
                setMeasuredWidth(next);
            });
        };

        update();

        if (typeof ResizeObserver === 'undefined') {
            const onResize = () => update();
            window.addEventListener('resize', onResize);
            return () => {
                window.removeEventListener('resize', onResize);
                if (rafId != null) cancelAnimationFrame(rafId);
            };
        }

        const observer = new ResizeObserver(() => update());
        observer.observe(el);
        return () => {
            observer.disconnect();
            if (rafId != null) cancelAnimationFrame(rafId);
        };
    }, []);

    const { data, isLoading, isError } = useQuery({
        queryKey: ['cumulative-pnl-chart', timeframe],
        queryFn: () => api.getPositionHistory(
            timeframe,
            'closed',
            1,
            200,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            'closed_at',
            'ASC',
        ),
        staleTime: 60_000,
    });

    const items = data?.items ?? [];
    const chartData = useMemo(() => buildChartData(items), [items]);
    const lastPnl = chartData[chartData.length - 1]?.cumPnl ?? 0;
    const isPositive = lastPnl >= 0;
    const lineColor = isPositive ? '#22c55e' : '#ef4444';

    const gridColor = darkBackground ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.08)';
    const labelColor = darkBackground ? '#6b7280' : '#9ca3af';
    const tooltipBg = darkBackground ? '#1a1f2e' : '#ffffff';
    const tooltipBorder = darkBackground ? '#2d3748' : '#e5e7eb';

    return (
        <div ref={containerRef} className="w-full min-w-0" style={{ minHeight: CHART_HEIGHT }}>
            {isLoading ? (
                <div className="h-[256px] flex items-center justify-center">
                    <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-primary" />
                </div>
            ) : isError ? (
                <div className="h-[256px] flex items-center justify-center text-sm text-muted-foreground">
                    Failed to load chart data.
                </div>
            ) : chartData.length === 0 ? (
                <div className="h-[256px] flex flex-col items-center justify-center gap-2 text-muted-foreground">
                    <TrendingUp className="w-8 h-8 opacity-20" />
                <p className="text-sm">Cumulative closed-trade net PnL will appear once closed trades are recorded.</p>
                </div>
            ) : measuredWidth <= 0 ? (
                <div className="h-[256px] flex items-center justify-center text-sm text-muted-foreground">
                    Loading chart…
                </div>
            ) : (
                <LineChart
                    width={measuredWidth}
                    height={CHART_HEIGHT}
                    data={chartData}
                    margin={{ top: 8, right: 8, left: 0, bottom: 0 }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
                    <XAxis
                        dataKey="date"
                        tick={{ fontSize: 10, fill: labelColor }}
                        tickLine={false}
                        axisLine={false}
                        interval="preserveStartEnd"
                    />
                    <YAxis
                        tick={{ fontSize: 10, fill: labelColor }}
                        tickLine={false}
                        axisLine={false}
                        tickFormatter={formatTick}
                        width={52}
                    />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: tooltipBg,
                            border: `1px solid ${tooltipBorder}`,
                            borderRadius: '8px',
                            fontSize: '12px',
                        }}
                    formatter={(v: number | undefined) => [`$${(v ?? 0).toFixed(2)}`, 'Cumulative closed-trade Net PnL']}
                        labelStyle={{ color: labelColor }}
                        cursor={{ stroke: lineColor, strokeWidth: 1, strokeDasharray: '4 2' }}
                    />
                    <Line
                        type="monotone"
                        dataKey="cumPnl"
                        stroke={lineColor}
                        strokeWidth={2}
                        dot={false}
                        activeDot={{ r: 4, strokeWidth: 0, fill: lineColor }}
                    />
                </LineChart>
            )}
        </div>
    );
}
