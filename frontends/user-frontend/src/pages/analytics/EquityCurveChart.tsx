import React from 'react';
import {
    AreaChart,
    Area,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer
} from 'recharts';

import { EquityCurvePoint } from '@/api/client';

interface EquityCurveChartProps {
    data: EquityCurvePoint[];
    isLoading?: boolean;
}

export const EquityCurveChart: React.FC<EquityCurveChartProps> = ({ data, isLoading }) => {
    if (isLoading) {
        return (
            <div className="h-80 w-full flex items-center justify-center bg-muted/10 rounded-lg animate-pulse">
                <div className="text-muted-foreground">Loading chart...</div>
            </div>
        );
    }

    if (!data || data.length === 0) {
        return (
            <div className="h-80 w-full flex items-center justify-center bg-muted/10 rounded-lg border border-dashed border-border">
                <div className="text-muted-foreground">No equity data available</div>
            </div>
        );
    }

    // Format data for chart
    const chartData = data.map(point => ({
        ...point,
        formattedDate: new Date(point.timestamp).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }),
        fullDate: new Date(point.timestamp).toLocaleString()
    }));

    // Calculate domain for Y-axis to look nice (zoom in on the action)
    const minEquity = Math.min(...data.map(d => d.equity));
    const maxEquity = Math.max(...data.map(d => d.equity));
    const padding = (maxEquity - minEquity) * 0.1;

    return (
        <div className="h-80 w-full">
            <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                    data={chartData}
                    margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                >
                    <defs>
                        <linearGradient id="colorEquity" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                        </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} opacity={0.3} />
                    <XAxis
                        dataKey="formattedDate"
                        stroke="#888888"
                        fontSize={12}
                        tickLine={false}
                        axisLine={false}
                        minTickGap={30}
                    />
                    <YAxis
                        stroke="#888888"
                        fontSize={12}
                        tickLine={false}
                        axisLine={false}
                        tickFormatter={(value) => `$${value.toLocaleString()}`}
                        domain={[minEquity - padding, maxEquity + padding]}
                        width={60}
                    />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: '#1f2937',
                            borderColor: '#374151',
                            color: '#f3f4f6',
                            borderRadius: '0.5rem'
                        }}
                        itemStyle={{ color: '#60a5fa' }}
                        labelStyle={{ color: '#9ca3af', marginBottom: '0.25rem' }}
                        formatter={(value: any) => [`$${Number(value).toLocaleString()}`, 'Equity']}
                        labelFormatter={(label, payload) => {
                            if (payload && payload.length > 0) {
                                return payload[0].payload.fullDate;
                            }
                            return label;
                        }}
                    />
                    <Area
                        type="monotone"
                        dataKey="equity"
                        stroke="#3b82f6"
                        strokeWidth={2}
                        fillOpacity={1}
                        fill="url(#colorEquity)"
                    />
                </AreaChart>
            </ResponsiveContainer>
        </div>
    );
};
