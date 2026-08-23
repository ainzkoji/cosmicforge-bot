/**
 * Equity Curve Chart Component
 * 
 * Displays time-series equity data from broker accounts
 * with filtering by broker account, bot instance, and time period
 */
import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportsAPI, EquityCurve as EquityCurveType } from '@/api/reports';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { TrendingUp, TrendingDown, DollarSign, Calendar } from 'lucide-react';

interface EquityDataPoint {
    timestamp: string;
    equity: number;
    wallet_balance: number;
    available_balance: number;
    unrealized_pnl: number;
    broker_id: string;
    source: string;
}

interface EquityCurveData {
    data: EquityDataPoint[];
    count: number;
    broker_account_id?: string;
    bot_instance_id?: string;
    period_days: number;
    currency: string;
}

interface EquityCurveProps {
    brokerAccountId?: string;
    botInstanceId?: string;
    defaultDays?: number;
}

export const EquityCurve: React.FC<EquityCurveProps> = ({
    brokerAccountId,
    botInstanceId,
    defaultDays = 30
}) => {
    const [days, setDays] = useState(defaultDays);

    const { data, isLoading, error } = useQuery<EquityCurveType>({
        queryKey: ['reports-equity-curve', brokerAccountId, days],
        queryFn: () => ReportsAPI.getEquityCurve({
            broker_account_id: brokerAccountId,
            days
        }),
        refetchInterval: 60000, // Refresh every minute
        staleTime: 30000
    });

    if (isLoading) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                <p className="text-red-700">Failed to load equity curve data</p>
            </div>
        );
    }

    if (!data || data.data.length === 0) {
        return (
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
                <TrendingUp className="mx-auto h-12 w-12 text-gray-400 mb-3" />
                <p className="text-gray-600">No equity data available</p>
                <p className="text-sm text-gray-500 mt-1">Start a bot to begin tracking equity</p>
            </div>
        );
    }

    // Calculate stats
    const firstPoint = data.data.length > 0 ? data.data[0] : null;
    const lastPoint = data.data.length > 0 ? data.data[data.data.length - 1] : null;

    // Fallbacks if no data
    if (!firstPoint || !lastPoint) return null;

    const equityChange = lastPoint.equity - firstPoint.equity;
    const equityChangePercent = firstPoint.equity !== 0 ? ((equityChange / firstPoint.equity) * 100).toFixed(2) : "0.00";
    const isPositive = equityChange >= 0;

    // Format data for recharts
    const chartData = data.data.map(point => ({
        time: new Date(point.timestamp).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        }),
        equity: point.equity,
        unrealized: point.unrealized_pnl
    }));

    return (
        <div className="space-y-4">
            {/* Header with Stats */}
            <div className="flex items-center justify-between">
                <div>
                    <h3 className="text-lg font-semibold text-gray-900">Equity Curve</h3>
                    <p className="text-sm text-gray-500">{data.data.length} data points</p>
                </div>

                {/* Time Period Selector */}
                <div className="flex gap-2">
                    {[7, 14, 30, 90].map((period) => (
                        <button
                            key={period}
                            onClick={() => setDays(period)}
                            className={`px-3 py-1 text-sm rounded-md transition-colors ${days === period
                                ? 'bg-blue-500 text-white'
                                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                                }`}
                        >
                            {period}d
                        </button>
                    ))}
                </div>
            </div>

            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-white border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-gray-600 mb-1">
                        <DollarSign className="h-4 w-4" />
                        <span>Current Equity</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900">
                        ${lastPoint.equity.toFixed(2)}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">{data.currency}</p>
                </div>

                <div className="bg-white border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-gray-600 mb-1">
                        {isPositive ? (
                            <TrendingUp className="h-4 w-4 text-green-500" />
                        ) : (
                            <TrendingDown className="h-4 w-4 text-red-500" />
                        )}
                        <span>P&L ({days}d)</span>
                    </div>
                    <p className={`text-2xl font-bold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                        {isPositive ? '+' : ''}${equityChange.toFixed(2)}
                    </p>
                    <p className={`text-xs mt-1 ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                        {isPositive ? '+' : ''}{equityChangePercent}%
                    </p>
                </div>

                <div className="bg-white border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-gray-600 mb-1">
                        <Calendar className="h-4 w-4" />
                        <span>Unrealized P&L</span>
                    </div>
                    <p className={`text-2xl font-bold ${lastPoint.unrealized_pnl >= 0 ? 'text-green-600' : 'text-red-600'
                        }`}>
                        {lastPoint.unrealized_pnl >= 0 ? '+' : ''}${lastPoint.unrealized_pnl.toFixed(2)}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">Open positions</p>
                </div>
            </div>

            {/* Chart */}
            <div className="bg-white border border-gray-200 rounded-lg p-4">
                <ResponsiveContainer width="100%" height={400}>
                    <LineChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis
                            dataKey="time"
                            stroke="#6b7280"
                            fontSize={12}
                            tickMargin={10}
                        />
                        <YAxis
                            stroke="#6b7280"
                            fontSize={12}
                            tickFormatter={(value) => `$${value}`}
                            domain={['auto', 'auto']}
                        />
                        <Tooltip
                            contentStyle={{
                                backgroundColor: 'white',
                                border: '1px solid #e5e7eb',
                                borderRadius: '8px',
                                padding: '12px'
                            }}
                            formatter={(value: number | undefined) => [value != null ? `$${value.toFixed(2)}` : 'N/A', '']}
                            labelStyle={{ fontWeight: 600, marginBottom: '8px' }}
                        />
                        <Legend
                            wrapperStyle={{ paddingTop: '20px' }}
                            iconType="line"
                        />
                        <Line
                            type="monotone"
                            dataKey="equity"
                            stroke="#3b82f6"
                            strokeWidth={2}
                            dot={false}
                            name="Equity"
                            activeDot={{ r: 6 }}
                        />
                        <Line
                            type="monotone"
                            dataKey="unrealized"
                            stroke="#10b981"
                            strokeWidth={1.5}
                            strokeDasharray="5 5"
                            dot={false}
                            name="Unrealized P&L"
                        />
                    </LineChart>
                </ResponsiveContainer>
            </div>

            {/* Source Info */}
            <div className="text-xs text-gray-500 text-center">
                Data sources: Bot cycles, position closes, daily snapshots
            </div>
        </div>
    );
};

export default EquityCurve;
