import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportsAPI, type TradeStatsResponse } from '@/api/reports';
import { ArrowUp, ArrowDown, Minus } from 'lucide-react';

interface TradeStatsWidgetProps {
    brokerAccountId?: string;
    botInstanceId?: string;
    days?: number;
}

export const TradeStatsWidget: React.FC<TradeStatsWidgetProps> = ({
    brokerAccountId,
    botInstanceId,
    days = 30
}) => {
    const { data, isLoading } = useQuery<TradeStatsResponse>({
        queryKey: ['reports-stats-summary', brokerAccountId, botInstanceId, days],
        queryFn: () => ReportsAPI.getTradeSummary({
            broker_account_id: brokerAccountId,
            bot_instance_id: botInstanceId,
            days
        }),
        refetchInterval: 60000
    });

    if (isLoading) {
        return <div className="h-64 bg-gray-100 rounded-lg animate-pulse" />;
    }

    if (!data) return null;

    const stats = [
        { label: 'Total Trades', value: data.total_trades.toString() },
        { label: 'Profit Factor', value: data.profit_factor.toFixed(2) },
        { label: 'Avg Win', value: `$${data.avg_win.toFixed(2)}`, color: 'text-green-600' },
        { label: 'Avg Loss', value: `$${data.avg_loss.toFixed(2)}`, color: 'text-red-600' },
        { label: 'Largest Win', value: `$${data.largest_win.toFixed(2)}`, color: 'text-green-600' },
        { label: 'Largest Loss', value: `$${data.largest_loss.toFixed(2)}`, color: 'text-red-600' },
    ];

    return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-sm">
            <div className="p-4 border-b border-gray-200">
                <h3 className="text-lg font-semibold text-gray-900">Trade Statistics</h3>
            </div>

            <div className="p-4">
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
                    {stats.map((stat, i) => (
                        <div key={i} className="bg-gray-50 rounded p-3">
                            <p className="text-xs text-gray-500 uppercase">{stat.label}</p>
                            <p className={`text-lg font-bold ${stat.color || 'text-gray-900'}`}>
                                {stat.value}
                            </p>
                        </div>
                    ))}
                </div>

                <div className="space-y-4">
                    <div className="flex items-center justify-between text-sm">
                        <span className="text-gray-600">Winning Trades</span>
                        <div className="flex items-center gap-2">
                            <span className="font-medium">{data.winning_trades}</span>
                            <span className="text-gray-400">({((data.winning_trades / data.total_trades) * 100 || 0).toFixed(1)}%)</span>
                        </div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                        <div
                            className="bg-green-500 h-2 rounded-full"
                            style={{ width: `${(data.winning_trades / data.total_trades) * 100 || 0}%` }}
                        />
                    </div>

                    <div className="flex items-center justify-between text-sm">
                        <span className="text-gray-600">Losing Trades</span>
                        <div className="flex items-center gap-2">
                            <span className="font-medium">{data.losing_trades}</span>
                            <span className="text-gray-400">({((data.losing_trades / data.total_trades) * 100 || 0).toFixed(1)}%)</span>
                        </div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                        <div
                            className="bg-red-500 h-2 rounded-full"
                            style={{ width: `${(data.losing_trades / data.total_trades) * 100 || 0}%` }}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
};
