import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportsAPI, type PnLSummaryResponse, type TradeStatsResponse } from '@/api/reports';
import { DollarSign, TrendingUp, TrendingDown, Activity } from 'lucide-react';

interface PnLSummaryCardsProps {
    brokerAccountId?: string;
    botInstanceId?: string;
    days?: number;
}

export const PnLSummaryCards: React.FC<PnLSummaryCardsProps> = ({
    brokerAccountId,
    botInstanceId,
    days = 30
}) => {
    // Fetch Total PnL
    const { data: pnlData, isLoading: pnlLoading } = useQuery<PnLSummaryResponse>({
        queryKey: ['reports-pnl-total', brokerAccountId, botInstanceId, days],
        queryFn: () => ReportsAPI.getTotalPnL({
            broker_account_id: brokerAccountId,
            bot_instance_id: botInstanceId,
            days
        }),
        refetchInterval: 60000
    });

    // Fetch Trade Stats (for Win Rate)
    const { data: statsData, isLoading: statsLoading } = useQuery<TradeStatsResponse>({
        queryKey: ['reports-stats-summary', brokerAccountId, botInstanceId, days],
        queryFn: () => ReportsAPI.getTradeSummary({
            broker_account_id: brokerAccountId,
            bot_instance_id: botInstanceId,
            days
        }),
        refetchInterval: 60000
    });

    const isLoading = pnlLoading || statsLoading;

    if (isLoading) {
        return (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {[...Array(4)].map((_, i) => (
                    <div key={i} className="h-32 bg-gray-100 rounded-lg animate-pulse" />
                ))}
            </div>
        );
    }

    const totalPnL = pnlData?.total_pnl || 0;
    const realizedPnL = pnlData?.realized_pnl || 0;
    const unrealizedPnL = pnlData?.unrealized_pnl || 0;
    const winRate = statsData?.win_rate || 0;
    const currency = pnlData?.currency || 'USDT';

    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Total PnL Card */}
            <div className="bg-white border border-gray-200 rounded-lg p-5 shadow-sm">
                <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-gray-500">Total P&L</h3>
                    <div className="p-2 bg-blue-50 rounded-full">
                        <DollarSign className="h-4 w-4 text-blue-600" />
                    </div>
                </div>
                <div className="flex items-baseline gap-2">
                    <span className={`text-2xl font-bold ${totalPnL >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {totalPnL >= 0 ? '+' : ''}{totalPnL.toFixed(2)}
                    </span>
                    <span className="text-sm text-gray-500">{currency}</span>
                </div>
                <p className="text-xs text-gray-400 mt-1">Realized + Unrealized</p>
            </div>

            {/* Realized PnL Card */}
            <div className="bg-white border border-gray-200 rounded-lg p-5 shadow-sm">
                <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-gray-500">Realized P&L</h3>
                    <div className="p-2 bg-green-50 rounded-full">
                        <TrendingUp className="h-4 w-4 text-green-600" />
                    </div>
                </div>
                <div className="flex items-baseline gap-2">
                    <span className={`text-2xl font-bold ${realizedPnL >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {realizedPnL >= 0 ? '+' : ''}{realizedPnL.toFixed(2)}
                    </span>
                    <span className="text-sm text-gray-500">{currency}</span>
                </div>
                <p className="text-xs text-gray-400 mt-1">Closed positions</p>
            </div>

            {/* Unrealized PnL Card */}
            <div className="bg-white border border-gray-200 rounded-lg p-5 shadow-sm">
                <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-gray-500">Unrealized P&L</h3>
                    <div className="p-2 bg-purple-50 rounded-full">
                        <Activity className="h-4 w-4 text-purple-600" />
                    </div>
                </div>
                <div className="flex items-baseline gap-2">
                    <span className={`text-2xl font-bold ${unrealizedPnL >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {unrealizedPnL >= 0 ? '+' : ''}{unrealizedPnL.toFixed(2)}
                    </span>
                    <span className="text-sm text-gray-500">{currency}</span>
                </div>
                <p className="text-xs text-gray-400 mt-1">Open positions</p>
            </div>

            {/* Win Rate Card */}
            <div className="bg-white border border-gray-200 rounded-lg p-5 shadow-sm">
                <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-gray-500">Win Rate</h3>
                    <div className="p-2 bg-yellow-50 rounded-full">
                        <Activity className="h-4 w-4 text-yellow-600" />
                    </div>
                </div>
                <div className="flex items-baseline gap-2">
                    <span className="text-2xl font-bold text-gray-900">
                        {winRate.toFixed(2)}%
                    </span>
                </div>
                <p className="text-xs text-gray-400 mt-1">{statsData?.total_trades || 0} total trades</p>
            </div>
        </div>
    );
};
