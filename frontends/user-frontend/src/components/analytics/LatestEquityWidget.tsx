/**
 * Latest Equity Widget
 * 
 * Displays current equity across all broker accounts
 * Compact widget for dashboard/header display
 */
import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, type LatestEquityResponse } from '@/api/client';
import { DollarSign, TrendingUp, Wallet } from 'lucide-react';

export const LatestEquityWidget: React.FC = () => {
    const { data, isLoading } = useQuery<LatestEquityResponse>({
        queryKey: ['equity-latest'],
        queryFn: () => api.getLatestEquity(),
        refetchInterval: 30000, // Refresh every 30 seconds
        staleTime: 15000
    });

    if (isLoading || !data) {
        return (
            <div className="bg-white border border-gray-200 rounded-lg p-4 animate-pulse">
                <div className="h-4 bg-gray-200 rounded w-24 mb-2"></div>
                <div className="h-8 bg-gray-200 rounded w-32"></div>
            </div>
        );
    }

    const isPositivePnl = data.total_unrealized_pnl >= 0;

    return (
        <div className="bg-gradient-to-br from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-6">
            <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                    <DollarSign className="h-5 w-5 text-blue-600" />
                    <span className="text-sm font-medium text-gray-700">Total Equity</span>
                </div>
                <span className="text-xs text-gray-500">{data.account_count} accounts</span>
            </div>

            <div className="space-y-3">
                <div>
                    <p className="text-3xl font-bold text-gray-900">
                        ${data.total_equity.toFixed(2)}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">{data.currency}</p>
                </div>

                <div className="flex items-center gap-2">
                    <TrendingUp className={`h-4 w-4 ${isPositivePnl ? 'text-green-500' : 'text-red-500'}`} />
                    <span className={`text-sm font-semibold ${isPositivePnl ? 'text-green-600' : 'text-red-600'
                        }`}>
                        {isPositivePnl ? '+' : ''}${data.total_unrealized_pnl.toFixed(2)} Unrealized
                    </span>
                </div>

                {/* Per-Account Breakdown */}
                {data.accounts.length > 1 && (
                    <div className="pt-3 border-t border-blue-200">
                        <p className="text-xs font-medium text-gray-600 mb-2">Breakdown</p>
                        <div className="space-y-1">
                            {data.accounts.map((account) => (
                                <div key={account.broker_account_id} className="flex items-center justify-between text-xs">
                                    <span className="text-gray-600 capitalize">{account.broker_id}</span>
                                    <span className="font-medium text-gray-900">${account.equity.toFixed(2)}</span>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};

export default LatestEquityWidget;
