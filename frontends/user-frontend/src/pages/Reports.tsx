import React, { useState } from 'react';
import { PnLSummaryCards } from '@/components/reports/PnLSummaryCards';
import { TradeStatsWidget } from '@/components/reports/TradeStatsWidget';
import { TaxReportExport } from '@/components/reports/TaxReportExport';
import EquityCurve from '@/components/analytics/EquityCurve'; // Enhanced existing component
import { BarChart2, Calendar } from 'lucide-react';

export const ReportsDashboard: React.FC = () => {
    const [days, setDays] = useState(30);
    const [brokerAccountId, setBrokerAccountId] = useState<string | undefined>(undefined);

    // TODO: Add broker account selector using api/brokers

    return (
        <div className="space-y-6 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">Reports & Analytics</h1>
                    <p className="text-gray-500 mt-1">Performance metrics, P&L analysis, and tax reporting.</p>
                </div>

                {/* Global Filter Controls */}
                <div className="flex items-center gap-3 bg-white p-2 rounded-lg border shadow-sm">
                    <Calendar className="h-4 w-4 text-gray-500 ml-2" />
                    <select
                        value={days}
                        onChange={(e) => setDays(Number(e.target.value))}
                        className="bg-transparent border-none text-sm font-medium focus:ring-0 cursor-pointer"
                    >
                        <option value={7}>Last 7 days</option>
                        <option value={30}>Last 30 days</option>
                        <option value={90}>Last 90 days</option>
                        <option value={180}>Last 180 days</option>
                        <option value={365}>Last 365 days</option>
                    </select>
                </div>
            </div>

            {/* PnL Summary Cards */}
            <PnLSummaryCards
                brokerAccountId={brokerAccountId}
                days={days}
            />

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Main Equity Chart (2/3 width) */}
                <div className="lg:col-span-2 space-y-6">
                    <div className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                        <h2 className="text-lg font-semibold mb-4 px-2">Equity Performance</h2>
                        <EquityCurve
                            brokerAccountId={brokerAccountId}
                            defaultDays={days}
                        />
                    </div>
                </div>

                {/* Side Widgets (1/3 width) */}
                <div className="space-y-6">
                    <TradeStatsWidget
                        brokerAccountId={brokerAccountId}
                        days={days}
                    />

                    <TaxReportExport
                        brokerAccountId={brokerAccountId}
                    />
                </div>
            </div>

            {/* Future: Benchmarking and detailed tables */}
        </div>
    );
};
