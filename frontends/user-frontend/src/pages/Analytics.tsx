
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion'; // Assuming framer-motion is used
import { useSearchParams } from 'react-router-dom';
import { useAnalyticsStream } from '@/hooks/useAnalyticsStream'; // Keep streaming hook
import { api } from '@/api/client';
import {
    RefreshCw, Printer, Download, FileText, BarChart3, TrendingUp, Landmark,
    List, Loader2 // New icon for Trades
} from 'lucide-react';

// Sub-page components
import { AnalyticsOverview } from '@/pages/analytics/AnalyticsOverview';
import { TradesView } from '@/pages/analytics/TradesView';
import { Benchmarks } from '@/pages/analytics/Benchmarks';
import { TaxReport } from '@/pages/analytics/TaxReport';

const VALID_TABS = ['overview', 'trades', 'benchmarks', 'tax'] as const;
type AnalyticsTab = typeof VALID_TABS[number];

export default function Analytics() {
    const [searchParams] = useSearchParams();
    const tabParam = searchParams.get('tab');
    const initialTab: AnalyticsTab = VALID_TABS.includes(tabParam as AnalyticsTab)
        ? (tabParam as AnalyticsTab)
        : 'overview';

    const [timeframe, setTimeframe] = useState<'1M' | '3M' | 'YTD' | 'ALL'>('YTD');
    const [activeTab, setActiveTab] = useState<AnalyticsTab>(initialTab);
    const [isExporting, setIsExporting] = useState(false);
    const [showExportMenu, setShowExportMenu] = useState(false);
    
    // Real-time SSE streaming for instant updates (global listener)
    useAnalyticsStream();

    const handleExport = async (format: string) => {
        try {
            setIsExporting(true);
            setShowExportMenu(false);
            const blob = await api.exportAnalytics(timeframe, format);
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `analytics_${timeframe}_${new Date().toISOString().slice(0, 10)}.${format}`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            console.error('Export failed:', error);
        } finally {
            setIsExporting(false);
        }
    };

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="max-w-[1600px] mx-auto space-y-8"
        >
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Analytics & Reports</h1>
                    <div className="flex items-center gap-2 text-muted-foreground">
                        <p>Comprehensive insights into your trading performance.</p>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <div className="flex bg-muted rounded-lg p-1">
                        {(['1M', '3M', 'YTD', 'ALL'] as const).map((tf) => (
                            <button
                                key={tf}
                                onClick={() => setTimeframe(tf)}
                                className={`px-3 py-1 rounded-md text-sm font-medium transition-all ${timeframe === tf
                                    ? 'bg-background shadow text-foreground'
                                    : 'text-muted-foreground hover:text-foreground'
                                    }`}
                            >
                                {tf}
                            </button>
                        ))}
                    </div>
                    
                    <button
                        onClick={() => window.print()}
                        className="flex items-center gap-2 px-4 py-2 bg-card border border-border rounded-lg hover:bg-muted transition-colors"
                    >
                        <Printer className="w-4 h-4" />
                        <span>Print</span>
                    </button>
                    <div className="relative">
                        <button
                            onClick={() => setShowExportMenu(!showExportMenu)}
                            disabled={isExporting}
                            className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg shadow hover:shadow-lg transition-all disabled:opacity-70"
                        >
                            {isExporting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
                            <span>Export</span>
                        </button>

                        {showExportMenu && (
                            <>
                                <div className="fixed inset-0 z-10" onClick={() => setShowExportMenu(false)} />
                                <div className="absolute right-0 mt-2 w-48 bg-card border border-border rounded-lg shadow-xl z-20 overflow-hidden">
                                    <button onClick={() => handleExport('csv')} className="w-full text-left px-4 py-3 hover:bg-muted text-sm transition-colors flex items-center gap-2">
                                        <FileText className="w-4 h-4 text-muted-foreground" />
                                        <span>Export as CSV</span>
                                    </button>
                                    <button onClick={() => handleExport('xlsx')} className="w-full text-left px-4 py-3 hover:bg-muted text-sm transition-colors flex items-center gap-2">
                                        <BarChart3 className="w-4 h-4 text-muted-foreground" />
                                        <span>Export as Excel</span>
                                    </button>
                                    <button onClick={() => handleExport('pdf')} className="w-full text-left px-4 py-3 hover:bg-muted text-sm transition-colors flex items-center gap-2">
                                        <Printer className="w-4 h-4 text-muted-foreground" />
                                        <span>Export as PDF</span>
                                    </button>
                                </div>
                            </>
                        )}
                    </div>
                </div>
            </div>

            {/* Navigation Tabs */}
            <div className="border-b border-border">
                <div className="flex gap-8">
                    {[
                        { id: 'overview', label: 'Overview', icon: TrendingUp },
                        { id: 'trades', label: 'Trade History', icon: List },
                        { id: 'benchmarks', label: 'Benchmarks', icon: BarChart3 },
                        { id: 'tax', label: 'Tax Reporting', icon: Landmark },
                    ].map((tab) => (
                        <button
                            key={tab.id}
                            onClick={() => setActiveTab(tab.id as any)}
                            className={`flex items-center gap-2 pb-4 text-sm font-medium transition-colors border-b-2 ${activeTab === tab.id
                                ? 'border-primary text-primary'
                                : 'border-transparent text-muted-foreground hover:text-foreground'
                                }`}
                        >
                            <tab.icon className="w-4 h-4" />
                            {tab.label}
                        </button>
                    ))}
                </div>
            </div>

            {/* Content Areas */}
            <div className="min-h-[500px]">
                {activeTab === 'overview' && <AnalyticsOverview timeframe={timeframe} />}
                {activeTab === 'trades' && <TradesView timeframe={timeframe} />}
                {activeTab === 'benchmarks' && <Benchmarks timeframe={timeframe} />}
                {activeTab === 'tax' && <TaxReport />}
            </div>
        </motion.div>
    );
}
