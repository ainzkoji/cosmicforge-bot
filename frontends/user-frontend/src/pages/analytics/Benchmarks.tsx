import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, AnalyticsBenchmark } from '@/api/client';
import { Loader2, TrendingUp, AlertTriangle } from 'lucide-react';

interface BenchmarksProps {
    timeframe: string;
}

export function Benchmarks({ timeframe }: BenchmarksProps) {
    const [benchmarkSymbol, setBenchmarkSymbol] = useState('BTCUSDT');

    const query = useQuery({
        queryKey: ['analytics-benchmarks', timeframe, benchmarkSymbol],
        queryFn: () => api.getAnalyticsBenchmarks(timeframe, benchmarkSymbol),
    });

    const data = query.data;
    const isLoading = query.isLoading;
    const isError = query.isError;

    if (isLoading && !data) {
        return (
            <div className="flex justify-center p-12">
                <Loader2 className="w-8 h-8 animate-spin text-primary" />
            </div>
        );
    }

    if (isError) {
        return (
            <div className="p-8 text-center text-red-500 bg-red-500/10 rounded-xl">
                Failed to load benchmarks.
            </div>
        );
    }

    // Default values if data undefined
    const {
        bot_return_pct = 0,
        benchmark_return_pct = 0,
        outperformance_pct = 0,
        correlation = 0,
        beta = 0,
        alpha = 0,
        warning
    } = data || {};

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold">Strategy vs Market</h2>
                <select
                    value={benchmarkSymbol}
                    onChange={(e) => setBenchmarkSymbol(e.target.value)}
                    className="bg-card border border-border rounded-lg px-3 py-1.5 text-sm"
                >
                    <option value="BTCUSDT">Bitcoin (BTC/USDT)</option>
                    <option value="ETHUSDT">Ethereum (ETH/USDT)</option>
                </select>
            </div>

            {/* Warning if insufficient data */}
            {warning && (
                <div className="flex items-center gap-2 p-4 bg-amber-500/10 text-amber-500 rounded-lg text-sm">
                    <AlertTriangle className="w-4 h-4" />
                    <span>{warning}</span>
                </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {/* Returns Comparison */}
                <div className="bg-card border border-border rounded-xl p-6">
                    <h3 className="text-sm font-medium text-muted-foreground mb-4">Total Return ({timeframe})</h3>
                    <div className="space-y-4">
                        <div className="flex justify-between items-center">
                            <span>Your Strategy</span>
                            <span className={`text-xl font-bold ${bot_return_pct >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                {bot_return_pct > 0 ? '+' : ''}{bot_return_pct.toFixed(2)}%
                            </span>
                        </div>
                        <div className="flex justify-between items-center">
                            <span>{benchmarkSymbol} (Buy & Hold)</span>
                            <span className={`text-xl font-bold ${benchmark_return_pct >= 0 ? 'text-blue-500' : 'text-blue-400'}`}>
                                {benchmark_return_pct > 0 ? '+' : ''}{benchmark_return_pct.toFixed(2)}%
                            </span>
                        </div>
                        <div className="h-px bg-border my-2" />
                        <div className="flex justify-between items-center">
                            <span className="font-medium">Outperformance</span>
                            <span className={`text-lg font-bold ${outperformance_pct >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                {outperformance_pct > 0 ? '+' : ''}{outperformance_pct.toFixed(2)}%
                            </span>
                        </div>
                    </div>
                </div>

                {/* Correlation Metrics */}
                <div className="bg-card border border-border rounded-xl p-6">
                    <h3 className="text-sm font-medium text-muted-foreground mb-4">Correlation Analysis</h3>
                    <div className="space-y-4">
                        <div className="flex justify-between items-center">
                            <div className="flex flex-col">
                                <span>Correlation</span>
                                <span className="text-xs text-muted-foreground">Similarity to market moves</span>
                            </div>
                            <span className="text-xl font-bold">{correlation.toFixed(2)}</span>
                        </div>
                        <div className="h-2 bg-muted rounded-full overflow-hidden">
                            <div
                                className="h-full bg-primary"
                                style={{ width: `${((correlation + 1) / 2) * 100}%` }}
                            />
                        </div>
                        <p className="text-xs text-muted-foreground">
                            {correlation > 0.7 ? "Highly correlated (moves with market)" :
                                correlation < 0.3 ? "Uncorrelated (independent performance)" :
                                    "Moderately correlated"}
                        </p>
                    </div>
                </div>

                {/* Risk Metrics (Alpha/Beta) */}
                <div className="bg-card border border-border rounded-xl p-6">
                    <h3 className="text-sm font-medium text-muted-foreground mb-4">Alpha & Beta</h3>
                    <div className="space-y-4">
                        <div className="flex justify-between items-center">
                            <span title="Excess return over market">Alpha (α)</span>
                            <span className={`text-xl font-bold ${alpha >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                                {alpha > 0 ? '+' : ''}{alpha.toFixed(2)}
                            </span>
                        </div>
                        <div className="flex justify-between items-center">
                            <span title="Market volatility sensitivity">Beta (β)</span>
                            <span className="text-xl font-bold">{beta.toFixed(2)}</span>
                        </div>
                        <p className="text-xs text-muted-foreground mt-2">
                            Positive Alpha indicates value added by strategy logic. Beta {'>'} 1 means higher volatility than market.
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
}
