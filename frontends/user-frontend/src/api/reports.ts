import { apiClient } from "./client";

// ============================================================================
// INTERFACES
// ============================================================================

export interface PnLRealized {
    total_pnl: number;
    total_fees: number;
    net_pnl: number;
    trade_count: number;
    profitable_trades: number;
    losing_trades: number;
    currency: string;
    filters: Record<string, any>;
}

export interface PnLUnrealizedAccount {
    broker_account_id: string;
    broker_id: string;
    unrealized_pnl: number;
    currency: string;
    timestamp: string;
}

export interface PnLUnrealized {
    total_unrealized_pnl: number;
    accounts: PnLUnrealizedAccount[];
    currency: string;
    timestamp: string;
}

export interface PnLTotal {
    realized_pnl: number;
    unrealized_pnl: number;
    total_pnl: number;
    total_fees: number;
    net_total_pnl: number;
    trade_count: number;
    profitable_trades: number;
    losing_trades: number;
    currency: string;
}

export interface PnLSummaryResponse extends PnLTotal {}

export interface PnLBreakdownItem {
    group_key: string; // broker_account_id, bot_instance_id, or symbol
    total_pnl: number;
    total_fees: number;
    net_pnl: number;
    trade_count: number;
    profitable_trades: number;
    losing_trades: number;
    win_rate: number;
}

export interface PnLBreakdown {
    group_by: string;
    breakdown: PnLBreakdownItem[];
    total_pnl: number;
    currency: string;
}

export interface TradeStatsSummary {
    total_trades: number;
    winning_trades: number;
    losing_trades: number;
    win_rate: number;
    avg_win: number;
    avg_loss: number;
    largest_win: number;
    largest_loss: number;
    profit_factor: number;
    total_pnl: number;
    total_fees: number;
    currency: string;
}

export interface TradeStatsResponse extends TradeStatsSummary {}

export interface TradeItem {
    symbol: string;
    side: string;
    qty: number;
    price: number;
    realized_pnl: number;
    fee: number;
    timestamp: string;
    broker_id: string;
}

export interface BestWorstTrades {
    best_trades: TradeItem[];
    worst_trades: TradeItem[];
}

export interface SymbolPerformanceItem {
    symbol: string;
    trade_count: number;
    winning_trades: number;
    win_rate: number;
    total_pnl: number;
    avg_pnl: number;
    total_fees: number;
}

export interface SymbolPerformance {
    symbols: SymbolPerformanceItem[];
    total_symbols: number;
}

export interface TimeSeriesPoint {
    period: string;
    period_pnl: number;
    cumulative_pnl: number;
    trade_count: number;
}

export interface TimeSeriesPerformance {
    interval: string;
    data: TimeSeriesPoint[];
    total_pnl: number;
    currency: string;
}

export interface EquityCurvePoint {
    timestamp: string;
    equity: number;
    unrealized_pnl: number;
    broker_account_id: string;
    broker_id: string;
}

export interface EquityCurve {
    data: EquityCurvePoint[];
    start_equity: number;
    end_equity: number;
    peak_equity: number;
    low_equity: number;
    currency: string;
}

export interface MaxDrawdown {
    max_drawdown_pct: number;
    max_drawdown_value: number;
    peak_equity: number;
    trough_equity: number;
    peak_timestamp: string | null;
    trough_timestamp: string | null;
    recovery_timestamp: string | null;
    recovery_days: number | null;
    currency: string;
}

export interface CurrentDrawdown {
    current_drawdown_pct: number;
    current_drawdown_value: number;
    current_equity: number;
    peak_equity: number;
    peak_timestamp: string | null;
    days_in_drawdown: number;
    currency: string;
}

export interface DrawdownPeriod {
    peak_equity: number;
    peak_timestamp: string | null;
    trough_timestamp: string | null;
    recovery_timestamp: string | null;
    max_drawdown_pct: number;
    recovered: boolean;
    recovery_days: number | null;
}

export interface DrawdownPeriods {
    periods: DrawdownPeriod[];
    total_periods: number;
    avg_recovery_days: number;
}

export interface TaxReportSummary {
    total_trades: number;
    gains_count: number;
    losses_count: number;
    total_gains: number;
    total_losses: number;
    net_pnl: number;
    total_fees: number;
}

export interface TaxReportTrade {
    date: string;
    timestamp: string;
    symbol: string;
    side: string;
    qty: number;
    price: number;
    proceeds: number;
    fee: number;
    realized_pnl: number;
    quote_currency: string;
    base_currency: string;
    broker: string;
    category: string;
}

export interface TaxReport {
    tax_year: number;
    disclaimer: string;
    summary: TaxReportSummary;
    trades: TaxReportTrade[];
    by_symbol: any[]; // Simplified
    currency: string;
    generated_at: string;
    method: string;
}

export interface BenchmarkComparison {
    benchmark_symbol: string;
    bot_return_pct: number;
    benchmark_return_pct: number;
    outperformance_pct: number;
    correlation: number;
    sharpe_ratio: number;
    period_days: number;
    warning: string | null;
}

export interface SharpeRatio {
    sharpe_ratio: number;
    annualized_return: number;
    volatility: number;
    period_days: number;
    risk_free_rate: number;
}

export interface BenchmarkOption {
    symbol: string;
    name: string;
    category: string;
}

// ============================================================================
// API CLIENT METHODS
// ============================================================================

export const ReportsAPI = {
    // P&L
    getRealizedPnL: async (params?: { broker_account_id?: string; bot_instance_id?: string; symbol?: string; days?: number }) => {
        const response = await apiClient.get<PnLRealized>("/reports/pnl/realized", { params });
        return response.data;
    },
    getUnrealizedPnL: async (params?: { broker_account_id?: string }) => {
        const response = await apiClient.get<PnLUnrealized>("/reports/pnl/unrealized", { params });
        return response.data;
    },
    getTotalPnL: async (params?: { broker_account_id?: string; bot_instance_id?: string; days?: number }) => {
        const response = await apiClient.get<PnLTotal>("/reports/pnl/total", { params });
        return response.data;
    },
    getPnLBreakdown: async (params?: { group_by?: string; days?: number }) => {
        const response = await apiClient.get<PnLBreakdown>("/reports/pnl/breakdown", { params });
        return response.data;
    },

    // Trade Stats
    getWinRate: async (params?: { broker_account_id?: string; days?: number }) => {
        const response = await apiClient.get<any>("/reports/stats/win-rate", { params });
        return response.data;
    },
    getTradeSummary: async (params?: { broker_account_id?: string; bot_instance_id?: string; days?: number }) => {
        const response = await apiClient.get<TradeStatsSummary>("/reports/stats/summary", { params });
        return response.data;
    },
    getBestWorstTrades: async (params?: { broker_account_id?: string; limit?: number }) => {
        const response = await apiClient.get<BestWorstTrades>("/reports/stats/best-worst", { params });
        return response.data;
    },
    getSymbolPerformance: async (params?: { broker_account_id?: string; days?: number }) => {
        const response = await apiClient.get<SymbolPerformance>("/reports/stats/by-symbol", { params });
        return response.data;
    },
    getTimeSeriesPerformance: async (params?: { interval?: string; days?: number }) => {
        const response = await apiClient.get<TimeSeriesPerformance>("/reports/stats/time-series", { params });
        return response.data;
    },

    // Drawdown
    getEquityCurve: async (params?: { broker_account_id?: string; days?: number }) => {
        const response = await apiClient.get<EquityCurve>("/api/v1/analytics/equity-curve", { params });
        return response.data;
    },
    getMaxDrawdown: async (params?: { broker_account_id?: string; days?: number }) => {
        const response = await apiClient.get<MaxDrawdown>("/reports/drawdown/max", { params });
        return response.data;
    },
    getCurrentDrawdown: async (params?: { broker_account_id?: string }) => {
        const response = await apiClient.get<CurrentDrawdown>("/reports/drawdown/current", { params });
        return response.data;
    },
    getDrawdownPeriods: async (params?: { broker_account_id?: string; days?: number }) => {
        const response = await apiClient.get<DrawdownPeriods>("/reports/drawdown/periods", { params });
        return response.data;
    },

    // Tax
    getTaxReport: async (taxYear: number, params?: { broker_account_id?: string }) => {
        const response = await apiClient.get<TaxReport>(`/reports/tax/report/${taxYear}`, { params });
        return response.data;
    },
    exportTaxReportCsvUrl: (taxYear: number, token: string, brokerAccountId?: string) => {
        const baseUrl = apiClient.defaults.baseURL || "";
        let url = `${baseUrl}/reports/tax/export/${taxYear}/csv?token=${token}`; // Assuming backend supports token in query for downloads or interceptor handles it if using fetch
        // Better approach for downloads with Auth header: use blob download helper
        return `${baseUrl}/reports/tax/export/${taxYear}/csv`;
    },
    exportTaxReportPdfUrl: (taxYear: number, token: string, brokerAccountId?: string) => {
        const baseUrl = apiClient.defaults.baseURL || "";
        return `${baseUrl}/reports/tax/export/${taxYear}/pdf`;
    },

    // Benchmark
    getAvailableBenchmarks: async () => {
        const response = await apiClient.get<{ benchmarks: BenchmarkOption[] }>("/reports/benchmark/available");
        return response.data;
    },
    getBenchmarkComparison: async (params?: { benchmark_symbol?: string; days?: number }) => {
        const response = await apiClient.get<BenchmarkComparison>("/reports/benchmark/comparison", { params });
        return response.data;
    },
    getSharpeRatio: async (params?: { days?: number; risk_free_rate?: number }) => {
        const response = await apiClient.get<SharpeRatio>("/reports/benchmark/sharpe-ratio", { params });
        return response.data;
    }
};
