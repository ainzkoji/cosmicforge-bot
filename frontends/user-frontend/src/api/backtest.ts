import { apiClient } from "./client";

export interface BacktestConfig {
    strategy_id: string;
    name: string;
    symbols: string[];
    interval: string;
    start_date: string;
    end_date: string;
    initial_capital: number;
    strategy_params?: Record<string, any>;
    risk_params?: Record<string, any>;
    slippage_bps?: number;
    fee_bps?: number;
    market_type?: string;
    data_source?: string;
}

export interface BacktestMetrics {
    total_trades: number;
    win_rate: number;
    net_pnl: number;
    gross_pnl: number;
    total_fees: number;
    max_drawdown: number;
    sharpe_ratio?: number;
    return_pct?: number;
}

export interface BacktestRun {
    id: string;
    user_id: string;
    name: string;
    strategy_id: string;
    status: "pending" | "processing" | "completed" | "failed" | "cancelled";
    created_at: string;
    completed_at?: string;
    symbols: string[];
    timeframe: string;
    start_date: string;
    end_date: string;
    initial_capital: number;
    metrics: BacktestMetrics;
    error_message?: string;
    progress_pct: number;
}

export interface BacktestListResponse {
    items: BacktestRun[];
    total: number;
    page: number;
    size: number;
}

export interface EquityPoint {
    timestamp: string;
    equity: number;
    balance: number;
    drawdown_pct: number;
    unrealized_pnl: number;
}

export interface EquityCurveResponse {
    run_id: string;
    datapoints: EquityPoint[];
}

export interface FillItem {
    timestamp: string;
    symbol: string;
    side: "BUY" | "SELL";
    price: number;
    quantity: number;
    fee_usdt: number;
    pnl?: number;
}

export interface FillListResponse {
    items: FillItem[];
    total: number;
    page: number;
    size: number;
}

export const BacktestAPI = {
    create: async (config: BacktestConfig): Promise<{ run_id: string; job_id: string }> => {
        const response = await apiClient.post("/api/v1/backtests/", config);
        return response.data;
    },

    list: async (params?: { status?: string; page?: number; size?: number }): Promise<BacktestListResponse> => {
        const response = await apiClient.get<BacktestListResponse>("/api/v1/backtests/", { params });
        return response.data;
    },

    get: async (runId: string): Promise<BacktestRun> => {
        const response = await apiClient.get<BacktestRun>(`/api/v1/backtests/${runId}`);
        return response.data;
    },

    getEquityCurve: async (runId: string): Promise<EquityCurveResponse> => {
        const response = await apiClient.get<EquityCurveResponse>(`/api/v1/backtests/${runId}/equity`);
        return response.data;
    },

    getFills: async (runId: string, params?: { page?: number; size?: number }): Promise<FillListResponse> => {
        const response = await apiClient.get<FillListResponse>(`/api/v1/backtests/${runId}/fills`, { params });
        return response.data;
    },

    cancel: async (runId: string): Promise<{ status: string; message: string }> => {
        const response = await apiClient.post(`/api/v1/backtests/${runId}/cancel`);
        return response.data;
    },

    getExportUrl: (runId: string, format: "csv" | "json" = "csv"): string => {
        const baseUrl = apiClient.defaults.baseURL || "";
        const token = localStorage.getItem("access_token");
        // Check if baseUrl ends with slash to avoid double slash, though usually fine
        const cleanBaseUrl = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
        return `${cleanBaseUrl}/api/v1/backtests/${runId}/export?format=${format}&token=${token}`;
    }
};
