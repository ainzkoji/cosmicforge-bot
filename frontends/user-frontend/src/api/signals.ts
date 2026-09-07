import { apiClient } from "@/api/client";

export type SignalSide = "BUY" | "SELL";

export type SignalStatus =
  | "PENDING_ENTRY"
  | "ACTIVE"
  | "EXPIRED"
  | "TP1_HIT"
  | "TP2_HIT"
  | "TP3_HIT"
  | "SL_HIT"
  | "CANCELLED"
  | "INVALIDATED";

export interface TradingSignal {
  id: string;
  asset_class: string;
  symbol: string;
  side: SignalSide | string;
  timeframe?: string | null;
  strategy_name?: string | null;
  entry_price: number;
  entry_zone_low?: number | null;
  entry_zone_high?: number | null;
  stop_loss: number;
  take_profit_1: number;
  take_profit_2?: number | null;
  take_profit_3?: number | null;
  risk_reward: number;
  confidence_score: number;
  signal_reason?: string | null;
  status: SignalStatus | string;
  is_published?: number | boolean;
  dev_mode?: number | boolean;
  published_at?: string | null;
  expires_at: string;
  created_at: string;
  updated_at: string;
  time_left_seconds?: number | null;
  disclaimer?: string;
}

export interface SignalListResponse {
  items: TradingSignal[];
  count: number;
  limit: number;
  offset: number;
}

export interface SignalPerformanceSummary {
  total_signals: number;
  active_signals: number;
  completed_signals: number;
  expired_signals: number;
  tp1_hit_rate: number | null;
  tp2_hit_rate: number | null;
  tp3_hit_rate: number | null;
  sl_hit_rate: number | null;
  win_rate: number | null;
  average_risk_reward: number | null;
  best_symbol: string | null;
  worst_symbol: string | null;
  message?: string;
}

export interface SignalQueryParams {
  asset_class?: string;
  status?: string;
  symbol?: string;
  search?: string;
  side?: string;
  timeframe?: string;
  min_confidence?: number;
  sort?: string;
  favorites_only?: boolean | number | string;
  majors_only?: boolean | number | string;
  include_hidden?: boolean | number | string;
  limit?: number;
  offset?: number;
}

export interface SignalPreferences {
  user_id: string;
  crypto_enabled: boolean;
  forex_enabled: boolean;
  favorite_symbols: string[];
  hidden_symbols: string[];
  minimum_confidence: number;
  majors_only: boolean;
  risk_style: "conservative" | "balanced" | "aggressive" | string;
  notifications_enabled: boolean;
  notify_new_signal: boolean;
  notify_signal_invalidated: boolean;
  notify_tp1_hit: boolean;
  notify_tp2_hit: boolean;
  notify_tp3_hit: boolean;
  notify_sl_hit: boolean;
  notify_entry_window_expiring: boolean;
}

export type SignalPreferencesUpdate = Partial<Omit<SignalPreferences, "user_id">>;

export interface SignalNotification {
  id: string;
  user_id?: string | null;
  signal_id?: string | null;
  symbol?: string | null;
  event_type: string;
  title: string;
  message: string;
  channel: string;
  status: string;
  read_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface SignalNotificationListResponse {
  items: SignalNotification[];
  count: number;
  limit: number;
  offset: number;
}

function cleanParams(params: SignalQueryParams = {}) {
  return Object.fromEntries(
    Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== "")
  );
}

export async function getSignals(params?: SignalQueryParams): Promise<SignalListResponse> {
  const response = await apiClient.get("/api/signals", { params: cleanParams(params) });
  return response.data;
}

export async function getActiveSignals(params?: SignalQueryParams): Promise<SignalListResponse> {
  const response = await apiClient.get("/api/signals/active", { params: cleanParams(params) });
  return response.data;
}

export async function getSignalHistory(params?: SignalQueryParams): Promise<SignalListResponse> {
  const response = await apiClient.get("/api/signals/history", { params: cleanParams(params) });
  return response.data;
}

export async function getSignalDetail(signalId: string): Promise<TradingSignal> {
  const response = await apiClient.get(`/api/signals/${signalId}`);
  return response.data;
}

export async function getSignalPerformance(params?: Pick<SignalQueryParams, "asset_class">): Promise<SignalPerformanceSummary> {
  const response = await apiClient.get("/api/signals/performance", { params: cleanParams(params) });
  return response.data;
}

export async function getSignalPreferences(): Promise<SignalPreferences> {
  const response = await apiClient.get("/api/signals/preferences");
  return response.data;
}

export async function updateSignalPreferences(payload: SignalPreferencesUpdate): Promise<SignalPreferences> {
  const response = await apiClient.put("/api/signals/preferences", payload);
  return response.data;
}

export async function addFavoriteSignalSymbol(symbol: string): Promise<SignalPreferences> {
  const response = await apiClient.post(`/api/signals/preferences/favorites/${symbol}`);
  return response.data;
}

export async function removeFavoriteSignalSymbol(symbol: string): Promise<SignalPreferences> {
  const response = await apiClient.delete(`/api/signals/preferences/favorites/${symbol}`);
  return response.data;
}

export async function hideSignalSymbol(symbol: string): Promise<SignalPreferences> {
  const response = await apiClient.post(`/api/signals/preferences/hidden/${symbol}`);
  return response.data;
}

export async function unhideSignalSymbol(symbol: string): Promise<SignalPreferences> {
  const response = await apiClient.delete(`/api/signals/preferences/hidden/${symbol}`);
  return response.data;
}

export async function getSignalNotifications(params?: { status?: string; limit?: number; offset?: number }): Promise<SignalNotificationListResponse> {
  const response = await apiClient.get("/api/signals/notifications", { params: cleanParams(params || {}) });
  return response.data;
}

export async function markSignalNotificationRead(notificationId: string): Promise<{ ok: boolean }> {
  const response = await apiClient.post(`/api/signals/notifications/${notificationId}/read`);
  return response.data;
}
