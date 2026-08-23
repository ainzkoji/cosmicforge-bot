import { apiClient, adminNewsApiClient as client } from './client';

// ─── Types ──────────────────────────────────────────────────────────────────

export interface RawNewsItem {
  id: number;
  provider: string;
  source_name: string | null;
  source_domain: string | null;
  source_url: string | null;
  title: string;
  body_snippet: string | null;
  published_utc: string;
  ingested_utc: string;
  latency_seconds: number | null;
  is_duplicate: number;
  created_at: string;
}

export interface NewsNarrative {
  id: number;
  cluster_id: number;
  narrative_type: string;
  narrative_confidence: number;
  severity_level: string;
  matched_keywords: string | null;
  created_at: string;
}

export interface NewsSignal {
  id: number;
  cluster_id: number;
  symbol: string | null;
  signal_type: string;
  sentiment_score: number | null;
  narrative_type: string | null;
  severity_level: string;
  reliability_score: number | null;
  confidence_score: number | null;
  // Hardening fields
  spam_score: number;
  latency_score: number;
  is_valid_signal: number;
  manipulation_flag: string | null;
  data_quality_status: string;
  // Invariants (always 0/1)
  should_affect_trading: 0;
  shadow_only: 1;
  suppression_reason: string | null;
  created_at: string;
}

export type DataQualityStatus =
  | 'HIGH_CONFIDENCE'
  | 'MEDIUM_CONFIDENCE'
  | 'LOW_CONFIDENCE'
  | 'SPAM'
  | 'MANIPULATED'
  | 'STALE';

export interface NewsCluster {
  id: number;
  canonical_title: string;
  summary: string | null;
  first_seen_utc: string;
  last_seen_utc: string;
  source_count: number;
  provider_count: number;
  highest_reliability_score: number;
  cluster_confidence: number;
  // Hardening fields
  spam_score: number;
  latency_score: number;
  is_valid_signal: number;
  manipulation_flag: string | null;
  data_quality_status: DataQualityStatus;
  is_manipulation_suspect: number;
  manipulation_reason: string | null;
  created_at: string;
  updated_at: string;
  narratives?: NewsNarrative[];
  signals?: NewsSignal[];
}

export interface NewsSource {
  id: string;
  source_name: string;
  base_reliability_score: number;
  dynamic_reliability_score: number;
  is_trusted: number;
  is_blocked: number;
  created_at: string;
  updated_at: string;
}

export interface DataQualitySummary {
  total_clusters: number;
  valid_clusters: number;
  spam_clusters: number;
  manipulation_suspects: number;
  by_status: { data_quality_status: string; count: number }[];
}

export interface IntelligenceStats {
  providers: {
    provider: string;
    item_count: number;
    avg_latency_seconds: number;
    last_ingested_utc: string;
  }[];
  signals: {
    symbol: string | null;
    signal_type: string;
    signal_count: number;
    avg_confidence: number;
    valid_ratio: number;
    last_signal_utc: string;
  }[];
  data_quality: DataQualitySummary;
}

// ─── API client ─────────────────────────────────────────────────────────────

export const newsIntelligenceApi = {
  getLiveNews: async (sinceHours = 24, limit = 100): Promise<RawNewsItem[]> => {
    const response = await client.get(`/api/admin/news/items`, {
      params: { hours: sinceHours, limit }
    });
    return response.data;
  },

  getClusters: async (params?: {
    sinceHours?: number;
    limit?: number;
    includeManipulation?: boolean;
    validOnly?: boolean;
    dataQualityStatus?: DataQualityStatus;
  }): Promise<NewsCluster[]> => {
    const response = await client.get(`/api/admin/news/clusters`, {
      params: {
        hours: params?.sinceHours ?? 24,
        limit: params?.limit ?? 100,
        include_manipulation: params?.includeManipulation ?? true,
        valid_only: params?.validOnly ?? false,
        data_quality_status: params?.dataQualityStatus,
      }
    });
    return response.data;
  },

  getClusterItems: async (clusterId: number): Promise<RawNewsItem[]> => {
    const response = await client.get(`/api/admin/news/clusters/${clusterId}/items`);
    return response.data;
  },

  getNarratives: async (sinceHours = 24, limit = 50): Promise<NewsNarrative[]> => {
    const response = await client.get(`/api/admin/news/narratives`, {
      params: { hours: sinceHours, limit }
    });
    return response.data;
  },

  getSignals: async (params?: {
    sinceHours?: number;
    limit?: number;
    symbol?: string;
    validOnly?: boolean;
  }): Promise<NewsSignal[]> => {
    const response = await client.get(`/api/admin/news/signals`, {
      params: {
        hours: params?.sinceHours ?? 24,
        limit: params?.limit ?? 50,
        symbol: params?.symbol,
        valid_only: params?.validOnly ?? false,
      }
    });
    return response.data;
  },

  getSources: async (params?: {
    trusted_only?: boolean;
    blocked_only?: boolean;
    limit?: number;
  }): Promise<NewsSource[]> => {
    const response = await client.get(`/api/admin/news/sources`, {
      params: {
        trusted_only: params?.trusted_only ?? false,
        blocked_only: params?.blocked_only ?? false,
        limit: params?.limit ?? 100,
      }
    });
    return response.data;
  },

  getDataQuality: async (): Promise<DataQualitySummary> => {
    const response = await client.get(`/api/admin/news/data-quality`);
    return response.data;
  },

  getStats: async (sinceHours = 24): Promise<IntelligenceStats> => {
    const response = await client.get(`/api/admin/news/stats`, {
      params: { hours: sinceHours }
    });
    return response.data;
  },

  // ── Market Validation ────────────────────────────────────────────────────

  getValidations: async (params?: {
    sinceHours?: number;
    limit?: number;
    symbol?: string;
    falseOnly?: boolean;
  }): Promise<any[]> => {
    const response = await client.get(`/api/admin/news/validations`, {
      params: {
        hours: params?.sinceHours ?? 24,
        limit: params?.limit ?? 100,
        symbol: params?.symbol,
        false_only: params?.falseOnly ?? false,
      }
    });
    return response.data;
  },

  getValidationSummary: async (): Promise<any> => {
    const response = await client.get(`/api/admin/news/validations/summary`);
    return response.data;
  },

  getNarrativeEffectiveness: async (limit = 50): Promise<any[]> => {
    const response = await client.get(`/api/admin/news/narrative-effectiveness`, {
      params: { limit }
    });
    return response.data;
  },

  getClusterValidation: async (clusterId: number): Promise<any[]> => {
    const response = await client.get(`/api/admin/news/clusters/${clusterId}/validation`);
    return response.data;
  },
};

// ─── Admin RSS / Source Layer (Phase 3 Real Sources) ────────────────────────

export interface FeedStatus {
  today_count: number;
  active_sources: number;
  failed_sources: number;
  rss_enabled_sources: number;
  rt_enabled_providers: number;
  rt_healthy_providers: number;
  has_live_data: boolean;
  latest_title: string | null;
  latest_ingested_utc: string | null;
  ingestion_warning: string | null;
  shadow_only: true;
  signal_can_open_trades: false;
  signal_can_close_trades: false;
  signal_can_block_trades: false;
}

export interface SourceStatus {
  id: string;
  source_name: string;
  source_type: string | null;
  category: string | null;
  is_enabled: number;
  rss_url: string | null;
  fetch_interval_seconds: number | null;
  last_fetch_utc: string | null;
  last_success_utc: string | null;
  last_error: string | null;
  base_reliability_score: number;
  dynamic_reliability_score: number;
  health_status: string | null;
  items_fetched_last_run: number | null;
  duplicate_count_last_run: number | null;
}

export interface ManualImportPayload {
  title: string;
  body_snippet?: string;
  source_name?: string;
  source_url?: string;
  published_utc?: string;
  affected_symbols?: string[];
  category?: string;
}

export interface ManualImportResult {
  raw_news_item_id: number | null;
  cluster_id: number | null;
  is_new_cluster: boolean;
  symbols: string[];
  top_narrative: string | null;
  signals_emitted: number;
  is_manipulation_suspect: boolean;
  error: string | null;
}

export interface RtProviderStatus {
  provider: string;
  is_enabled: number;
  last_fetch_utc: string | null;
  last_success_utc: string | null;
  last_error: string | null;
  latency_avg_seconds: number;
  items_fetched_today: number;
  duplicate_rate: number;
  health_status: string;
  updated_at: string | null;
}

export interface ConflictCluster {
  id: number;
  canonical_title: string;
  first_seen_utc: string;
  source_count: number;
  conflict_flag: number;
  fake_news_risk_score: number | null;
  market_confirmation_status: string | null;
  first_seen_provider: string | null;
}

export interface MarketConfirmation {
  id: number;
  canonical_title: string;
  first_seen_utc: string;
  market_confirmation_status: string | null;
  fake_news_risk_score: number | null;
  confirmation_count: number;
  conflict_flag: number;
}

export const newsAdminApi = {
  getFeedStatus: async (): Promise<FeedStatus> => {
    const response = await client.get('/api/admin/news/feed-status');
    return response.data;
  },

  getSources: async (): Promise<SourceStatus[]> => {
    const response = await client.get('/api/admin/news/sources');
    return response.data;
  },

  getProviderStats: async (hours = 24): Promise<any[]> => {
    const response = await client.get('/api/admin/news/provider-stats', { params: { hours } });
    return response.data;
  },

  importManual: async (payload: ManualImportPayload): Promise<ManualImportResult> => {
    const response = await apiClient.post('/api/admin/news/import', payload);
    return response.data;
  },

  getImports: async (limit = 50): Promise<any[]> => {
    const response = await client.get('/api/admin/news/imports', { params: { limit } });
    return response.data;
  },

  getAdminItems: async (hours = 24, limit = 100): Promise<any[]> => {
    const response = await client.get('/api/admin/news/items', { params: { hours, limit } });
    return response.data;
  },

  getAdminClusters: async (hours = 24, limit = 50): Promise<any[]> => {
    const response = await client.get('/api/admin/news/clusters', { params: { hours, limit } });
    return response.data;
  },

  getAdminSignals: async (hours = 24, limit = 50): Promise<any[]> => {
    const response = await client.get('/api/admin/news/signals', { params: { hours, limit } });
    return response.data;
  },

  // ── Real-Time Hardening ─────────────────────────────────────────────────────

  getRtProviderStatus: async (): Promise<RtProviderStatus[]> => {
    const response = await client.get('/api/admin/news/rt-provider-status');
    return response.data;
  },

  getRtFeed: async (hours = 6, limit = 100): Promise<any[]> => {
    const response = await client.get('/api/admin/news/rt-feed', { params: { hours, limit } });
    return response.data;
  },

  getDuplicateClusters: async (hours = 24, minSources = 2, limit = 50): Promise<any[]> => {
    const response = await client.get('/api/admin/news/duplicate-clusters', {
      params: { hours, min_sources: minSources, limit },
    });
    return response.data;
  },

  getConflicts: async (hours = 24, limit = 50): Promise<ConflictCluster[]> => {
    const response = await client.get('/api/admin/news/conflicts', { params: { hours, limit } });
    return response.data;
  },

  getMarketConfirmations: async (hours = 24, status?: string, limit = 50): Promise<MarketConfirmation[]> => {
    const response = await client.get('/api/admin/news/market-confirmations', {
      params: { hours, status, limit },
    });
    return response.data;
  },
};
