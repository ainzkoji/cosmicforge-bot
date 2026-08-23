import React, { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  newsIntelligenceApi,
  newsAdminApi,
  RawNewsItem,
  NewsCluster,
  NewsSignal,
  IntelligenceStats,
  FeedStatus,
  SourceStatus,
} from '../../api/newsIntelligenceApi';
import { LiveNewsFlowStatus }   from '../../components/admin/news/LiveNewsFlowStatus';
import { NewsSourceHealthPanel } from '../../components/admin/news/NewsSourceHealthPanel';
import { ManualNewsImport }      from '../../components/admin/news/ManualNewsImport';
import { LiveNewsFeed }      from '../../components/admin/LiveNewsFeed';
import { NewsClusterCard }   from '../../components/admin/NewsClusterCard';
import { NarrativeMonitor }  from '../../components/admin/NarrativeMonitor';
import { SentimentTrendChart } from '../../components/admin/SentimentTrendChart';
import { ShadowSignalPanel } from '../../components/admin/ShadowSignalPanel';
import { DataQualityPanel }  from './components/DataQualityPanel';
import { SourceTrustMonitor } from './components/SourceTrustMonitor';
import { NewsVsMarketTable }  from './components/NewsVsMarketTable';
import { SignalAccuracyPanel } from './components/SignalAccuracyPanel';
import { NarrativePerformanceChart } from './components/NarrativePerformanceChart';
import { RealTimeNewsMonitor } from '../../components/admin/news/RealTimeNewsMonitor';

type TabId = 'feed' | 'data-quality' | 'sources' | 'validation' | 'live-sources' | 'real-time';

const TABS: { id: TabId; label: string; icon: string }[] = [
  { id: 'feed',         label: 'Live Intelligence',  icon: '📰' },
  { id: 'live-sources', label: 'Live Sources',       icon: '🔌' },
  { id: 'real-time',    label: 'Real-Time APIs',     icon: '⚡' },
  { id: 'data-quality', label: 'Data Quality',       icon: '🛡️' },
  { id: 'sources',      label: 'Source Trust',       icon: '📡' },
  { id: 'validation',   label: 'Market Validation',  icon: '📊' },
];

export const NewsIntelligence: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const routeTab: TabId =
    location.pathname.endsWith('/realtime') ? 'real-time'
    : location.pathname.endsWith('/validation') ? 'validation'
    : 'feed';
  const [activeTab, setActiveTab] = useState<TabId>(routeTab);
  const [loading, setLoading]     = useState(true);
  const [news, setNews]           = useState<RawNewsItem[]>([]);
  const [clusters, setClusters]   = useState<NewsCluster[]>([]);
  const [narratives, setNarratives] = useState<any[]>([]);
  const [signals, setSignals]     = useState<NewsSignal[]>([]);
  const [stats, setStats]         = useState<IntelligenceStats | null>(null);
  const [clusterFilter, setClusterFilter] = useState<'all' | 'valid' | 'warning'>('all');

  // Live sources tab state
  const [feedStatus, setFeedStatus]     = useState<FeedStatus | null>(null);
  const [sources, setSources]           = useState<SourceStatus[]>([]);
  const [sourcesLoading, setSourcesLoading] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [liveNewsData, clustersData, narrativesData, signalsData, statsData] =
        await Promise.all([
          newsIntelligenceApi.getLiveNews(24, 50),
          newsIntelligenceApi.getClusters({ sinceHours: 24, limit: 30 }),
          newsIntelligenceApi.getNarratives(24, 50),
          newsIntelligenceApi.getSignals({ sinceHours: 24, limit: 30 }),
          newsIntelligenceApi.getStats(24),
        ]);
      setNews(liveNewsData);
      setClusters(clustersData);
      setNarratives(narrativesData);
      setSignals(signalsData);
      setStats(statsData);
    } catch (err) {
      console.error('Failed to fetch news intelligence data', err);
    } finally {
      setLoading(false);
    }
  };

  const fetchSources = async () => {
    setSourcesLoading(true);
    try {
      const [statusData, sourcesData] = await Promise.all([
        newsAdminApi.getFeedStatus(),
        newsAdminApi.getSources(),
      ]);
      setFeedStatus(statusData);
      setSources(sourcesData);
    } catch (err) {
      console.error('Failed to fetch source data', err);
    } finally {
      setSourcesLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60_000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    setActiveTab(routeTab);
  }, [routeTab]);

  useEffect(() => {
    if (activeTab === 'live-sources') fetchSources();
  }, [activeTab]);

  // Local cluster filter
  const filteredClusters = clusters.filter(c => {
    if (clusterFilter === 'valid')   return c.is_valid_signal === 1;
    if (clusterFilter === 'warning') return c.is_manipulation_suspect === 1 || c.data_quality_status === 'SPAM' || c.data_quality_status === 'MANIPULATED';
    return true;
  });

  // Warning count for badge
  const warningCount = clusters.filter(c => c.is_manipulation_suspect === 1 || c.data_quality_status === 'SPAM' || c.data_quality_status === 'MANIPULATED').length;

  return (
    <div style={{
      minHeight: '100vh',
      background: 'linear-gradient(135deg, #0a0f1e 0%, #0f172a 50%, #0a0f1e 100%)',
      fontFamily: "'Inter', 'Segoe UI', sans-serif",
      color: '#f1f5f9',
      padding: '24px',
    }}>
      {/* ── Page Header ─────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '24px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '4px' }}>
            <div style={{
              width: '40px', height: '40px', borderRadius: '10px',
              background: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
              display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '20px',
            }}>📡</div>
            <h1 style={{ margin: 0, fontSize: '22px', fontWeight: 800, color: '#f1f5f9' }}>
              News Intelligence
            </h1>
            <span style={{
              background: 'rgba(99,102,241,0.2)', color: '#a5b4fc', border: '1px solid rgba(99,102,241,0.3)',
              padding: '3px 10px', borderRadius: '20px', fontSize: '11px', fontWeight: 700,
            }}>
              SHADOW MODE — Phase 3
            </span>
            {warningCount > 0 && (
              <span style={{
                background: 'rgba(239,68,68,0.2)', color: '#f87171', border: '1px solid rgba(239,68,68,0.3)',
                padding: '3px 10px', borderRadius: '20px', fontSize: '11px', fontWeight: 700,
              }}>
                ⚠ {warningCount} Warning{warningCount > 1 ? 's' : ''}
              </span>
            )}
          </div>
          <p style={{ margin: 0, fontSize: '13px', color: '#475569' }}>
            Real-time market sentiment · Narrative classification · Data quality enforcement
          </p>
        </div>

        <button
          id="news-refresh-btn"
          onClick={fetchData}
          disabled={loading}
          style={{
            padding: '9px 20px', borderRadius: '10px', border: 'none', cursor: 'pointer',
            background: loading ? 'rgba(99,102,241,0.3)' : 'linear-gradient(135deg, #6366f1, #4f46e5)',
            color: '#f1f5f9', fontSize: '13px', fontWeight: 600, transition: 'all 0.2s',
            opacity: loading ? 0.7 : 1,
          }}
        >
          {loading ? '⟳ Refreshing…' : '↻ Refresh Feed'}
        </button>
      </div>

      {/* ── Tab nav ──────────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: '6px', marginBottom: '24px', borderBottom: '1px solid rgba(255,255,255,0.06)', paddingBottom: '0' }}>
        {TABS.map(tab => (
          <button
            key={tab.id}
            id={`news-tab-${tab.id}`}
            onClick={() => {
              setActiveTab(tab.id);
              if (tab.id === 'real-time') navigate('/admin/news-intelligence/realtime');
              else if (tab.id === 'validation') navigate('/admin/news-intelligence/validation');
              else if (location.pathname !== '/admin/news-intelligence') navigate('/admin/news-intelligence');
            }}
            style={{
              padding: '10px 20px', border: 'none', cursor: 'pointer', borderRadius: '8px 8px 0 0',
              fontSize: '13px', fontWeight: 600, transition: 'all 0.2s',
              background: activeTab === tab.id ? 'rgba(99,102,241,0.2)' : 'transparent',
              color: activeTab === tab.id ? '#a5b4fc' : '#64748b',
              borderBottom: activeTab === tab.id ? '2px solid #6366f1' : '2px solid transparent',
            }}
          >
            {tab.icon} {tab.label}
          </button>
        ))}
      </div>

      {/* ── Tab: Live Intelligence ────────────────────────────────────────── */}
      {activeTab === 'feed' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>

          {/* LEFT: Live Feed + Clusters */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
            <LiveNewsFeed news={news} loading={loading} />

            {/* Cluster section */}
            <div style={{
              background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.06)',
              borderRadius: '16px', overflow: 'hidden',
            }}>
              <div style={{
                padding: '16px 20px', borderBottom: '1px solid rgba(255,255,255,0.06)',
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              }}>
                <div>
                  <h3 style={{ margin: 0, fontSize: '15px', fontWeight: 700, color: '#f1f5f9' }}>
                    Event Clusters
                  </h3>
                  <p style={{ margin: 0, fontSize: '12px', color: '#64748b' }}>
                    {filteredClusters.length} of {clusters.length} shown
                  </p>
                </div>
                {/* Filter pills */}
                <div style={{ display: 'flex', gap: '5px' }}>
                  {([['all', 'All'], ['valid', '✅ Valid'], ['warning', '⚠ Warnings']] as const).map(([f, label]) => (
                    <button
                      key={f}
                      id={`cluster-filter-${f}`}
                      onClick={() => setClusterFilter(f)}
                      style={{
                        padding: '4px 12px', borderRadius: '20px', border: 'none', cursor: 'pointer',
                        fontSize: '11px', fontWeight: 600, transition: 'all 0.2s',
                        background: clusterFilter === f ? 'rgba(99,102,241,0.25)' : 'rgba(255,255,255,0.04)',
                        color: clusterFilter === f ? '#a5b4fc' : '#64748b',
                      }}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              </div>
              <div style={{ padding: '12px', display: 'flex', flexDirection: 'column', gap: '10px', maxHeight: '480px', overflowY: 'auto' }}>
                {loading && filteredClusters.length === 0 ? (
                  <div style={{ textAlign: 'center', color: '#64748b', padding: '20px' }}>Loading clusters…</div>
                ) : filteredClusters.length === 0 ? (
                  <div style={{ textAlign: 'center', color: '#64748b', padding: '20px' }}>No clusters match filter.</div>
                ) : (
                  filteredClusters.map(c => <NewsClusterCard key={c.id} cluster={c} />)
                )}
              </div>
            </div>
          </div>

          {/* RIGHT: Analytics + Signals */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
              <NarrativeMonitor narratives={narratives} loading={loading} />
              <SentimentTrendChart stats={stats} loading={loading} />
            </div>
            <ShadowSignalPanel signals={signals} loading={loading} />
          </div>
        </div>
      )}

      {/* ── Tab: Live Sources ────────────────────────────────────────────── */}
      {activeTab === 'live-sources' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px', maxWidth: '1100px' }}>
          <LiveNewsFlowStatus status={feedStatus} loading={sourcesLoading} />
          <NewsSourceHealthPanel sources={sources} loading={sourcesLoading} />
          <ManualNewsImport />
        </div>
      )}

      {/* ── Tab: Real-Time APIs ──────────────────────────────────────────── */}
      {activeTab === 'real-time' && (
        <div style={{ maxWidth: '1100px' }}>
          <RealTimeNewsMonitor />
        </div>
      )}

      {/* ── Tab: Data Quality ─────────────────────────────────────────────── */}
      {activeTab === 'data-quality' && (
        <div style={{ maxWidth: '860px' }}>
          <DataQualityPanel />
        </div>
      )}

      {/* ── Tab: Source Trust ─────────────────────────────────────────────── */}
      {activeTab === 'sources' && (
        <div style={{ maxWidth: '860px' }}>
          <SourceTrustMonitor />
        </div>
      )}

      {/* ── Tab: Market Validation ──────────────────────────────────────── */}
      {activeTab === 'validation' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          {/* Top row: accuracy panel + narrative performance */}
          <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: '20px' }}>
            <SignalAccuracyPanel />
            <NarrativePerformanceChart />
          </div>
          {/* Full-width: News vs Market table */}
          <NewsVsMarketTable />
        </div>
      )}
    </div>
  );
};
