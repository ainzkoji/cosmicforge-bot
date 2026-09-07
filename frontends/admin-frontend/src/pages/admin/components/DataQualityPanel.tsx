import React, { useEffect, useState } from 'react';
import { newsIntelligenceApi } from '@/api/newsIntelligenceApi';

interface QualitySummary {
  total_clusters: number;
  valid_clusters: number;
  spam_clusters: number;
  manipulation_suspects: number;
  by_status: { data_quality_status: string; count: number }[];
}

const STATUS_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  HIGH_CONFIDENCE:   { bg: 'rgba(16, 185, 129, 0.15)', text: '#10b981', border: '#10b981' },
  MEDIUM_CONFIDENCE: { bg: 'rgba(245, 158, 11, 0.15)', text: '#f59e0b', border: '#f59e0b' },
  LOW_CONFIDENCE:    { bg: 'rgba(156, 163, 175, 0.15)', text: '#9ca3af', border: '#6b7280' },
  SPAM:              { bg: 'rgba(239, 68, 68, 0.15)',   text: '#ef4444', border: '#ef4444' },
  MANIPULATED:       { bg: 'rgba(220, 38, 38, 0.20)',   text: '#dc2626', border: '#dc2626' },
  STALE:             { bg: 'rgba(107, 114, 128, 0.15)', text: '#6b7280', border: '#6b7280' },
};

const STATUS_ICONS: Record<string, string> = {
  HIGH_CONFIDENCE:   '✅',
  MEDIUM_CONFIDENCE: '⚠️',
  LOW_CONFIDENCE:    '📉',
  SPAM:              '🚫',
  MANIPULATED:       '⛔',
  STALE:             '🕰️',
};

export const DataQualityPanel: React.FC = () => {
  const [data, setData] = useState<QualitySummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const result = await newsIntelligenceApi.getDataQuality();
        setData(result);
      } catch (e: any) {
        setError(e.message || 'Failed to load data quality');
      } finally {
        setLoading(false);
      }
    };
    load();
    const interval = setInterval(load, 30_000);
    return () => clearInterval(interval);
  }, []);

  const validRatio = data ? (data.valid_clusters / Math.max(1, data.total_clusters)) : 0;
  const spamRatio  = data ? (data.spam_clusters / Math.max(1, data.total_clusters)) : 0;

  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(15,23,42,0.95) 0%, rgba(30,41,59,0.90) 100%)',
      border: '1px solid rgba(99,102,241,0.25)',
      borderRadius: '16px',
      padding: '24px',
      backdropFilter: 'blur(12px)',
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '20px' }}>
        <div style={{
          width: '36px', height: '36px', borderRadius: '8px',
          background: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: '18px',
        }}>🛡️</div>
        <div>
          <h3 style={{ margin: 0, color: '#f1f5f9', fontSize: '16px', fontWeight: 700 }}>
            Data Quality Panel
          </h3>
          <p style={{ margin: 0, color: '#64748b', fontSize: '12px' }}>
            Intelligence integrity filter status
          </p>
        </div>
        <div style={{ marginLeft: 'auto' }}>
          <span style={{
            background: 'rgba(99,102,241,0.15)', color: '#a5b4fc',
            padding: '4px 10px', borderRadius: '20px', fontSize: '11px', fontWeight: 600,
          }}>SHADOW MODE</span>
        </div>
      </div>

      {loading && (
        <div style={{ textAlign: 'center', color: '#64748b', padding: '20px' }}>
          Loading quality metrics…
        </div>
      )}
      {error && (
        <div style={{ color: '#f87171', background: 'rgba(239,68,68,0.1)', borderRadius: '8px', padding: '12px', fontSize: '13px' }}>
          {error}
        </div>
      )}

      {data && (
        <>
          {/* Summary row */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '12px', marginBottom: '20px' }}>
            {[
              { label: 'Total Clusters', value: data.total_clusters, color: '#94a3b8', icon: '📰' },
              { label: 'Valid Signals',  value: data.valid_clusters,        color: '#10b981', icon: '✅' },
              { label: 'Spam Clusters',  value: data.spam_clusters,         color: '#ef4444', icon: '🚫' },
              { label: 'Manip. Suspects', value: data.manipulation_suspects, color: '#f97316', icon: '⛔' },
            ].map(({ label, value, color, icon }) => (
              <div key={label} style={{
                background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.06)',
                borderRadius: '12px', padding: '14px', textAlign: 'center',
              }}>
                <div style={{ fontSize: '22px', marginBottom: '6px' }}>{icon}</div>
                <div style={{ fontSize: '22px', fontWeight: 800, color, fontFamily: 'monospace' }}>
                  {value}
                </div>
                <div style={{ fontSize: '11px', color: '#64748b', marginTop: '2px' }}>{label}</div>
              </div>
            ))}
          </div>

          {/* Quality bar */}
          <div style={{ marginBottom: '20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '6px' }}>
              <span style={{ fontSize: '12px', color: '#94a3b8' }}>Signal Validity Rate</span>
              <span style={{ fontSize: '12px', color: '#10b981', fontWeight: 700 }}>
                {(validRatio * 100).toFixed(1)}%
              </span>
            </div>
            <div style={{ height: '8px', background: 'rgba(255,255,255,0.06)', borderRadius: '4px', overflow: 'hidden' }}>
              <div style={{
                height: '100%', width: `${validRatio * 100}%`,
                background: 'linear-gradient(90deg, #10b981, #34d399)',
                borderRadius: '4px', transition: 'width 0.5s ease',
              }} />
            </div>
          </div>

          {/* Status breakdown */}
          <div>
            <div style={{ fontSize: '12px', color: '#64748b', marginBottom: '10px', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
              Cluster Status Breakdown
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
              {data.by_status
                .sort((a, b) => b.count - a.count)
                .map(({ data_quality_status, count }) => {
                  const style = STATUS_COLORS[data_quality_status] || STATUS_COLORS.LOW_CONFIDENCE;
                  const icon  = STATUS_ICONS[data_quality_status] || '📌';
                  const pct   = ((count / Math.max(1, data.total_clusters)) * 100).toFixed(1);
                  return (
                    <div key={data_quality_status} style={{
                      display: 'flex', alignItems: 'center', gap: '10px',
                      background: style.bg, border: `1px solid ${style.border}22`,
                      borderRadius: '8px', padding: '8px 12px',
                    }}>
                      <span style={{ fontSize: '14px' }}>{icon}</span>
                      <span style={{ flex: 1, fontSize: '12px', color: style.text, fontWeight: 600 }}>
                        {data_quality_status.replace(/_/g, ' ')}
                      </span>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <div style={{ width: '80px', height: '4px', background: 'rgba(255,255,255,0.06)', borderRadius: '2px', overflow: 'hidden' }}>
                          <div style={{ height: '100%', width: `${pct}%`, background: style.border, borderRadius: '2px' }} />
                        </div>
                        <span style={{ fontSize: '12px', color: style.text, fontFamily: 'monospace', minWidth: '32px', textAlign: 'right' }}>
                          {count}
                        </span>
                      </div>
                    </div>
                  );
                })}
            </div>
          </div>
        </>
      )}
    </div>
  );
};
