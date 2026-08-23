import React from 'react';
import { FeedStatus } from '../../../api/newsIntelligenceApi';

interface Props {
  status: FeedStatus | null;
  loading: boolean;
}

const _card = (label: string, value: React.ReactNode, accent: string) => (
  <div style={{
    background: 'rgba(255,255,255,0.04)',
    border: `1px solid ${accent}33`,
    borderRadius: '12px',
    padding: '16px 20px',
    minWidth: '140px',
    flex: 1,
  }}>
    <div style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '6px' }}>
      {label}
    </div>
    <div style={{ fontSize: '22px', fontWeight: 800, color: accent }}>
      {value}
    </div>
  </div>
);

export const LiveNewsFlowStatus: React.FC<Props> = ({ status, loading }) => {
  if (loading) {
    return (
      <div style={{
        background: 'rgba(99,102,241,0.06)',
        border: '1px solid rgba(99,102,241,0.15)',
        borderRadius: '14px', padding: '20px', marginBottom: '20px',
        color: '#475569', fontSize: '13px',
      }}>
        Loading feed status…
      </div>
    );
  }

  if (!status) return null;

  const emptyState = !status.has_live_data;

  return (
    <div style={{ marginBottom: '20px' }}>
      {emptyState && (
        <div style={{
          background: 'rgba(245,158,11,0.08)',
          border: '1px solid rgba(245,158,11,0.3)',
          borderRadius: '10px', padding: '14px 18px', marginBottom: '14px',
          color: '#fbbf24', fontSize: '13px', fontWeight: 500,
          display: 'flex', alignItems: 'center', gap: '8px',
        }}>
          <span style={{ fontSize: '16px' }}>⚠</span>
          {status.ingestion_warning || 'No live news has been ingested today. RSS is the primary source path and optional API providers may remain disabled.'}
        </div>
      )}

      <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
        {_card('Today', status.today_count.toLocaleString(), '#6366f1')}
        {_card('RSS Sources', `${status.active_sources}/${status.rss_enabled_sources}`, '#22c55e')}
        {_card('Failed Sources', status.failed_sources, status.failed_sources > 0 ? '#ef4444' : '#64748b')}
        {_card('API Providers', `${status.rt_healthy_providers}/${status.rt_enabled_providers}`, '#38bdf8')}
        <div style={{
          background: 'rgba(255,255,255,0.04)',
          border: '1px solid rgba(99,102,241,0.2)',
          borderRadius: '12px',
          padding: '16px 20px',
          flex: 2,
          minWidth: '200px',
        }}>
          <div style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '6px' }}>
            Latest Ingested
          </div>
          {status.latest_title ? (
            <>
              <div style={{ fontSize: '13px', color: '#e2e8f0', fontWeight: 500, marginBottom: '2px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {status.latest_title}
              </div>
              <div style={{ fontSize: '11px', color: '#475569' }}>
                {status.latest_ingested_utc ? new Date(status.latest_ingested_utc).toLocaleString() : '—'}
              </div>
            </>
          ) : (
            <div style={{ fontSize: '13px', color: '#475569' }}>No items yet</div>
          )}
        </div>
        <div style={{
          background: 'rgba(34,197,94,0.06)',
          border: '1px solid rgba(34,197,94,0.2)',
          borderRadius: '12px',
          padding: '16px 20px',
          display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center',
          minWidth: '120px',
        }}>
          <div style={{ fontSize: '10px', color: '#64748b', fontWeight: 700, textTransform: 'uppercase', marginBottom: '4px' }}>Mode</div>
          <div style={{ fontSize: '11px', fontWeight: 700, color: '#86efac', textAlign: 'center' }}>SHADOW ONLY</div>
          <div style={{ fontSize: '10px', color: '#475569', marginTop: '2px' }}>No trade impact</div>
        </div>
      </div>
    </div>
  );
};
