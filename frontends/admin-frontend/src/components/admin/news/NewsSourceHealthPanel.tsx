import React from 'react';
import { SourceStatus } from '../../../api/newsIntelligenceApi';

interface Props {
  sources: SourceStatus[];
  loading: boolean;
}

const STATUS_COLOR: Record<string, string> = {
  HEALTHY:  '#22c55e',
  DEGRADED: '#f59e0b',
  FAILED:   '#ef4444',
  STALE:    '#f97316',
  UNKNOWN:  '#64748b',
};

const STATUS_BG: Record<string, string> = {
  HEALTHY:  'rgba(34,197,94,0.12)',
  DEGRADED: 'rgba(245,158,11,0.12)',
  FAILED:   'rgba(239,68,68,0.12)',
  STALE:    'rgba(249,115,22,0.12)',
  UNKNOWN:  'rgba(100,116,139,0.12)',
};

function _badge(status: string | null) {
  const s = status || 'UNKNOWN';

  return (
    <span style={{
      background: STATUS_BG[s] || STATUS_BG.UNKNOWN,
      color: STATUS_COLOR[s] || STATUS_COLOR.UNKNOWN,
      border: `1px solid ${STATUS_COLOR[s] || STATUS_COLOR.UNKNOWN}44`,
      borderRadius: '6px', padding: '2px 8px', fontSize: '10px', fontWeight: 700,
      textTransform: 'uppercase',
    }}>
      {s}
    </span>
  );
}

function _relTime(utc: string | null): string {
  if (!utc) return '—';
  const diff = Date.now() - new Date(utc).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1) return 'just now';
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  return `${h}h ago`;
}

export const NewsSourceHealthPanel: React.FC<Props> = ({ sources, loading }) => {
  if (loading) {
    return (
      <div style={{ color: '#475569', fontSize: '13px', padding: '20px' }}>
        Loading source health…
      </div>
    );
  }

  if (!sources.length) {
    return (
      <div style={{
        background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.06)',
        borderRadius: '12px', padding: '30px', textAlign: 'center', color: '#475569', fontSize: '13px',
      }}>
        No sources configured. Run migration #42 to seed RSS sources.
      </div>
    );
  }

  const hasHealthSamples = sources.some((src) => src.last_fetch_utc || src.last_success_utc || src.items_fetched_last_run != null);

  return (
    <div style={{
      background: 'rgba(255,255,255,0.03)',
      border: '1px solid rgba(255,255,255,0.06)',
      borderRadius: '14px',
      overflow: 'hidden',
    }}>
      <div style={{
        padding: '14px 20px',
        borderBottom: '1px solid rgba(255,255,255,0.06)',
        fontSize: '13px', fontWeight: 700, color: '#e2e8f0',
        display: 'flex', alignItems: 'center', gap: '8px',
      }}>
        <span>📡</span> Source Health ({sources.length} sources)
      </div>
      {!hasHealthSamples && (
        <div style={{
          padding: '10px 20px',
          borderBottom: '1px solid rgba(255,255,255,0.06)',
          color: '#f59e0b',
          background: 'rgba(245,158,11,0.08)',
          fontSize: '12px',
        }}>
          Provider status rows exist, but no provider health samples have been recorded yet.
        </div>
      )}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
          <thead>
            <tr style={{ background: 'rgba(255,255,255,0.03)' }}>
              {['Source', 'Type', 'Category', 'Status', 'Last Fetch', 'Items', 'Error'].map(h => (
                <th key={h} style={{
                  padding: '10px 14px', textAlign: 'left',
                  color: '#64748b', fontWeight: 600, fontSize: '11px',
                  textTransform: 'uppercase', letterSpacing: '0.04em',
                  borderBottom: '1px solid rgba(255,255,255,0.06)',
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sources.map(src => (
              <tr key={src.id} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                <td style={{ padding: '10px 14px', color: '#e2e8f0', fontWeight: 500 }}>
                  {src.source_name}
                  {src.is_enabled === 0 && (
                    <span style={{ marginLeft: '6px', fontSize: '10px', color: '#475569' }}>(disabled)</span>
                  )}
                </td>
                <td style={{ padding: '10px 14px', color: '#94a3b8' }}>
                  {src.source_type || '—'}
                </td>
                <td style={{ padding: '10px 14px', color: '#94a3b8' }}>
                  {src.category || '—'}
                </td>
                <td style={{ padding: '10px 14px' }}>
                  {_badge(src.health_status)}
                </td>
                <td style={{ padding: '10px 14px', color: '#64748b', fontSize: '11px' }}>
                  {_relTime(src.last_fetch_utc)}
                </td>
                <td style={{ padding: '10px 14px', color: '#94a3b8', textAlign: 'right' }}>
                  {src.items_fetched_last_run ?? '—'}
                </td>
                <td style={{
                  padding: '10px 14px', color: '#f87171', fontSize: '11px',
                  maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}>
                  {src.last_error || '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
