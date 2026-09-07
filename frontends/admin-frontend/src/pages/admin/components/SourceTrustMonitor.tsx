import React, { useEffect, useState } from 'react';
import { newsIntelligenceApi } from '@/api/newsIntelligenceApi';

interface NewsSource {
  id: string;
  source_name: string;
  base_reliability_score: number;
  dynamic_reliability_score: number;
  is_trusted: number;
  is_blocked: number;
  updated_at: string;
}

const reliabilityColor = (score: number): string => {
  if (score >= 0.85) return '#10b981';
  if (score >= 0.65) return '#f59e0b';
  if (score >= 0.40) return '#f97316';
  return '#ef4444';
};

const reliabilityLabel = (score: number): string => {
  if (score >= 0.85) return 'HIGH';
  if (score >= 0.65) return 'MEDIUM';
  if (score >= 0.40) return 'LOW';
  return 'VERY LOW';
};

type FilterMode = 'all' | 'trusted' | 'blocked';

export const SourceTrustMonitor: React.FC = () => {
  const [sources, setSources]   = useState<NewsSource[]>([]);
  const [loading, setLoading]   = useState(true);
  const [error, setError]       = useState<string | null>(null);
  const [filter, setFilter]     = useState<FilterMode>('all');
  const [search, setSearch]     = useState('');

  useEffect(() => {
    const load = async () => {
      try {
        const result = await newsIntelligenceApi.getSources({
          trusted_only: filter === 'trusted',
          blocked_only: filter === 'blocked',
        });
        setSources(result);
      } catch (e: any) {
        setError(e.message || 'Failed to load sources');
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [filter]);

  const filtered = sources.filter(s =>
    s.source_name.toLowerCase().includes(search.toLowerCase()) ||
    s.id.toLowerCase().includes(search.toLowerCase())
  );

  const trusted  = sources.filter(s => s.is_trusted && !s.is_blocked).length;
  const blocked  = sources.filter(s => s.is_blocked).length;
  const untrusted = sources.filter(s => !s.is_trusted && !s.is_blocked).length;

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
          background: 'linear-gradient(135deg, #0ea5e9, #6366f1)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: '18px',
        }}>📡</div>
        <div>
          <h3 style={{ margin: 0, color: '#f1f5f9', fontSize: '16px', fontWeight: 700 }}>
            Source Trust Monitor
          </h3>
          <p style={{ margin: 0, color: '#64748b', fontSize: '12px' }}>
            Domain-level reliability registry
          </p>
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: '8px' }}>
          <span style={{ background: 'rgba(16,185,129,0.15)', color: '#10b981', padding: '3px 10px', borderRadius: '20px', fontSize: '11px', fontWeight: 600 }}>
            {trusted} Trusted
          </span>
          <span style={{ background: 'rgba(239,68,68,0.15)', color: '#ef4444', padding: '3px 10px', borderRadius: '20px', fontSize: '11px', fontWeight: 600 }}>
            {blocked} Blocked
          </span>
        </div>
      </div>

      {/* Filter tabs */}
      <div style={{ display: 'flex', gap: '6px', marginBottom: '14px' }}>
        {(['all', 'trusted', 'blocked'] as FilterMode[]).map(f => (
          <button
            key={f}
            id={`source-filter-${f}`}
            onClick={() => { setFilter(f); setLoading(true); }}
            style={{
              padding: '5px 14px', borderRadius: '20px', border: 'none',
              fontSize: '12px', fontWeight: 600, cursor: 'pointer',
              background: filter === f ? 'rgba(99,102,241,0.25)' : 'rgba(255,255,255,0.04)',
              color: filter === f ? '#a5b4fc' : '#64748b',
              transition: 'all 0.2s',
            }}
          >
            {f.toUpperCase()}
          </button>
        ))}
        <input
          placeholder="Search domain or name…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{
            marginLeft: 'auto', padding: '5px 12px', borderRadius: '20px',
            background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.08)',
            color: '#f1f5f9', fontSize: '12px', outline: 'none', width: '180px',
          }}
        />
      </div>

      {/* Table */}
      {loading && <div style={{ textAlign: 'center', color: '#64748b', padding: '20px' }}>Loading sources…</div>}
      {error   && <div style={{ color: '#f87171', padding: '12px', borderRadius: '8px', background: 'rgba(239,68,68,0.1)', fontSize: '13px' }}>{error}</div>}

      {!loading && !error && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', maxHeight: '380px', overflowY: 'auto' }}>
          {filtered.length === 0 && (
            <div style={{ textAlign: 'center', color: '#64748b', padding: '24px', fontSize: '13px' }}>
              No sources match the current filter.
            </div>
          )}
          {filtered.map(src => {
            const dynScore = src.dynamic_reliability_score;
            const drift    = +(dynScore - src.base_reliability_score).toFixed(3);
            const color    = reliabilityColor(dynScore);
            return (
              <div key={src.id} style={{
                display: 'flex', alignItems: 'center', gap: '12px',
                background: src.is_blocked
                  ? 'rgba(239,68,68,0.06)'
                  : src.is_trusted
                    ? 'rgba(16,185,129,0.05)'
                    : 'rgba(255,255,255,0.03)',
                border: `1px solid ${src.is_blocked ? 'rgba(239,68,68,0.15)' : src.is_trusted ? 'rgba(16,185,129,0.12)' : 'rgba(255,255,255,0.05)'}`,
                borderRadius: '10px', padding: '10px 14px',
              }}>
                {/* Status icon */}
                <span style={{ fontSize: '16px' }}>
                  {src.is_blocked ? '🚫' : src.is_trusted ? '✅' : '⚪'}
                </span>

                {/* Source name + domain */}
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: '13px', color: '#f1f5f9', fontWeight: 600 }}>
                    {src.source_name}
                  </div>
                  <div style={{ fontSize: '11px', color: '#64748b' }}>{src.id}</div>
                </div>

                {/* Reliability label */}
                <span style={{
                  fontSize: '10px', fontWeight: 700, color, padding: '2px 8px',
                  background: `${color}18`, borderRadius: '12px',
                }}>
                  {reliabilityLabel(dynScore)}
                </span>

                {/* Score bar */}
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <div style={{ width: '60px', height: '4px', background: 'rgba(255,255,255,0.06)', borderRadius: '2px', overflow: 'hidden' }}>
                    <div style={{ height: '100%', width: `${dynScore * 100}%`, background: color, borderRadius: '2px' }} />
                  </div>
                  <span style={{ fontSize: '12px', color, fontFamily: 'monospace', minWidth: '34px' }}>
                    {(dynScore * 100).toFixed(0)}%
                  </span>
                </div>

                {/* Score drift */}
                {drift !== 0 && (
                  <span style={{
                    fontSize: '10px', fontFamily: 'monospace',
                    color: drift > 0 ? '#10b981' : '#ef4444',
                  }}>
                    {drift > 0 ? '▲' : '▼'}{Math.abs(drift * 100).toFixed(1)}%
                  </span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};
