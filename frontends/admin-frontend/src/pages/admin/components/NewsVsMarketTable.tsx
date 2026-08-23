import React, { useEffect, useState } from 'react';
import { newsIntelligenceApi } from '@/api/newsIntelligenceApi';

interface ValidationRow {
  id: number;
  cluster_id: number;
  symbol: string;
  canonical_title: string;
  sentiment_score: number | null;
  sentiment_direction: string;
  actual_direction: string | null;
  sentiment_accuracy: string;
  impact_score: number;
  reaction_latency_category: string;
  signal_effectiveness_score: number;
  is_false_signal: number;
  false_signal_reason: string | null;
  reaction_type: string;
  created_at: string;
}

const ACCURACY_CONFIG: Record<string, { label: string; color: string; icon: string }> = {
  CORRECT:   { label: 'Correct',   color: '#10b981', icon: '✅' },
  INCORRECT: { label: 'Incorrect', color: '#ef4444', icon: '❌' },
  MIXED:     { label: 'Mixed',     color: '#f59e0b', icon: '⚡' },
  NEUTRAL:   { label: 'Neutral',   color: '#64748b', icon: '➖' },
};

const LATENCY_CONFIG: Record<string, { label: string; color: string }> = {
  IMMEDIATE:   { label: 'Immediate',   color: '#10b981' },
  DELAYED:     { label: 'Delayed',     color: '#f59e0b' },
  NO_REACTION: { label: 'No Reaction', color: '#475569' },
};

function ImpactBar({ score }: { score: number }) {
  const color = score >= 0.5 ? '#10b981' : score >= 0.25 ? '#f59e0b' : '#475569';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
      <div style={{ width: '48px', height: '4px', background: 'rgba(255,255,255,0.08)', borderRadius: '2px', overflow: 'hidden' }}>
        <div style={{ height: '100%', width: `${score * 100}%`, background: color, borderRadius: '2px' }} />
      </div>
      <span style={{ fontSize: '10px', fontFamily: 'monospace', color, minWidth: '28px' }}>
        {(score * 100).toFixed(0)}%
      </span>
    </div>
  );
}

export const NewsVsMarketTable: React.FC = () => {
  const [rows, setRows]     = useState<ValidationRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState<string | null>(null);
  const [falseOnly, setFalseOnly] = useState(false);
  const [sinceHours, setSinceHours] = useState(24);

  const load = async () => {
    setLoading(true);
    try {
      const data = await newsIntelligenceApi.getValidations({ sinceHours, falseOnly, limit: 50 });
      setRows(data);
    } catch (e: any) {
      setError(e.message || 'Failed to load');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, [falseOnly, sinceHours]);

  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(15,23,42,0.95), rgba(30,41,59,0.90))',
      border: '1px solid rgba(99,102,241,0.2)', borderRadius: '16px',
      overflow: 'hidden', backdropFilter: 'blur(12px)',
    }}>
      {/* Header */}
      <div style={{ padding: '16px 20px', borderBottom: '1px solid rgba(255,255,255,0.06)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '10px' }}>
        <div>
          <h3 style={{ margin: 0, fontSize: '15px', fontWeight: 700, color: '#f1f5f9' }}>
            📊 News vs Market
          </h3>
          <p style={{ margin: 0, fontSize: '12px', color: '#64748b' }}>
            Did the news actually move the market?
          </p>
        </div>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <select
            value={sinceHours}
            onChange={e => setSinceHours(+e.target.value)}
            style={{ background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.1)', color: '#94a3b8', borderRadius: '8px', padding: '5px 10px', fontSize: '12px' }}
          >
            {[4, 12, 24, 48, 72].map(h => <option key={h} value={h}>{h}h</option>)}
          </select>
          <button
            id="false-only-toggle"
            onClick={() => setFalseOnly(!falseOnly)}
            style={{
              padding: '5px 12px', borderRadius: '20px', border: 'none', cursor: 'pointer',
              fontSize: '11px', fontWeight: 600,
              background: falseOnly ? 'rgba(239,68,68,0.25)' : 'rgba(255,255,255,0.05)',
              color: falseOnly ? '#f87171' : '#64748b',
            }}
          >
            {falseOnly ? '🚫 False Only' : 'All Signals'}
          </button>
        </div>
      </div>

      {loading && <div style={{ textAlign: 'center', color: '#64748b', padding: '24px' }}>Loading…</div>}
      {error && <div style={{ color: '#f87171', padding: '16px', fontSize: '13px' }}>{error}</div>}

      {!loading && !error && (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
                {['Headline', 'Symbol', 'Sentiment', 'Actual', 'Accuracy', 'Impact', 'Timing', 'Effectiveness'].map(h => (
                  <th key={h} style={{ padding: '8px 12px', textAlign: 'left', color: '#475569', fontWeight: 600, fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.06em', whiteSpace: 'nowrap' }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr><td colSpan={8} style={{ textAlign: 'center', color: '#475569', padding: '24px' }}>No validation records found. News-market validation is implemented but has not captured runtime data yet.</td></tr>
              )}
              {rows.map(row => {
                const acc = ACCURACY_CONFIG[row.sentiment_accuracy] ?? ACCURACY_CONFIG.NEUTRAL;
                const lat = LATENCY_CONFIG[row.reaction_latency_category] ?? LATENCY_CONFIG.NO_REACTION;
                const isFalse = row.is_false_signal === 1;
                return (
                  <tr key={row.id} style={{
                    borderBottom: '1px solid rgba(255,255,255,0.04)',
                    background: isFalse ? 'rgba(239,68,68,0.06)' : 'transparent',
                    transition: 'background 0.15s',
                  }}>
                    {/* Headline */}
                    <td style={{ padding: '10px 12px', maxWidth: '240px' }}>
                      <div style={{ color: '#e2e8f0', fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {row.canonical_title || '—'}
                      </div>
                      {isFalse && (
                        <div style={{ fontSize: '10px', color: '#f87171', marginTop: '2px' }}>
                          🚫 {row.false_signal_reason?.replace(/_/g, ' ')}
                        </div>
                      )}
                    </td>
                    {/* Symbol */}
                    <td style={{ padding: '10px 12px', whiteSpace: 'nowrap' }}>
                      <span style={{ background: 'rgba(99,102,241,0.15)', color: '#a5b4fc', padding: '2px 8px', borderRadius: '12px', fontFamily: 'monospace', fontSize: '11px', fontWeight: 700 }}>
                        {row.symbol}
                      </span>
                    </td>
                    {/* Sentiment */}
                    <td style={{ padding: '10px 12px', whiteSpace: 'nowrap' }}>
                      <span style={{
                        color: row.sentiment_direction === 'BULLISH' ? '#10b981' : row.sentiment_direction === 'BEARISH' ? '#ef4444' : '#64748b',
                        fontWeight: 600, fontSize: '11px',
                      }}>
                        {row.sentiment_direction}
                        {row.sentiment_score != null && (
                          <span style={{ opacity: 0.6, fontWeight: 400, marginLeft: '4px' }}>
                            ({(row.sentiment_score > 0 ? '+' : '')}{row.sentiment_score?.toFixed(2)})
                          </span>
                        )}
                      </span>
                    </td>
                    {/* Actual */}
                    <td style={{ padding: '10px 12px', whiteSpace: 'nowrap' }}>
                      <span style={{
                        color: row.actual_direction === 'UP' ? '#10b981' : row.actual_direction === 'DOWN' ? '#ef4444' : '#64748b',
                        fontWeight: 600, fontSize: '11px',
                      }}>
                        {row.actual_direction || '—'}
                      </span>
                    </td>
                    {/* Accuracy */}
                    <td style={{ padding: '10px 12px', whiteSpace: 'nowrap' }}>
                      <span style={{ color: acc.color, fontSize: '12px' }}>
                        {acc.icon} {acc.label}
                      </span>
                    </td>
                    {/* Impact */}
                    <td style={{ padding: '10px 12px' }}>
                      <ImpactBar score={row.impact_score} />
                    </td>
                    {/* Timing */}
                    <td style={{ padding: '10px 12px', whiteSpace: 'nowrap' }}>
                      <span style={{ color: lat.color, fontSize: '11px', fontWeight: 600 }}>
                        {lat.label}
                      </span>
                    </td>
                    {/* Effectiveness */}
                    <td style={{ padding: '10px 12px' }}>
                      <ImpactBar score={row.signal_effectiveness_score} />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};
