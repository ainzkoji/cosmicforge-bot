import React, { useEffect, useState } from 'react';
import { newsIntelligenceApi } from '@/api/newsIntelligenceApi';

interface NarrativeRow {
  narrative_type: string;
  sample_count: number;
  avg_impact_score: number;
  avg_price_move_pct: number;
  correct_sentiment_ratio: number;
  false_signal_ratio: number;
  avg_effectiveness_score: number;
}

const NARRATIVE_ICONS: Record<string, string> = {
  ETF_APPROVAL:          '📋',
  REGULATORY_ACTION:     '⚖️',
  HACK_EXPLOIT:          '🔓',
  PARTNERSHIP_ADOPTION:  '🤝',
  WHALE_MOVEMENT:        '🐳',
  MARKET_SENTIMENT:      '📊',
  FUNDING_INVESTMENT:    '💰',
  EXCHANGE_NEWS:         '🏦',
  MACRO_POLICY:          '🏛️',
  TOKEN_UNLOCK:          '🔐',
  RUMOR_SPECULATION:     '💬',
  GENERAL_CRYPTO_NEWS:   '📰',
};

function ImpactBar({ value, max = 1.0, color }: { value: number; max?: number; color: string }) {
  const pct = Math.min(100, (value / max) * 100);
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
      <div style={{ flex: 1, height: '5px', background: 'rgba(255,255,255,0.06)', borderRadius: '3px', overflow: 'hidden' }}>
        <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: '3px', transition: 'width 0.5s ease' }} />
      </div>
      <span style={{ fontSize: '10px', fontFamily: 'monospace', color, minWidth: '32px', textAlign: 'right' }}>
        {(value * 100).toFixed(0)}%
      </span>
    </div>
  );
}

export const NarrativePerformanceChart: React.FC = () => {
  const [rows, setRows]     = useState<NarrativeRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<'impact' | 'correct' | 'false'>('impact');

  useEffect(() => {
    const load = async () => {
      try {
        const data = await newsIntelligenceApi.getNarrativeEffectiveness();
        setRows(data);
      } catch (e: any) {
        setError(e.message || 'Failed');
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  const sorted = [...rows].sort((a, b) => {
    if (sortBy === 'impact')  return b.avg_impact_score - a.avg_impact_score;
    if (sortBy === 'correct') return b.correct_sentiment_ratio - a.correct_sentiment_ratio;
    return b.false_signal_ratio - a.false_signal_ratio;
  });

  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(15,23,42,0.95), rgba(30,41,59,0.90))',
      border: '1px solid rgba(99,102,241,0.2)', borderRadius: '16px',
      padding: '20px', backdropFilter: 'blur(12px)',
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '18px' }}>📈</span>
          <div>
            <h3 style={{ margin: 0, fontSize: '15px', fontWeight: 700, color: '#f1f5f9' }}>Narrative Performance</h3>
            <p style={{ margin: 0, fontSize: '11px', color: '#64748b' }}>Which narratives actually move markets?</p>
          </div>
        </div>
        {/* Sort pills */}
        <div style={{ display: 'flex', gap: '4px' }}>
          {(['impact', 'correct', 'false'] as const).map(k => (
            <button
              key={k}
              id={`nar-sort-${k}`}
              onClick={() => setSortBy(k)}
              style={{
                padding: '4px 10px', borderRadius: '16px', border: 'none', cursor: 'pointer',
                fontSize: '10px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em',
                background: sortBy === k ? 'rgba(99,102,241,0.25)' : 'rgba(255,255,255,0.04)',
                color: sortBy === k ? '#a5b4fc' : '#64748b',
              }}
            >
              {k === 'impact' ? '⚡ Impact' : k === 'correct' ? '✅ Accuracy' : '🚫 False'}
            </button>
          ))}
        </div>
      </div>

      {loading && <div style={{ textAlign: 'center', color: '#64748b', padding: '20px' }}>Loading…</div>}
      {error   && <div style={{ color: '#f87171', fontSize: '12px' }}>{error}</div>}

      {!loading && !error && sorted.length === 0 && (
        <div style={{ textAlign: 'center', color: '#475569', padding: '24px', fontSize: '13px' }}>
          No narrative data yet — run validation to populate.
        </div>
      )}

      <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', maxHeight: '440px', overflowY: 'auto' }}>
        {sorted.map((row, i) => {
          const icon = NARRATIVE_ICONS[row.narrative_type] ?? '📌';
          const impactColor  = row.avg_impact_score >= 0.4  ? '#10b981' : row.avg_impact_score >= 0.2 ? '#f59e0b' : '#475569';
          const correctColor = row.correct_sentiment_ratio >= 0.6 ? '#10b981' : row.correct_sentiment_ratio >= 0.4 ? '#f59e0b' : '#ef4444';
          const falseColor   = row.false_signal_ratio <= 0.2 ? '#10b981' : row.false_signal_ratio <= 0.4 ? '#f59e0b' : '#ef4444';
          return (
            <div key={row.narrative_type} style={{
              background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.05)',
              borderRadius: '12px', padding: '12px 14px',
            }}>
              {/* Row header */}
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span style={{ fontSize: '16px' }}>{icon}</span>
                  <span style={{ fontSize: '12px', fontWeight: 700, color: '#e2e8f0' }}>
                    {row.narrative_type.replace(/_/g, ' ')}
                  </span>
                </div>
                <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                  <span style={{ fontSize: '10px', color: '#475569' }}>n={row.sample_count}</span>
                  <span style={{ fontSize: '10px', fontWeight: 700, color: '#6366f1', background: 'rgba(99,102,241,0.12)', padding: '2px 6px', borderRadius: '8px' }}>
                    #{i + 1}
                  </span>
                </div>
              </div>

              {/* Metric bars */}
              <div style={{ display: 'grid', gridTemplateColumns: '80px 1fr', gap: '4px 8px', alignItems: 'center' }}>
                <span style={{ fontSize: '10px', color: '#475569', textAlign: 'right' }}>Avg Impact</span>
                <ImpactBar value={row.avg_impact_score} color={impactColor} />

                <span style={{ fontSize: '10px', color: '#475569', textAlign: 'right' }}>Accuracy</span>
                <ImpactBar value={row.correct_sentiment_ratio} color={correctColor} />

                <span style={{ fontSize: '10px', color: '#475569', textAlign: 'right' }}>False Rate</span>
                <ImpactBar value={row.false_signal_ratio} color={falseColor} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
