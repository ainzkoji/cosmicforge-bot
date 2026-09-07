import React, { useEffect, useState } from 'react';
import { newsIntelligenceApi } from '@/api/newsIntelligenceApi';

interface ValidationSummary {
  total_validations: number;
  correct_count: number;
  incorrect_count: number;
  false_signal_count: number;
  correct_pct: number;
  incorrect_pct: number;
  false_signal_pct: number;
  avg_impact_score: number;
  avg_effectiveness_score: number;
  by_latency_category: { reaction_latency_category: string; count: number }[];
  by_false_reason: { false_signal_reason: string; count: number }[];
}

function DonutRing({ segments }: { segments: { value: number; color: string; label: string }[] }) {
  const total = segments.reduce((s, x) => s + x.value, 0) || 1;
  let offset = 0;
  const r = 36;
  const circ = 2 * Math.PI * r;

  return (
    <svg width="100" height="100" viewBox="0 0 100 100">
      {segments.map((seg, i) => {
        const pct = seg.value / total;
        const dash = pct * circ;
        const gap  = circ - dash;
        const el = (
          <circle
            key={i}
            cx="50" cy="50" r={r}
            fill="none"
            stroke={seg.color}
            strokeWidth="14"
            strokeDasharray={`${dash} ${gap}`}
            strokeDashoffset={-offset}
            transform="rotate(-90 50 50)"
            style={{ transition: 'stroke-dasharray 0.5s ease' }}
          />
        );
        offset += dash;
        return el;
      })}
      <circle cx="50" cy="50" r="26" fill="rgba(15,23,42,0.9)" />
    </svg>
  );
}

export const SignalAccuracyPanel: React.FC = () => {
  const [data, setData]     = useState<ValidationSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const result = await newsIntelligenceApi.getValidationSummary();
        setData(result);
      } catch (e: any) {
        setError(e.message || 'Failed');
      } finally {
        setLoading(false);
      }
    };
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, []);

  const segments = data ? [
    { value: data.correct_count,   color: '#10b981', label: 'Correct' },
    { value: data.incorrect_count, color: '#ef4444', label: 'Incorrect' },
    { value: data.false_signal_count, color: '#f97316', label: 'False Signal' },
    { value: Math.max(0, data.total_validations - data.correct_count - data.incorrect_count - data.false_signal_count),
      color: '#334155', label: 'Neutral/Mixed' },
  ] : [];

  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(15,23,42,0.95), rgba(30,41,59,0.90))',
      border: '1px solid rgba(99,102,241,0.2)', borderRadius: '16px',
      padding: '20px', backdropFilter: 'blur(12px)',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
        <span style={{ fontSize: '18px' }}>🎯</span>
        <div>
          <h3 style={{ margin: 0, fontSize: '15px', fontWeight: 700, color: '#f1f5f9' }}>Signal Accuracy</h3>
          <p style={{ margin: 0, fontSize: '11px', color: '#64748b' }}>Sentiment vs actual market direction</p>
        </div>
      </div>

      {loading && <div style={{ textAlign: 'center', color: '#64748b', padding: '20px' }}>Loading…</div>}
      {error   && <div style={{ color: '#f87171', fontSize: '12px' }}>{error}</div>}

      {data && (
        <>
          <div style={{ display: 'flex', gap: '20px', alignItems: 'center', marginBottom: '16px' }}>
            {/* Donut */}
            <div style={{ flexShrink: 0 }}>
              <DonutRing segments={segments} />
            </div>
            {/* Legend */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', flex: 1 }}>
              {[
                { color: '#10b981', label: 'Correct',     value: data.correct_pct },
                { color: '#ef4444', label: 'Incorrect',   value: data.incorrect_pct },
                { color: '#f97316', label: 'False Signal', value: data.false_signal_pct },
              ].map(({ color, label, value }) => (
                <div key={label} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                    <div style={{ width: '8px', height: '8px', borderRadius: '2px', background: color }} />
                    <span style={{ fontSize: '12px', color: '#94a3b8' }}>{label}</span>
                  </div>
                  <span style={{ fontSize: '12px', fontFamily: 'monospace', color, fontWeight: 700 }}>
                    {value.toFixed(1)}%
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Avg metrics */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', marginBottom: '12px' }}>
            {[
              { label: 'Avg Impact',       value: (data.avg_impact_score * 100).toFixed(1) + '%',       color: '#6366f1' },
              { label: 'Avg Effectiveness', value: (data.avg_effectiveness_score * 100).toFixed(1) + '%', color: '#8b5cf6' },
            ].map(({ label, value, color }) => (
              <div key={label} style={{ background: 'rgba(255,255,255,0.03)', borderRadius: '10px', padding: '10px', textAlign: 'center', border: '1px solid rgba(255,255,255,0.05)' }}>
                <div style={{ fontSize: '18px', fontWeight: 800, color, fontFamily: 'monospace' }}>{value}</div>
                <div style={{ fontSize: '10px', color: '#475569', marginTop: '2px' }}>{label}</div>
              </div>
            ))}
          </div>

          {/* Latency breakdown */}
          {data.by_latency_category.length > 0 && (
            <div>
              <div style={{ fontSize: '10px', color: '#475569', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '6px' }}>
                Reaction Timing
              </div>
              <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                {data.by_latency_category.map(({ reaction_latency_category, count }) => {
                  const col = reaction_latency_category === 'IMMEDIATE' ? '#10b981'
                            : reaction_latency_category === 'DELAYED'   ? '#f59e0b' : '#475569';
                  return (
                    <span key={reaction_latency_category} style={{
                      padding: '3px 10px', borderRadius: '20px', fontSize: '11px', fontWeight: 600,
                      background: `${col}18`, color: col, border: `1px solid ${col}30`,
                    }}>
                      {reaction_latency_category.replace('_', ' ')} · {count}
                    </span>
                  );
                })}
              </div>
            </div>
          )}

          {/* False signal breakdown */}
          {data.by_false_reason.length > 0 && (
            <div style={{ marginTop: '10px' }}>
              <div style={{ fontSize: '10px', color: '#475569', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '6px' }}>
                False Signal Types
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                {data.by_false_reason.map(({ false_signal_reason, count }) => (
                  <div key={false_signal_reason} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: '11px', color: '#f87171' }}>
                      {false_signal_reason?.replace(/_/g, ' ')}
                    </span>
                    <span style={{ fontSize: '11px', fontFamily: 'monospace', color: '#f87171', fontWeight: 700 }}>×{count}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div style={{ marginTop: '12px', textAlign: 'right', fontSize: '10px', color: '#334155' }}>
            {data.total_validations} total validations
          </div>
        </>
      )}
    </div>
  );
};
