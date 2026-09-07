import React from 'react';
import { NewsCluster, DataQualityStatus } from '../../api/newsIntelligenceApi';

interface NewsClusterCardProps {
  cluster: NewsCluster;
}

// ── Quality status config ────────────────────────────────────────────────────

const DQ_CONFIG: Record<DataQualityStatus | string, {
  bg: string; text: string; border: string; icon: string; label: string;
}> = {
  HIGH_CONFIDENCE:   { bg: 'bg-emerald-500/10', text: 'text-emerald-400', border: 'border-emerald-500/30', icon: '✅', label: 'High Confidence' },
  MEDIUM_CONFIDENCE: { bg: 'bg-amber-500/10',   text: 'text-amber-400',   border: 'border-amber-500/30',   icon: '⚠️', label: 'Medium Confidence' },
  LOW_CONFIDENCE:    { bg: 'bg-slate-500/10',   text: 'text-slate-400',   border: 'border-slate-500/30',   icon: '📉', label: 'Low Confidence' },
  SPAM:              { bg: 'bg-red-500/10',      text: 'text-red-400',     border: 'border-red-500/40',     icon: '🚫', label: 'SPAM' },
  MANIPULATED:       { bg: 'bg-red-700/15',      text: 'text-red-300',     border: 'border-red-600/50',     icon: '⛔', label: 'MANIPULATED' },
  STALE:             { bg: 'bg-gray-500/10',     text: 'text-gray-400',    border: 'border-gray-500/25',    icon: '🕰️', label: 'Stale' },
};

const MANIP_FLAG_CONFIG: Record<string, { label: string; color: string }> = {
  POSSIBLE_MANIPULATION: { label: '⚠ Possible Manipulation', color: 'text-orange-400 bg-orange-500/10 border-orange-500/30' },
  BOT_AMPLIFICATION:     { label: '🤖 Bot Amplification',     color: 'text-red-400 bg-red-500/10 border-red-500/30' },
  RUMOR_ONLY:            { label: '💬 Rumor Only',             color: 'text-yellow-400 bg-yellow-500/10 border-yellow-500/30' },
  LOW_CONFIDENCE_EVENT:  { label: '📉 Low-Quality Sources',   color: 'text-slate-400 bg-slate-500/10 border-slate-500/30' },
};

function ScoreBar({ value, color }: { value: number; color: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-12 h-1.5 rounded-full bg-white/5 overflow-hidden">
        <div
          className={`h-full rounded-full ${color}`}
          style={{ width: `${Math.max(2, value * 100)}%`, transition: 'width 0.4s ease' }}
        />
      </div>
      <span className="text-[10px] font-mono tabular-nums opacity-70">{(value * 100).toFixed(0)}%</span>
    </div>
  );
}

// ── Component ────────────────────────────────────────────────────────────────

export const NewsClusterCard: React.FC<NewsClusterCardProps> = ({ cluster }) => {
  const dq    = cluster.data_quality_status as DataQualityStatus ?? 'LOW_CONFIDENCE';
  const dqCfg = DQ_CONFIG[dq] ?? DQ_CONFIG.LOW_CONFIDENCE;
  const isWarn = dq === 'SPAM' || dq === 'MANIPULATED';
  const isValid = cluster.is_valid_signal === 1;
  const flagCfg = cluster.manipulation_flag ? MANIP_FLAG_CONFIG[cluster.manipulation_flag] : null;

  return (
    <div className={`
      rounded-xl border p-4 transition-all duration-200
      ${isWarn
        ? 'border-red-500/40 bg-red-900/10'
        : isValid
          ? 'border-emerald-500/20 bg-emerald-900/5'
          : 'border-white/8 bg-white/3'
      }
    `}>
      {/* Header row */}
      <div className="flex justify-between items-start gap-3 mb-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            {/* Data quality badge */}
            <span className={`
              inline-flex items-center gap-1 text-[10px] font-bold uppercase tracking-wider
              px-2 py-0.5 rounded-full border ${dqCfg.bg} ${dqCfg.text} ${dqCfg.border}
            `}>
              {dqCfg.icon} {dqCfg.label}
            </span>

            {/* Valid signal badge */}
            {isValid && (
              <span className="inline-flex items-center text-[10px] font-bold px-2 py-0.5 rounded-full bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">
                VALID SIGNAL
              </span>
            )}
          </div>

          <h4 className="text-sm font-semibold text-white/90 leading-snug line-clamp-2">
            {cluster.canonical_title}
          </h4>
          {cluster.summary && (
            <p className="text-xs text-white/40 line-clamp-1 mt-0.5">{cluster.summary}</p>
          )}
        </div>

        <div className="flex flex-col items-end text-[10px] text-white/30 whitespace-nowrap shrink-0">
          <span className="font-mono">{new Date(cluster.first_seen_utc).toLocaleTimeString()}</span>
          <span className="mt-0.5">First Seen</span>
        </div>
      </div>

      {/* Manipulation flag warning banner */}
      {flagCfg && (
        <div className={`rounded-lg border px-3 py-1.5 mb-3 text-xs font-semibold ${flagCfg.color}`}>
          {flagCfg.label}
          {cluster.manipulation_reason && cluster.manipulation_reason !== cluster.manipulation_flag && (
            <span className="opacity-60 ml-1 font-normal">— {cluster.manipulation_reason}</span>
          )}
        </div>
      )}

      {/* Score metrics row */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 mb-3">
        <div>
          <div className="text-[9px] text-white/30 uppercase tracking-wider mb-0.5">Confidence</div>
          <ScoreBar value={cluster.cluster_confidence} color="bg-indigo-400" />
        </div>
        <div>
          <div className="text-[9px] text-white/30 uppercase tracking-wider mb-0.5">Reliability</div>
          <ScoreBar value={cluster.highest_reliability_score} color="bg-emerald-400" />
        </div>
        <div>
          <div className="text-[9px] text-white/30 uppercase tracking-wider mb-0.5">Spam Score</div>
          <ScoreBar
            value={cluster.spam_score ?? 0}
            color={cluster.spam_score > 0.45 ? 'bg-red-400' : cluster.spam_score > 0.25 ? 'bg-amber-400' : 'bg-slate-400'}
          />
        </div>
        <div>
          <div className="text-[9px] text-white/30 uppercase tracking-wider mb-0.5">Freshness</div>
          <ScoreBar value={cluster.latency_score ?? 0} color="bg-sky-400" />
        </div>
      </div>

      {/* Footer: source count + narratives */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2 text-[10px] text-white/40">
          <span className="bg-white/6 rounded px-1.5 py-0.5 font-mono">
            {cluster.source_count} src · {cluster.provider_count} prov
          </span>
        </div>

        {cluster.narratives && cluster.narratives.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {cluster.narratives.slice(0, 3).map((n: any) => (
              <span key={n.id} className="text-[9px] uppercase font-bold tracking-wide px-1.5 py-0.5 rounded border border-violet-500/30 text-violet-300 bg-violet-500/10">
                {n.narrative_type.replace(/_/g, ' ')}
              </span>
            ))}
            {cluster.narratives.length > 3 && (
              <span className="text-[9px] text-white/30">+{cluster.narratives.length - 3}</span>
            )}
          </div>
        )}
      </div>
    </div>
  );
};
