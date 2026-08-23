import { useQuery } from '@tanstack/react-query';
import { newsAdminApi, ConflictCluster } from '../../../api/newsIntelligenceApi';

function RiskBadge({ score }: { score: number | null }) {
  if (score === null) return <span className="text-gray-400 text-xs">—</span>;
  const pct = Math.round(score * 100);
  const cls =
    score >= 0.80 ? 'bg-red-100 text-red-800' :
    score >= 0.60 ? 'bg-orange-100 text-orange-800' :
                    'bg-green-100 text-green-700';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {pct}%
    </span>
  );
}

function fmtTime(ts: string) {
  try { return new Date(ts).toLocaleString(); } catch { return ts; }
}

export function NewsConflictWarningPanel() {
  const { data = [], isLoading, error } = useQuery<ConflictCluster[]>({
    queryKey: ['news-conflicts'],
    queryFn: () => newsAdminApi.getConflicts(24, 50),
    refetchInterval: 60_000,
  });

  return (
    <div>
      <div className="flex items-center gap-3 mb-3">
        <h3 className="text-sm font-semibold text-gray-700">Conflict Warnings</h3>
        <span className="text-xs bg-orange-50 text-orange-700 px-2 py-0.5 rounded-full font-medium">
          Suppressed from signals
        </span>
        {data.length > 0 && (
          <span className="text-xs bg-red-50 text-red-700 px-2 py-0.5 rounded-full">
            {data.length} active
          </span>
        )}
      </div>

      {isLoading && <p className="text-sm text-gray-500">Loading…</p>}
      {error && <p className="text-sm text-red-500">Failed to load conflicts.</p>}

      {!isLoading && data.length === 0 && (
        <p className="text-sm text-gray-500 py-4 text-center">No conflict warnings in the last 24 h.</p>
      )}

      {data.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 text-sm">
            <thead className="bg-gray-50">
              <tr>
                {['#', 'Title', 'Sources', 'Provider', 'Fake Risk', 'Mkt Status', 'First Seen'].map(h => (
                  <th key={h} className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-100">
              {data.map((row) => (
                <tr key={row.id} className="hover:bg-orange-50">
                  <td className="px-3 py-2 text-gray-400">{row.id}</td>
                  <td className="px-3 py-2 max-w-xs">
                    <span className="block truncate" title={row.canonical_title}>
                      {row.canonical_title}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-center">{row.source_count}</td>
                  <td className="px-3 py-2 text-gray-500 capitalize">{row.first_seen_provider ?? '—'}</td>
                  <td className="px-3 py-2"><RiskBadge score={row.fake_news_risk_score} /></td>
                  <td className="px-3 py-2 text-xs text-gray-600">{row.market_confirmation_status ?? '—'}</td>
                  <td className="px-3 py-2 text-gray-500 text-xs">{fmtTime(row.first_seen_utc)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
