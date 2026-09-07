import { useQuery } from '@tanstack/react-query';
import { newsAdminApi } from '../../../api/newsIntelligenceApi';

function fmtTime(ts: string) {
  try { return new Date(ts).toLocaleString(); } catch { return ts; }
}

export function DuplicateNewsClusterView() {
  const { data = [], isLoading, error } = useQuery<any[]>({
    queryKey: ['duplicate-clusters'],
    queryFn: () => newsAdminApi.getDuplicateClusters(24, 2, 50),
    refetchInterval: 60_000,
  });

  return (
    <div>
      <div className="flex items-center gap-3 mb-3">
        <h3 className="text-sm font-semibold text-gray-700">Cross-Provider Deduplication</h3>
        <span className="text-xs bg-indigo-50 text-indigo-700 px-2 py-0.5 rounded-full font-medium">
          {data.length} multi-source clusters
        </span>
      </div>

      <p className="text-xs text-gray-400 mb-3">
        Clusters confirmed by 2+ providers — same story detected across different sources.
      </p>

      {isLoading && <p className="text-sm text-gray-500">Loading…</p>}
      {error && <p className="text-sm text-red-500">Failed to load clusters.</p>}

      {!isLoading && data.length === 0 && (
        <p className="text-sm text-gray-500 py-4 text-center">No multi-source clusters in last 24 h.</p>
      )}

      {data.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 text-sm">
            <thead className="bg-gray-50">
              <tr>
                {['#', 'Title', 'Sources', 'Confirmations', 'First Provider', 'Conflict', 'Fake Risk', 'First Seen'].map(h => (
                  <th key={h} className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-100">
              {data.map((row) => (
                <tr key={row.id} className="hover:bg-gray-50">
                  <td className="px-3 py-2 text-gray-400">{row.id}</td>
                  <td className="px-3 py-2 max-w-xs">
                    <span className="block truncate" title={row.canonical_title}>
                      {row.canonical_title}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-center">
                    <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-indigo-100 text-indigo-700 text-xs font-bold">
                      {row.source_count}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-center text-gray-600">{row.confirmation_count ?? 0}</td>
                  <td className="px-3 py-2 text-gray-500 capitalize">{row.first_seen_provider ?? '—'}</td>
                  <td className="px-3 py-2 text-center">
                    {row.conflict_flag ? (
                      <span className="text-red-500 text-xs font-bold">Yes</span>
                    ) : (
                      <span className="text-green-500 text-xs">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    {row.fake_news_risk_score !== null && row.fake_news_risk_score !== undefined ? (
                      <span className={`text-xs font-medium ${
                        row.fake_news_risk_score >= 0.80 ? 'text-red-700' :
                        row.fake_news_risk_score >= 0.60 ? 'text-orange-600' :
                        'text-green-700'
                      }`}>
                        {Math.round(row.fake_news_risk_score * 100)}%
                      </span>
                    ) : '—'}
                  </td>
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
