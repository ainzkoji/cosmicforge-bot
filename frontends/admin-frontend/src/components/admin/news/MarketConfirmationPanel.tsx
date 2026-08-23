import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { newsAdminApi, MarketConfirmation } from '../../../api/newsIntelligenceApi';

const STATUS_STYLES: Record<string, string> = {
  MARKET_CONFIRMED:          'bg-green-100 text-green-800',
  NO_MARKET_REACTION:        'bg-gray-100 text-gray-600',
  DELAYED_REACTION:          'bg-yellow-100 text-yellow-800',
  CONFLICTING_MARKET_REACTION: 'bg-red-100 text-red-800',
  POSSIBLE_FAKE_NEWS:        'bg-orange-100 text-orange-700',
  HIGH_FAKE_NEWS_RISK:       'bg-red-200 text-red-900',
  CONFLICTING_REPORTS:       'bg-purple-100 text-purple-800',
};

function StatusBadge({ status }: { status: string | null }) {
  if (!status) return <span className="text-gray-400 text-xs">—</span>;
  const cls = STATUS_STYLES[status] ?? 'bg-gray-100 text-gray-600';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {status.replace(/_/g, ' ')}
    </span>
  );
}

function fmtTime(ts: string) {
  try { return new Date(ts).toLocaleString(); } catch { return ts; }
}

const FILTERS = [
  { label: 'All', value: '' },
  { label: 'Confirmed', value: 'MARKET_CONFIRMED' },
  { label: 'No Reaction', value: 'NO_MARKET_REACTION' },
  { label: 'Delayed', value: 'DELAYED_REACTION' },
  { label: 'Conflicting', value: 'CONFLICTING_MARKET_REACTION' },
];

export function MarketConfirmationPanel() {
  const [filter, setFilter] = useState('');

  const { data = [], isLoading, error } = useQuery<MarketConfirmation[]>({
    queryKey: ['market-confirmations', filter],
    queryFn: () => newsAdminApi.getMarketConfirmations(24, filter || undefined, 50),
    refetchInterval: 60_000,
  });

  return (
    <div>
      <div className="flex items-center gap-3 mb-3 flex-wrap">
        <h3 className="text-sm font-semibold text-gray-700">Market Confirmations</h3>
        <div className="flex gap-1">
          {FILTERS.map(f => (
            <button
              key={f.value}
              onClick={() => setFilter(f.value)}
              className={`text-xs px-3 py-1 rounded-full border transition-colors ${
                filter === f.value
                  ? 'bg-indigo-600 text-white border-indigo-600'
                  : 'bg-white text-gray-600 border-gray-200 hover:border-indigo-300'
              }`}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      {isLoading && <p className="text-sm text-gray-500">Loading…</p>}
      {error && <p className="text-sm text-red-500">Failed to load confirmations.</p>}

      {!isLoading && data.length === 0 && (
        <p className="text-sm text-gray-500 py-4 text-center">No market confirmations yet.</p>
      )}

      {data.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 text-sm">
            <thead className="bg-gray-50">
              <tr>
                {['#', 'Title', 'Status', 'Confirmations', 'Conflict', 'Fake Risk', 'First Seen'].map(h => (
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
                  <td className="px-3 py-2">
                    <StatusBadge status={row.market_confirmation_status} />
                  </td>
                  <td className="px-3 py-2 text-center">{row.confirmation_count}</td>
                  <td className="px-3 py-2 text-center">
                    {row.conflict_flag ? (
                      <span className="text-red-500 font-bold text-xs">Yes</span>
                    ) : (
                      <span className="text-gray-400 text-xs">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    {row.fake_news_risk_score !== null ? (
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
