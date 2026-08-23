import { useQuery } from '@tanstack/react-query';
import { newsAdminApi, RtProviderStatus } from '../../../api/newsIntelligenceApi';

const HEALTH_COLORS: Record<string, string> = {
  HEALTHY: 'bg-green-100 text-green-800',
  DEGRADED: 'bg-yellow-100 text-yellow-800',
  FAILED: 'bg-red-100 text-red-800',
  STALE: 'bg-orange-100 text-orange-800',
  DISABLED: 'bg-gray-100 text-gray-600',
  WAITING_CONFIG: 'bg-sky-100 text-sky-800',
  PLACEHOLDER: 'bg-violet-100 text-violet-800',
  UNKNOWN: 'bg-gray-100 text-gray-600',
};

function HealthBadge({ status }: { status: string }) {
  const cls = HEALTH_COLORS[status] ?? HEALTH_COLORS.UNKNOWN;
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {status}
    </span>
  );
}

function fmtLatency(s: number) {
  return s > 0 ? `${s.toFixed(2)}s` : '—';
}

function fmtTime(ts: string | null) {
  if (!ts) return '—';
  try {
    return new Date(ts).toLocaleTimeString();
  } catch {
    return ts;
  }
}

export function NewsProviderStatusPanel() {
  const { data = [], isLoading, error } = useQuery<RtProviderStatus[]>({
    queryKey: ['rt-provider-status'],
    queryFn: () => newsAdminApi.getRtProviderStatus(),
    refetchInterval: 30_000,
  });

  if (isLoading) return <p className="text-sm text-gray-500 p-4">Loading provider status…</p>;
  if (error) return <p className="text-sm text-red-500 p-4">Failed to load provider status.</p>;

  return (
    <div>
      <div className="flex items-center gap-3 mb-3">
        <h3 className="text-sm font-semibold text-gray-700">Real-Time API Providers</h3>
        <span className="text-xs bg-blue-50 text-blue-700 px-2 py-0.5 rounded-full font-medium">
          Shadow Only
        </span>
      </div>

      {data.length === 0 ? (
        <p className="text-sm text-gray-500">No real-time providers configured.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 text-sm">
            <thead className="bg-gray-50">
              <tr>
                {['Provider', 'Status', 'Health', 'Last Fetch', 'Last Success', 'Avg Latency', 'Items Today', 'Error'].map(h => (
                  <th key={h} className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-100">
              {data.map((row) => (
                <tr key={row.provider} className="hover:bg-gray-50">
                  <td className="px-3 py-2 font-medium capitalize">{row.provider}</td>
                  <td className="px-3 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded ${row.is_enabled ? 'bg-green-50 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
                      {row.is_enabled ? 'Enabled' : 'Disabled'}
                    </span>
                  </td>
                  <td className="px-3 py-2"><HealthBadge status={row.health_status} /></td>
                  <td className="px-3 py-2 text-gray-500">{fmtTime(row.last_fetch_utc)}</td>
                  <td className="px-3 py-2 text-gray-500">{fmtTime(row.last_success_utc)}</td>
                  <td className="px-3 py-2">{fmtLatency(row.latency_avg_seconds)}</td>
                  <td className="px-3 py-2 text-center">{row.items_fetched_today}</td>
                  <td className="px-3 py-2 text-red-600 text-xs max-w-xs truncate" title={row.last_error ?? ''}>
                    {row.last_error ?? '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
