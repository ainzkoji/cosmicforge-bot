import { useQuery } from '@tanstack/react-query';
import { newsAdminApi } from '../../../api/newsIntelligenceApi';
import { NewsProviderStatusPanel } from './NewsProviderStatusPanel';
import { NewsConflictWarningPanel } from './NewsConflictWarningPanel';
import { MarketConfirmationPanel } from './MarketConfirmationPanel';
import { DuplicateNewsClusterView } from './DuplicateNewsClusterView';

function KpiCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 min-w-[120px]">
      <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">{label}</p>
      <p className="text-2xl font-bold text-gray-900">{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
    </div>
  );
}

function fmtTitle(title: string | null) {
  if (!title) return '—';
  return title.length > 80 ? title.slice(0, 77) + '…' : title;
}

export function RealTimeNewsMonitor() {
  const { data: rtFeed = [], isLoading: feedLoading } = useQuery<any[]>({
    queryKey: ['rt-feed', 6],
    queryFn: () => newsAdminApi.getRtFeed(6, 20),
    refetchInterval: 30_000,
  });

  const { data: providerStatus = [] } = useQuery<any[]>({
    queryKey: ['rt-provider-status'],
    queryFn: () => newsAdminApi.getRtProviderStatus(),
    refetchInterval: 30_000,
  });

  const enabledProviders = providerStatus.filter((p: any) => p.is_enabled).length;
  const healthyProviders = providerStatus.filter((p: any) => p.health_status === 'HEALTHY').length;
  const totalToday = providerStatus.reduce((s: number, p: any) => s + (p.items_fetched_today || 0), 0);
  const latestItem = rtFeed[0];

  return (
    <div className="space-y-6">
      {/* Shadow-mode banner */}
      <div className="flex items-center gap-2 bg-blue-50 border border-blue-200 rounded-lg px-4 py-2 text-xs text-blue-700">
        <span className="font-bold uppercase tracking-wide">Shadow Mode Active</span>
        <span className="text-blue-500">—</span>
        <span>Real-time API ingestion is observe-only. No trades are opened, closed, or blocked.</span>
      </div>

      <div className="flex items-center gap-2 bg-amber-50 border border-amber-200 rounded-lg px-4 py-2 text-xs text-amber-700">
        <span className="font-bold uppercase tracking-wide">RSS First</span>
        <span className="text-amber-500">—</span>
        <span>RSS sources are the primary live news path. CryptoPanic and Benzinga stay optional and should show as disabled or waiting for config when keys are absent.</span>
      </div>

      {/* KPI row */}
      <div className="flex flex-wrap gap-3">
        <KpiCard label="Items Today" value={totalToday} sub="from API providers" />
        <KpiCard label="Enabled Providers" value={enabledProviders} sub={`${healthyProviders} healthy`} />
        <KpiCard
          label="Latest Item"
          value={feedLoading ? '…' : (rtFeed.length > 0 ? rtFeed.length + ' in 6h' : '0')}
          sub={latestItem ? fmtTitle(latestItem.title) : 'No items yet'}
        />
      </div>

      {/* Provider Status */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <NewsProviderStatusPanel />
      </div>

      {/* Recent RT Feed */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Recent Items (Last 6h)</h3>
        {feedLoading && <p className="text-sm text-gray-500">Loading…</p>}
        {rtFeed.length === 0 && !feedLoading && (
          <p className="text-sm text-gray-500 py-4 text-center">
            No real-time API items yet. This is expected when optional API providers are disabled or waiting for keys; RSS ingestion can still be healthy.
          </p>
        )}
        {rtFeed.length > 0 && (
          <ul className="divide-y divide-gray-100">
            {rtFeed.slice(0, 10).map((item: any) => (
              <li key={item.id} className="py-2 flex items-start gap-3">
                <span className="text-xs text-gray-400 w-20 shrink-0 pt-0.5">
                  {item.provider}
                </span>
                <span className="text-sm text-gray-800 flex-1 truncate">{item.title}</span>
                <span className="text-xs text-gray-400 shrink-0">
                  {item.is_duplicate ? '(dup)' : ''}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* Cross-provider deduplication */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <DuplicateNewsClusterView />
      </div>

      {/* Conflict warnings */}
      <div className="bg-white rounded-lg border border-orange-200 p-4">
        <NewsConflictWarningPanel />
      </div>

      {/* Market confirmations */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <MarketConfirmationPanel />
      </div>
    </div>
  );
}
