import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api/client';

type Account = {
  account_id: string; status: string; age_seconds?: number;
  balance?: Record<string, unknown> | null; positions?: Record<string, unknown>[] | null;
  orders?: Record<string, unknown>[] | null; reconciliation_status?: string;
};
type State = {
  status: string; strategy: string; configuration: Record<string, string | boolean>;
  accounts: Account[]; execution_permission: string; order_submission_enabled: boolean;
  daily_hard_loss_fraction: number;
};
const display = (value: unknown) => value == null ? '—' : typeof value === 'object' ? JSON.stringify(value) : String(value);

export function CatiTrading() {
  const query = useQuery<State>({ queryKey: ['cati-production'], queryFn: async () =>
    (await apiClient.get('/api/v1/cati/runtime/status', {
      baseURL: import.meta.env.VITE_CATI_API_BASE || 'http://localhost:9000',
    })).data, refetchInterval: 5000 });
  const state = query.data;
  return <section className="mb-6 rounded-xl border border-slate-700 bg-slate-900 p-5 text-slate-100" aria-label="CATI production trading">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <h2 className="text-lg font-semibold">CATI · Production · LIVE</h2>
      <span className="rounded bg-indigo-950 px-3 py-1 text-sm">Order submission {state?.order_submission_enabled ? 'enabled · risk approval required' : 'disabled'}</span>
    </div>
    {query.isPending && <p className="mt-3">Loading production state…</p>}
    {query.isError && <p role="alert" className="mt-3 text-red-300">Production state unavailable: {query.error.message}</p>}
    {state && <>
      <p className="mt-2 text-sm text-slate-300">{state.status} · {state.strategy} · Entry permission {state.execution_permission}</p>
      <p className="mt-2">Permanent daily hard-loss ceiling: {(state.daily_hard_loss_fraction * 100).toFixed(1)}%</p>
      {state.accounts.length === 0 && <p className="mt-4 text-amber-300">Connect a validated LIVE broker account to receive balances, positions, and orders.</p>}
      {state.accounts.map(account => <div key={account.account_id} className="mt-5 border-t border-slate-700 pt-4">
        <h3 className="font-semibold">LIVE account {account.account_id} · {account.status}</h3>
        <p className="text-sm text-slate-400">Snapshot age {account.age_seconds == null ? '—' : `${account.age_seconds.toFixed(1)}s`} · Reconciliation {account.reconciliation_status || 'Awaiting broker reads'}</p>
        <h4 className="mt-3 font-semibold">Broker balances</h4>
        {account.balance ? <dl>{Object.entries(account.balance).map(([key, value]) => <div key={key} className="flex gap-3 text-sm"><dt>{key}</dt><dd>{display(value)}</dd></div>)}</dl> : <p>Unavailable</p>}
        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          {(['positions', 'orders'] as const).map(kind => <div key={kind}><h4 className="font-semibold capitalize">{kind}</h4>
            {account[kind] == null ? <p>Unavailable</p> : account[kind]?.length === 0 ? <p>No broker {kind}.</p> :
              <ul className="max-h-64 overflow-auto text-sm">{account[kind]?.map((row, index) => <li key={`${display(row.symbol)}-${display(row.orderId)}-${index}`} className="border-b border-slate-800 py-2">
                {Object.entries(row).map(([key, value]) => <span key={key} className="mr-3 inline-block">{key}: {display(value)}</span>)}
              </li>)}</ul>}
          </div>)}
        </div>
      </div>)}
      <details className="mt-5 text-sm"><summary>Resolved production configuration</summary>
        <dl>{Object.entries(state.configuration).map(([key, value]) => <div key={key} className="flex gap-3"><dt>{key}</dt><dd>{display(value)}</dd></div>)}</dl>
      </details>
    </>}
  </section>;
}
