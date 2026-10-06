import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api/client';

type Account = {
  account_id: string; status: string; age_seconds?: number; broker: string; environment: 'DEMO' | 'LIVE';
  canonical_base_url?: string; execution_permission: string;
  order_submission_gate: { name: string; enabled: boolean };
  balance?: Record<string, unknown> | null; positions?: Record<string, unknown>[] | null;
  orders?: Record<string, unknown>[] | null; reconciliation_status?: string;
  execution?: {
    execution_permission: string; reason?: string; block_reason_before_order_gate?: string;
    latest_cati_decision?: Record<string, unknown> | null;
    eligibility?: { eligible: boolean; reason?: string } | null;
    kill_switch?: boolean; risk?: Record<string, unknown>;
    execution_history?: Record<string, unknown>[];
  };
};
type State = {
  status: string; strategy: string; configuration: Record<string, string | boolean>;
  accounts: Account[]; execution_permission: string; broker_execution_scope: string;
  demo_order_submission_enabled: boolean; live_order_submission_enabled: boolean;
  daily_loss_limit_source: string;
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
      <span className="rounded bg-indigo-950 px-3 py-1 text-sm">DEMO orders {state?.demo_order_submission_enabled ? 'enabled' : 'disabled'} · Real-money LIVE orders {state?.live_order_submission_enabled ? 'enabled' : 'disabled'}</span>
    </div>
    {query.isPending && <p className="mt-3">Loading production state…</p>}
    {query.isError && <p role="alert" className="mt-3 text-red-300">Production state unavailable: {query.error.message}</p>}
    {state && <>
      <p className="mt-2 text-sm text-slate-300">{state.status} · {state.strategy} · Execution scope {state.broker_execution_scope}</p>
      <p className="mt-2">Daily loss limit: per-bot policy (user setting or risk profile default) · {state.daily_loss_limit_source}</p>
      {state.accounts.length === 0 && <p className="mt-4 text-amber-300">Connect a DEMO or LIVE broker account to receive balances, positions, and orders.</p>}
      {state.accounts.map(account => <div key={account.account_id} className="mt-5 border-t border-slate-700 pt-4">
        <h3 className="font-semibold">{account.broker} · {account.environment} account {account.account_id} · {account.status}</h3>
        <p className="text-sm">{account.order_submission_gate.name}: {account.order_submission_gate.enabled ? 'enabled · risk approval required' : 'disabled'} · {account.execution_permission}</p>
        <p className="text-sm text-slate-400">{account.canonical_base_url}</p>
        <p className="text-sm text-slate-400">Snapshot age {account.age_seconds == null ? '—' : `${account.age_seconds.toFixed(1)}s`} · Reconciliation {account.reconciliation_status || 'Awaiting broker reads'}</p>
        {account.execution && <div className="mt-3 text-sm">
          <p>Entry permission {account.execution.execution_permission} · {account.execution.reason}</p>
          {account.execution.block_reason_before_order_gate && <p>Account/risk result: {account.execution.block_reason_before_order_gate}</p>}
          <p>Kill switch {account.execution.kill_switch == null ? 'Unavailable' : account.execution.kill_switch ? 'Active' : 'Inactive'}</p>
          <p>Latest decision {display(account.execution.latest_cati_decision?.decision_id)} · Eligibility {account.execution.eligibility?.eligible ? 'Accepted' : account.execution.eligibility?.reason || 'Unavailable'}</p>
          {account.execution.risk && <dl>{['equity', 'realized_pnl', 'unrealized_pnl', 'daily_loss_usage', 'daily_loss_limit_fraction', 'remaining_daily_risk'].map(key =>
            <div key={key} className="flex gap-3"><dt>{key.split('_').join(' ')}</dt><dd>{display(account.execution?.risk?.[key])}</dd></div>)}</dl>}
          <details className="mt-2"><summary>Broker order and fill history</summary>
            <pre className="max-h-64 overflow-auto">{display(account.execution.execution_history || [])}</pre>
          </details>
        </div>}
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
