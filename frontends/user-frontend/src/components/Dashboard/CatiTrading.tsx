import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api/client';

type Position = {
  id: string; symbol: string; side: string; status: string; quantity: number;
  entry_price: number; stop: number; target: number; timeout_at: number;
  mark: number; unrealized_gross_pnl: number; realized_net_pnl: number; outcome?: string;
};
type Order = { id: string; symbol: string; kind: string; status: string; quantity: number; price: number | null };
type Fill = { id: string; symbol: string; side: string; quantity: number; price: number; created_at: number };
type State = {
  status: string; strategy?: string; execution_reference?: string; equity?: number; cash?: number;
  market_age_seconds?: number | null; error?: string | null; first_decision_time?: number;
  pnl?: { realized_net: number; unrealized_net: number; total_net: number };
  risk?: { daily_cap_fraction: number; daily_loss_cash: number; daily_budget_cash: number; daily_halted: boolean; persistent_halt: string | null };
  positions: Position[]; orders: Order[]; fills: Fill[]; decision_reasons?: Record<string, number>;
};
const money = (n?: number) => n === undefined ? '—' : n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const price = (n?: number | null) => n == null ? '—' : n.toLocaleString(undefined, { maximumSignificantDigits: 8 });

export function CatiTrading() {
  const query = useQuery<State>({ queryKey: ['cati-simulation'], queryFn: async () =>
    (await apiClient.get('/api/v1/cati/simulation/status')).data, refetchInterval: 5000 });
  const s = query.data;
  return <section className="mb-6 rounded-xl border border-slate-700 bg-slate-900 p-5 text-slate-100" aria-label="CATI simulated trading">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <h2 className="text-lg font-semibold">CATI · Live market simulation</h2>
      <span className="rounded bg-indigo-950 px-3 py-1 text-sm">Simulated funds · Real broker orders disabled</span>
    </div>
    {query.isPending && <p className="mt-3">Loading trading state…</p>}
    {query.isError && <p role="alert" className="mt-3 text-red-300">Trading state unavailable: {query.error.message}</p>}
    {s && <>
      <p className="mt-2 text-sm text-slate-300">{s.status} · {s.strategy || 'Not activated'} · Market age {s.market_age_seconds == null ? '—' : `${s.market_age_seconds.toFixed(1)}s`}</p>
      {s.error && <p role="alert" className="text-amber-300">{s.error}</p>}
      <div className="mt-4 grid grid-cols-2 gap-4 md:grid-cols-5">
        {[["Virtual equity", money(s.equity)], ["Realized net PnL", money(s.pnl?.realized_net)], ["Open net PnL", money(s.pnl?.unrealized_net)], ["Total net PnL", money(s.pnl?.total_net)], ["Daily loss / cap", `${money(s.risk?.daily_loss_cash)} / ${money(s.risk?.daily_budget_cash)}`]].map(([label, value]) =>
          <div key={label}><p className="text-xs text-slate-400">{label} · USDT</p><p className="font-mono">{value}</p></div>)}
      </div>
      <p className="mt-3 text-sm">Permanent daily cap: {((s.risk?.daily_cap_fraction ?? .025) * 100).toFixed(1)}% · Risk {s.risk?.persistent_halt || (s.risk?.daily_halted ? 'Daily halt' : 'Clear')}</p>
      <h3 className="mt-5 font-semibold">Positions</h3>
      {s.positions.length === 0 ? <p className="text-sm text-slate-400">Flat. Waiting for a new hourly decision that passes hard risk.</p> :
        <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr>{['Symbol / side', 'State', 'Quantity', 'Entry / mark', 'Stop / target', 'Timeout', 'PnL / outcome'].map(h => <th className="p-2" key={h}>{h}</th>)}</tr></thead>
          <tbody>{s.positions.map(p => <tr className="border-t border-slate-700" key={p.id}><td className="p-2">{p.symbol} {p.side}</td><td>{p.status}</td><td>{price(p.quantity)}</td><td>{price(p.entry_price)} / {price(p.mark)}</td><td>{price(p.stop)} / {price(p.target)}</td><td>{new Date(p.timeout_at).toLocaleString()}</td><td>{money(p.status === 'OPEN' ? p.unrealized_gross_pnl : p.realized_net_pnl)} {p.outcome}</td></tr>)}</tbody></table></div>}
      <div className="mt-5 grid gap-5 lg:grid-cols-2">
        <div><h3 className="font-semibold">Orders ({s.orders.length})</h3>{s.orders.length === 0 && <p className="text-sm text-slate-400">No simulation orders yet.</p>}
          <ul className="max-h-48 overflow-auto text-sm">{s.orders.map(o => <li className="border-b border-slate-800 py-2" key={o.id}>{o.symbol} · {o.kind} · {o.status} · {price(o.quantity)} @ {price(o.price)}<div className="text-xs text-slate-500">{o.id}</div></li>)}</ul></div>
        <div><h3 className="font-semibold">Fills ({s.fills.length})</h3>{s.fills.length === 0 && <p className="text-sm text-slate-400">No simulation fills yet.</p>}
          <ul className="max-h-48 overflow-auto text-sm">{s.fills.map(f => <li className="border-b border-slate-800 py-2" key={f.id}>{f.symbol} {f.side} · {price(f.quantity)} @ {price(f.price)} · {new Date(f.created_at).toLocaleString()}<div className="text-xs text-slate-500">{f.id}</div></li>)}</ul></div>
      </div>
      <p className="mt-4 text-xs text-slate-400">Model fills use the frozen next 15-minute open reference and costs. Stops and targets settle on native closed bars with stop priority; emergency risk uses live equity.</p>
      <p className="mt-2 text-sm text-slate-300">Decision results: {Object.entries(s.decision_reasons || {}).map(([r, n]) => `${r}: ${n}`).join(' · ') || 'Awaiting first prospective boundary'}</p>
    </>}
  </section>;
}
