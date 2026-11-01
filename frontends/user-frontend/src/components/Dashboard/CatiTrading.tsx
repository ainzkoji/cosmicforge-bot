import type { ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useInRouterContext } from 'react-router-dom';
import { apiClient } from '@/api/client';
import { isDemoEnvironment } from '@/utils/tradingMode';

/*
 * Customer trading status panel, fed by GET /api/v1/cati/runtime/dashboard
 * (through the user backend's /api/v1/cati proxy, never the engine directly).
 *
 * Every state is shown in plain language; a raw reason code appears only as
 * small secondary monospace text next to its human message. No raw
 * configuration or JSON is rendered here: that belongs to operator
 * diagnostics.
 *
 * Bot configuration (the order switch) is shown separately from the actual
 * entry permission, which is what decides whether an order can be placed.
 */

type Severity = 'info' | 'waiting' | 'blocked' | 'attention' | 'critical' | 'unknown';

type Reason = {
  code?: string | null;
  severity?: Severity | string | null;
  message?: string | null;
  action?: string | null;
  detail?: string | null;
};

type LatestSignal = {
  state?: 'SIGNAL' | 'NO_SIGNAL' | string | null;
  decision_time_ms?: number | null;
  symbol?: string | null;
  side?: string | null;
  score?: number | null;
  entry_reference?: number | string | null;
  stop?: number | string | null;
  target?: number | string | null;
  tradable_now?: boolean | null;
  reason?: Reason | null;
};

type ExperimentalDemo = {
  authorization_id?: string | null;
  label?: string | null;
  state?: 'ACTIVE' | 'EXPIRED' | 'REVOKED' | string | null;
  family_id?: string | null;
  research_status_at_grant?: string | null;
  max_entry_notional_usdt?: number | string | null;
  expires_at?: string | number | null;
  seconds_remaining?: number | null;
  granted_by?: string | null;
  approval_ref?: string | null;
};

type ProtectiveStop = {
  state?: 'PLACED' | 'UNVERIFIED' | 'NOT_RECORDED' | string | null;
  trigger_price?: number | string | null;
  requested_at_ms?: number | null;
  detail?: string | null;
};

type Position = {
  symbol?: string | null;
  side?: string | null;
  quantity?: number | string | null;
  entry_price?: number | string | null;
  mark_price?: number | string | null;
  unrealized_pnl?: number | string | null;
  leverage?: number | string | null;
  liquidation_price?: number | string | null;
  protective_stop?: ProtectiveStop | null;
};

type Trade = {
  time_ms?: number | null;
  symbol?: string | null;
  side?: string | null;
  price?: number | string | null;
  quantity?: number | string | null;
  realized_pnl?: number | string | null;
  commission?: number | string | null;
  leg?: string | null;
  label?: 'STRATEGY' | 'EXPERIMENTAL_DEMO' | 'VENUE_CERTIFICATION_TEST' | string | null;
};

type Account = {
  account_id: string;
  exchange?: string | null;
  environment?: string | null;
  real_money?: boolean | null;
  connection?: {
    health?: 'CONNECTED' | 'CONNECTING' | 'STALE' | 'ERROR' | string | null;
    status?: string | null;
    last_read_age_seconds?: number | null;
    problem?: Reason | null;
  } | null;
  bot?: {
    bots?: { id: string; status?: string | null; mode?: string | null }[] | null;
    running?: boolean | null;
    auto_trading_enabled?: boolean | null;
    engine_last_cycle_age_seconds?: number | null;
  } | null;
  last_evaluation?: {
    evaluated_at_ms?: number | null;
    stage?: string | null;
    result?: string | null;
    symbol?: string | null;
    side?: string | null;
    score?: number | null;
    signal_eligible?: boolean | null;
    reason?: Reason | null;
    orders_submitted?: boolean | null;
  } | null;
  entry_permission?: {
    state?: string | null;
    entries_allowed?: boolean | null;
    summary?: string | null;
    reason?: Reason | null;
    blockers?: Reason[] | null;
    strategy_authority?: string | null;
    reason_before_order_switch?: Reason | string | null;
  } | null;
  experimental_demo?: ExperimentalDemo | null;
  open_positions?: Position[] | null;
  recent_trades?: Trade[] | null;
  order_switch?: { demo_orders_enabled?: boolean | null; live_orders_enabled?: boolean | null } | null;
};

type DashboardState = {
  strategy?: string | null;
  latest_signal?: LatestSignal | null;
  accounts?: Account[] | null;
  status?: string | null;
  real_money_trading_possible?: boolean | null;
  generated_at_ms?: number | null;
};

// ---------------------------------------------------------------- formatting

function readable(value: string | null | undefined): string {
  if (!value) return 'Not available';
  const text = value.replace(/_/g, ' ').trim().toLowerCase();
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatTime(ms: number | null | undefined): string {
  if (typeof ms !== 'number' || !Number.isFinite(ms)) return 'Not available';
  const date = new Date(ms);
  return Number.isNaN(date.getTime()) ? 'Not available' : date.toLocaleString();
}

function formatInstant(value: string | number | null | undefined): string {
  if (typeof value === 'number') return formatTime(value);
  if (!value) return 'Not available';
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? value : new Date(parsed).toLocaleString();
}

function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

function formatAge(seconds: number | null | undefined): string {
  return typeof seconds === 'number' && Number.isFinite(seconds) ? `${formatDuration(seconds)} ago` : 'Not available';
}

/** Broker values arrive as decimal strings; they are shown as received, never re-rounded. */
function num(value: number | string | null | undefined): string {
  if (value === null || value === undefined || value === '') return '—';
  return String(value);
}

function signedClass(value: number | string | null | undefined): string {
  const n = Number(value);
  if (value === null || value === undefined || value === '' || !Number.isFinite(n) || n === 0) return 'text-slate-200';
  return n > 0 ? 'text-green-400' : 'text-red-400';
}

// ---------------------------------------------------------------- small parts

/** Router-aware link: the panel also renders on the standalone cati.html page, which has no router. */
function AppLink({ to, className, children }: { to: string; className?: string; children: ReactNode }) {
  const inRouter = useInRouterContext();
  return inRouter
    ? <Link to={to} className={className}>{children}</Link>
    : <a href={to} className={className}>{children}</a>;
}

const SEVERITY: Record<string, { label: string; className: string }> = {
  info: { label: 'Info', className: 'border-sky-500/30 bg-sky-500/10 text-sky-200' },
  waiting: { label: 'Waiting', className: 'border-slate-500/40 bg-slate-500/10 text-slate-200' },
  blocked: { label: 'Blocked', className: 'border-amber-500/30 bg-amber-500/10 text-amber-200' },
  attention: { label: 'Needs attention', className: 'border-amber-500/30 bg-amber-500/10 text-amber-200' },
  critical: { label: 'Critical', className: 'border-red-500/40 bg-red-500/10 text-red-200' },
  unknown: { label: 'Unknown', className: 'border-slate-500/40 bg-slate-500/10 text-slate-300' },
};

function severityOf(value: string | null | undefined) {
  return SEVERITY[(value || 'unknown').toLowerCase()] ?? SEVERITY.unknown;
}

function Badge({ className, children }: { className: string; children: ReactNode }) {
  return <span className={`inline-flex items-center gap-1 rounded border px-2 py-0.5 text-xs font-semibold ${className}`}>{children}</span>;
}

function Code({ children }: { children: ReactNode }) {
  return <span className="ml-2 font-mono text-[11px] text-slate-500">{children}</span>;
}

/** A backend reason: human message first, the action, and the raw code as secondary text. */
function ReasonText({ reason, withSeverity = false }: { reason: Reason | null | undefined; withSeverity?: boolean }) {
  if (!reason) return null;
  const severity = severityOf(reason.severity);
  return (
    <div className="text-sm">
      <p>
        {withSeverity && <Badge className={`${severity.className} mr-2`}>{severity.label}</Badge>}
        <span>{reason.message || readable(reason.code)}</span>
        {reason.code && <Code>{reason.code}</Code>}
      </p>
      {reason.action && <p className="mt-0.5 text-xs text-slate-400">What to do: {reason.action}</p>}
      {reason.detail && <p className="mt-0.5 font-mono text-[11px] text-slate-500">{reason.detail}</p>}
    </div>
  );
}

function Tile({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-lg bg-white/5 p-3">
      <h4 className="text-xs font-medium uppercase tracking-wider text-slate-400">{title}</h4>
      <div className="mt-2 space-y-1">{children}</div>
    </div>
  );
}

const HEALTH: Record<string, { label: string; className: string }> = {
  CONNECTED: { label: 'Connected', className: 'border-green-500/30 bg-green-500/10 text-green-300' },
  CONNECTING: { label: 'Connecting', className: 'border-sky-500/30 bg-sky-500/10 text-sky-200' },
  STALE: { label: 'Data out of date', className: 'border-amber-500/30 bg-amber-500/10 text-amber-200' },
  ERROR: { label: 'Connection error', className: 'border-red-500/40 bg-red-500/10 text-red-200' },
};

const TRADE_LABEL: Record<string, { label: string; className: string }> = {
  STRATEGY: { label: 'Strategy', className: 'border-sky-500/30 bg-sky-500/10 text-sky-200' },
  EXPERIMENTAL_DEMO: { label: 'Experimental demo', className: 'border-purple-500/30 bg-purple-500/10 text-purple-200' },
  VENUE_CERTIFICATION_TEST: { label: 'Exchange test (not strategy)', className: 'border-slate-500/40 bg-slate-500/10 text-slate-300' },
};

// ---------------------------------------------------------------- sections

function EnvironmentBadge({ account }: { account: Account }) {
  // Demo only when the environment is positively DEMO/testnet and the backend does not say real money.
  const demo = isDemoEnvironment(account.environment) && account.real_money !== true;
  if (demo) {
    return <Badge className="border-blue-500/30 bg-blue-500/10 text-blue-200">DEMO account — no real money</Badge>;
  }
  if (account.environment) {
    return <Badge className="border-red-500/40 bg-red-600 text-white">LIVE account — real money</Badge>;
  }
  return <Badge className="border-red-500/40 bg-red-500/15 text-red-200">Environment unknown — treated as real money</Badge>;
}

function SignalPanel({ signal }: { signal: LatestSignal | null | undefined }) {
  if (!signal || signal.state !== 'SIGNAL') {
    return (
      <div className="rounded-lg bg-white/5 p-3">
        <h3 className="text-xs font-medium uppercase tracking-wider text-slate-400">Latest strategy signal</h3>
        <p className="mt-2 font-medium">{signal ? 'No signal' : 'No signal information yet'}</p>
        {signal?.decision_time_ms != null && <p className="text-xs text-slate-400">Last decision {formatTime(signal.decision_time_ms)}</p>}
        <div className="mt-1"><ReasonText reason={signal?.reason} /></div>
      </div>
    );
  }
  return (
    <div className="rounded-lg bg-white/5 p-3">
      <h3 className="text-xs font-medium uppercase tracking-wider text-slate-400">Latest strategy signal</h3>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <span className="font-semibold">{signal.symbol || 'Unknown symbol'}</span>
        {signal.side && (
          <Badge className={signal.side === 'LONG' ? 'border-green-500/30 bg-green-500/10 text-green-300' : 'border-red-500/30 bg-red-500/10 text-red-300'}>
            {signal.side}
          </Badge>
        )}
        {typeof signal.score === 'number' && <span className="text-xs text-slate-400">score {signal.score}</span>}
        <Badge className={signal.tradable_now ? 'border-green-500/30 bg-green-500/10 text-green-300' : 'border-slate-500/40 bg-slate-500/10 text-slate-300'}>
          {signal.tradable_now ? 'Entry window open' : 'Entry window closed'}
        </Badge>
      </div>
      <dl className="mt-2 grid grid-cols-3 gap-2 text-sm">
        <div><dt className="text-xs text-slate-400">Entry reference</dt><dd className="font-mono">{num(signal.entry_reference)}</dd></div>
        <div><dt className="text-xs text-slate-400">Stop</dt><dd className="font-mono">{num(signal.stop)}</dd></div>
        <div><dt className="text-xs text-slate-400">Target</dt><dd className="font-mono">{num(signal.target)}</dd></div>
      </dl>
      <p className="mt-2 text-xs text-slate-400">Decided {formatTime(signal.decision_time_ms)}</p>
      <div className="mt-1"><ReasonText reason={signal.reason} /></div>
    </div>
  );
}

function ExperimentalDemoNotice({ demo }: { demo: ExperimentalDemo | null | undefined }) {
  if (!demo) return null;
  if (demo.state !== 'ACTIVE') {
    return (
      <p className="mt-3 text-xs text-slate-400">
        Experimental demo authorization {demo.state === 'REVOKED' ? 'revoked' : demo.state === 'EXPIRED' ? 'expired' : 'not active'}.
        {demo.state && <Code>{demo.state}</Code>}
      </p>
    );
  }
  return (
    <div className="mt-3 rounded-lg border border-purple-500/30 bg-purple-500/10 p-3 text-sm" role="status">
      <div className="flex flex-wrap items-center gap-2">
        <Badge className="border-purple-400/40 bg-purple-500/20 text-purple-100">EXPERIMENTAL DEMO</Badge>
        {typeof demo.seconds_remaining === 'number' && <span className="text-purple-100">{formatDuration(demo.seconds_remaining)} remaining</span>}
      </div>
      <p className="mt-2 text-purple-100">
        A time-limited demo authorization lets this strategy place orders on this demo account only. The strategy is not
        certified, and this authorization gives it no authority to trade real money.
      </p>
      <p className="mt-1 text-xs text-purple-200/80">
        {demo.max_entry_notional_usdt != null && <>Maximum entry size {num(demo.max_entry_notional_usdt)} USDT. </>}
        {demo.expires_at != null && <>Expires {formatInstant(demo.expires_at)}. </>}
        {demo.granted_by && <>Granted by {demo.granted_by}. </>}
        {demo.research_status_at_grant && <>Research status when granted: {readable(demo.research_status_at_grant)}.</>}
      </p>
    </div>
  );
}

function EntryPermission({ permission, orderSwitch, demoAccount }: {
  permission: Account['entry_permission'];
  orderSwitch: Account['order_switch'];
  demoAccount: boolean;
}) {
  const blockers = permission?.blockers ?? [];
  const before = permission?.reason_before_order_switch;
  const switchOn = demoAccount ? orderSwitch?.demo_orders_enabled : orderSwitch?.live_orders_enabled;
  return (
    <div className="mt-3 rounded-lg border border-slate-700 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h4 className="text-sm font-semibold">Can the bot open new trades?</h4>
        {permission?.entries_allowed === true ? (
          <Badge className="border-green-500/30 bg-green-500/10 text-green-300">Yes — entries allowed</Badge>
        ) : (
          <Badge className="border-amber-500/30 bg-amber-500/10 text-amber-200">
            {permission ? 'No — entries blocked' : 'Unknown — treated as blocked'}
          </Badge>
        )}
      </div>
      {permission?.summary && <p className="mt-2 text-sm">{permission.summary}{permission.state && <Code>{permission.state}</Code>}</p>}
      {permission?.reason && <div className="mt-1 text-slate-300"><ReasonText reason={permission.reason} /></div>}

      {blockers.length > 0 && (
        <div className="mt-3">
          <h5 className="text-xs font-medium uppercase tracking-wider text-slate-400">Blockers ({blockers.length})</h5>
          <ul className="mt-1 space-y-2">
            {blockers.map((blocker, index) => (
              <li key={`${blocker.code ?? 'blocker'}-${index}`} className="rounded bg-white/5 p-2">
                <ReasonText reason={blocker} withSeverity />
              </li>
            ))}
          </ul>
        </div>
      )}

      {before && (
        <div className="mt-3">
          <h5 className="text-xs font-medium uppercase tracking-wider text-slate-400">Result before the order switch</h5>
          {typeof before === 'string' ? <p className="text-sm">{readable(before)}<Code>{before}</Code></p> : <ReasonText reason={before} />}
        </div>
      )}

      {orderSwitch && (
        <div className="mt-3 border-t border-slate-800 pt-2 text-xs text-slate-400">
          <p>
            Order switch (configuration): demo orders {orderSwitch.demo_orders_enabled ? 'on' : 'off'} · real-money orders{' '}
            {orderSwitch.live_orders_enabled ? 'on' : 'off'}.
          </p>
          <p className="mt-0.5">
            {switchOn
              ? 'The switch for this account is on, but it does not by itself allow orders: the entry permission above decides.'
              : 'The switch for this account is off, so no order is sent even when an entry would otherwise be allowed.'}
          </p>
        </div>
      )}
    </div>
  );
}

function StopStatus({ stop }: { stop: ProtectiveStop | null | undefined }) {
  if (stop?.state === 'PLACED') {
    return <span className="text-green-300">Stop placed{stop.trigger_price != null ? ` at ${num(stop.trigger_price)}` : ''}</span>;
  }
  if (stop?.state === 'UNVERIFIED') {
    return <span className="text-amber-200">Stop not verified{stop.detail && <Code>{stop.detail}</Code>}</span>;
  }
  return <span className="text-red-300">No stop recorded{stop?.detail && <Code>{stop.detail}</Code>}</span>;
}

function PositionsTable({ positions }: { positions: Position[] | null | undefined }) {
  return (
    <div className="mt-3">
      <h4 className="text-sm font-semibold">Open positions</h4>
      {positions == null ? (
        <p className="mt-1 text-sm text-slate-400">Positions are not available.</p>
      ) : positions.length === 0 ? (
        <p className="mt-1 text-sm text-slate-400">No open positions.</p>
      ) : (
        <div className="mt-1 overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-left text-xs text-slate-400">
              <tr>
                <th className="py-1 pr-3 font-medium">Symbol</th><th className="py-1 pr-3 font-medium">Side</th>
                <th className="py-1 pr-3 text-right font-medium">Quantity</th><th className="py-1 pr-3 text-right font-medium">Entry</th>
                <th className="py-1 pr-3 text-right font-medium">Mark</th><th className="py-1 pr-3 text-right font-medium">Unrealized PnL</th>
                <th className="py-1 pr-3 text-right font-medium">Leverage</th><th className="py-1 pr-3 text-right font-medium">Liquidation</th>
                <th className="py-1 font-medium">Protective stop</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {positions.map((position, index) => (
                <tr key={`${position.symbol ?? 'position'}-${position.side ?? ''}-${index}`}>
                  <td className="py-1.5 pr-3 font-medium">{position.symbol || '—'}</td>
                  <td className="py-1.5 pr-3">{position.side || '—'}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{num(position.quantity)}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{num(position.entry_price)}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{num(position.mark_price)}</td>
                  <td className={`py-1.5 pr-3 text-right font-mono ${signedClass(position.unrealized_pnl)}`}>{num(position.unrealized_pnl)}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{position.leverage != null ? `${num(position.leverage)}x` : '—'}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{num(position.liquidation_price)}</td>
                  <td className="py-1.5"><StopStatus stop={position.protective_stop} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function TradesTable({ trades }: { trades: Trade[] | null | undefined }) {
  return (
    <div className="mt-3">
      <h4 className="text-sm font-semibold">Recent trades</h4>
      {trades == null ? (
        <p className="mt-1 text-sm text-slate-400">Trades are not available.</p>
      ) : trades.length === 0 ? (
        <p className="mt-1 text-sm text-slate-400">No recent trades.</p>
      ) : (
        <div className="mt-1 overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-left text-xs text-slate-400">
              <tr>
                <th className="py-1 pr-3 font-medium">Time</th><th className="py-1 pr-3 font-medium">Symbol</th>
                <th className="py-1 pr-3 font-medium">Side</th><th className="py-1 pr-3 text-right font-medium">Price</th>
                <th className="py-1 pr-3 text-right font-medium">Quantity</th><th className="py-1 pr-3 text-right font-medium">Realized PnL</th>
                <th className="py-1 pr-3 text-right font-medium">Fee</th><th className="py-1 font-medium">Type</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {trades.map((trade, index) => {
                const label = TRADE_LABEL[trade.label ?? ''];
                return (
                  <tr key={`${trade.time_ms ?? 't'}-${trade.symbol ?? ''}-${index}`}>
                    <td className="py-1.5 pr-3 whitespace-nowrap text-xs text-slate-300">{formatTime(trade.time_ms)}</td>
                    <td className="py-1.5 pr-3 font-medium">{trade.symbol || '—'}</td>
                    <td className="py-1.5 pr-3">{trade.side || '—'}{trade.leg && <span className="ml-1 text-xs text-slate-400">({readable(trade.leg).toLowerCase()})</span>}</td>
                    <td className="py-1.5 pr-3 text-right font-mono">{num(trade.price)}</td>
                    <td className="py-1.5 pr-3 text-right font-mono">{num(trade.quantity)}</td>
                    <td className={`py-1.5 pr-3 text-right font-mono ${signedClass(trade.realized_pnl)}`}>{num(trade.realized_pnl)}</td>
                    <td className="py-1.5 pr-3 text-right font-mono">{num(trade.commission)}</td>
                    <td className="py-1.5">
                      {label
                        ? <Badge className={label.className}>{label.label}</Badge>
                        : <Badge className="border-slate-500/40 bg-slate-500/10 text-slate-300">{trade.label ? readable(trade.label) : 'Unlabelled'}</Badge>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function AccountCard({ account }: { account: Account }) {
  const demoAccount = isDemoEnvironment(account.environment) && account.real_money !== true;
  const health = HEALTH[account.connection?.health ?? ''];
  const bots = account.bot?.bots ?? [];
  const evaluation = account.last_evaluation;
  return (
    <div className="rounded-lg border border-slate-700 p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="font-semibold">
          {account.exchange ? readable(account.exchange) : 'Unknown exchange'}
          <Code>{account.account_id}</Code>
        </h3>
        <EnvironmentBadge account={account} />
      </div>

      <ExperimentalDemoNotice demo={account.experimental_demo} />

      <div className="mt-3 grid gap-3 md:grid-cols-3">
        <Tile title="Exchange connection">
          <div role="status">
            <Badge className={health?.className ?? 'border-slate-500/40 bg-slate-500/10 text-slate-300'}>{health?.label ?? 'Unknown'}</Badge>
            {account.connection?.status && <Code>{account.connection.status}</Code>}
          </div>
          <p className="text-xs text-slate-400">Last account read {formatAge(account.connection?.last_read_age_seconds)}</p>
          {account.connection?.health !== 'CONNECTED' && account.connection?.problem && <ReasonText reason={account.connection.problem} />}
        </Tile>

        <Tile title="Bot">
          <p className="font-medium">
            {account.bot?.running ? 'Running' : account.bot?.running === false ? 'Not running' : 'Status unknown'}
          </p>
          <p className="text-xs text-slate-400">
            Automatic trading {account.bot?.auto_trading_enabled ? 'enabled' : account.bot?.auto_trading_enabled === false ? 'disabled' : 'unknown'}
            {' · '}Last engine cycle {formatAge(account.bot?.engine_last_cycle_age_seconds)}
          </p>
          {bots.length > 0 ? (
            <ul className="text-xs">
              {bots.map((bot) => (
                <li key={bot.id}>
                  <AppLink to={`/dashboard/bots/${bot.id}`} className="text-cyan-300 hover:underline">View bot</AppLink>
                  <span className="text-slate-400"> · {readable(bot.status)} · {bot.mode === 'paper' ? 'simulated (paper)' : 'places orders on this account'}</span>
                  <Code>{bot.id}</Code>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-slate-400">{account.bot ? 'No bot is assigned to this account.' : 'Bot information is not available.'}</p>
          )}
        </Tile>

        <Tile title="Last strategy evaluation">
          {evaluation ? (
            <>
              <p className="font-medium">{readable(evaluation.result)}{evaluation.result && <Code>{evaluation.result}</Code>}</p>
              <p className="text-xs text-slate-400">
                {formatTime(evaluation.evaluated_at_ms)}
                {evaluation.symbol && ` · ${evaluation.symbol}${evaluation.side ? ` ${evaluation.side}` : ''}`}
                {evaluation.orders_submitted === true ? ' · order submitted' : evaluation.orders_submitted === false ? ' · no order submitted' : ''}
              </p>
              <ReasonText reason={evaluation.reason} />
            </>
          ) : (
            <p className="text-sm text-slate-400">No evaluation recorded yet.</p>
          )}
        </Tile>
      </div>

      <EntryPermission permission={account.entry_permission} orderSwitch={account.order_switch} demoAccount={demoAccount} />
      <PositionsTable positions={account.open_positions} />
      <TradesTable trades={account.recent_trades} />
    </div>
  );
}

// ---------------------------------------------------------------- panel

/** Customer trading status. Raw configuration and broker payloads belong in operator diagnostics. */
export function CatiTrading() {
  const query = useQuery<DashboardState>({
    queryKey: ['cati-dashboard'],
    queryFn: async () => (await apiClient.get('/api/v1/cati/runtime/dashboard')).data,
    refetchInterval: 5000,
  });
  const state = query.data;
  const accounts = state?.accounts ?? [];

  return (
    <section className="mb-6 rounded-xl border border-slate-700 bg-slate-900 p-5 text-slate-100" aria-label="Bot trading status">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">Bot trading status</h2>
          <p className="mt-1 text-sm text-slate-400">
            What the bot is doing on each connected exchange account, and why it may not be trading.
            {state?.strategy && <Code>{state.strategy}</Code>}
          </p>
        </div>
        <AppLink className="rounded-lg border border-slate-600 px-3 py-2 text-sm hover:bg-slate-800" to="/dashboard/bots">My bots</AppLink>
      </div>

      {query.isPending && <p className="mt-4 text-sm" role="status">Checking trading status…</p>}
      {query.isError && (
        <p className="mt-4 text-sm text-red-300" role="alert">
          Trading status is unavailable. Check your connection and refresh this page.
        </p>
      )}

      {state && (
        <>
          <div className="mt-4" role="status">
            {state.real_money_trading_possible === false ? (
              <Badge className="border-blue-500/30 bg-blue-500/10 text-blue-200">No account can trade real money</Badge>
            ) : state.real_money_trading_possible === true ? (
              <Badge className="border-red-500/40 bg-red-600 text-white">Real-money trading is possible on a LIVE account</Badge>
            ) : (
              <Badge className="border-amber-500/30 bg-amber-500/10 text-amber-200">Real-money status unknown — treat as real money</Badge>
            )}
          </div>

          <div className="mt-3">
            <SignalPanel signal={state.latest_signal} />
          </div>

          {accounts.length === 0 ? (
            <div className="mt-4 rounded-lg border border-amber-500/20 p-4">
              <p className="text-sm text-amber-200">No connected trading accounts are reported by the engine.</p>
              <AppLink className="mt-2 inline-block text-sm font-medium text-cyan-300 hover:underline" to="/dashboard/brokers">
                Check broker connections
              </AppLink>
            </div>
          ) : (
            <div className="mt-4 space-y-4">
              {accounts.map((account) => <AccountCard key={account.account_id} account={account} />)}
            </div>
          )}

          {state.generated_at_ms != null && (
            <p className="mt-3 text-xs text-slate-500">Updated {formatTime(state.generated_at_ms)}</p>
          )}
        </>
      )}
    </section>
  );
}
