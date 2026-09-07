import React from 'react';
import { cn } from '@/lib/utils';

type BotHealthStatus =
  | 'TRADING'
  | 'WAITING_FOR_SETUP'
  | 'PAUSED_RISK_LIMIT'
  | 'PAUSED_CIRCUIT_BREAKER'
  | 'PAUSED_KILL_SWITCH'
  | 'PAUSED_EVENT_BLACKOUT'
  | 'PAUSED_CONSECUTIVE_LOSS_COOLDOWN'
  | 'PAUSED_MAX_DAILY_TRADES'
  | 'PAUSED_MAX_OPEN_POSITIONS'
  | 'ERROR_SIZING_FAILURE'
  | 'ERROR_EXCHANGE_DISCONNECTED'
  | 'ERROR_STRATEGY_UNAVAILABLE'
  | 'ERROR_EXECUTION_FAILURE'
  | 'UNKNOWN'
  | string;

function normalize(status?: string | null): BotHealthStatus {
  return (status || 'UNKNOWN') as BotHealthStatus;
}

function badgeStyle(status: BotHealthStatus) {
  const s = normalize(status);
  if (s === 'TRADING') return 'bg-green-500/10 text-green-500 border-green-500/20';
  if (s === 'WAITING_FOR_SETUP') return 'bg-slate-500/10 text-slate-300 border-slate-500/20';
  if (s.startsWith('PAUSED_')) return 'bg-amber-500/10 text-amber-500 border-amber-500/20';
  if (s.startsWith('ERROR_')) return 'bg-red-500/10 text-red-500 border-red-500/20';
  return 'bg-muted text-muted-foreground border-border';
}

function label(status: BotHealthStatus) {
  const s = normalize(status);
  if (s === 'TRADING') return 'Trading';
  if (s === 'WAITING_FOR_SETUP') return 'Waiting for Setup';
  if (s.startsWith('PAUSED_')) return `Paused`;
  if (s.startsWith('ERROR_')) return `Error`;
  return 'Unknown';
}

export function BotHealthBadge({
  status,
  className,
}: {
  status?: string | null;
  className?: string;
}) {
  const s = normalize(status);
  return (
    <span className={cn('inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium', badgeStyle(s), className)}>
      {label(s)}
    </span>
  );
}

