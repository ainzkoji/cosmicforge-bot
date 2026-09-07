import { CheckCircle2, Clock3, OctagonAlert, ShieldX, TimerOff, TrendingUp, XCircle } from "lucide-react";

interface SignalStatusBadgeProps {
  status: string;
  compact?: boolean;
}

const statusMeta: Record<string, { label: string; className: string; Icon: typeof Clock3 }> = {
  PENDING_ENTRY: {
    label: "Pending Entry",
    className: "border-amber-400/30 bg-amber-400/10 text-amber-200",
    Icon: Clock3,
  },
  ACTIVE: {
    label: "Active",
    className: "border-cyan-400/30 bg-cyan-400/10 text-cyan-200",
    Icon: TrendingUp,
  },
  EXPIRED: {
    label: "Expired",
    className: "border-slate-500/30 bg-slate-500/10 text-slate-300",
    Icon: TimerOff,
  },
  TP1_HIT: {
    label: "TP1 Hit / Partial",
    className: "border-sky-400/30 bg-sky-400/10 text-sky-200",
    Icon: TrendingUp,
  },
  TP2_HIT: {
    label: "TP2 Hit / Win",
    className: "border-emerald-400/30 bg-emerald-400/10 text-emerald-200",
    Icon: CheckCircle2,
  },
  TP3_HIT: {
    label: "TP3 Hit / Strong Win",
    className: "border-green-300/30 bg-green-300/10 text-green-100",
    Icon: CheckCircle2,
  },
  SL_HIT: {
    label: "Stop Loss Hit",
    className: "border-red-400/30 bg-red-400/10 text-red-200",
    Icon: ShieldX,
  },
  CANCELLED: {
    label: "Cancelled",
    className: "border-zinc-500/30 bg-zinc-500/10 text-zinc-300",
    Icon: XCircle,
  },
  INVALIDATED: {
    label: "Invalidated",
    className: "border-orange-400/30 bg-orange-400/10 text-orange-200",
    Icon: OctagonAlert,
  },
};

export function formatSignalStatus(status: string): string {
  return statusMeta[status]?.label || status.replace(/_/g, " ");
}

export function SignalStatusBadge({ status, compact = false }: SignalStatusBadgeProps) {
  const meta = statusMeta[status] || {
    label: formatSignalStatus(status),
    className: "border-white/10 bg-white/5 text-gray-300",
    Icon: Clock3,
  };
  const Icon = meta.Icon;

  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border font-semibold ${compact ? "px-2 py-0.5 text-[10px]" : "px-2.5 py-1 text-xs"} ${meta.className}`}
    >
      <Icon className={compact ? "h-3 w-3" : "h-3.5 w-3.5"} />
      {meta.label}
    </span>
  );
}
