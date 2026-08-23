import { ArrowDownRight, ArrowUpRight, Eye, EyeOff, Heart, ShieldAlert, Target } from "lucide-react";
import type { TradingSignal } from "@/api/signals";
import { SignalCountdown } from "@/components/Signals/SignalCountdown";
import { SignalStatusBadge } from "@/components/Signals/SignalStatusBadge";

interface SignalCardProps {
  signal: TradingSignal;
  onViewDetails: (signalId: string) => void;
  isFavorite?: boolean;
  isHidden?: boolean;
  onToggleFavorite?: (symbol: string, isFavorite: boolean) => void;
  onHideSymbol?: (symbol: string) => void;
}

function formatNumber(value?: number | null): string {
  if (value === undefined || value === null || Number.isNaN(Number(value))) return "--";
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 8 });
}

function isClosed(signal: TradingSignal): boolean {
  return (
    ["EXPIRED", "INVALIDATED", "SL_HIT", "CANCELLED"].includes(signal.status) ||
    (signal.expires_at ? new Date(signal.expires_at).getTime() <= Date.now() : false)
  );
}

function closedWarning(signal: TradingSignal): string {
  if (signal.status === "INVALIDATED") return "Invalidated — do not enter now.";
  if (signal.status === "SL_HIT") return "Stop loss hit — signal closed.";
  return "Entry window expired — do not enter now.";
}

function PriceCell({
  label,
  value,
  tone = "neutral",
}: {
  label: string;
  value?: number | null;
  tone?: "neutral" | "risk" | "target";
}) {
  const toneClass =
    tone === "risk"
      ? "border-red-400/10 bg-red-400/[0.06] text-red-100"
      : tone === "target"
        ? "border-emerald-400/10 bg-emerald-400/[0.06] text-emerald-100"
        : "border-white/5 bg-black/20 text-white";

  return (
    <div className={`rounded-lg border px-3 py-2 ${toneClass}`}>
      <div className="text-[10px] uppercase tracking-[0.16em] text-slate-500">{label}</div>
      <div className="mt-0.5 truncate text-sm font-semibold">{formatNumber(value)}</div>
    </div>
  );
}

export function SignalCard({ signal, onViewDetails, isFavorite = false, isHidden = false, onToggleFavorite, onHideSymbol }: SignalCardProps) {
  const side = String(signal.side).toUpperCase();
  const closed = isClosed(signal);
  const sideClass =
    side === "BUY"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
      : "border-red-400/30 bg-red-400/10 text-red-200";
  const SideIcon = side === "BUY" ? ArrowUpRight : ArrowDownRight;

  return (
    <article
      className={`group relative overflow-hidden rounded-2xl border bg-[#111722]/90 p-4 shadow-lg shadow-black/10 transition-all hover:-translate-y-0.5 hover:border-cyan-300/30 ${
        closed ? "border-white/5 opacity-75 grayscale-[0.2]" : "border-white/10"
      }`}
    >
      <div className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-cyan-300/35 to-transparent" />

      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="truncate text-lg font-bold text-white">{signal.symbol}</h3>
            <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-semibold ${sideClass}`}>
              <SideIcon className="h-3 w-3" />
              {side}
            </span>
            {Boolean(signal.dev_mode) && (
              <span className="rounded-full border border-fuchsia-300/30 bg-fuchsia-300/10 px-2 py-0.5 text-[10px] font-bold text-fuchsia-100">
                DEV/TEST
              </span>
            )}
          </div>
          <p className="mt-1 truncate text-[11px] uppercase tracking-[0.18em] text-slate-500">
            {(signal.timeframe || "timeframe").toUpperCase()} • Confidence {formatNumber(signal.confidence_score)}% • R/R{" "}
            {formatNumber(signal.risk_reward)}
          </p>
        </div>
        <SignalStatusBadge status={signal.status} compact />
      </div>

      <div className="mt-3 grid grid-cols-2 gap-2">
        <PriceCell label="Entry" value={signal.entry_price} />
        <PriceCell label="SL" value={signal.stop_loss} tone="risk" />
        <PriceCell label="TP1" value={signal.take_profit_1} tone="target" />
        <PriceCell label="TP2" value={signal.take_profit_2} tone="target" />
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
        <span className="inline-flex items-center gap-1 rounded-full border border-sky-400/10 bg-sky-400/5 px-2.5 py-1 text-sky-100">
          <Target className="h-3 w-3" />
          TP1 partial
        </span>
        <span className="inline-flex items-center gap-1 rounded-full border border-emerald-400/10 bg-emerald-400/5 px-2.5 py-1 text-emerald-100">
          <Target className="h-3 w-3" />
          TP2 win
        </span>
        {signal.take_profit_3 !== undefined && signal.take_profit_3 !== null && (
          <span className="rounded-full border border-emerald-400/10 bg-emerald-400/5 px-2.5 py-1 text-emerald-100">
            TP3 {formatNumber(signal.take_profit_3)}
          </span>
        )}
      </div>

      <div className="mt-3 flex flex-col gap-2 border-t border-white/5 pt-3 sm:flex-row sm:items-center sm:justify-between">
        <SignalCountdown expiresAt={signal.expires_at} status={signal.status} compact={!closed} />
        <div className="flex flex-wrap gap-2">
          {onToggleFavorite && (
            <button
              type="button"
              onClick={() => onToggleFavorite(signal.symbol, isFavorite)}
              className={`inline-flex items-center justify-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-semibold transition-colors ${
                isFavorite ? "border-rose-300/30 bg-rose-300/10 text-rose-100" : "border-white/10 bg-white/5 text-white hover:border-rose-300/30"
              }`}
            >
              <Heart className="h-3.5 w-3.5" />
              {isFavorite ? "Saved" : "Save"}
            </button>
          )}
          {onHideSymbol && !isHidden && (
            <button
              type="button"
              onClick={() => onHideSymbol(signal.symbol)}
              className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-white/10 bg-white/5 px-3 py-1.5 text-xs font-semibold text-white transition-colors hover:border-amber-300/40 hover:bg-amber-300/10"
            >
              <EyeOff className="h-3.5 w-3.5" />
              Hide
            </button>
          )}
          <button
            type="button"
            onClick={() => onViewDetails(signal.id)}
            className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-white/10 bg-white/5 px-3 py-1.5 text-xs font-semibold text-white transition-colors hover:border-cyan-300/40 hover:bg-cyan-300/10"
          >
            <Eye className="h-3.5 w-3.5" />
            Details
          </button>
        </div>
      </div>

      {closed && (
        <div className="mt-3 flex items-center gap-2 rounded-lg border border-amber-300/20 bg-amber-300/10 px-3 py-2 text-xs text-amber-100">
          <ShieldAlert className="h-3.5 w-3.5" />
          {closedWarning(signal)}
        </div>
      )}
    </article>
  );
}
