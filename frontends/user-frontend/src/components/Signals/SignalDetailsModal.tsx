import { X } from "lucide-react";
import type { TradingSignal } from "@/api/signals";
import { SignalCountdown } from "@/components/Signals/SignalCountdown";
import { SignalStatusBadge } from "@/components/Signals/SignalStatusBadge";

interface SignalDetailsModalProps {
  signal?: TradingSignal | null;
  isOpen: boolean;
  isLoading?: boolean;
  onClose: () => void;
}

const DISCLAIMER =
  "Signals are for educational and informational purposes only. Trading involves risk. Past performance does not guarantee future results.";

function formatValue(value: unknown): string {
  if (value === undefined || value === null || value === "") return "--";
  if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 8 });
  return String(value);
}

function DetailRow({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="rounded-xl border border-white/5 bg-black/20 p-3">
      <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-1 text-sm font-semibold text-white">{formatValue(value)}</div>
    </div>
  );
}

export function SignalDetailsModal({ signal, isOpen, isLoading = false, onClose }: SignalDetailsModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm">
      <div className="max-h-[90vh] w-full max-w-4xl overflow-y-auto rounded-3xl border border-white/10 bg-[#0F1218] shadow-2xl shadow-black/50">
        <div className="sticky top-0 z-10 flex items-start justify-between border-b border-white/10 bg-[#0F1218]/95 p-5 backdrop-blur">
          <div>
            <div className="text-xs uppercase tracking-[0.28em] text-cyan-300">Manual Signal Details</div>
            <h2 className="mt-1 text-2xl font-bold text-white">{signal?.symbol || "Signal"}</h2>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-full border border-white/10 bg-white/5 p-2 text-slate-300 transition-colors hover:bg-white/10 hover:text-white"
            aria-label="Close signal details"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="space-y-6 p-5">
          {isLoading ? (
            <div className="rounded-2xl border border-white/10 bg-white/5 p-8 text-center text-slate-300">Loading signal details...</div>
          ) : signal ? (
            <>
              <div className="flex flex-wrap items-center gap-3">
                <span
                  className={`rounded-full border px-3 py-1 text-sm font-bold ${
                    String(signal.side).toUpperCase() === "BUY"
                      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
                      : "border-red-400/30 bg-red-400/10 text-red-200"
                  }`}
                >
                  {signal.side}
                </span>
                {Boolean(signal.dev_mode) && (
                  <span className="rounded-full border border-fuchsia-300/30 bg-fuchsia-300/10 px-3 py-1 text-sm font-bold text-fuchsia-100">
                    DEV/TEST
                  </span>
                )}
                <SignalStatusBadge status={signal.status} />
                <SignalCountdown expiresAt={signal.expires_at} status={signal.status} />
              </div>

              <div className="grid gap-3 md:grid-cols-3">
                <DetailRow label="Asset Class" value={signal.asset_class} />
                <DetailRow label="Timeframe" value={signal.timeframe} />
                <DetailRow label="Strategy" value={signal.strategy_name} />
                <DetailRow label="Entry Price" value={signal.entry_price} />
                <DetailRow label="Entry Zone Low" value={signal.entry_zone_low} />
                <DetailRow label="Entry Zone High" value={signal.entry_zone_high} />
                <DetailRow label="Stop Loss" value={signal.stop_loss} />
                <DetailRow label="Take Profit 1" value={signal.take_profit_1} />
                <DetailRow label="Take Profit 2" value={signal.take_profit_2} />
                <DetailRow label="Take Profit 3" value={signal.take_profit_3} />
                <DetailRow label="Risk / Reward" value={signal.risk_reward} />
                <DetailRow label="Confidence" value={`${formatValue(signal.confidence_score)}%`} />
                <DetailRow label="Published" value={signal.published_at} />
                <DetailRow label="Entry Window Ends" value={signal.expires_at} />
                <DetailRow label="Created" value={signal.created_at} />
                <DetailRow label="Updated" value={signal.updated_at} />
                <DetailRow label="Time Left Seconds" value={signal.time_left_seconds} />
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Signal Reason</div>
                <p className="mt-2 text-sm leading-6 text-slate-200">{signal.signal_reason || "No signal reason provided."}</p>
              </div>

              <div className="rounded-2xl border border-amber-300/20 bg-amber-300/10 p-4 text-sm leading-6 text-amber-50">
                {signal.disclaimer || DISCLAIMER}
              </div>
            </>
          ) : (
            <div className="rounded-2xl border border-red-400/20 bg-red-400/10 p-8 text-center text-red-100">
              Unable to load signal details right now.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
