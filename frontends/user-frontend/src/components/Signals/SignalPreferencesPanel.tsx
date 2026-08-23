import { Bell, Heart, Settings2 } from "lucide-react";
import type { SignalPreferences } from "@/api/signals";

interface SignalPreferencesPanelProps {
  preferences?: SignalPreferences;
  isSaving?: boolean;
  onUpdate: (updates: Partial<SignalPreferences>) => void;
  onRemoveFavorite?: (symbol: string) => void;
  onUnhideSymbol?: (symbol: string) => void;
}

export function SignalPreferencesPanel({ preferences, isSaving = false, onUpdate, onRemoveFavorite, onUnhideSymbol }: SignalPreferencesPanelProps) {
  if (!preferences) {
    return (
      <section className="rounded-3xl border border-white/10 bg-[#0B0E14]/80 p-4 text-sm text-slate-400">
        Loading signal preferences...
      </section>
    );
  }

  const toggle = (key: keyof SignalPreferences) => {
    onUpdate({ [key]: !preferences[key] } as Partial<SignalPreferences>);
  };

  return (
    <section className="rounded-3xl border border-white/10 bg-[#0B0E14]/80 p-4">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-200">
            <Settings2 className="h-4 w-4 text-cyan-200" />
            Signal preferences
          </div>
          <p className="mt-1 text-xs text-slate-500">These filters affect display only. They do not weaken signal generation quality gates.</p>
        </div>
        {isSaving && <span className="text-xs text-cyan-200">Saving...</span>}
      </div>

      <div className="grid gap-3 lg:grid-cols-3">
        <label className="rounded-2xl border border-white/10 bg-white/[0.03] p-3 text-sm text-slate-200">
          <span className="block text-xs uppercase tracking-[0.18em] text-slate-500">Risk style</span>
          <select
            className="mt-2 w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-white outline-none"
            value={preferences.risk_style}
            onChange={(event) => onUpdate({ risk_style: event.target.value })}
          >
            <option value="conservative">Conservative</option>
            <option value="balanced">Balanced</option>
            <option value="aggressive">Aggressive</option>
          </select>
        </label>
        <label className="rounded-2xl border border-white/10 bg-white/[0.03] p-3 text-sm text-slate-200">
          <span className="block text-xs uppercase tracking-[0.18em] text-slate-500">Minimum confidence</span>
          <input
            className="mt-2 w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-white outline-none"
            type="number"
            min={50}
            max={95}
            value={preferences.minimum_confidence}
            onChange={(event) => onUpdate({ minimum_confidence: Number(event.target.value) })}
          />
        </label>
        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-3 text-sm text-slate-200">
          <span className="block text-xs uppercase tracking-[0.18em] text-slate-500">Pair mode</span>
          <button
            type="button"
            onClick={() => toggle("majors_only")}
            className={`mt-2 rounded-xl border px-3 py-2 text-sm font-semibold ${preferences.majors_only ? "border-cyan-300/30 bg-cyan-300/10 text-cyan-100" : "border-white/10 bg-white/5 text-slate-300"}`}
          >
            {preferences.majors_only ? "Majors only enabled" : "All eligible pairs"}
          </button>
        </div>
      </div>

      <div className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-3">
        <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-200">
          <Bell className="h-4 w-4 text-cyan-200" />
          Notification preferences
        </div>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
          {[
            ["notifications_enabled", "Notifications enabled"],
            ["notify_new_signal", "New signal"],
            ["notify_signal_invalidated", "Invalidated"],
            ["notify_tp1_hit", "TP1 partial"],
            ["notify_tp2_hit", "TP2 win"],
            ["notify_tp3_hit", "TP3 strong win"],
            ["notify_sl_hit", "Stop loss"],
            ["notify_entry_window_expiring", "Entry window expiring"],
          ].map(([key, label]) => (
            <label key={key} className="flex items-center gap-2 rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-xs text-slate-200">
              <input
                type="checkbox"
                checked={Boolean(preferences[key as keyof SignalPreferences])}
                onChange={() => toggle(key as keyof SignalPreferences)}
              />
              {label}
            </label>
          ))}
        </div>
        <p className="mt-3 text-xs text-slate-500">Prepared for in-app signal alerts only. Email, Telegram, and WhatsApp sending are not enabled in this phase.</p>
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-2">
        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-3">
          <div className="mb-2 flex items-center gap-2 text-sm font-semibold text-slate-200">
            <Heart className="h-4 w-4 text-rose-200" />
            Favorites
          </div>
          {preferences.favorite_symbols.length ? (
            <div className="flex flex-wrap gap-2">
              {preferences.favorite_symbols.map((symbol) => (
                <button key={symbol} type="button" onClick={() => onRemoveFavorite?.(symbol)} className="rounded-full border border-rose-300/20 bg-rose-300/10 px-3 py-1 text-xs font-semibold text-rose-100">
                  {symbol} ×
                </button>
              ))}
            </div>
          ) : (
            <p className="text-xs text-slate-400">No favorite symbols yet. Use the heart on a signal card.</p>
          )}
        </div>
        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-3">
          <div className="text-sm font-semibold text-slate-200">Hidden symbols</div>
          {preferences.hidden_symbols.length ? (
            <div className="mt-2 flex flex-wrap gap-2">
              {preferences.hidden_symbols.map((symbol) => (
                <button key={symbol} type="button" onClick={() => onUnhideSymbol?.(symbol)} className="rounded-full border border-amber-300/20 bg-amber-300/10 px-3 py-1 text-xs font-semibold text-amber-100">
                  Show {symbol}
                </button>
              ))}
            </div>
          ) : (
            <p className="mt-2 text-xs text-slate-400">No hidden symbols.</p>
          )}
        </div>
      </div>
    </section>
  );
}
