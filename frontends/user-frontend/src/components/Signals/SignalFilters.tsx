import { Search, SlidersHorizontal, X } from "lucide-react";
import { SignalSortSelect } from "@/components/Signals/SignalSortSelect";

export interface SignalFilterState {
  search: string;
  side: string;
  timeframe: string;
  status: string;
  minConfidence: number;
  sort: string;
  favoritesOnly: boolean;
  majorsOnly: boolean;
  includeHidden: boolean;
}

interface SignalFiltersProps {
  filters: SignalFilterState;
  onChange: (filters: SignalFilterState) => void;
  onReset: () => void;
}

export function SignalFilters({ filters, onChange, onReset }: SignalFiltersProps) {
  const update = <K extends keyof SignalFilterState>(key: K, value: SignalFilterState[K]) => {
    onChange({ ...filters, [key]: value });
  };

  return (
    <section className="rounded-3xl border border-white/10 bg-[#0B0E14]/80 p-4">
      <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-200">
        <SlidersHorizontal className="h-4 w-4 text-cyan-200" />
        Signal filters
      </div>
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-6">
        <div className="relative lg:col-span-2">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-500" />
          <input
            className="w-full rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 pl-9 text-sm font-semibold text-white outline-none placeholder:text-slate-500"
            value={filters.search}
            onChange={(event) => update("search", event.target.value.toUpperCase())}
            placeholder="Search symbol e.g. BTCUSDT"
          />
        </div>
        <select className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm font-semibold text-white outline-none" value={filters.side} onChange={(event) => update("side", event.target.value)}>
          <option value="">All sides</option>
          <option value="BUY">BUY</option>
          <option value="SELL">SELL</option>
        </select>
        <select className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm font-semibold text-white outline-none" value={filters.timeframe} onChange={(event) => update("timeframe", event.target.value)}>
          <option value="">All timeframes</option>
          <option value="15m">15m</option>
          <option value="30m">30m</option>
          <option value="1h">1h</option>
          <option value="4h">4h</option>
        </select>
        <select className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm font-semibold text-white outline-none" value={filters.status} onChange={(event) => update("status", event.target.value)}>
          <option value="">All statuses</option>
          <option value="PENDING_ENTRY">Pending Entry</option>
          <option value="ACTIVE">Active</option>
          <option value="EXPIRED">Expired</option>
          <option value="TP1_HIT">TP1 Partial</option>
          <option value="TP2_HIT">TP2 Win</option>
          <option value="TP3_HIT">TP3 Strong Win</option>
          <option value="SL_HIT">Stop Loss Hit</option>
          <option value="CANCELLED">Cancelled</option>
          <option value="INVALIDATED">Invalidated</option>
        </select>
        <SignalSortSelect value={filters.sort} onChange={(value) => update("sort", value)} />
      </div>
      <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-5">
        <label className="flex items-center justify-between gap-3 rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2 text-sm text-slate-200">
          <span>Min confidence</span>
          <input
            className="w-20 rounded-lg border border-white/10 bg-black/20 px-2 py-1 text-right text-white outline-none"
            type="number"
            min={50}
            max={95}
            value={filters.minConfidence}
            onChange={(event) => update("minConfidence", Number(event.target.value))}
          />
        </label>
        <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2 text-sm text-slate-200">
          <input type="checkbox" checked={filters.majorsOnly} onChange={(event) => update("majorsOnly", event.target.checked)} />
          Majors only
        </label>
        <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2 text-sm text-slate-200">
          <input type="checkbox" checked={filters.favoritesOnly} onChange={(event) => update("favoritesOnly", event.target.checked)} />
          Favorites only
        </label>
        <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2 text-sm text-slate-200">
          <input type="checkbox" checked={filters.includeHidden} onChange={(event) => update("includeHidden", event.target.checked)} />
          Include hidden
        </label>
        <button type="button" onClick={onReset} className="inline-flex items-center justify-center gap-2 rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-white transition-colors hover:border-cyan-300/30 hover:bg-cyan-300/10">
          <X className="h-4 w-4" />
          Clear filters
        </button>
      </div>
    </section>
  );
}
