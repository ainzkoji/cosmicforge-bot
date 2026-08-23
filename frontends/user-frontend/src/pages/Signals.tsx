import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { AlertTriangle, BarChart3, Clock3, LockKeyhole, RadioTower, ShieldCheck, Sparkles, Trophy } from "lucide-react";
import {
  getActiveSignals,
  addFavoriteSignalSymbol,
  getSignalDetail,
  getSignalHistory,
  getSignalNotifications,
  getSignalPerformance,
  getSignalPreferences,
  hideSignalSymbol,
  removeFavoriteSignalSymbol,
  unhideSignalSymbol,
  updateSignalPreferences,
  type TradingSignal,
  type SignalPreferences,
} from "@/api/signals";
import { SignalCard } from "@/components/Signals/SignalCard";
import { SignalDetailsModal } from "@/components/Signals/SignalDetailsModal";
import { SignalFilters, type SignalFilterState } from "@/components/Signals/SignalFilters";
import { SignalPreferencesPanel } from "@/components/Signals/SignalPreferencesPanel";

const DISCLAIMER =
  "Signals are for educational and informational purposes only. Trading involves risk. Past performance does not guarantee future results.";

type SignalTab = "active" | "completed" | "expired" | "forex";

const completedStatuses = new Set(["TP1_HIT", "TP2_HIT", "TP3_HIT", "SL_HIT", "CANCELLED", "INVALIDATED", "EXPIRED"]);
const activeStatuses = new Set(["PENDING_ENTRY", "ACTIVE"]);

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="rounded-3xl border border-dashed border-white/10 bg-white/[0.03] p-10 text-center">
      <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl bg-white/5 text-slate-300">
        <RadioTower className="h-6 w-6" />
      </div>
      <h3 className="mt-4 text-lg font-semibold text-white">{title}</h3>
      <p className="mx-auto mt-2 max-w-xl text-sm text-slate-400">{description}</p>
    </div>
  );
}

function LoadingGrid() {
  return (
    <div className="grid gap-3 md:grid-cols-2 2xl:grid-cols-3">
      {[0, 1, 2, 3, 4, 5].map((item) => (
        <div key={item} className="h-56 animate-pulse rounded-2xl border border-white/10 bg-white/[0.04]" />
      ))}
    </div>
  );
}

function ErrorState() {
  return (
    <div className="rounded-2xl border border-red-400/20 bg-red-400/10 p-5 text-red-100">
      Unable to load signals right now. Please try again.
    </div>
  );
}

function StatCard({
  label,
  value,
  helper,
  Icon,
}: {
  label: string;
  value: string | number;
  helper: string;
  Icon: typeof BarChart3;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-[#111722]/80 p-5 shadow-xl shadow-black/10">
      <div className="flex items-center justify-between">
        <div className="text-sm text-slate-400">{label}</div>
        <div className="rounded-xl border border-cyan-300/20 bg-cyan-300/10 p-2 text-cyan-200">
          <Icon className="h-4 w-4" />
        </div>
      </div>
      <div className="mt-3 text-3xl font-bold text-white">{value}</div>
      <div className="mt-1 text-xs text-slate-500">{helper}</div>
    </div>
  );
}

function renderSignals(
  signals: TradingSignal[],
  loading: boolean,
  error: boolean,
  emptyTitle: string,
  emptyDescription: string,
  onViewDetails: (signalId: string) => void,
  preferences?: SignalPreferences,
  onToggleFavorite?: (symbol: string, isFavorite: boolean) => void,
  onHideSymbol?: (symbol: string) => void
) {
  if (loading) return <LoadingGrid />;
  if (error) return <ErrorState />;
  if (!signals.length) return <EmptyState title={emptyTitle} description={emptyDescription} />;
  return (
    <div className="grid gap-3 md:grid-cols-2 2xl:grid-cols-3">
      {signals.map((signal) => (
        <SignalCard
          key={signal.id}
          signal={signal}
          onViewDetails={onViewDetails}
          isFavorite={Boolean(preferences?.favorite_symbols.includes(signal.symbol))}
          isHidden={Boolean(preferences?.hidden_symbols.includes(signal.symbol))}
          onToggleFavorite={onToggleFavorite}
          onHideSymbol={onHideSymbol}
        />
      ))}
    </div>
  );
}

export default function Signals() {
  const [activeTab, setActiveTab] = useState<SignalTab>("active");
  const [selectedSignalId, setSelectedSignalId] = useState<string | null>(null);
  const queryClient = useQueryClient();

  const preferences = useQuery({
    queryKey: ["signals", "preferences"],
    queryFn: getSignalPreferences,
  });

  const defaultMinConfidence = preferences.data?.minimum_confidence ?? 70;
  const [filters, setFilters] = useState<SignalFilterState>({
    search: "",
    side: "",
    timeframe: "",
    status: "",
    minConfidence: 70,
    sort: "newest",
    favoritesOnly: false,
    majorsOnly: false,
    includeHidden: false,
  });

  const effectiveFilters = {
    search: filters.search || undefined,
    side: filters.side || undefined,
    timeframe: filters.timeframe || undefined,
    min_confidence: filters.minConfidence || defaultMinConfidence,
    sort: filters.sort,
    favorites_only: filters.favoritesOnly ? 1 : undefined,
    majors_only: filters.majorsOnly ? 1 : undefined,
    include_hidden: filters.includeHidden ? 1 : undefined,
  };
  const activeStatusFilter = filters.status && activeStatuses.has(filters.status) ? filters.status : undefined;
  const historyStatusFilter = filters.status && completedStatuses.has(filters.status) ? filters.status : undefined;

  useEffect(() => {
    if (!preferences.data) return;
    setFilters((current) => ({
      ...current,
      minConfidence: preferences.data.minimum_confidence,
      majorsOnly: preferences.data.majors_only,
    }));
  }, [preferences.data?.minimum_confidence, preferences.data?.majors_only]);

  const activeSignals = useQuery({
    queryKey: ["signals", "active", effectiveFilters],
    queryFn: () =>
      getActiveSignals({
        asset_class: "crypto",
        ...effectiveFilters,
        status: activeStatusFilter,
        limit: 100,
      }),
  });

  const historySignals = useQuery({
    queryKey: ["signals", "history", effectiveFilters, filters.status],
    queryFn: () =>
      getSignalHistory({
        asset_class: "crypto",
        ...effectiveFilters,
        status: historyStatusFilter,
        limit: 150,
      }),
  });

  const performance = useQuery({
    queryKey: ["signals", "performance"],
    queryFn: () => getSignalPerformance({ asset_class: "crypto" }),
  });

  const signalDetail = useQuery({
    queryKey: ["signals", "detail", selectedSignalId],
    queryFn: () => getSignalDetail(selectedSignalId as string),
    enabled: Boolean(selectedSignalId),
  });

  const notifications = useQuery({
    queryKey: ["signals", "notifications"],
    queryFn: () => getSignalNotifications({ limit: 5 }),
  });

  const refreshPreferences = () => {
    queryClient.invalidateQueries({ queryKey: ["signals", "preferences"] });
    queryClient.invalidateQueries({ queryKey: ["signals", "active"] });
    queryClient.invalidateQueries({ queryKey: ["signals", "history"] });
  };

  const updatePreferencesMutation = useMutation({
    mutationFn: updateSignalPreferences,
    onSuccess: refreshPreferences,
  });
  const addFavoriteMutation = useMutation({ mutationFn: addFavoriteSignalSymbol, onSuccess: refreshPreferences });
  const removeFavoriteMutation = useMutation({ mutationFn: removeFavoriteSignalSymbol, onSuccess: refreshPreferences });
  const hideSymbolMutation = useMutation({ mutationFn: hideSignalSymbol, onSuccess: refreshPreferences });
  const unhideSymbolMutation = useMutation({ mutationFn: unhideSignalSymbol, onSuccess: refreshPreferences });

  const handleToggleFavorite = (symbol: string, isFavorite: boolean) => {
    if (isFavorite) removeFavoriteMutation.mutate(symbol);
    else addFavoriteMutation.mutate(symbol);
  };

  const resetFilters = () => {
    setFilters({
      search: "",
      side: "",
      timeframe: "",
      status: "",
      minConfidence: preferences.data?.minimum_confidence ?? 70,
      sort: "newest",
      favoritesOnly: false,
      majorsOnly: preferences.data?.majors_only ?? false,
      includeHidden: false,
    });
  };

  const historyItems = historySignals.data?.items || [];
  const completedItems = useMemo(
    () => historyItems.filter((signal) => completedStatuses.has(signal.status) && signal.status !== "EXPIRED"),
    [historyItems]
  );
  const expiredItems = useMemo(
    () => historyItems.filter((signal) => signal.status === "EXPIRED"),
    [historyItems]
  );

  const perf = performance.data;
  const winRateLabel = perf?.win_rate === null || perf?.win_rate === undefined ? "--" : `${perf.win_rate}%`;
  const performanceMessage = perf?.message || "Performance data will appear after enough completed signals.";

  const tabs: { id: SignalTab; label: string; count?: number }[] = [
    { id: "active", label: "Active", count: activeSignals.data?.count || 0 },
    { id: "completed", label: "Completed", count: completedItems.length },
    { id: "expired", label: "Expired", count: expiredItems.length },
    { id: "forex", label: "Forex Coming Later" },
  ];

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.45 }}
      className="mx-auto max-w-[1600px] space-y-8"
    >
      <section className="relative overflow-hidden rounded-3xl border border-white/10 bg-[#101722] p-6 shadow-2xl shadow-black/20 md:p-8">
        <div className="absolute -right-20 -top-24 h-64 w-64 rounded-full bg-cyan-400/10 blur-3xl" />
        <div className="absolute -bottom-28 left-1/3 h-64 w-64 rounded-full bg-emerald-400/10 blur-3xl" />
        <div className="relative flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full border border-cyan-300/20 bg-cyan-300/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.24em] text-cyan-100">
              <Sparkles className="h-3.5 w-3.5" />
              Manual crypto ideas
            </div>
            <h1 className="mt-4 text-4xl font-black tracking-tight text-white md:text-5xl">Crypto Signals</h1>
            <p className="mt-3 max-w-3xl text-slate-300">
              Daily crypto trade setups with entry, stop loss, take profit, and a clear latest-entry window.
            </p>
          </div>
          <div className="rounded-2xl border border-amber-300/20 bg-amber-300/10 p-4 text-sm leading-6 text-amber-50 lg:max-w-xl">
            <div className="flex gap-3">
              <AlertTriangle className="mt-0.5 h-5 w-5 flex-shrink-0" />
              <p>{DISCLAIMER}</p>
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Active Signals"
          value={perf?.active_signals ?? activeSignals.data?.count ?? "--"}
          helper="Published crypto signals still in play"
          Icon={RadioTower}
        />
        <StatCard
          label="Completed Signals"
          value={perf?.completed_signals ?? "--"}
          helper="Closed, invalidated, cancelled, or expired"
          Icon={ShieldCheck}
        />
        <StatCard
          label="TP2/TP3 Win Rate"
          value={winRateLabel}
          helper="TP1 is partial progress only"
          Icon={Trophy}
        />
        <StatCard
          label="Expired Signals"
          value={perf?.expired_signals ?? "--"}
          helper={performance.isSuccess ? performanceMessage : "Performance data will appear after enough completed signals."}
          Icon={Clock3}
        />
      </section>

      <SignalFilters filters={filters} onChange={setFilters} onReset={resetFilters} />

      <SignalPreferencesPanel
        preferences={preferences.data}
        isSaving={updatePreferencesMutation.isPending}
        onUpdate={(updates) => updatePreferencesMutation.mutate(updates)}
        onRemoveFavorite={(symbol) => removeFavoriteMutation.mutate(symbol)}
        onUnhideSymbol={(symbol) => unhideSymbolMutation.mutate(symbol)}
      />

      <section className="rounded-3xl border border-white/10 bg-[#0B0E14]/80 p-4">
        <div className="mb-3 flex items-center justify-between gap-3">
          <div>
            <h2 className="text-sm font-semibold text-slate-200">In-app signal notification prep</h2>
            <p className="mt-1 text-xs text-slate-500">Signal event records can appear here. External email, Telegram, and WhatsApp sending are not enabled.</p>
          </div>
        </div>
        {notifications.data?.items?.length ? (
          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
            {notifications.data.items.map((item) => (
              <div key={item.id} className="rounded-2xl border border-white/10 bg-white/[0.03] p-3">
                <div className="text-xs uppercase tracking-[0.18em] text-cyan-200">{item.event_type.replace(/_/g, " ")}</div>
                <div className="mt-1 text-sm font-semibold text-white">{item.title}</div>
                <p className="mt-1 text-xs text-slate-400">{item.message}</p>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-slate-500">No in-app signal notifications yet.</p>
        )}
      </section>

      <section className="rounded-3xl border border-white/10 bg-[#0B0E14]/80 p-2">
        <div className="flex flex-wrap gap-2">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveTab(tab.id)}
              className={`rounded-2xl px-4 py-2 text-sm font-semibold transition-all ${
                activeTab === tab.id
                  ? "bg-cyan-300 text-slate-950 shadow-lg shadow-cyan-300/20"
                  : "text-slate-400 hover:bg-white/5 hover:text-white"
              }`}
            >
              {tab.label}
              {tab.count !== undefined && <span className="ml-2 opacity-70">{tab.count}</span>}
            </button>
          ))}
        </div>
      </section>

      <section>
        {activeTab === "active" &&
          renderSignals(
            activeSignals.data?.items || [],
            activeSignals.isLoading,
            activeSignals.isError,
            "No active crypto signals right now.",
            "New signals appear only when valid setups are found.",
            setSelectedSignalId,
            preferences.data,
            handleToggleFavorite,
            (symbol) => hideSymbolMutation.mutate(symbol)
          )}

        {activeTab === "completed" &&
          renderSignals(
            completedItems,
            historySignals.isLoading,
            historySignals.isError,
            "No completed signal history yet.",
            "Completed outcomes will appear after published signals finish tracking.",
            setSelectedSignalId,
            preferences.data,
            handleToggleFavorite,
            (symbol) => hideSymbolMutation.mutate(symbol)
          )}

        {activeTab === "expired" &&
          renderSignals(
            expiredItems,
            historySignals.isLoading,
            historySignals.isError,
            "No expired signals yet.",
            "Expired setups will be listed here with clear warnings not to enter late.",
            setSelectedSignalId,
            preferences.data,
            handleToggleFavorite,
            (symbol) => hideSymbolMutation.mutate(symbol)
          )}

        {activeTab === "forex" && (
          <div className="rounded-3xl border border-dashed border-white/10 bg-white/[0.03] p-10 text-center">
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-2xl bg-white/5 text-slate-300">
              <LockKeyhole className="h-7 w-7" />
            </div>
            <h3 className="mt-4 text-xl font-bold text-white">Forex Signals are coming later.</h3>
            <p className="mx-auto mt-2 max-w-xl text-slate-400">Crypto signals are available first. Forex will stay locked until the signal engine and review flow are ready for that asset class.</p>
          </div>
        )}
      </section>

      <SignalDetailsModal
        isOpen={Boolean(selectedSignalId)}
        signal={signalDetail.data}
        isLoading={signalDetail.isLoading}
        onClose={() => setSelectedSignalId(null)}
      />
    </motion.div>
  );
}
