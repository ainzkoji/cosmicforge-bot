/**
 * Step 1.9 -- deploy a CATI bot on the shared deployment contract.
 *
 * The user chooses an exchange account, a risk level and a budget. Everything
 * else on this screen (environment, what the level means in money, the typical
 * position, the minimum budget, whether deployment is possible and why not) is
 * the backend's preview of exactly what it will enforce. There is no leverage
 * control and no paper / live switch: the environment is the connected
 * account's own, and the engine sets leverage inside the level's ceiling.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Activity, AlertTriangle, Check, Info, Loader2, Play, RefreshCw, Shield, TrendingUp } from "lucide-react";
import { api } from "@/api/client";
import {
    DeploymentRefusedError, ageLabel, buildDeploymentRequest, formFingerprint, formatPct, formatUsdt, isStale, newRequestId,
    validateForm,
} from "@/lib/deployment";
import type { Blocker, BudgetType, DeploymentForm, DeploymentPreview, RiskLevel } from "@/lib/deployment";

interface BrokerAccount {
    id: string;
    broker_id: string;
    label?: string;
    name?: string;
    environment?: string;
    status?: string;
}

interface Props {
    accounts: BrokerAccount[];
    /** Pre-filled values (onboarding). Nothing is deployed until the user confirms here. */
    initial?: { riskLevel?: RiskLevel; budgetValue?: string; brokerAccountId?: string };
}

const LEVELS: { id: RiskLevel; title: string; icon: typeof Shield; tone: string; active: string; text: string }[] = [
    { id: "conservative", title: "Conservative", icon: Shield, tone: "text-blue-400", active: "bg-blue-400/10 border-blue-400",
        text: "Smallest risk per trade and the tightest limits of the three. Losses are still possible." },
    { id: "balanced", title: "Balanced", icon: Activity, tone: "text-primary", active: "bg-primary/10 border-primary",
        text: "Between Conservative and Aggressive in risk per trade and limits." },
    { id: "aggressive", title: "Aggressive", icon: TrendingUp, tone: "text-amber-400", active: "bg-amber-400/10 border-amber-400",
        text: "Largest risk per trade and the widest limits. Larger and faster losses are possible." },
];

const PREVIEW_DEBOUNCE_MS = 450;

function Row({ label, value, hint }: { label: string; value: string; hint?: string }) {
    return (
        <div className="flex justify-between items-start gap-4 py-2 border-b border-white/5">
            <span className="text-gray-500 text-sm">{label}{hint && <span className="block text-xs text-gray-600">{hint}</span>}</span>
            <span className="text-white font-medium text-sm text-right font-mono">{value}</span>
        </div>
    );
}

function BlockerList({ blockers }: { blockers: Blocker[] }) {
    if (blockers.length === 0) return null;
    return (
        <ul role="alert" className="space-y-2">
            {blockers.map((b, i) => (
                <li key={`${b.code}-${i}`} className="p-3 bg-red-500/10 border border-red-500/20 rounded-xl text-sm text-red-300 flex gap-2">
                    <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                    <div>
                        <div>{b.message || b.code}</div>
                        {b.action && <div className="text-red-200/70 text-xs mt-1">{b.action}</div>}
                        {typeof b.minimum_deployable_budget === "string" && (
                            <div className="text-red-200/70 text-xs mt-1">Minimum budget for this level: {formatUsdt(b.minimum_deployable_budget)}</div>
                        )}
                        <div className="text-red-200/40 text-[10px] mt-1 font-mono">{b.code}</div>
                    </div>
                </li>
            ))}
        </ul>
    );
}

export function CatiDeployment({ accounts, initial }: Props) {
    const navigate = useNavigate();
    const [form, setForm] = useState<DeploymentForm>({
        brokerAccountId: initial?.brokerAccountId ?? "",
        riskLevel: initial?.riskLevel ?? "balanced",
        budgetType: "fixed_amount",
        budgetValue: initial?.budgetValue ?? "",
        maxPositionUsdt: "",
        dailyLossLimitPct: "",
        riskAcknowledged: false,
    });
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [preview, setPreview] = useState<DeploymentPreview | null>(null);
    const [previewFor, setPreviewFor] = useState<string | null>(null);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [previewError, setPreviewError] = useState<string | null>(null);
    const [deploying, setDeploying] = useState(false);
    const [deployError, setDeployError] = useState<string | null>(null);
    const [deployBlockers, setDeployBlockers] = useState<Blocker[]>([]);
    const [refreshTick, setRefreshTick] = useState(0);
    const [now, setNow] = useState(() => Date.now());

    const fingerprint = formFingerprint(form);
    // One request id per deploy intent: it changes when the intent changes and
    // is reused on every retry, so a retry can never create a second bot.
    const intent = useRef<{ fingerprint: string; requestId: string }>({ fingerprint, requestId: newRequestId() });
    if (intent.current.fingerprint !== fingerprint) intent.current = { fingerprint, requestId: newRequestId() };

    const set = <K extends keyof DeploymentForm>(key: K, value: DeploymentForm[K]) => {
        setDeployError(null);
        setDeployBlockers([]);
        setForm((f) => ({ ...f, [key]: value }));
    };

    // Select the only / first account once accounts are known.
    useEffect(() => {
        if (accounts.length === 0) return;
        if (!accounts.some((a) => a.id === form.brokerAccountId)) setForm((f) => ({ ...f, brokerAccountId: accounts[0].id }));
    }, [accounts, form.brokerAccountId]);

    useEffect(() => {
        const timer = window.setInterval(() => setNow(Date.now()), 15_000);
        return () => window.clearInterval(timer);
    }, []);

    const formProblem = validateForm(form);

    // Debounced preview of exactly what the backend will enforce.
    useEffect(() => {
        if (formProblem) {
            setPreview(null);
            setPreviewFor(null);
            setPreviewError(null);
            setPreviewLoading(false);
            return;
        }
        const controller = new AbortController();
        setPreviewLoading(true);
        const timer = window.setTimeout(async () => {
            try {
                const result = await api.previewDeployment(buildDeploymentRequest(form, intent.current.requestId), controller.signal);
                setPreview(result);
                setPreviewFor(fingerprint);
                setPreviewError(null);
            } catch (error) {
                if (controller.signal.aborted) return;
                if (error instanceof DeploymentRefusedError && error.preview) {
                    setPreview(error.preview);
                    setPreviewFor(fingerprint);
                    setPreviewError(null);
                } else {
                    setPreview(null);
                    setPreviewFor(null);
                    setPreviewError(error instanceof Error && error.message ? error.message : "The preview could not be loaded.");
                }
            } finally {
                if (!controller.signal.aborted) setPreviewLoading(false);
            }
        }, PREVIEW_DEBOUNCE_MS);
        return () => {
            controller.abort();
            window.clearTimeout(timer);
        };
        // The acknowledgement is not part of the previewed intent.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [fingerprint, formProblem, refreshTick]);

    const previewCurrent = preview !== null && previewFor === fingerprint;
    const blockers = useMemo(() => {
        const fromPreview = previewCurrent ? preview.blockers : [];
        const seen = new Set(fromPreview.map((b) => b.code));
        return [...fromPreview, ...deployBlockers.filter((b) => !seen.has(b.code))];
    }, [previewCurrent, preview, deployBlockers]);
    const balanceStale = previewCurrent && preview.account ? isStale(preview.account.observed_at, now) : false;
    const canDeploy = previewCurrent && preview.can_deploy && deployBlockers.length === 0 && form.riskAcknowledged
        && !previewLoading && !deploying && !balanceStale;

    const handleDeploy = async () => {
        if (!canDeploy) return;
        setDeploying(true);
        setDeployError(null);
        setDeployBlockers([]);
        try {
            const result = await api.deployBot(buildDeploymentRequest(form, intent.current.requestId));
            navigate(`/dashboard/bots/${result.bot.id}`);
        } catch (error) {
            if (error instanceof DeploymentRefusedError) {
                setDeployBlockers(error.blockers);
                if (error.preview) {
                    setPreview(error.preview);
                    setPreviewFor(fingerprint);
                }
                if (error.blockers.length === 0) setDeployError(error.message);
            } else {
                // The outcome is unknown (timeout / network): the same request id is
                // kept, so pressing Deploy again returns the same bot if it was created.
                setDeployError(`${error instanceof Error && error.message ? error.message : "The deployment request did not complete."} `
                    + "It is safe to press Deploy again: the same request can only ever create one bot.");
            }
        } finally {
            setDeploying(false);
        }
    };

    const money = previewCurrent ? preview.money : undefined;
    const selected = accounts.find((a) => a.id === form.brokerAccountId);
    const environment = previewCurrent ? preview.environment : undefined;

    if (accounts.length === 0) {
        return (
            <div className="p-4 bg-yellow-500/10 border border-yellow-500/20 rounded-xl text-yellow-500 text-sm flex items-center gap-3">
                <AlertTriangle className="w-5 h-5 flex-shrink-0" />
                <div>
                    No connected exchange account. Connect a Binance demo account first.
                    <br />
                    <a href="/dashboard/brokers" className="underline font-bold mt-1 inline-block">Connect an exchange account</a>
                </div>
            </div>
        );
    }

    return (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div className="lg:col-span-2 space-y-8">
                {/* Account */}
                <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                    <h2 className="text-lg font-semibold text-white">1. Exchange account</h2>
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                        {accounts.map((account) => (
                            <button
                                type="button"
                                key={account.id}
                                onClick={() => set("brokerAccountId", account.id)}
                                aria-pressed={form.brokerAccountId === account.id}
                                className={`text-left p-4 rounded-xl border transition-all ${form.brokerAccountId === account.id
                                    ? "bg-primary/10 border-primary" : "bg-[#0F1218] border-white/5 hover:border-white/10"}`}
                            >
                                <div className="flex justify-between items-start">
                                    <div className="font-bold text-white">{account.label || account.name || account.broker_id}</div>
                                    {form.brokerAccountId === account.id && <Check className="w-4 h-4 text-primary" />}
                                </div>
                                <div className="text-sm text-gray-500 mt-1 uppercase">{account.broker_id}</div>
                                {account.environment && <div className="text-xs text-gray-600 uppercase mt-2">{account.environment}</div>}
                            </button>
                        ))}
                    </div>
                    <p className="text-xs text-gray-500">One bot per exchange account. The bot trades in the account's own environment.</p>
                </section>

                {/* Risk level */}
                <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                    <h2 className="text-lg font-semibold text-white">2. Risk level</h2>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        {LEVELS.map((level) => (
                            <button
                                type="button"
                                key={level.id}
                                onClick={() => set("riskLevel", level.id)}
                                aria-pressed={form.riskLevel === level.id}
                                className={`text-left rounded-xl border p-4 transition-all ${form.riskLevel === level.id
                                    ? level.active : "bg-[#0F1218] border-white/5 hover:border-white/10"}`}
                            >
                                <level.icon className={`w-6 h-6 mb-3 ${level.tone}`} />
                                <div className="font-bold text-white mb-1">{level.title}</div>
                                <p className="text-xs text-gray-400">{level.text}</p>
                            </button>
                        ))}
                    </div>
                    <p className="text-xs text-gray-500">
                        A risk level sets how much of the budget one trade may lose at its stop and when the bot pauses. It does not
                        guarantee a result or cap your loss.
                    </p>
                </section>

                {/* Budget */}
                <section className="bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-4">
                    <h2 className="text-lg font-semibold text-white">3. Budget</h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div>
                            <span className="text-sm text-gray-400 mb-2 block">Budget type</span>
                            <div className="flex bg-[#0F1218] p-1 rounded-lg border border-white/5">
                                {([["fixed_amount", "Fixed amount"], ["percent_balance", "% of balance"]] as [BudgetType, string][]).map(([id, label]) => (
                                    <button
                                        type="button"
                                        key={id}
                                        onClick={() => set("budgetType", id)}
                                        aria-pressed={form.budgetType === id}
                                        className={`flex-1 py-2 text-sm font-medium rounded-md transition-all ${form.budgetType === id
                                            ? "bg-white/10 text-white" : "text-gray-500 hover:text-gray-300"}`}
                                    >
                                        {label}
                                    </button>
                                ))}
                            </div>
                        </div>
                        <div>
                            <label htmlFor="deploy-budget" className="text-sm text-gray-400 mb-2 block">
                                {form.budgetType === "fixed_amount" ? "Budget (USDT)" : "Budget (% of account balance)"}
                            </label>
                            <div className="relative">
                                <input
                                    id="deploy-budget"
                                    inputMode="decimal"
                                    value={form.budgetValue}
                                    placeholder={form.budgetType === "fixed_amount" ? "e.g. 1000" : "e.g. 25"}
                                    onChange={(e) => set("budgetValue", e.target.value)}
                                    className="w-full bg-[#0F1218] border border-white/10 rounded-lg px-4 py-2 text-white focus:border-primary focus:outline-none font-mono"
                                />
                                <div className="absolute right-4 top-2 text-gray-500 text-sm">{form.budgetType === "fixed_amount" ? "USDT" : "%"}</div>
                            </div>
                        </div>
                    </div>
                    <p className="text-xs text-gray-500">
                        The budget is the amount the risk level is applied to. It is not transferred anywhere and stays in your exchange account.
                    </p>

                    <button type="button" onClick={() => setShowAdvanced((v) => !v)} className="text-sm text-primary hover:underline" aria-expanded={showAdvanced}>
                        {showAdvanced ? "Hide advanced settings" : "Advanced settings (optional)"}
                    </button>
                    {showAdvanced && (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div>
                                <label htmlFor="deploy-max-position" className="text-sm text-gray-400 mb-2 block">Maximum position size (USDT)</label>
                                <input
                                    id="deploy-max-position"
                                    inputMode="decimal"
                                    value={form.maxPositionUsdt}
                                    placeholder="No extra cap"
                                    onChange={(e) => set("maxPositionUsdt", e.target.value)}
                                    className="w-full bg-[#0F1218] border border-white/10 rounded-lg px-4 py-2 text-white focus:border-primary focus:outline-none font-mono"
                                />
                                <p className="text-xs text-gray-500 mt-1">Can only make positions smaller than the risk level allows.</p>
                            </div>
                            <div>
                                <label htmlFor="deploy-daily-loss" className="text-sm text-gray-400 mb-2 block">Daily loss pause (% of budget)</label>
                                <input
                                    id="deploy-daily-loss"
                                    inputMode="decimal"
                                    value={form.dailyLossLimitPct}
                                    placeholder="Use the risk level's value"
                                    onChange={(e) => set("dailyLossLimitPct", e.target.value)}
                                    className="w-full bg-[#0F1218] border border-white/10 rounded-lg px-4 py-2 text-white focus:border-primary focus:outline-none font-mono"
                                />
                                <p className="text-xs text-gray-500 mt-1">Can only be tighter than the risk level's daily pause.</p>
                            </div>
                        </div>
                    )}
                </section>
            </div>

            {/* What this means */}
            <div className="lg:col-span-1">
                <div className="sticky top-24 bg-[#0B0E14] border border-white/5 rounded-2xl p-6 space-y-5">
                    <div className="flex items-center justify-between">
                        <h3 className="font-bold text-white text-lg">What this means</h3>
                        <button
                            type="button"
                            onClick={() => setRefreshTick((t) => t + 1)}
                            disabled={previewLoading || !!formProblem}
                            className="text-gray-500 hover:text-white disabled:opacity-40"
                            aria-label="Refresh preview"
                        >
                            {previewLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                        </button>
                    </div>

                    {formProblem && <p className="text-sm text-gray-400">{formProblem}</p>}

                    {!formProblem && previewLoading && !previewCurrent && (
                        <div className="text-sm text-gray-400 flex items-center gap-2" aria-live="polite">
                            <Loader2 className="w-4 h-4 animate-spin" /> Checking this with the engine…
                        </div>
                    )}

                    {!formProblem && previewError && (
                        <div role="alert" className="p-3 bg-red-500/10 border border-red-500/20 rounded-xl text-red-300 text-sm">
                            The preview could not be loaded, so nothing can be deployed yet. {previewError}
                        </div>
                    )}

                    {previewCurrent && (
                        <div className={previewLoading ? "opacity-60" : ""}>
                            <Row label="Engine" value="CATI" />
                            <Row label="Exchange" value={`${preview.exchange ?? selected?.broker_id?.toUpperCase() ?? "—"}`} />
                            <div className="flex justify-between items-center py-2 border-b border-white/5">
                                <span className="text-gray-500 text-sm">Environment</span>
                                <span className={`text-xs font-bold px-2 py-1 rounded ${environment === "DEMO"
                                    ? "bg-blue-500/20 text-blue-300" : "bg-amber-500/20 text-amber-300"}`}>
                                    {environment ?? "—"}{environment === "DEMO" ? " · no real money" : ""}
                                </span>
                            </div>
                            {preview.account ? (
                                <Row
                                    label="Account balance"
                                    hint={`${balanceStale ? "stale · " : ""}read ${ageLabel(preview.account.observed_at, now)}`}
                                    value={formatUsdt(preview.account.equity)}
                                />
                            ) : (
                                <Row label="Account balance" value="unavailable" />
                            )}
                            {money ? (
                                <>
                                    <Row label="Budget" value={formatUsdt(preview.effective_budget)} />
                                    <Row
                                        label="Risk per trade"
                                        hint={`${formatPct(money.effective_per_trade_risk_pct)} of budget, at the stop`}
                                        value={formatUsdt(money.risk_per_trade)}
                                    />
                                    <Row label="Open risk, all positions" hint={formatPct(money.max_open_risk_pct)} value={formatUsdt(money.max_open_risk)} />
                                    <Row label="Daily loss pause" hint={formatPct(money.daily_loss_pause_pct)} value={formatUsdt(money.daily_loss_pause)} />
                                    <Row label="Drawdown: size reduced" hint={formatPct(money.drawdown_reduce_pct)} value={formatUsdt(money.drawdown_reduce_threshold)} />
                                    <Row label="Drawdown: bot stops" hint={formatPct(money.drawdown_stop_pct)} value={formatUsdt(money.drawdown_stop_threshold)} />
                                    <Row label="Positions at once" value={`up to ${money.max_positions}`} />
                                    <Row label="Leverage" hint="set by the engine" value={`up to ${money.leverage_ceiling}x`} />
                                    {preview.typical_position?.typical ? (
                                        <Row
                                            label="Typical position"
                                            hint="from the engine's recent stop distances"
                                            value={preview.typical_position.typical.approved
                                                ? formatUsdt(preview.typical_position.typical.notional_usdt) : "below exchange minimum"}
                                        />
                                    ) : (
                                        <Row label="Typical position" hint="not enough engine history yet" value="not estimated" />
                                    )}
                                    {preview.minimum_deployable_budget && (
                                        <Row label="Minimum budget for this level" value={formatUsdt(preview.minimum_deployable_budget)} />
                                    )}
                                </>
                            ) : (
                                <p className="text-sm text-gray-400 py-2">The money view is unavailable until the account balance can be read.</p>
                            )}
                            {preview.ceiling_conflict && (
                                <div className="mt-3 p-3 bg-blue-500/10 border border-blue-500/20 rounded-xl text-blue-300 text-xs flex gap-2">
                                    <Info className="w-4 h-4 flex-shrink-0" />
                                    <div>
                                        This level's approved risk per trade is {formatPct(money?.per_trade_risk_pct)}; the engine currently
                                        applies its stricter {formatPct(preview.ceiling_conflict.system_per_trade_risk_ceiling_pct)} ceiling,
                                        which is what the figures above show.
                                    </div>
                                </div>
                            )}
                            {balanceStale && (
                                <div role="alert" className="mt-3 p-3 bg-amber-500/10 border border-amber-500/20 rounded-xl text-amber-300 text-xs">
                                    The account balance shown is out of date. Refresh the preview before deploying.
                                </div>
                            )}
                        </div>
                    )}

                    <BlockerList blockers={blockers} />

                    {previewCurrent && preview.consent && (
                        <label className="flex gap-3 items-start text-sm text-gray-300 cursor-pointer">
                            <input
                                type="checkbox"
                                checked={form.riskAcknowledged}
                                onChange={(e) => set("riskAcknowledged", e.target.checked)}
                                className="mt-1 h-4 w-4"
                            />
                            <span>{preview.consent.text}</span>
                        </label>
                    )}

                    {deployError && (
                        <div role="alert" className="p-3 bg-red-500/10 border border-red-500/20 rounded-xl text-red-300 text-sm">{deployError}</div>
                    )}

                    <button
                        type="button"
                        onClick={handleDeploy}
                        disabled={!canDeploy}
                        className="w-full py-4 rounded-xl font-bold text-lg flex items-center justify-center gap-2 transition-all bg-gradient-to-r from-primary to-purple-600 text-white disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                        {deploying ? <Loader2 className="w-5 h-5 animate-spin" /> : <Play className="w-5 h-5 fill-current" />}
                        {deploying ? "Deploying…" : environment === "DEMO" ? "Deploy demo bot" : "Deploy bot"}
                    </button>
                    {previewCurrent && preview.can_deploy && !form.riskAcknowledged && (
                        <p className="text-center text-xs text-gray-500">Tick the acknowledgement above to deploy.</p>
                    )}
                    {previewCurrent && preview.disclaimer && <p className="text-xs text-gray-500">{preview.disclaimer}</p>}
                </div>
            </div>
        </div>
    );
}

export default CatiDeployment;
