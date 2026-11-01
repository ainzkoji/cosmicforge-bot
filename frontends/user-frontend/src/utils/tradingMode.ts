/**
 * Single source of truth for "is this bot paper or real money?" in the UI.
 *
 * Bot instances carry `mode: "paper" | "live"` (bot-backend
 * bot_instance_models). `mode: "live"` only means the bot EXECUTES at a broker
 * instead of on paper; whether that money is real is decided by the
 * environment of the broker ACCOUNT the bot trades on (DEMO/testnet = no real
 * money, LIVE = real money).
 *
 * The UI fails towards LIVE at both levels:
 * - a mode is paper only when its normalised value is exactly "paper" or
 *   "demo". Anything else -- "live", undefined, null, an empty or
 *   unrecognised string, a non-string, or a bot that could not be found -- is
 *   a broker-executing bot;
 * - a broker-executing bot is "demo exchange, no real money" only when its
 *   broker account was loaded AND that account's environment is positively a
 *   DEMO alias. An unknown, unloaded or unrecognised account environment is
 *   real money.
 *
 * So the real-money confirmation and the live indicator are never skipped by
 * accident.
 *
 * (bot-backend's plan gate `is_live_mode` is the mirror image, exactly
 * "live"; both agree on the two values the backend actually stores.)
 *
 * Pure TypeScript with no imports: it is unit-tested with node --test.
 */

const PAPER_MODES: ReadonlySet<string> = new Set(["paper", "demo"]);

/**
 * Broker account environments that are positively NOT real money. Kept in
 * step with the DEMO aliases of src/lib/brokerEnvironment.ts (duplicated
 * because this module must stay import-free).
 */
const DEMO_ENVIRONMENTS: ReadonlySet<string> = new Set(["demo", "testnet", "sandbox", "paper", "test", "practice"]);

export function isPaperMode(mode: unknown): boolean {
    return typeof mode === "string" && PAPER_MODES.has(mode.trim().toLowerCase());
}

/** Everything that is not positively paper executes at a broker (live mode). */
export function isLiveMode(mode: unknown): boolean {
    return !isPaperMode(mode);
}

/** Badge text. An unrecognised mode is shown as such, never as paper. */
export function tradingModeLabel(mode: unknown): string {
    if (isPaperMode(mode)) return "paper";
    if (typeof mode === "string" && mode.trim().toLowerCase() === "live") return "live";
    return "unknown mode (treated as live)";
}

/** True only for an environment value that is positively a DEMO/testnet alias. */
export function isDemoEnvironment(environment: unknown): boolean {
    return typeof environment === "string" && DEMO_ENVIRONMENTS.has(environment.trim().toLowerCase());
}

/** Real money unless the account environment is positively DEMO/testnet. */
export function isRealMoneyEnvironment(environment: unknown): boolean {
    return !isDemoEnvironment(environment);
}

/**
 * What a bot actually risks:
 * - "paper": simulated, never reaches a broker;
 * - "demo": executes at a broker on a DEMO/testnet account (no real money);
 * - "live": executes at a broker with real money, or the account is unknown.
 */
export type BotMoneyKind = "paper" | "demo" | "live";

/** Broker account id -> environment, for the accounts that loaded. */
export type AccountEnvironments = ReadonlyMap<string, unknown>;

/**
 * Index a broker accounts list (`GET /api/v1/brokers/accounts`, items carry
 * `id` and `environment`) by account id. Entries without an id are skipped.
 */
export function indexAccountEnvironments(
    accounts: ReadonlyArray<{ id?: unknown; account_id?: unknown; environment?: unknown }> | null | undefined,
): Map<string, unknown> {
    const index = new Map<string, unknown>();
    for (const account of accounts ?? []) {
        const id = typeof account?.id === "string" && account.id ? account.id
            : typeof account?.account_id === "string" && account.account_id ? account.account_id
            : null;
        if (id) index.set(id, account.environment);
    }
    return index;
}

/**
 * Classify one bot. `accountEnvironments` is null/undefined when the broker
 * accounts could not be loaded: every broker-executing bot is then "live".
 */
export function classifyBotMoney(
    bot: { mode?: unknown; broker_account_id?: unknown },
    accountEnvironments?: AccountEnvironments | null,
): BotMoneyKind {
    if (isPaperMode(bot.mode)) return "paper";
    if (!accountEnvironments || typeof bot.broker_account_id !== "string" || !accountEnvironments.has(bot.broker_account_id)) {
        return "live";
    }
    return isDemoEnvironment(accountEnvironments.get(bot.broker_account_id)) ? "demo" : "live";
}

export interface TradingModeSummary {
    /**
     * paper_only: the list loaded and every bot is paper (also true for no bots).
     * demo_exchange: no real-money bot, but at least one bot executes on a DEMO/testnet broker account.
     * live_running: at least one real-money bot is not stopped (active, paused, error, unknown status).
     * live_stopped: real-money bots exist but all of them are stopped.
     */
    kind: "paper_only" | "demo_exchange" | "live_running" | "live_stopped";
    /** Real-money bots: broker-executing bots whose account is LIVE or unknown. */
    liveCount: number;
    /** Real-money bots whose status is "active". */
    liveActiveCount: number;
    /** Broker-executing bots on a positively DEMO/testnet account. */
    demoCount: number;
    /** DEMO-account bots whose status is "active". */
    demoActiveCount: number;
}

/**
 * Summarise a SUCCESSFULLY loaded bot list. Callers handle loading/error
 * themselves. Without `accountEnvironments` (accounts not loaded) every
 * broker-executing bot counts as real money.
 */
export function summarizeTradingMode(
    bots: ReadonlyArray<{ mode?: unknown; status?: unknown; broker_account_id?: unknown }>,
    accountEnvironments?: AccountEnvironments | null,
): TradingModeSummary {
    const live = bots.filter((bot) => classifyBotMoney(bot, accountEnvironments) === "live");
    const demo = bots.filter((bot) => classifyBotMoney(bot, accountEnvironments) === "demo");
    const liveActiveCount = live.filter((bot) => bot.status === "active").length;
    const demoCount = demo.length;
    const demoActiveCount = demo.filter((bot) => bot.status === "active").length;
    if (live.length === 0) {
        return { kind: demoCount > 0 ? "demo_exchange" : "paper_only", liveCount: 0, liveActiveCount: 0, demoCount, demoActiveCount };
    }
    const anyNotStopped = live.some((bot) => bot.status !== "stopped");
    return {
        kind: anyNotStopped ? "live_running" : "live_stopped",
        liveCount: live.length,
        liveActiveCount,
        demoCount,
        demoActiveCount,
    };
}
