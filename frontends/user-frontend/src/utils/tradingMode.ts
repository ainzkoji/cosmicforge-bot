/**
 * Single source of truth for "is this bot paper or real money?" in the UI.
 *
 * Bot instances carry `mode: "paper" | "live"` (bot-backend
 * bot_instance_models). The UI fails towards LIVE: a mode is paper only when
 * its normalised value is exactly "paper" or "demo". Anything else --
 * "live", undefined, null, an empty or unrecognised string, a non-string, or
 * a bot that could not be found -- is treated as live, so the real-money
 * confirmation and the live indicator are never skipped by accident.
 *
 * (bot-backend's plan gate `is_live_mode` is the mirror image, exactly
 * "live"; both agree on the two values the backend actually stores.)
 *
 * Pure TypeScript with no imports: it is unit-tested with node --test.
 */

const PAPER_MODES: ReadonlySet<string> = new Set(["paper", "demo"]);

export function isPaperMode(mode: unknown): boolean {
    return typeof mode === "string" && PAPER_MODES.has(mode.trim().toLowerCase());
}

/** Everything that is not positively paper is live (real money). */
export function isLiveMode(mode: unknown): boolean {
    return !isPaperMode(mode);
}

/** Badge text. An unrecognised mode is shown as such, never as paper. */
export function tradingModeLabel(mode: unknown): string {
    if (isPaperMode(mode)) return "paper";
    if (typeof mode === "string" && mode.trim().toLowerCase() === "live") return "live";
    return "unknown mode (treated as live)";
}

export interface TradingModeSummary {
    /**
     * paper_only: the list loaded and every bot is paper (also true for no bots).
     * live_running: at least one non-paper bot is not stopped (active, paused, error, unknown status).
     * live_stopped: non-paper bots exist but all of them are stopped.
     */
    kind: "paper_only" | "live_running" | "live_stopped";
    /** Bots that are not positively paper. */
    liveCount: number;
    /** Non-paper bots whose status is "active". */
    liveActiveCount: number;
}

/** Summarise a SUCCESSFULLY loaded bot list. Callers handle loading/error themselves. */
export function summarizeTradingMode(
    bots: ReadonlyArray<{ mode?: unknown; status?: unknown }>,
): TradingModeSummary {
    const live = bots.filter((bot) => isLiveMode(bot.mode));
    const liveActiveCount = live.filter((bot) => bot.status === "active").length;
    if (live.length === 0) return { kind: "paper_only", liveCount: 0, liveActiveCount: 0 };
    const anyNotStopped = live.some((bot) => bot.status !== "stopped");
    return {
        kind: anyNotStopped ? "live_running" : "live_stopped",
        liveCount: live.length,
        liveActiveCount,
    };
}
