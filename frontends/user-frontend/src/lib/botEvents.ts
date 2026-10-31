/**
 * Section K3 -- the engine's per-user bot events and what each one refreshes on screen.
 *
 * The stream is owner-filtered by the engine; this module only decides which cached reads are stale.
 * Each frame carries `id` (the event id), so a reconnecting EventSource resumes with Last-Event-ID and the
 * engine replays what was missed.
 */
export const BOT_EVENT_TYPES = [
    "ENTRY_FILLED",
    "EXIT_FILLED",
    "POSITION_UNPROTECTED",
    "BOT_PAUSED",
    "BOT_RESUMED",
    "BOT_STOPPED",
    "DAILY_LOSS_PAUSE",
    "ENTRY_BLOCKED",
] as const;

export type BotEventType = (typeof BOT_EVENT_TYPES)[number];

export interface BotEventFrame {
    event_id: string;
    event_type: BotEventType;
    timestamp: number;
    user_id: string;
    bot_id: string | null;
    broker_account_id: string | null;
    environment: string | null;
    payload: Record<string, unknown>;
}

const REFRESH: Record<BotEventType, readonly string[]> = {
    ENTRY_FILLED: ["status", "positions", "trades", "summary", "events"],
    EXIT_FILLED: ["status", "positions", "trades", "summary", "events"],
    POSITION_UNPROTECTED: ["status", "positions", "events"],
    BOT_PAUSED: ["status", "events"],
    BOT_RESUMED: ["status", "events"],
    BOT_STOPPED: ["status", "events"],
    DAILY_LOSS_PAUSE: ["status", "events"],
    ENTRY_BLOCKED: ["status", "events"],
};

/** Parse one frame; null for anything that is not a well-formed bot event. */
export function parseBotEvent(type: string, data: string): BotEventFrame | null {
    if (!(BOT_EVENT_TYPES as readonly string[]).includes(type)) return null;
    try {
        const frame = JSON.parse(data);
        if (!frame || typeof frame !== "object" || typeof frame.event_id !== "string") return null;
        return { ...frame, event_type: type as BotEventType, payload: frame.payload ?? {} };
    } catch {
        return null;
    }
}

/** The react-query keys an event makes stale: the bot's own reads, plus the bot list when its status moves. */
export function queriesToRefresh(frame: BotEventFrame): string[][] {
    const keys: string[][] = [];
    if (frame.bot_id) for (const part of REFRESH[frame.event_type]) keys.push(["cati", frame.bot_id, part]);
    if (["BOT_PAUSED", "BOT_RESUMED", "BOT_STOPPED", "DAILY_LOSS_PAUSE"].includes(frame.event_type)) keys.push(["botInstances"]);
    return keys;
}
