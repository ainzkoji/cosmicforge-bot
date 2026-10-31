/**
 * Section K3 -- live bot events (ENTRY_FILLED, EXIT_FILLED, POSITION_UNPROTECTED, BOT_PAUSED, BOT_RESUMED,
 * BOT_STOPPED, DAILY_LOSS_PAUSE, ENTRY_BLOCKED) refresh the bot's reads as they happen.
 *
 * The stream is never closed on a transient error: EventSource reconnects by itself and sends the last
 * event id it saw, and the engine replays what was missed. It is closed when the session ends
 * (`auth_revoked`) or the component unmounts. The polling intervals of the reads stay as the fallback.
 */
import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { hasSessionHint } from "@/api/http";
import { BOT_EVENT_TYPES, parseBotEvent, queriesToRefresh } from "@/lib/botEvents";

const API_BASE_URL = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export function useBotEventStream(botId?: string) {
    const queryClient = useQueryClient();

    useEffect(() => {
        if (!hasSessionHint() || typeof EventSource === "undefined") return;
        const source = new EventSource(`${API_BASE_URL}/api/v1/events/stream`, { withCredentials: true });
        const onEvent = (e: MessageEvent) => {
            const frame = parseBotEvent(e.type, e.data);
            if (!frame || (botId && frame.bot_id !== botId)) return;
            for (const queryKey of queriesToRefresh(frame)) queryClient.invalidateQueries({ queryKey });
        };
        for (const type of BOT_EVENT_TYPES) source.addEventListener(type, onEvent as EventListener);
        source.addEventListener("auth_revoked", () => source.close());
        return () => source.close();
    }, [queryClient, botId]);
}
