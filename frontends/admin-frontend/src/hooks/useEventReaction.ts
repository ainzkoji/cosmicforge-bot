import { useQuery } from "@tanstack/react-query";
import {
    getRecentReactions,
    getReactionsForEvent,
    getSingleReaction,
    getEventSnapshots,
} from "@/api/eventReactionApi";

export function useRecentReactionsQuery(days: number = 7, limit: number = 50) {
    return useQuery({
        queryKey: ["admin", "reactions", "recent", days, limit],
        queryFn: () => getRecentReactions(days, limit),
        staleTime: 60_000,
        refetchInterval: 120_000,
    });
}

export function useReactionsForEventQuery(eventId: string | null) {
    return useQuery({
        queryKey: ["admin", "reactions", "event", eventId],
        queryFn: () => getReactionsForEvent(eventId!),
        enabled: Boolean(eventId),
        staleTime: 60_000,
    });
}

export function useSingleReactionQuery(eventId: string | null, symbol: string | null) {
    return useQuery({
        queryKey: ["admin", "reactions", "single", eventId, symbol],
        queryFn: () => getSingleReaction(eventId!, symbol!),
        enabled: Boolean(eventId) && Boolean(symbol),
        staleTime: 60_000,
    });
}

export function useEventSnapshotsQuery(eventId: string | null, symbol: string | null) {
    return useQuery({
        queryKey: ["admin", "snapshots", eventId, symbol],
        queryFn: () => getEventSnapshots(eventId!, symbol!),
        enabled: Boolean(eventId) && Boolean(symbol),
        staleTime: 30_000,
    });
}
