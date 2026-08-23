import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
    cleanupExpiredWindows,
    getActiveBlackouts,
    getEventFeedStatus,
    getUpcomingEvents,
    seedEvent,
    type SeedEventRequest,
} from "@/api/eventCalendarApi";

export function useUpcomingEventsQuery(days: number = 7, impact?: string) {
    return useQuery({
        queryKey: ["admin", "events", "upcoming", days, impact],
        queryFn: () => getUpcomingEvents(days, impact),
        staleTime: 60_000,
        refetchInterval: 120_000,
    });
}

export function useActiveBlackoutsQuery() {
    return useQuery({
        queryKey: ["admin", "events", "active-blackouts"],
        queryFn: getActiveBlackouts,
        staleTime: 15_000,
        refetchInterval: 30_000,
    });
}

export function useEventFeedStatusQuery() {
    return useQuery({
        queryKey: ["admin", "events", "feed-status"],
        queryFn: getEventFeedStatus,
        staleTime: 30_000,
        refetchInterval: 60_000,
    });
}

export function useSeedEventMutation() {
    const qc = useQueryClient();
    return useMutation({
        mutationFn: (data: SeedEventRequest) => seedEvent(data),
        onSuccess: () => {
            qc.invalidateQueries({ queryKey: ["admin", "events"] });
        },
    });
}

export function useCleanupExpiredMutation() {
    const qc = useQueryClient();
    return useMutation({
        mutationFn: cleanupExpiredWindows,
        onSuccess: () => {
            qc.invalidateQueries({ queryKey: ["admin", "events"] });
        },
    });
}
