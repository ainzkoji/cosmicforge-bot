/**
 * Analytics Real-Time Updates Hook
 * 
 * Custom React hook for receiving real-time analytics updates via SSE.
 */
import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

/**
 * Hook to establish SSE connection for real-time analytics updates.
 * 
 * Automatically invalidates analytics queries when trade events occur,
 * triggering immediate UI updates without manual refresh.
 * 
 * Events listened to:
 * - POSITION_OPENED: New trade started
 * - POSITION_CLOSED: Trade closed (includes P&L)
 * - TP1_HIT: Take profit 1 hit
 * - ADD_FILLED: Add to position filled
 */
export function useAnalyticsStream() {
    const queryClient = useQueryClient();

    useEffect(() => {
        // Get auth token from localStorage
        const token = localStorage.getItem('access_token');

        if (!token) {
            console.warn('[Analytics Stream] No auth token, skipping SSE connection');
            return;
        }

        // Note: EventSource doesn't support custom headers in standard browsers
        // We'll use URL parameter for auth (less secure but works)
        const eventSource = new EventSource(
            `${API_BASE_URL}/api/v1/events/stream?token=${encodeURIComponent(token)}`
        );

        // Connection established
        eventSource.addEventListener('connected', (e) => {
            const data = JSON.parse(e.data);
            console.log('[Analytics Stream] Connected:', data);
        });

        // Trade closed - invalidate analytics
        eventSource.addEventListener('POSITION_CLOSED', (e) => {
            const event = JSON.parse(e.data);
            console.log('[Analytics Stream] Position closed:', event.payload);

            // Invalidate analytics cache to trigger refetch
            queryClient.invalidateQueries({ queryKey: ['analytics-overview'] });
        });

        // Trade opened
        eventSource.addEventListener('POSITION_OPENED', (e) => {
            const event = JSON.parse(e.data);
            console.log('[Analytics Stream] Position opened:', event.payload);

            queryClient.invalidateQueries({ queryKey: ['analytics-overview'] });
        });

        // TP1 hit
        eventSource.addEventListener('TP1_HIT', (e) => {
            const event = JSON.parse(e.data);
            console.log('[Analytics Stream] TP1 hit:', event.payload);

            queryClient.invalidateQueries({ queryKey: ['analytics-overview'] });
        });

        // Add filled
        eventSource.addEventListener('ADD_FILLED', (e) => {
            const event = JSON.parse(e.data);
            console.log('[Analytics Stream] Add filled:', event.payload);

            queryClient.invalidateQueries({ queryKey: ['analytics-overview'] });
        });

        // Connection error
        eventSource.onerror = (error) => {
            console.error('[Analytics Stream] Error:', error);
            eventSource.close();
            // EventSource auto-reconnects, but we close to prevent spam
        };

        // Cleanup on unmount
        return () => {
            console.log('[Analytics Stream] Disconnecting');
            eventSource.close();
        };
    }, [queryClient]);
}
