/**
 * React Hook for Firebase Push Notifications
 * Handles FCM initialization, token registration, and message handling
 */

import { useState, useEffect } from 'react';
import type { MessagePayload } from 'firebase/messaging';

// Types
interface NotificationPayload {
    title: string;
    body: string;
    data?: Record<string, string>;
}

interface UseFirebaseNotificationsOptions {
    userId?: string;
    onMessage?: (payload: NotificationPayload) => void;
    autoRegister?: boolean;
}

interface UseFirebaseNotificationsReturn {
    isSupported: boolean;
    permission: NotificationPermission;
    token: string | null;
    isRegistering: boolean;
    error: Error | null;
    requestPermission: () => Promise<void>;
    registerToken: () => Promise<boolean>;
}

/**
 * Custom hook for managing Firebase push notifications
 * 
 * Usage:
 * ```tsx
 * const { permission, requestPermission, registerToken } = useFirebaseNotifications({
 *   userId: user?.id,
 *   onMessage: (payload) => {
 *     showToast(payload.title, payload.body);
 *   },
 *   autoRegister: true
 * });
 * ```
 */
export function useFirebaseNotifications(
    options: UseFirebaseNotificationsOptions = {}
): UseFirebaseNotificationsReturn {
    const { userId, onMessage, autoRegister = false } = options;

    const [isSupported, setIsSupported] = useState(false);
    const [permission, setPermission] = useState<NotificationPermission>('default');
    const [token, setToken] = useState<string | null>(null);
    const [isRegistering, setIsRegistering] = useState(false);
    const [error, setError] = useState<Error | null>(null);

    // Check if notifications are supported
    useEffect(() => {
        const supported = 'Notification' in window && 'serviceWorker' in navigator;
        setIsSupported(supported);

        if (supported) {
            setPermission(Notification.permission);
        }
    }, []);

    // Request notification permission
    const requestPermission = async () => {
        if (!isSupported) {
            setError(new Error('Notifications not supported'));
            return;
        }

        try {
            const result = await Notification.requestPermission();
            setPermission(result);

            if (result === 'granted') {
                await registerToken();
            }
        } catch (err) {
            setError(err as Error);
            console.error('Error requesting permission:', err);
        }
    };

    // Register FCM token with backend
    const registerToken = async (): Promise<boolean> => {
        if (!isSupported || permission !== 'granted' || !userId) {
            return false;
        }

        setIsRegistering(true);
        setError(null);

        try {
            // Dynamically import Firebase (only if installed)
            const { initializeFirebaseMessaging, registerFCMToken } = await import(
                '../services/firebaseMessaging'
            );

            // Get FCM token
            const fcmToken = await initializeFirebaseMessaging();

            if (!fcmToken) {
                throw new Error('Failed to get FCM token');
            }

            setToken(fcmToken);

            // Register with backend
            const success = await registerFCMToken(userId, fcmToken);

            if (!success) {
                throw new Error('Failed to register token with backend');
            }

            console.log('✅ Push notifications registered successfully');
            return true;

        } catch (err) {
            setError(err as Error);
            console.error('Error registering FCM token:', err);
            return false;
        } finally {
            setIsRegistering(false);
        }
    };

    // Auto-register on mount if enabled
    useEffect(() => {
        if (autoRegister && userId && permission === 'granted' && !token) {
            registerToken();
        }
    }, [autoRegister, userId, permission, token]);

    // Setup foreground message listener
    useEffect(() => {
        if (!isSupported || !onMessage) return;

        let unsubscribe: (() => void) | undefined;

        const setupMessageListener = async () => {
            try {
                const firebase = await import('firebase/app');
                const messaging = await import('firebase/messaging');

                const { firebaseConfig } = await import('../config/firebase');

                // Initialize Firebase if not already done
                if (!firebase.getApps().length) {
                    firebase.initializeApp(firebaseConfig);
                }

                const messagingInstance = messaging.getMessaging();

                // Listen for foreground messages
                unsubscribe = messaging.onMessage(messagingInstance, (payload: MessagePayload) => {
                    console.log('📬 Foreground message received:', payload);

                    if (payload.notification) {
                        onMessage({
                            title: payload.notification.title || 'Notification',
                            body: payload.notification.body || '',
                            data: payload.data as Record<string, string>
                        });
                    }
                });

            } catch (err) {
                console.error('Error setting up message listener:', err);
            }
        };

        setupMessageListener();

        return () => {
            if (unsubscribe) {
                unsubscribe();
            }
        };
    }, [isSupported, onMessage]);

    return {
        isSupported,
        permission,
        token,
        isRegistering,
        error,
        requestPermission,
        registerToken
    };
}

export default useFirebaseNotifications;
