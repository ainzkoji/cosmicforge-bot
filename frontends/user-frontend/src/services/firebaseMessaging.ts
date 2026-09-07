/**
 * Firebase Cloud Messaging Service
 * Handles FCM token registration and push notifications
 */

import { apiClient } from '../api/client';
import type { MessagePayload } from 'firebase/messaging';

// Firebase SDK installed


interface FCMTokenRegistration {
    userId: string;
    fcmToken: string;
    deviceId?: string;
    deviceName?: string;
}

interface Device {
    deviceId: string;
    deviceName: string;
    token: string;
    status: string;
    registeredAt: string;
}

/**
 * Initialize Firebase and request notification permission
 * 
 * IMPORTANT: This is a placeholder implementation.
 * You need to:
 * 1. Install firebase: npm install firebase
 * 2. Add firebase-messaging-sw.js to public folder
 * 3. Update firebaseConfig with your project values
 */
export async function initializeFirebaseMessaging(): Promise<string | null> {
    const { initializeApp } = await import('firebase/app');
    const { getMessaging, getToken } = await import('firebase/messaging');
    const { firebaseConfig, vapidPublicKey } = await import('../config/firebase');

    try {
        // Request notification permission
        const permission = await Notification.requestPermission();

        if (permission !== 'granted') {
            console.log('Notification permission denied');
            return null;
        }

        // Initialize Firebase
        const app = initializeApp(firebaseConfig);
        const messaging = getMessaging(app);

        // Get FCM token
        const token = await getToken(messaging, {
            vapidKey: vapidPublicKey
        });

        console.log('FCM Token obtained:', token.substring(0, 20) + '...');
        return token;

    } catch (error) {
        console.error('Error initializing Firebase messaging:', error);
        return null;
    }
}

/**
 * Register FCM token with backend
 */
export async function registerFCMToken(
    userId: string,
    fcmToken: string,
    deviceName?: string
): Promise<boolean> {
    try {
        const payload: FCMTokenRegistration = {
            userId,
            fcmToken,
            deviceName: deviceName || getBrowserDeviceName()
        };

        const response = await apiClient.post('/api/notifications/token', payload);

        console.log('✅ FCM token registered:', response.data);
        return true;

    } catch (error) {
        console.error('❌ Failed to register FCM token:', error);
        return false;
    }
}

/**
 * Get all registered devices for current user
 */
export async function getUserDevices(): Promise<Device[]> {
    try {
        const response = await apiClient.get('/api/notifications/tokens');
        return response.data.devices || [];
    } catch (error) {
        console.error('Failed to get user devices:', error);
        return [];
    }
}

/**
 * Remove a device token
 */
export async function removeDevice(deviceId: string): Promise<boolean> {
    try {
        await apiClient.delete(`/api/notifications/token/${deviceId}`);
        return true;
    } catch (error) {
        console.error('Failed to remove device:', error);
        return false;
    }
}

/**
 * Get browser/device name for identification
 */
function getBrowserDeviceName(): string {
    const ua = navigator.userAgent;
    let browser = 'Unknown Browser';
    let os = 'Unknown OS';

    // Detect browser
    if (ua.includes('Firefox')) browser = 'Firefox';
    else if (ua.includes('Chrome')) browser = 'Chrome';
    else if (ua.includes('Safari') && !ua.includes('Chrome')) browser = 'Safari';
    else if (ua.includes('Edge')) browser = 'Edge';

    // Detect OS
    if (ua.includes('Windows')) os = 'Windows';
    else if (ua.includes('Mac')) os = 'macOS';
    else if (ua.includes('Linux')) os = 'Linux';
    else if (ua.includes('Android')) os = 'Android';
    else if (ua.includes('iOS')) os = 'iOS';

    return `${browser} on ${os}`;
}

/**
 * Check if push notifications are supported
 */
export function isPushNotificationSupported(): boolean {
    return 'Notification' in window && 'serviceWorker' in navigator;
}

/**
 * Get notification permission status
 */
/**
 * Listen for foreground messages
 */
export async function onForegroundMessage(callback: (payload: MessagePayload) => void) {
    try {
        const { getMessaging, onMessage } = await import('firebase/messaging');
        const { initializeApp } = await import('firebase/app');
        const { firebaseConfig } = await import('../config/firebase');

        // Initialize Firebase (idempotent)
        const app = initializeApp(firebaseConfig);
        const messaging = getMessaging(app);

        return onMessage(messaging, (payload: MessagePayload) => {
            console.log('Received foreground message:', payload);
            callback(payload);
        });
    } catch (error) {
        console.error('Error setting up foreground message listener:', error);
        return null; // Return null instead of function if failed
    }
}


export function getNotificationPermission(): NotificationPermission {
    if (!('Notification' in window)) {
        return 'denied';
    }
    return Notification.permission;
}

export default {
    initializeFirebaseMessaging,
    registerFCMToken,
    getUserDevices,
    removeDevice,
    isPushNotificationSupported,
    getNotificationPermission,
    onForegroundMessage
};
