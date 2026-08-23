// Firebase Messaging Service Worker
// Place this file in: public/firebase-messaging-sw.js

importScripts('https://www.gstatic.com/firebasejs/9.17.1/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/9.17.1/firebase-messaging-compat.js');

// ⚠️ REPLACE WITH YOUR ACTUAL FIREBASE CONFIG
// Get from: Firebase Console > Project Settings > General > Your apps > Web app
firebase.initializeApp({
    apiKey: "YOUR_API_KEY",
    authDomain: "YOUR_PROJECT_ID.firebaseapp.com",
    projectId: "YOUR_PROJECT_ID",
    storageBucket: "YOUR_PROJECT_ID.appspot.com",
    messagingSenderId: "YOUR_SENDER_ID",
    appId: "YOUR_APP_ID",
    measurementId: "YOUR_MEASUREMENT_ID"
});

const messaging = firebase.messaging();

// Handle background messages
messaging.onBackgroundMessage((payload) => {
    console.log('[firebase-messaging-sw.js] Received background message:', payload);

    const notificationTitle = payload.notification?.title || 'CosmicForge Notification';
    const notificationOptions = {
        body: payload.notification?.body || 'You have a new notification',
        icon: '/logo192.png',
        badge: '/badge.png',
        tag: payload.data?.event_type || 'default',
        data: payload.data,
        requireInteraction: false,
        vibrate: [200, 100, 200]
    };

    return self.registration.showNotification(notificationTitle, notificationOptions);
});

// Handle notification clicks
self.addEventListener('notificationclick', (event) => {
    console.log('[firebase-messaging-sw.js] Notification clicked:', event.notification);

    event.notification.close();

    // Determine URL based on notification data
    const data = event.notification.data || {};
    let targetUrl = '/dashboard';

    if (data.event_type === 'trade_executed') {
        targetUrl = '/portfolio';
    } else if (data.event_type === 'signal_generated') {
        targetUrl = `/trading?symbol=${data.symbol}`;
    } else if (data.event_type === 'risk_alert') {
        targetUrl = '/settings';
    }

    // Open or focus the app
    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true })
            .then((clientList) => {
                // Check if app is already open
                for (const client of clientList) {
                    if (client.url.includes(self.registration.scope) && 'focus' in client) {
                        client.focus();
                        client.postMessage({
                            type: 'notification_click',
                            url: targetUrl,
                            data: data
                        });
                        return;
                    }
                }

                // Open new window if not already open
                if (clients.openWindow) {
                    return clients.openWindow(targetUrl);
                }
            })
    );
});

console.log('[firebase-messaging-sw.js] Service worker loaded');
