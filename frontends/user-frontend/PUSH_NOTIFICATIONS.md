# Push Notifications - Frontend Setup Guide

## ⚠️ Current Status

The **backend is fully implemented**, but the **frontend requires Firebase SDK installation** and configuration.

---

## What's Already Done ✅

1. **Service Layer Created:**
   - `src/services/firebaseMessaging.ts` - FCM token management
   - `src/config/firebase.ts` - Firebase configuration template

2. **UI Updates:**
   - `src/pages/NotificationSettings.tsx` - Ready to integrate push notifications
   - Includes device management UI
   - Push channel added to notification preferences

3. **Backend Integration:**
   - API endpoints ready (`/api/notifications/token`, `/test`, `/tokens`)
   - Multi-device support
   - Automatic token cleanup

---

## Setup Steps

### Step 1: Install Firebase SDK

```bash
cd frontends/user-frontend
npm install firebase
```

### Step 2: Get Firebase Configuration

1. Go to [Firebase Console](https://console.firebase.google.com/)
2. Select your project (or create one)
3. Go to **Project Settings** > **General**
4. Scroll to **Your apps** > **Web app**
5. Copy the Firebase configuration object

### Step 3: Update Firebase Config

Edit `src/config/firebase.ts` and replace with your actual values:

```typescript
export const firebaseConfig = {
  apiKey: "AIzaSy...",  // Your actual API key
  authDomain: "your-project.firebaseapp.com",
  projectId: "your-project-id",
  storageBucket: "your-project.appspot.com",
  messagingSenderId: "123456789",
  appId: "1:123456789:web:...",
  measurementId: "G-XXXXXXX"
};

export const vapidPublicKey = "YOUR_ACTUAL_VAPID_KEY";
```

### Step 4: Create Service Worker

Create `public/firebase-messaging-sw.js`:

```javascript
importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-messaging-compat.js');

// Initialize Firebase in service worker
firebase.initializeApp({
  apiKey: "YOUR_API_KEY",
  authDomain: "YOUR_PROJECT.firebaseapp.com",
  projectId: "YOUR_PROJECT_ID",
  storageBucket: "YOUR_PROJECT.appspot.com",
  messagingSenderId: "YOUR_SENDER_ID",
  appId: "YOUR_APP_ID"
});

const messaging = firebase.messaging();

// Handle background messages
messaging.onBackgroundMessage((payload) => {
  console.log('Received background message:', payload);
  
  const notificationTitle = payload.notification.title;
  const notificationOptions = {
    body: payload.notification.body,
    icon: '/logo.png',
    badge: '/badge.png',
    data: payload.data
  };

  self.registration.showNotification(notificationTitle, notificationOptions);
});
```

### Step 5: Update Service Initialization

Edit `src/services/firebaseMessaging.ts` and **uncomment** the Firebase SDK code:

```typescript
// Uncomment the import and initialization code marked with /* */
// This enables actual Firebase functionality
```

### Step 6: Add Push Notification UI

The NotificationSettings page is already updated - just needs the backend API calls to work.

If you want to add a "Request Push Notifications" button elsewhere:

```typescript
import { initializeFirebaseMessaging, registerFCMToken } from '@/services/firebaseMessaging';

const handleEnablePush = async () => {
  const token = await initializeFirebaseMessaging();
  if (token && currentUser) {
    await registerFCMToken(currentUser.id, token);
    alert('Push notifications enabled!');
  }
};
```

---

## Testing

### Test with Browser DevTools

1. Open Chrome DevTools > Application > Service Workers
2. Check if `firebase-messaging-sw.js` is registered
3. Test notification permission in Console:
   ```javascript
   Notification.requestPermission().then(console.log)
   ```

### Test Token Registration

1. Enable push notifications in your app
2. Check Network tab for `POST /api/notifications/token`
3. Verify token is stored in backend

### Test Notifications

Use the backend test endpoint:

```bash
curl -X POST http://localhost:8000/api/notifications/test \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "userId": "your_user_id",
    "title": "Test Notification",
    "body": "This is a test from the backend"
  }'
```

---

## Integration Points

### On User Login

```typescript
// In your login success handler
const user = await api.login(credentials);
localStorage.setItem('access_token', user.access_token);

// Request push notification permission
if (isPushNotificationSupported()) {
  const token = await initializeFirebaseMessaging();
  if (token) {
    await registerFCMToken(user.id, token);
  }
}
```

### In App Component

```typescript
import { useEffect } from 'react';
import { initializeFirebaseMessaging } from '@/services/firebaseMessaging';
import { getMessaging, onMessage } from 'firebase/messaging';

function App() {
  useEffect(() => {
    // Listen for foreground messages
    const setupMessaging = async () => {
      const messaging = getMessaging();
      
      // Handle foreground messages
      onMessage(messaging, (payload) => {
        console.log('Foreground message:', payload);
        
        // Show in-app notification or toast
        showToast(payload.notification.title, payload.notification.body);
      });
    };
    
    setupMessaging();
  }, []);

  return <AppContent />;
}
```

---

## Firebase Console Configuration

### Enable Cloud Messaging

1. Go to Firebase Console > **Project Settings** > **Cloud Messaging**
2. Enable **Cloud Messaging API** (legacy)
3. Generate a **Web Push certificate** (VAPID key)
4. Copy the key pair to your config

### Server Key (Backend)

The backend uses the **Service Account JSON** (already configured in `.env`):
- `FIREBASE_SERVICE_ACCOUNT_PATH=trading-bot-9926d-firebase-adminsdk-fbsvc-9cf71de1c9.json`

---

## Troubleshooting

### "Firebase SDK not installed" Warning

- Run `npm install firebase` in frontend directory
- Uncomment the Firebase code in `firebaseMessaging.ts`

### Permission Denied

- User must grant notification permission in browser
- Check browser settings: chrome://settings/content/notifications

### Service Worker Not Registering

- Ensure `firebase-messaging-sw.js` is in `public/` folder
- Check Console for service worker errors
- HTTPS required (or localhost)

### Token Not Saving

- Check Network tab for `/api/notifications/token` request
- Verify backend is running
- Check for CORS issues

---

## Current Limitations

1. **Firebase SDK Not Installed:**
   - Frontend will show warnings until `npm install firebase`
   - Service methods will return null

2. **UI Not Fully Integrated:**
   - Notification Settings page has push channel
   - But enable/test buttons need to be added

3. **No Foreground Handler:**
   - Messages shown only in background
   - Need to add `onMessage` handler for foreground

---

## Next Steps (Post-Installation)

1. ✅ Install firebase package
2. ✅ Configure firebase.ts with real values  
3. ✅ Create firebase-messaging-sw.js service worker
4. ✅ Uncomment Firebase code in firebaseMessaging.ts
5. ⏳ Add "Enable Push" button to NotificationSettings
6. ⏳ Add foreground message handler
7. ⏳ Test with real devices
8. ⏳ Add notification bell with unread count

---

## Summary

**Backend:** ✅ Fully implemented and working

**Frontend:** ⚠️ Infrastructure ready, **Firebase SDK installation required**

**Mobile:** ❌ Not implemented (React Native required)

To complete the frontend:
1. `npm install firebase`
2. Update `src/config/firebase.ts`
3. Create `public/firebase-messaging-sw.js`
4. Uncomment code in `firebaseMessaging.ts`
5. Test!
