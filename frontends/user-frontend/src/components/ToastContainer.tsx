/**
 * Toast Notification Component
 * Displays in-app notifications for foreground push messages
 */

import React, { useState, useEffect } from 'react';
import { Bell, X } from 'lucide-react';

interface ToastNotification {
    id: string;
    title: string;
    body: string;
    timestamp: Date;
}

interface NotificationToastProps {
    notification: ToastNotification;
    onClose: (id: string) => void;
}

const NotificationToast: React.FC<NotificationToastProps> = ({ notification, onClose }) => {
    useEffect(() => {
        // Auto-dismiss after 5 seconds
        const timer = setTimeout(() => {
            onClose(notification.id);
        }, 5000);

        return () => clearTimeout(timer);
    }, [notification.id, onClose]);

    return (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-4 mb-3 max-w-sm w-full border-l-4 border-blue-500 animate-slide-in">
            <div className="flex items-start gap-3">
                <div className="flex-shrink-0">
                    <Bell className="w-5 h-5 text-blue-500" />
                </div>

                <div className="flex-1 min-w-0">
                    <h4 className="text-sm font-semibold text-gray-900 dark:text-white mb-1">
                        {notification.title}
                    </h4>
                    <p className="text-sm text-gray-600 dark:text-gray-300">
                        {notification.body}
                    </p>
                    <p className="text-xs text-gray-400 mt-1">
                        {notification.timestamp.toLocaleTimeString()}
                    </p>
                </div>

                <button
                    onClick={() => onClose(notification.id)}
                    className="flex-shrink-0 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
                >
                    <X className="w-4 h-4" />
                </button>
            </div>
        </div>
    );
};

/**
 * Toast Container - Manages multiple toast notifications
 * Place this in your root App component
 */
export const ToastContainer: React.FC = () => {
    const [notifications, setNotifications] = useState<ToastNotification[]>([]);

    // Expose global function to show toast
    useEffect(() => {
        (window as any).showPushNotification = (title: string, body: string) => {
            const notification: ToastNotification = {
                id: Date.now().toString(),
                title,
                body,
                timestamp: new Date()
            };

            setNotifications(prev => [notification, ...prev].slice(0, 3)); // Keep max 3
        };

        return () => {
            delete (window as any).showPushNotification;
        };
    }, []);

    const handleClose = (id: string) => {
        setNotifications(prev => prev.filter(n => n.id !== id));
    };

    if (notifications.length === 0) return null;

    return (
        <div className="fixed top-4 right-4 z-50 pointer-events-none">
            <div className="pointer-events-auto">
                {notifications.map(notification => (
                    <NotificationToast
                        key={notification.id}
                        notification={notification}
                        onClose={handleClose}
                    />
                ))}
            </div>
        </div>
    );
};

// Add animation to your global CSS or tailwind.config.js
// @keyframes slide-in {
//   from {
//     transform: translateX(100%);
//     opacity: 0;
//   }
//   to {
//     transform: translateX(0);
//     opacity: 1;
//   }
// }
// .animate-slide-in {
//   animation: slide-in 0.3s ease-out;
// }

export default ToastContainer;
