/// <reference types="vite/client" />

interface ImportMetaEnv {
    readonly VITE_API_BASE: string;
    readonly VITE_ADMIN_API_BASE: string;
    readonly VITE_USE_ADMIN_BACKEND_DASHBOARD: string;
    readonly VITE_USE_ADMIN_BACKEND_REVENUE: string;
    readonly VITE_USE_ADMIN_BACKEND_TRADINGVIEW: string;
    readonly VITE_USE_ADMIN_BACKEND_BOT_MONITOR: string;
    readonly VITE_USE_ADMIN_BACKEND_SIGNALS: string;
    readonly VITE_USE_ADMIN_BACKEND_EVENTS: string;
    readonly VITE_USE_ADMIN_BACKEND_NEWS: string;
    readonly VITE_USE_ADMIN_BACKEND_PROFITABILITY: string;
    readonly VITE_USE_ADMIN_BACKEND_ML: string;
}

interface ImportMeta {
    readonly env: ImportMetaEnv;
}
