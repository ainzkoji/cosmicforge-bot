import { createContext, useContext, useState, useEffect, ReactNode } from "react";
import { api, LoginRequest, RegisterRequest } from "@/api/client";
import { AUTH_UNAUTHORIZED_EVENT, forgetLocalSession, hasSessionHint, markSignedIn } from "@/api/http";

/*
 * Section H 8.4: the session is held in httpOnly cookies the page cannot read.
 * "Signed in" therefore means "GET /me succeeds" (apiFetch refreshes an expired
 * session through the refresh cookie on the way). No token is kept here or in
 * localStorage; only the email/name of the profile are cached for display.
 */

interface AuthState {
    isAuthenticated: boolean;
    userEmail: string | null;
    userName: string | null;  // User's actual name (if set)
    isLoading: boolean;  // Track if auth check is in progress
}

interface AuthContextType extends AuthState {
    login: (data: LoginRequest) => Promise<boolean>;
    register: (data: RegisterRequest) => Promise<void>;
    logout: () => void;
    refreshUser: () => Promise<void>;
}

const SIGNED_OUT: AuthState = {
    isAuthenticated: false,
    userEmail: null,
    userName: null,
    isLoading: false,
};

const AuthContext = createContext<AuthContextType | undefined>(undefined);

function cache(key: string, value: string | null | undefined): void {
    try {
        if (value) localStorage.setItem(key, value);
    } catch {
        // Storage unavailable: display cache only.
    }
}

function cached(key: string): string | null {
    try {
        return localStorage.getItem(key);
    } catch {
        return null;
    }
}

export function AuthProvider({ children }: { children: ReactNode }) {
    // Loading only when there may be a session to check (see the effect below).
    const [state, setState] = useState<AuthState>(() => ({
        isAuthenticated: false,
        userEmail: null,
        userName: null,
        isLoading: hasSessionHint(),
    }));

    // Fetch user profile from API
    const fetchProfile = async () => {
        try {
            const profile = await api.getMe();
            cache("user_email", profile.email);
            cache("user_name", profile.name);
            setState(prev => ({
                ...prev,
                userEmail: profile.email,
                userName: profile.name || null,
            }));
        } catch (e) {
            console.error("Failed to fetch profile:", e);
        }
    };

    // On mount: is there a cookie session?
    useEffect(() => {
        let cancelled = false;
        // Never signed in on this browser (or signed out): nothing to check.
        if (hasSessionHint()) {
            api.getMe()
                .then((profile) => {
                    if (cancelled) return;
                    cache("user_email", profile.email);
                    cache("user_name", profile.name);
                    setState({
                        isAuthenticated: true,
                        userEmail: profile.email,
                        userName: profile.name || cached("user_name"),
                        isLoading: false,
                    });
                })
                .catch(() => {
                    // Session over (the HTTP layer already cleared it on a refused
                    // refresh) or backend unreachable: not signed in for now.
                    if (!cancelled) setState(SIGNED_OUT);
                });
        }

        // The HTTP layer clears the local session and dispatches this event when
        // the refresh is refused; the protected routes then redirect to /login.
        const handleUnauthorized = () => {
            setState(SIGNED_OUT);
        };
        window.addEventListener(AUTH_UNAUTHORIZED_EVENT, handleUnauthorized);

        return () => {
            cancelled = true;
            window.removeEventListener(AUTH_UNAUTHORIZED_EVENT, handleUnauthorized);
        };
    }, []);

    const login = async (data: LoginRequest) => {
        const res = await api.login(data);
        // The backend set the session cookies; keep only the CSRF token (in memory).
        markSignedIn(res.csrf_token);
        cache("user_email", data.username);

        setState({
            isAuthenticated: true,
            userEmail: data.username,
            userName: null,
            isLoading: false,
        });

        // Fetch full profile in background
        fetchProfile();

        return false; // Return false (not admin)
    };

    const register = async (data: RegisterRequest) => {
        await api.register(data);
        // Explicit login after registration (email verification comes first).
    };

    const logout = () => {
        // Revoke the server session and clear its cookies, then forget it locally.
        void api.logout().finally(() => forgetLocalSession());
        setState(SIGNED_OUT);
    };

    const refreshProfile = async () => {
        await fetchProfile();
    };

    return (
        <AuthContext.Provider value={{ ...state, login, register, logout, refreshUser: refreshProfile }}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth() {
    const context = useContext(AuthContext);
    if (!context) {
        throw new Error("useAuth must be used within an AuthProvider");
    }
    return context;
}
