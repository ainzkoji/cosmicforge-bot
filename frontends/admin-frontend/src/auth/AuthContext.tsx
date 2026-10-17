import { createContext, useContext, useState, useEffect, ReactNode } from "react";
import { api, AuthResponse, LoginRequest, RegisterRequest } from "@/api/client";
import { AUTH_UNAUTHORIZED_EVENT, forgetLocalSession, hasSessionHint, markSignedIn } from "@/api/http";
import { classifyAdminLogin } from "@/lib/adminMfa";
import type { AdminLoginStep } from "@/lib/adminMfa";

/*
 * Section H 8.4: the admin session is held in httpOnly cookies the console
 * cannot read; "signed in" means GET /admin-auth/me succeeds. Sign-in can take
 * a second step (TOTP) or an enrolment first -- see lib/adminMfa.ts and
 * pages/Login.tsx. No token is kept here or in localStorage.
 */

interface AuthState {
    isAuthenticated: boolean;
    userEmail: string | null;
    userName: string | null;  // User's actual name (if set)
    isAdmin: boolean;  // Admin role flag
    isLoading: boolean;  // Track if auth check is in progress
}

interface AuthContextType extends AuthState {
    /** Password step. Resolves with the next step; "session" means signed in. */
    login: (data: LoginRequest) => Promise<AdminLoginStep>;
    /** Finish a sign-in with the session answer of /mfa/verify or /mfa/enroll. */
    completeSignIn: (res: AuthResponse, email?: string) => void;
    register: (data: RegisterRequest) => Promise<void>;
    logout: () => void;
    refreshUser: () => Promise<void>;
}

const SIGNED_OUT: AuthState = {
    isAuthenticated: false,
    userEmail: null,
    userName: null,
    isAdmin: false,
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

export function AuthProvider({ children }: { children: ReactNode }) {
    // Loading only when there may be a session to check (see the effect below).
    const [state, setState] = useState<AuthState>(() => ({
        isAuthenticated: false,
        userEmail: null,
        userName: null,
        isAdmin: false,
        isLoading: hasSessionHint(),
    }));

    // Fetch user profile from API
    const fetchProfile = async () => {
        try {
            const profile = await api.getMe() as any;
            // Admin /me returns full_name; user /me returns name — handle both.
            const displayName: string | null = profile.full_name || profile.name || null;
            cache("admin_email", profile.email);
            cache("admin_name", displayName);
            setState(prev => ({
                ...prev,
                userEmail: profile.email,
                userName: displayName,
            }));
        } catch (e) {
            console.error("Failed to fetch profile:", e);
        }
    };

    // On mount: is there a cookie session?
    useEffect(() => {
        let cancelled = false;
        if (hasSessionHint()) {
            (api.getMe() as Promise<any>)
                .then((profile) => {
                    if (cancelled) return;
                    const displayName: string | null = profile.full_name || profile.name || null;
                    cache("admin_email", profile.email);
                    cache("admin_name", displayName);
                    setState({
                        isAuthenticated: true,
                        userEmail: profile.email,
                        userName: displayName,
                        isAdmin: true, // admin-auth /me answers for admin sessions only
                        isLoading: false,
                    });
                })
                .catch(() => {
                    if (!cancelled) setState(SIGNED_OUT);
                });
        }

        // The HTTP layer (api/http.ts) clears the local session and dispatches this
        // event when the refresh is refused; the protected routes then redirect to /login.
        const handleUnauthorized = () => {
            setState(SIGNED_OUT);
        };
        window.addEventListener(AUTH_UNAUTHORIZED_EVENT, handleUnauthorized);
        return () => {
            cancelled = true;
            window.removeEventListener(AUTH_UNAUTHORIZED_EVENT, handleUnauthorized);
        };
    }, []);

    const completeSignIn = (res: AuthResponse, email?: string) => {
        // The backend set the session cookies; keep only the CSRF token (in memory).
        markSignedIn(res.csrf_token);
        cache("admin_email", email);
        setState({
            isAuthenticated: true,
            userEmail: email || null,
            userName: null,
            isAdmin: true, // Login to admin auth inherently provides admin rights
            isLoading: false,
        });
        fetchProfile();
    };

    const login = async (data: LoginRequest): Promise<AdminLoginStep> => {
        const res = await api.login(data);
        const step = classifyAdminLogin(res);
        if (!step) throw new Error("Unexpected sign-in answer");
        if (step.kind === "session") completeSignIn(res, data.username);
        return step;
    };

    const register = async (data: RegisterRequest) => {
        await api.register(data);
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
        <AuthContext.Provider value={{ ...state, login, completeSignIn, register, logout, refreshUser: refreshProfile }}>
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
