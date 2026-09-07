import { createContext, useContext, useState, useEffect, ReactNode } from "react";
import { api, LoginRequest, RegisterRequest } from "@/api/client";
import { jwtDecode } from "jwt-decode";

interface AuthState {
    isAuthenticated: boolean;
    token: string | null;
    userEmail: string | null;
    userName: string | null;  // User's actual name (if set)
    isAdmin: boolean;  // Admin role flag
    isLoading: boolean;  // Track if auth check is in progress
}

interface AuthContextType extends AuthState {
    login: (data: LoginRequest) => Promise<boolean>;
    register: (data: RegisterRequest) => Promise<void>;
    logout: () => void;
    refreshUser: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
    const [state, setState] = useState<AuthState>({
        isAuthenticated: false,
        token: null,
        userEmail: null,
        userName: null,
        isAdmin: false,
        isLoading: true,  // Start as loading until we check localStorage
    });

    // Fetch user profile from API
    const fetchProfile = async () => {
        try {
            const profile = await api.getMe() as any;
            // Admin /me returns full_name; user /me returns name — handle both.
            const displayName: string | null = profile.full_name || profile.name || null;
            localStorage.setItem("admin_email", profile.email);
            if (displayName) {
                localStorage.setItem("admin_name", displayName);
            }
            setState(prev => ({
                ...prev,
                userEmail: profile.email,
                userName: displayName,
            }));
        } catch (e) {
            console.error("Failed to fetch profile:", e);
        }
    };

    // Check if user has admin role - Admin token natively proves admin role!
    const checkAdminRole = async (tokenToCheck?: string) => {
        const accessToken = tokenToCheck || localStorage.getItem("admin_access_token");
        return !!accessToken; // In the new model, having a valid admin token inherently means they are an admin
    };

    // Check localStorage on mount
    useEffect(() => {
        const token = localStorage.getItem("admin_access_token");
        if (token) {
            try {
                const decoded: any = jwtDecode(token);
                // Basic expiration check
                if (decoded.exp * 1000 < Date.now()) {
                    // Token expired, clear it
                    localStorage.removeItem("admin_access_token");
                    localStorage.removeItem("admin_refresh_token");
                    localStorage.removeItem("admin_email");
                    localStorage.removeItem("admin_name");
                    setState({
                        isAuthenticated: false,
                        token: null,
                        userEmail: null,
                        userName: null,
                        isAdmin: false,
                        isLoading: false,
                    });
                } else {
                    // Get cached data from localStorage
                    const storedEmail = localStorage.getItem("admin_email");
                    const storedName = localStorage.getItem("admin_name");
                    // Admin token inherently proves admin
                    setState({
                        isAuthenticated: true,
                        token,
                        userEmail: storedEmail || null,
                        userName: storedName || null,
                        isAdmin: true,
                        isLoading: false,
                    });
                    fetchProfile();
                }
            } catch (e) {
                // Invalid token...
                localStorage.removeItem("admin_access_token");
                localStorage.removeItem("admin_refresh_token");
                localStorage.removeItem("admin_email");
                localStorage.removeItem("admin_name");
                setState({
                    isAuthenticated: false,
                    token: null,
                    userEmail: null,
                    userName: null,
                    isAdmin: false,
                    isLoading: false,
                });
            }
        } else {
            // No token found
            setState(prev => ({ ...prev, isLoading: false }));
        }
    }, []);

    const login = async (data: LoginRequest) => {
        const res = await api.login(data);
        localStorage.setItem("admin_access_token", res.access_token);
        localStorage.setItem("admin_refresh_token", res.refresh_token);
        localStorage.setItem("admin_email", data.username);

        // Update state
        setState({
            isAuthenticated: true,
            token: res.access_token,
            userEmail: data.username,
            userName: null,
            isAdmin: true, // Login to admin auth inherently provides admin rights
            isLoading: false,
        });

        // Fetch full profile in background
        fetchProfile();

        return true; // Return for component redirect logic
    };



    const register = async (data: RegisterRequest) => {
        await api.register(data);
        // Auto-login after register? Or require explicit login. 
        // For MVP, require explicit login
    };

    const logout = () => {
        localStorage.removeItem("admin_access_token");
        localStorage.removeItem("admin_refresh_token");
        localStorage.removeItem("admin_email");
        localStorage.removeItem("admin_name");
        setState({
            isAuthenticated: false,
            token: null,
            userEmail: null,
            userName: null,
            isAdmin: false,
            isLoading: false,
        });
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
