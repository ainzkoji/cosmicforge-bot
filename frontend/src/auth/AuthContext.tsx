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
            const profile = await api.getMe();
            localStorage.setItem("user_email", profile.email);
            if (profile.name) {
                localStorage.setItem("user_name", profile.name);
            }
            setState(prev => ({
                ...prev,
                userEmail: profile.email,
                userName: profile.name || null,
            }));
        } catch (e) {
            console.error("Failed to fetch profile:", e);
        }
    };

    // Check if user has admin role
    const checkAdminRole = async (tokenToCheck?: string) => {
        try {
            // Try to fetch admin dashboard stats - if it succeeds, user is admin
            const accessToken = tokenToCheck || localStorage.getItem("access_token");
            if (!accessToken) return false;

            // Use relative URL or configured API_BASE
            const res = await fetch("http://localhost:8000/api/admin/dashboard/stats", {
                headers: { Authorization: `Bearer ${accessToken}` }
            });
            return res.ok;  // If 200, user is admin; if 403, not admin
        } catch (e) {
            console.error("Admin check failed:", e);
            return false;
        }
    };

    // Check localStorage on mount
    useEffect(() => {
        const token = localStorage.getItem("access_token");
        if (token) {
            try {
                const decoded: any = jwtDecode(token);
                // Basic expiration check
                if (decoded.exp * 1000 < Date.now()) {
                    // Token expired, clear it
                    localStorage.removeItem("access_token");
                    localStorage.removeItem("refresh_token");
                    localStorage.removeItem("user_email");
                    localStorage.removeItem("user_name");
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
                    const storedEmail = localStorage.getItem("user_email");
                    const storedName = localStorage.getItem("user_name");
                    // Fetch fresh profile data and check admin role
                    checkAdminRole(token).then(isAdmin => {
                        setState({
                            isAuthenticated: true,
                            token,
                            userEmail: storedEmail || null,
                            userName: storedName || null,
                            isAdmin,  // Set determined admin status
                            isLoading: false,
                        });
                        fetchProfile();
                    });
                }
            } catch (e) {
                // Invalid token... (existing code)
                localStorage.removeItem("access_token");
                localStorage.removeItem("refresh_token");
                localStorage.removeItem("user_email");
                localStorage.removeItem("user_name");
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
        localStorage.setItem("access_token", res.access_token);
        localStorage.setItem("refresh_token", res.refresh_token);
        localStorage.setItem("user_email", data.username);

        // Check admin role immediately
        const isAdmin = await checkAdminRole(res.access_token);

        // Update state
        setState({
            isAuthenticated: true,
            token: res.access_token,
            userEmail: data.username,
            userName: null,
            isAdmin: isAdmin,
            isLoading: false,
        });

        // Fetch full profile in background
        fetchProfile();

        return isAdmin; // Return for component redirect logic
    };



    const register = async (data: RegisterRequest) => {
        await api.register(data);
        // Auto-login after register? Or require explicit login. 
        // For MVP, require explicit login
    };

    const logout = () => {
        localStorage.removeItem("access_token");
        localStorage.removeItem("refresh_token");
        localStorage.removeItem("user_email");
        localStorage.removeItem("user_name");
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
