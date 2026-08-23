import { createContext, useContext, useState, useEffect, ReactNode } from "react";
import { api, LoginRequest, RegisterRequest } from "@/api/client";
import { jwtDecode } from "jwt-decode";

interface AuthState {
    isAuthenticated: boolean;
    token: string | null;
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

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
    const [state, setState] = useState<AuthState>({
        isAuthenticated: false,
        token: null,
        userEmail: null,
        userName: null,
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
                        isLoading: false,
                    });
                } else {
                    // Get cached data from localStorage
                    const storedEmail = localStorage.getItem("user_email");
                    const storedName = localStorage.getItem("user_name");

                    setState({
                        isAuthenticated: true,
                        token,
                        userEmail: storedEmail || null,
                        userName: storedName || null,
                        isLoading: false,
                    });
                    fetchProfile();
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
                    isLoading: false,
                });
            }
        } else {
            // No token found
            setState(prev => ({ ...prev, isLoading: false }));
        }

        // Listen for 401 events from client.ts
        const handleUnauthorized = () => {
            logout();
        };
        window.addEventListener("auth:unauthorized", handleUnauthorized);

        return () => {
            window.removeEventListener("auth:unauthorized", handleUnauthorized);
        };
    }, []);

    const login = async (data: LoginRequest) => {
        const res = await api.login(data);
        localStorage.setItem("access_token", res.access_token);
        localStorage.setItem("refresh_token", res.refresh_token);
        localStorage.setItem("user_email", data.username);

        // Check admin role immediately
        // const isAdmin = await checkAdminRole(res.access_token); // Removed

        // Update state
        setState({
            isAuthenticated: true,
            token: res.access_token,
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
