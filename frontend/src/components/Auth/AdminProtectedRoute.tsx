import { Navigate, Outlet } from "react-router-dom";
import { useAuth } from "@/auth/AuthContext";
import { Loader2 } from "lucide-react";

export function AdminProtectedRoute() {
    const { isAuthenticated, token, isLoading, isAdmin } = useAuth();

    // Wait for auth check to complete before deciding
    if (isLoading) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-gray-50">
                <Loader2 className="w-8 h-8 animate-spin text-[#1E1B4B]" />
            </div>
        );
    }

    // Check authentication first
    if (!isAuthenticated && !token) {
        return <Navigate to="/login" replace />;
    }

    // Check admin role
    if (!isAdmin) {
        return <Navigate to="/dashboard" replace />;
    }

    return <Outlet />;
}
