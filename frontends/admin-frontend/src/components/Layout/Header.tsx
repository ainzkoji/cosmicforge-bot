import { Bell, Settings, User, LogOut } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "@/auth/AuthContext";

export function Header() {
    const [online] = useState(true);
    const { userEmail, userName, logout, isAuthenticated } = useAuth();

    // Use actual name if set, otherwise extract from email (part before @)
    const displayName = userName
        || (userEmail?.includes('@') ? userEmail.split('@')[0] : null)
        || 'User';

    return (
        <header className="border-b bg-background/80 backdrop-blur-md sticky top-0 z-50 shadow-sm border-border/40">
            <div className="container mx-auto px-4 h-16 flex items-center justify-between">

                {/* Logo & Brand */}
                <Link to="/dashboard" className="flex items-center gap-3">
                    <img
                        src="/src/assets/logo.png"
                        alt="CosmicForge"
                        className="w-10 h-10 object-contain"
                    />
                    <div className="flex flex-col">
                        <h1 className="text-lg font-bold tracking-tight leading-none bg-clip-text text-transparent bg-gradient-to-r from-foreground to-foreground/70">
                            CosmicForge
                        </h1>
                        <span className="text-[10px] font-medium tracking-widest text-primary uppercase ml-0.5">
                            Stratos
                        </span>
                    </div>
                </Link>

                {/* Primary Navigation (Center) - Only if logged in */}
                {isAuthenticated && (
                    <nav className="hidden md:flex items-center gap-1 bg-muted/30 p-1 rounded-full border border-border/50">
                        <Link
                            to="/dashboard"
                            className="px-4 py-1.5 rounded-full text-sm font-medium bg-background text-foreground shadow-sm ring-1 ring-border/50 transition-all"
                        >
                            Dashboard
                        </Link>
                        <Link
                            to="/dashboard/profile"
                            className="px-4 py-1.5 rounded-full text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                        >
                            Profile
                        </Link>
                        <Link
                            to="/dashboard/brokers"
                            className="px-4 py-1.5 rounded-full text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                        >
                            Brokers
                        </Link>
                        <Link
                            to="/dashboard/analytics"
                            className="px-4 py-1.5 rounded-full text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                        >
                            Analytics
                        </Link>
                        <Link
                            to="/dashboard/social"
                            className="px-4 py-1.5 rounded-full text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                        >
                            Social
                        </Link>
                        <Link
                            to="/dashboard/academy"
                            className="px-4 py-1.5 rounded-full text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                        >
                            Academy
                        </Link>
                        <Link
                            to="/dashboard/security"
                            className="px-4 py-1.5 rounded-full text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                        >
                            Security
                        </Link>
                        <Link
                            to="/dashboard/subscription"
                            className="px-4 py-1.5 rounded-full text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                        >
                            Billing
                        </Link>
                        <Link
                            to="/dashboard/onboarding"
                            className="px-4 py-1.5 rounded-full text-sm font-medium bg-primary/10 text-primary hover:bg-primary/20 transition-all border border-primary/20"
                        >
                            Setup
                        </Link>
                    </nav>
                )}

                {/* Status & Settings (Right) */}
                <div className="flex items-center gap-4">

                    {/* Online Status */}
                    <div className="hidden sm:flex items-center gap-3 pr-4 border-r border-border/50">
                        <div className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-bold tracking-wider border ${online
                            ? 'bg-green-500/10 text-green-500 border-green-500/20'
                            : 'bg-red-500/10 text-red-500 border-red-500/20'
                            }`}>
                            <span className={`w-1.5 h-1.5 rounded-full ${online ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
                            {online ? 'ONLINE' : 'OFFLINE'}
                        </div>
                    </div>

                    {/* Actions */}
                    {isAuthenticated && (
                        <div className="flex items-center gap-2">
                            <button className="p-2 rounded-full text-muted-foreground hover:text-foreground hover:bg-muted transition-colors relative">
                                <Bell className="w-5 h-5" />
                                <span className="absolute top-1.5 right-1.5 w-2 h-2 bg-primary rounded-full ring-2 ring-background" />
                            </button>
                            <Link to="/dashboard/security" className="p-2 rounded-full text-muted-foreground hover:text-foreground hover:bg-muted transition-colors">
                                <Settings className="w-5 h-5" />
                            </Link>

                            <div className="flex items-center gap-2 ml-2 pl-2 border-l border-border/50">
                                <div className="text-right hidden sm:block">
                                    <p className="text-sm font-medium leading-none capitalize">{displayName}</p>
                                </div>
                                <Link to="/dashboard/profile" className="p-1 rounded-full bg-muted/40 border border-border/50 hover:bg-muted/60 transition-colors">
                                    <div className="w-7 h-7 bg-primary/20 rounded-full flex items-center justify-center">
                                        <User className="w-4 h-4 text-primary" />
                                    </div>
                                </Link>
                                <button
                                    onClick={logout}
                                    className="p-2 rounded-full text-muted-foreground hover:text-red-500 hover:bg-red-500/10 transition-colors ml-1"
                                    title="Log Out"
                                >
                                    <LogOut className="w-4 h-4" />
                                </button>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </header>
    );
}
