

import { useState } from "react";
import { Link, useLocation, Outlet } from "react-router-dom";
import {
    LayoutDashboard, LineChart, Users, GraduationCap,
    CreditCard, Settings, Bell, Search, Menu, X,
    LogOut, User, ChevronRight, Zap, Shield, Wallet,
    Layers, BookOpen, LifeBuoy, Bot, ShoppingBag, PenTool, List, History, Activity
} from "lucide-react";
import { useAuth } from "@/auth/AuthContext";
import { motion, AnimatePresence } from "framer-motion";
import { NotificationBell } from "../Dashboard/NotificationBell";

export function DashboardLayout() {
    const [isSidebarOpen, setSidebarOpen] = useState(true);
    // Track open submenus. Key = parent path or name
    const [openSubmenus, setOpenSubmenus] = useState<Record<string, boolean>>({
        "Strategies": true
    });

    const location = useLocation();
    const { logout, userEmail, userName } = useAuth();

    // Toggle submenu
    const toggleSubmenu = (name: string) => {
        setOpenSubmenus(prev => ({ ...prev, [name]: !prev[name] }));
    };

    interface NavItem {
        name: string;
        path: string;
        icon: any;
        children?: { name: string; path: string; icon?: any }[];
    }

    // Navigation Items
    const navItems: NavItem[] = [
        { name: "Dashboard", path: "/dashboard", icon: LayoutDashboard },
        { name: "Bot Instances", path: "/dashboard/bots", icon: Bot },
        { name: "Auto Pilot", path: "/dashboard/auto-pilot", icon: Zap },
        { name: "Signals", path: "/dashboard/signals", icon: Activity },
        { name: "Simulation", path: "/dashboard/backtests", icon: History },
        { name: "My Brokers", path: "/dashboard/brokers", icon: Wallet },
        { name: "Portfolio", path: "/dashboard/portfolio", icon: Layers },
        { name: "Analytics", path: "/dashboard/analytics", icon: LineChart },

        { name: "Academy", path: "/dashboard/academy", icon: GraduationCap },
        { name: "Subscription", path: "/dashboard/subscription", icon: CreditCard },
    ];

    const bottomNavItems = [
        { name: "Security", path: "/dashboard/security", icon: Shield },
        { name: "Profile", path: "/dashboard/profile", icon: User },
    ];

    const displayName = userName || (userEmail?.split('@')[0] || 'Trader');

    return (
        <div className="min-h-screen bg-[#0F1218] text-gray-300 flex overflow-hidden font-sans selection:bg-primary/30">
            {/* --- Sidebar --- */}
            <aside
                className={`fixed inset-y-0 left-0 z-50 w-64 bg-[#0B0E14] border-r border-white/5 flex flex-col transition-all duration-300 ${isSidebarOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0 lg:w-20"
                    } lg:static`}
            >
                {/* Logo Area */}
                <div className="h-16 flex items-center px-6 border-b border-white/5">
                    <Link to="/dashboard" className="flex items-center gap-3">
                        <div className="w-8 h-8 bg-gradient-to-br from-primary to-purple-600 rounded-lg flex items-center justify-center text-white font-bold text-xl shadow-lg shadow-primary/20">
                            C
                        </div>
                        <div className={`font-bold text-white tracking-wide transition-opacity duration-300 ${!isSidebarOpen && "lg:opacity-0 lg:w-0 overflow-hidden"
                            }`}>
                            CosmicForge
                        </div>
                    </Link>
                </div>

                {/* Main Navigation */}
                <div className="flex-1 overflow-y-auto py-6 px-3 space-y-1 custom-scrollbar">
                    {/* Section Label */}
                    <div className={`text-[10px] font-bold text-gray-600 uppercase tracking-widest px-3 mb-2 transition-opacity ${!isSidebarOpen && "lg:opacity-0"
                        }`}>
                        Main Menu
                    </div>

                    {navItems.map((item) => {
                        // Check if active or child is active
                        const isChildActive = item.children?.some(child => location.pathname === child.path || (child.path.includes('?') && location.pathname === child.path.split('?')[0]));
                        const isActive = item.path === location.pathname || isChildActive;
                        const isOpen = openSubmenus[item.name];

                        // Render Parent with Children
                        if (item.children) {
                            return (
                                <div key={item.name} className="space-y-1">
                                    <button
                                        onClick={() => {
                                            if (!isSidebarOpen) setSidebarOpen(true);
                                            toggleSubmenu(item.name);
                                        }}
                                        className={`w-full flex items-center justify-between px-3 py-2.5 rounded-lg text-sm font-medium transition-all group ${isActive && !isOpen ? "bg-primary/10 text-primary" : "text-gray-400 hover:text-white hover:bg-white/5"
                                            }`}
                                        title={!isSidebarOpen ? item.name : ""}
                                    >
                                        <div className="flex items-center gap-3">
                                            <item.icon className={`w-5 h-5 flex-shrink-0 ${isActive ? "text-primary" : "text-gray-500 group-hover:text-gray-300"}`} />
                                            <span className={`whitespace-nowrap transition-opacity duration-300 ${!isSidebarOpen && "lg:opacity-0 lg:w-0 overflow-hidden"}`}>
                                                {item.name}
                                            </span>
                                        </div>
                                        {isSidebarOpen && (
                                            <ChevronRight className={`w-4 h-4 transition-transform ${isOpen ? "rotate-90" : ""}`} />
                                        )}
                                    </button>

                                    {/* Submenu */}
                                    <AnimatePresence>
                                        {isOpen && isSidebarOpen && (
                                            <motion.div
                                                initial={{ height: 0, opacity: 0 }}
                                                animate={{ height: "auto", opacity: 1 }}
                                                exit={{ height: 0, opacity: 0 }}
                                                className="overflow-hidden ml-4 pl-4 border-l border-white/10 space-y-1"
                                            >
                                                {item.children.map(child => {
                                                    const isChildActive = location.pathname === child.path || (child.path.includes('?') && location.pathname === child.path.split('?')[0] && location.search === (child.path.split('?')[1] ? `?${child.path.split('?')[1]}` : ''));
                                                    // Simple active check
                                                    const isChildReallyActive = location.pathname === child.path.split('?')[0];

                                                    return (
                                                        <Link
                                                            key={child.name}
                                                            to={child.path}
                                                            className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-all ${isChildReallyActive && location.search === (child.path.split('?')[1] ? `?${child.path.split('?')[1]}` : '')
                                                                ? "text-primary bg-primary/5"
                                                                : "text-gray-500 hover:text-white"
                                                                }`}
                                                        >
                                                            {child.icon && <child.icon className="w-4 h-4" />}
                                                            <span>{child.name}</span>
                                                        </Link>
                                                    )
                                                })}
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>
                            );
                        }

                        // Render Simple Item
                        return (
                            <Link
                                key={item.path}
                                to={item.path!}
                                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all group ${isActive
                                    ? "bg-primary/10 text-primary"
                                    : "text-gray-400 hover:text-white hover:bg-white/5"
                                    }`}
                                title={!isSidebarOpen ? item.name : ""}
                            >
                                <item.icon className={`w-5 h-5 flex-shrink-0 ${isActive ? "text-primary" : "text-gray-500 group-hover:text-gray-300"
                                    }`} />
                                <span className={`whitespace-nowrap transition-opacity duration-300 ${!isSidebarOpen && "lg:opacity-0 lg:w-0 overflow-hidden"
                                    }`}>
                                    {item.name}
                                </span>
                                {isActive && isSidebarOpen && (
                                    <motion.div layoutId="activeNav" className="ml-auto w-1 h-1 rounded-full bg-primary" />
                                )}
                            </Link>
                        );
                    })}

                    <div className="my-6 border-t border-white/5" />

                    {/* Bottom Section */}
                    <div className={`text-[10px] font-bold text-gray-600 uppercase tracking-widest px-3 mb-2 transition-opacity ${!isSidebarOpen && "lg:opacity-0"
                        }`}>
                        Settings
                    </div>
                    {bottomNavItems.map((item) => {
                        const isActive = location.pathname === item.path;
                        return (
                            <Link
                                key={item.path}
                                to={item.path}
                                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all group ${isActive
                                    ? "bg-primary/10 text-primary"
                                    : "text-gray-400 hover:text-white hover:bg-white/5"
                                    }`}
                            >
                                <item.icon className="w-5 h-5 flex-shrink-0 text-gray-500 group-hover:text-gray-300" />
                                <span className={`whitespace-nowrap transition-opacity duration-300 ${!isSidebarOpen && "lg:opacity-0 lg:w-0 overflow-hidden"
                                    }`}>
                                    {item.name}
                                </span>
                            </Link>
                        );
                    })}
                </div>

                {/* User Info / Logout */}
                <div className="p-4 border-t border-white/5 bg-[#080a0f]">
                    <div className={`flex items-center gap-3 ${!isSidebarOpen ? "justify-center" : ""}`}>
                        <div className="w-9 h-9 rounded-full bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white font-bold text-sm shadow-lg shadow-indigo-500/20">
                            {displayName.charAt(0).toUpperCase()}
                        </div>
                        <div className={`flex-1 min-w-0 transition-opacity duration-300 ${!isSidebarOpen && "lg:hidden"
                            }`}>
                            <div className="text-sm font-medium text-white truncate">{displayName}</div>
                            <div className="text-xs text-green-500 flex items-center gap-1">
                                <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse" /> Online
                            </div>
                        </div>
                        <button
                            onClick={logout}
                            className={`p-2 rounded-lg text-gray-500 hover:text-red-500 hover:bg-red-500/10 transition-colors ${!isSidebarOpen && "hidden"
                                }`}
                            title="Logout"
                        >
                            <LogOut className="w-4 h-4" />
                        </button>
                    </div>
                </div>
            </aside>

            {/* --- Main Content Area --- */}
            <div className="flex-1 flex flex-col min-w-0">
                {/* Header for Mobile / Collapsing */}
                <header className="h-16 border-b border-white/5 bg-[#0F1218]/80 backdrop-blur-md sticky top-0 z-40 flex items-center justify-between px-6">
                    <div className="flex items-center gap-4">
                        <button
                            onClick={() => setSidebarOpen(!isSidebarOpen)}
                            className="p-2 -ml-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
                        >
                            <Menu className="w-5 h-5" />
                        </button>

                        {/* Search Bar */}
                        <div className="hidden md:flex items-center bg-[#0B0E14] border border-white/5 rounded-full px-4 py-1.5 w-64 focus-within:border-primary/50 transition-colors">
                            <Search className="w-4 h-4 text-gray-500 mr-2" />
                            <input
                                type="text"
                                placeholder="Search markets..."
                                className="bg-transparent border-none outline-none text-sm text-white placeholder-gray-600 w-full"
                            />
                        </div>
                    </div>

                    <div className="flex items-center gap-4 relative">
                        <NotificationBell />
                    </div>
                </header>

                {/* Page Content */}
                <main className="flex-1 overflow-y-auto p-6 md:p-8 scroll-smooth">
                    <Outlet />
                </main>
            </div>

            {/* Mobile Overlay */}
            {isSidebarOpen && (
                <div
                    className="fixed inset-0 bg-black/50 backdrop-blur-sm z-40 lg:hidden"
                    onClick={() => setSidebarOpen(false)}
                />
            )}
        </div>
    );
}
