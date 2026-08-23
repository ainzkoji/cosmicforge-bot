import { useState } from "react";
import { Link, useLocation, Outlet } from "react-router-dom";
import {
    LayoutDashboard, LineChart, Users, GraduationCap,
    CreditCard, Settings, Bell, Search, Menu, X,
    LogOut, User, ChevronRight, Zap, Shield, Wallet,
    Layers, BookOpen, LifeBuoy, Bot
} from "lucide-react";
import { useAuth } from "@/auth/AuthContext";
import { motion, AnimatePresence } from "framer-motion";

export function DashboardLayout() {
    const [isSidebarOpen, setSidebarOpen] = useState(true);
    const [showNotifications, setShowNotifications] = useState(false);
    const location = useLocation();
    const { logout, userEmail, userName } = useAuth();

    // Navigation Items
    const navItems = [
        { name: "Dashboard", path: "/dashboard", icon: LayoutDashboard },
        { name: "My Bots", path: "/dashboard/bots", icon: Bot },
        { name: "Strategies", path: "/dashboard/strategies", icon: Zap },
        { name: "My Brokers", path: "/dashboard/brokers", icon: Wallet },
        { name: "Analytics", path: "/dashboard/analytics", icon: LineChart },
        { name: "Social Trading", path: "/dashboard/social", icon: Users },
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
                    <img src="/src/assets/logo.png" alt="Logo" className="w-8 h-8 object-contain" />
                    <div className={`ml-3 font-bold text-white tracking-wide transition-opacity duration-300 ${!isSidebarOpen && "lg:opacity-0 lg:w-0 overflow-hidden"
                        }`}>
                        CosmicForge
                    </div>
                </div>

                {/* Main Navigation */}
                <div className="flex-1 overflow-y-auto py-6 px-3 space-y-1 custom-scrollbar">
                    {/* Section Label */}
                    <div className={`text-[10px] font-bold text-gray-600 uppercase tracking-widest px-3 mb-2 transition-opacity ${!isSidebarOpen && "lg:opacity-0"
                        }`}>
                        Main Menu
                    </div>

                    {navItems.map((item) => {
                        const isActive = location.pathname === item.path;
                        return (
                            <Link
                                key={item.path}
                                to={item.path}
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
                        <button
                            onClick={() => setShowNotifications(!showNotifications)}
                            className="p-2 rounded-full text-gray-400 hover:text-white hover:bg-white/5 relative"
                        >
                            <Bell className="w-5 h-5" />
                            <span className="absolute top-2 right-2 w-2 h-2 bg-red-500 rounded-full border-2 border-[#0F1218]" />
                        </button>

                        <AnimatePresence>
                            {showNotifications && (
                                <motion.div
                                    initial={{ opacity: 0, y: 10, scale: 0.95 }}
                                    animate={{ opacity: 1, y: 0, scale: 1 }}
                                    exit={{ opacity: 0, y: 10, scale: 0.95 }}
                                    className="absolute top-full right-0 mt-2 w-80 bg-[#0B0E14] border border-white/10 rounded-xl shadow-2xl z-50 overflow-hidden"
                                >
                                    <div className="p-4 border-b border-white/5 flex justify-between items-center bg-[#0F1218]">
                                        <h3 className="font-bold text-white">Notifications</h3>
                                        <button className="text-xs text-primary hover:text-primary/80">Mark all read</button>
                                    </div>
                                    <div className="max-h-[400px] overflow-y-auto custom-scrollbar">
                                        {[
                                            { id: 1, title: "Trade Executed", desc: "Bought 0.5 BTC at $42,000", time: "2m ago", type: "success" },
                                            { id: 2, title: "Stop Loss Hit", desc: "Sold ETH at $2,100 (-2.4%)", time: "1h ago", type: "warning" },
                                            { id: 3, title: "New Feature", desc: "Strategy Builder v2 is now live!", time: "5h ago", type: "info" },
                                            { id: 4, title: "Market Alert", desc: "High volatility detected on SOL/USDT", time: "1d ago", type: "error" },
                                        ].map((n) => (
                                            <div key={n.id} className="p-4 border-b border-white/5 hover:bg-white/5 transition-colors cursor-pointer group">
                                                <div className="flex justify-between items-start mb-1">
                                                    <span className={`font-bold text-sm ${n.type === 'success' ? 'text-green-500' :
                                                        n.type === 'warning' ? 'text-amber-500' :
                                                            n.type === 'error' ? 'text-red-500' : 'text-blue-500'
                                                        }`}>{n.title}</span>
                                                    <span className="text-[10px] text-gray-500">{n.time}</span>
                                                </div>
                                                <p className="text-xs text-gray-400 group-hover:text-gray-300">{n.desc}</p>
                                            </div>
                                        ))}
                                    </div>
                                    <div className="p-2 text-center border-t border-white/5 bg-[#0F1218]">
                                        <Link to="/dashboard/settings" onClick={() => setShowNotifications(false)} className="text-xs text-gray-500 hover:text-white transition-colors">View All Notifications</Link>
                                    </div>
                                </motion.div>
                            )}
                        </AnimatePresence>
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
