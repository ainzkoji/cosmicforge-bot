import { Link, useLocation } from "react-router-dom";
import {
    LayoutDashboard,
    Users,
    TrendingUp,
    Link2,
    Cpu,
    FileText,
    Shield,
    Settings,
    LogOut,
    Activity,
    CalendarClock,
    Newspaper,
    Radio,
    BarChart3,
    Webhook,
    RadioTower,
    Network,
} from "lucide-react";

type NavItem = {
    path: string;
    icon: React.ElementType;
    label: string;
    exact?: boolean;
    sub?: boolean;
};

const navItems: NavItem[] = [
    { path: "/admin", icon: LayoutDashboard, label: "Dashboard", exact: true },
    { path: "/admin/users", icon: Users, label: "User Management" },
    { path: "/admin/revenue", icon: TrendingUp, label: "Revenue Analytics" },
    { path: "/admin/profitability", icon: BarChart3, label: "Profitability" },
    { path: "/admin/affiliate-revenue", icon: Link2, label: "Affiliate & Broker Revenue" },
    { path: "/admin/ml", icon: Cpu, label: "ML Monitoring" },
    { path: "/admin/audit", icon: FileText, label: "Audit Logs" },
    { path: "/admin/bot-monitor", icon: Activity, label: "Bot Monitor" },
    { path: "/admin/events", icon: CalendarClock, label: "Event Calendar", exact: true },
    { path: "/admin/events/reactions", icon: Radio, label: "Event Reactions", sub: true },
    { path: "/admin/news-intelligence", icon: Newspaper, label: "News Intelligence", exact: true },
    { path: "/admin/news-intelligence/realtime", icon: Radio, label: "Real-Time News", sub: true },
    { path: "/admin/news-intelligence/validation", icon: Activity, label: "News Market Validation", sub: true },
    { path: "/admin/tradingview", icon: Webhook, label: "TradingView" },
    { path: "/admin/signals", icon: RadioTower, label: "Admin Signals", exact: true },
    { path: "/admin/signals/pairs", icon: Network, label: "Signal Pairs", sub: true },
    { path: "/admin/compliance", icon: Shield, label: "Compliance" },
];

export function AdminSidebar() {
    const location = useLocation();

    const isActive = (path: string, exact?: boolean) => {
        if (exact) {
            return location.pathname === path;
        }
        return location.pathname.startsWith(path);
    };

    return (
        <div className="admin-sidebar">
            {/* Logo — fixed at top, never scrolls */}
            <div className="p-6 border-b" style={{ borderColor: "var(--admin-border-color)", flexShrink: 0 }}>
                <div className="flex items-center gap-3">
                    <img src="/src/assets/logo.png" alt="CosmicForge" className="h-8 w-8" />
                    <div>
                        <div className="text-sm font-bold" style={{ color: "var(--admin-text-primary)" }}>
                            CosmicForge
                        </div>
                        <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>
                            Admin Portal
                        </div>
                    </div>
                </div>
            </div>

            {/* Navigation — scrollable, grows to fill remaining space */}
            <nav className="admin-sidebar-nav">
                {navItems.map((item) => {
                    const Icon = item.icon;
                    const active = isActive(item.path, item.exact);
                    const classes = [
                        "admin-nav-item",
                        item.sub ? "sub-item" : "",
                        active ? "active" : "",
                    ]
                        .filter(Boolean)
                        .join(" ");

                    return (
                        <Link key={item.path} to={item.path} className={classes}>
                            <Icon className="w-4 h-4" />
                            <span>{item.label}</span>
                        </Link>
                    );
                })}
            </nav>

            {/* Footer — naturally sits at the bottom, never overlays nav */}
            <div className="admin-sidebar-footer">
                <Link to="/admin/settings" className="admin-nav-item">
                    <Settings className="w-4 h-4" />
                    <span>Settings</span>
                </Link>
                <button
                    className="admin-nav-item"
                    onClick={() => {/* Handle logout */}}
                >
                    <LogOut className="w-4 h-4" />
                    <span>Logout</span>
                </button>
            </div>
        </div>
    );
}
