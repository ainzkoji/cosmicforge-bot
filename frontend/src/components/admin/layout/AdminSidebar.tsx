import { Link, useLocation } from "react-router-dom";
import {
    LayoutDashboard,
    Users,
    CreditCard,
    TrendingUp,
    Percent,
    FileText,
    Shield,
    Settings,
    LogOut
} from "lucide-react";

const navItems = [
    { path: "/admin", icon: LayoutDashboard, label: "Dashboard", exact: true },
    { path: "/admin/users", icon: Users, label: "User Management" },
    { path: "/admin/subscriptions", icon: CreditCard, label: "Subscriptions" },
    { path: "/admin/revenue", icon: TrendingUp, label: "Revenue Analytics" },
    { path: "/admin/commissions", icon: Percent, label: "Commission Settings" },
    { path: "/admin/audit", icon: FileText, label: "Audit Logs" },
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
            {/* Logo */}
            <div className="p-6 border-b" style={{ borderColor: 'var(--admin-border-color)' }}>
                <div className="flex items-center gap-3">
                    <img src="/src/assets/logo.png" alt="CosmicForge" className="h-8 w-8" />
                    <div>
                        <div className="text-sm font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            CosmicForge
                        </div>
                        <div className="text-xs" style={{ color: 'var(--admin-text-muted)' }}>
                            Admin Portal
                        </div>
                    </div>
                </div>
            </div>

            {/* Navigation */}
            <nav className="py-4">
                {navItems.map((item) => {
                    const Icon = item.icon;
                    const active = isActive(item.path, item.exact);

                    return (
                        <Link
                            key={item.path}
                            to={item.path}
                            className={`admin-nav-item ${active ? 'active' : ''}`}
                        >
                            <Icon className="w-5 h-5" />
                            <span>{item.label}</span>
                        </Link>
                    );
                })}
            </nav>

            {/* Bottom Section */}
            <div className="absolute bottom-0 left-0 right-0 p-4 border-t" style={{ borderColor: 'var(--admin-border-color)' }}>
                <Link to="/admin/settings" className="admin-nav-item">
                    <Settings className="w-5 h-5" />
                    <span>Settings</span>
                </Link>
                <button className="admin-nav-item w-full" onClick={() => {/* Handle logout */ }}>
                    <LogOut className="w-5 h-5" />
                    <span>Logout</span>
                </button>
            </div>
        </div>
    );
}
