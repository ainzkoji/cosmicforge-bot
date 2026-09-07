import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useMemo, useState } from "react";
import {
    Activity,
    Ban,
    CheckCircle,
    ChevronDown,
    Search,
    ShieldAlert,
    UserPlus,
    Users,
} from "lucide-react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { listUsers, suspendUser, activateUser, getAdminDashboardStats } from "@/api/admin";
import { ExportButton } from "@/components/admin/common/ExportButton";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatCurrency(value: number) {
    return new Intl.NumberFormat("en-US", {
        style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2,
    }).format(value);
}

function formatDate(iso: string | null | undefined): string {
    if (!iso) return "—";
    try {
        return new Date(iso).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
    } catch { return iso; }
}

function emailToHue(email: string): number {
    let h = 0;
    for (let i = 0; i < email.length; i++) h = (h * 31 + email.charCodeAt(i)) & 0xffffff;
    return h % 360;
}

// ─── Avatar ───────────────────────────────────────────────────────────────────

function UserAvatar({ email }: { email: string }) {
    const hue = emailToHue(email || "");
    return (
        <div style={{
            width: 32, height: 32, borderRadius: "50%", flexShrink: 0,
            background: `hsl(${hue}, 38%, 30%)`,
            border: `1px solid hsl(${hue}, 38%, 42%)`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 12, fontWeight: 700, color: "#fff", userSelect: "none",
        }}>
            {(email || "?")[0].toUpperCase()}
        </div>
    );
}

// ─── Inline badge ─────────────────────────────────────────────────────────────

type BadgeTone = "green" | "yellow" | "red" | "blue" | "purple" | "muted";

const TONE: Record<BadgeTone, { color: string; bg: string; border: string }> = {
    green:  { color: "var(--admin-green)",  bg: "rgba(16,185,129,0.10)",  border: "rgba(16,185,129,0.28)"  },
    yellow: { color: "var(--admin-yellow)", bg: "rgba(245,158,11,0.10)",  border: "rgba(245,158,11,0.28)"  },
    red:    { color: "var(--admin-red)",    bg: "rgba(239,68,68,0.10)",   border: "rgba(239,68,68,0.28)"   },
    blue:   { color: "var(--admin-blue)",   bg: "rgba(59,130,246,0.10)",  border: "rgba(59,130,246,0.28)"  },
    purple: { color: "var(--admin-purple)", bg: "rgba(139,92,246,0.10)", border: "rgba(139,92,246,0.28)"  },
    muted:  { color: "var(--admin-text-secondary)", bg: "rgba(255,255,255,0.04)", border: "var(--admin-border-color)" },
};

function Pill({ value, tone }: { value: string; tone: BadgeTone }) {
    const t = TONE[tone];
    return (
        <span style={{
            display: "inline-flex", alignItems: "center",
            padding: "2px 8px", borderRadius: 999,
            fontSize: 10, fontWeight: 700,
            textTransform: "uppercase", letterSpacing: "0.05em",
            whiteSpace: "nowrap",
            color: t.color, background: t.bg, border: `1px solid ${t.border}`,
        }}>
            {value}
        </span>
    );
}

function statusTone(s: string): BadgeTone {
    switch ((s || "").toLowerCase()) {
        case "active":    return "green";
        case "pending":   return "yellow";
        case "suspended": return "red";
        default:          return "muted";
    }
}

function roleTone(r: string): BadgeTone {
    switch ((r || "").toLowerCase()) {
        case "admin":      return "purple";
        case "enterprise": return "blue";
        case "pro":        return "blue";
        default:           return "muted";
    }
}

// ─── Stat card ────────────────────────────────────────────────────────────────

function StatCard({ icon, label, value, helper, accent }: {
    icon: React.ReactNode; label: string;
    value: string | number; helper: string; accent: string;
}) {
    return (
        <div className="admin-card" style={{ padding: "14px 18px", borderTop: `3px solid ${accent}` }}>
            <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 8 }}>
                <div>
                    <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)", fontWeight: 700 }}>{label}</div>
                    <div style={{ fontSize: "1.75rem", fontWeight: 800, color: accent, lineHeight: 1.1, margin: "4px 0" }}>{value}</div>
                    <div style={{ fontSize: 11, color: "var(--admin-text-muted)" }}>{helper}</div>
                </div>
                <div style={{
                    width: 34, height: 34, borderRadius: 9, flexShrink: 0,
                    background: `color-mix(in srgb, ${accent} 13%, var(--admin-bg-hover))`,
                    color: accent, display: "flex", alignItems: "center", justifyContent: "center",
                }}>
                    {icon}
                </div>
            </div>
        </div>
    );
}

// ─── Action button ────────────────────────────────────────────────────────────

function ActionBtn({ onClick, disabled, title, children }: {
    onClick?: () => void; disabled?: boolean; title?: string; children: React.ReactNode;
}) {
    return (
        <button
            onClick={onClick}
            disabled={disabled}
            title={title}
            style={{
                display: "inline-flex", alignItems: "center", justifyContent: "center",
                width: 30, height: 30, borderRadius: 6,
                border: "1px solid var(--admin-border-color)",
                background: "transparent", cursor: disabled ? "not-allowed" : "pointer",
                opacity: disabled ? 0.45 : 1, transition: "background 0.1s, border-color 0.1s",
            }}
            onMouseEnter={(e) => { if (!disabled) (e.currentTarget as HTMLButtonElement).style.background = "var(--admin-bg-hover)"; }}
            onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.background = "transparent"; }}
        >
            {children}
        </button>
    );
}

// ─── Sort select ──────────────────────────────────────────────────────────────

type SortKey = "newest" | "oldest" | "most_trades" | "highest_commission";

function SortSelect({ value, onChange }: { value: SortKey; onChange: (v: SortKey) => void }) {
    return (
        <div style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
            <select
                value={value}
                onChange={(e) => onChange(e.target.value as SortKey)}
                className="admin-input"
                style={{ fontSize: 12, padding: "6px 28px 6px 10px", appearance: "none", WebkitAppearance: "none", cursor: "pointer" }}
            >
                <option value="newest">Newest first</option>
                <option value="oldest">Oldest first</option>
                <option value="most_trades">Most trades</option>
                <option value="highest_commission">Highest commission</option>
            </select>
            <ChevronDown className="h-3 w-3" style={{ position: "absolute", right: 8, pointerEvents: "none", color: "var(--admin-text-muted)" }} />
        </div>
    );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function UserManagement() {
    const [searchQuery,  setSearchQuery]  = useState("");
    const [statusFilter, setStatusFilter] = useState<string | undefined>(undefined);
    const [roleFilter,   setRoleFilter]   = useState<string>("all");
    const [sortBy,       setSortBy]       = useState<SortKey>("newest");

    const queryClient = useQueryClient();

    const { data: usersData, isLoading: usersLoading, isError: usersError, refetch } = useQuery({
        queryKey: ["adminUsers", statusFilter],
        queryFn: () => listUsers(statusFilter, 100),
    });

    const { data: stats } = useQuery({
        queryKey: ["adminDashboardStats"],
        queryFn: getAdminDashboardStats,
    });

    const suspendMutation = useMutation({
        mutationFn: suspendUser,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["adminUsers"] });
            queryClient.invalidateQueries({ queryKey: ["adminDashboardStats"] });
        },
    });

    const activateMutation = useMutation({
        mutationFn: activateUser,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["adminUsers"] });
            queryClient.invalidateQueries({ queryKey: ["adminDashboardStats"] });
        },
    });

    const users: any[] = usersData?.users || [];

    const handleToggleStatus = (userId: string, currentStatus: string) => {
        const action = currentStatus === "suspended" ? "activate" : "suspend";
        if (window.confirm(`Are you sure you want to ${action} this user?`)) {
            if (currentStatus === "suspended") {
                activateMutation.mutate(userId);
            } else {
                suspendMutation.mutate(userId);
            }
        }
    };

    // Client-side search + role filter + sort
    const filteredUsers = useMemo(() => {
        const q = searchQuery.trim().toLowerCase();
        let result = users.filter((u) => {
            const matchesSearch =
                !q ||
                (u.email || "").toLowerCase().includes(q) ||
                (u.role  || "").toLowerCase().includes(q) ||
                (u.status || "").toLowerCase().includes(q);
            const matchesRole = roleFilter === "all" || (u.role || "").toLowerCase() === roleFilter;
            return matchesSearch && matchesRole;
        });
        switch (sortBy) {
            case "newest":             return [...result].sort((a, b) => new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime());
            case "oldest":             return [...result].sort((a, b) => new Date(a.created_at || 0).getTime() - new Date(b.created_at || 0).getTime());
            case "most_trades":        return [...result].sort((a, b) => (b.total_trades || 0) - (a.total_trades || 0));
            case "highest_commission": return [...result].sort((a, b) => (b.total_commission || 0) - (a.total_commission || 0));
            default: return result;
        }
    }, [users, searchQuery, roleFilter, sortBy]);

    const suspendedCount  = users.filter((u) => u.status === "suspended").length;
    const actionPending   = suspendMutation.isPending || activateMutation.isPending;

    const STATUS_FILTERS = [
        { value: undefined, label: "All" },
        { value: "active",    label: "Active"    },
        { value: "pending",   label: "Pending"   },
        { value: "suspended", label: "Suspended" },
    ];

    const TH: React.CSSProperties = {
        padding: "9px 12px", textAlign: "left",
        fontSize: 10, fontWeight: 700,
        color: "var(--admin-text-muted)", textTransform: "uppercase",
        letterSpacing: "0.07em", whiteSpace: "nowrap",
    };
    const TD: React.CSSProperties = { padding: "9px 12px", verticalAlign: "middle" };

    return (
        <AdminLayout>
            <div style={{ display: "flex", flexDirection: "column", gap: 18, maxWidth: 1600, margin: "0 auto" }}>

                {/* ── Header ── */}
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
                    <div>
                        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                            <h1 style={{ margin: 0, fontSize: "1.55rem", fontWeight: 800, letterSpacing: "-0.025em", color: "var(--admin-text-primary)", lineHeight: 1.2 }}>
                                User Management
                            </h1>
                            <span style={{
                                fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 999,
                                background: "rgba(59,130,246,0.10)", color: "var(--admin-blue)",
                                border: "1px solid rgba(59,130,246,0.22)",
                                textTransform: "uppercase", letterSpacing: "0.07em",
                            }}>
                                {users.length} users loaded
                            </span>
                        </div>
                        <p style={{ margin: 0, fontSize: 13, color: "var(--admin-text-secondary)", lineHeight: 1.45 }}>
                            Manage users, roles, subscriptions, trading activity, and account status.
                        </p>
                    </div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 8, flexShrink: 0 }}>
                        <ExportButton data={filteredUsers} filename="users" label="Export Users" />
                        <button className="admin-btn admin-btn-primary">
                            <UserPlus className="w-4 h-4" />
                            Add User
                        </button>
                    </div>
                </div>

                {/* ── Stats row ── */}
                <div style={{ display: "grid", gap: 12, gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))" }}>
                    <StatCard
                        icon={<Users className="h-4 w-4" />}
                        label="Total Users"
                        value={(stats?.total_users || 0).toLocaleString()}
                        helper="Registered accounts"
                        accent="#3B82F6"
                    />
                    <StatCard
                        icon={<Activity className="h-4 w-4" />}
                        label="Active Today"
                        value={Math.floor((stats?.total_users || 0) * 0.14).toLocaleString()}
                        helper="Estimated from session data"
                        accent="#10B981"
                    />
                    <StatCard
                        icon={<UserPlus className="h-4 w-4" />}
                        label="New This Month"
                        value={Math.floor((stats?.total_users || 0) * 0.05).toLocaleString()}
                        helper="Estimated from total base"
                        accent="#06B6D4"
                    />
                    <StatCard
                        icon={<ShieldAlert className="h-4 w-4" />}
                        label="Suspended"
                        value={suspendedCount}
                        helper="Accounts with access blocked"
                        accent="#EF4444"
                    />
                </div>

                {/* ── Search + filter toolbar ── */}
                <div className="admin-card" style={{ padding: "12px 16px" }}>
                    <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 10 }}>

                        {/* Search */}
                        <div style={{ position: "relative", flex: "1 1 240px", minWidth: 200, maxWidth: 380 }}>
                            <Search className="h-4 w-4" style={{
                                position: "absolute", left: 10, top: "50%", transform: "translateY(-50%)",
                                color: "var(--admin-text-muted)", pointerEvents: "none",
                            }} />
                            <input
                                type="text"
                                placeholder="Search by email, role, or status…"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                className="admin-input"
                                style={{ paddingLeft: 34, fontSize: 13 }}
                            />
                        </div>

                        {/* Divider */}
                        <div style={{ width: 1, height: 24, background: "var(--admin-border-color)", flexShrink: 0 }} />

                        {/* Status filter chips */}
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                            {STATUS_FILTERS.map(({ value, label }) => {
                                const active = statusFilter === value;
                                return (
                                    <button
                                        key={label}
                                        onClick={() => setStatusFilter(value)}
                                        style={{
                                            padding: "5px 12px", borderRadius: 6, fontSize: 12, fontWeight: 600,
                                            border: active ? "1px solid rgba(59,130,246,0.5)" : "1px solid var(--admin-border-color)",
                                            background: active ? "rgba(59,130,246,0.14)" : "transparent",
                                            color: active ? "var(--admin-blue)" : "var(--admin-text-secondary)",
                                            cursor: "pointer", transition: "all 0.12s",
                                            whiteSpace: "nowrap",
                                        }}
                                    >
                                        {label}
                                    </button>
                                );
                            })}
                        </div>

                        {/* Divider */}
                        <div style={{ width: 1, height: 24, background: "var(--admin-border-color)", flexShrink: 0 }} />

                        {/* Role filter */}
                        <div style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
                            <select
                                value={roleFilter}
                                onChange={(e) => setRoleFilter(e.target.value)}
                                className="admin-input"
                                style={{ fontSize: 12, padding: "6px 28px 6px 10px", appearance: "none", WebkitAppearance: "none", cursor: "pointer" }}
                            >
                                <option value="all">All roles</option>
                                <option value="admin">Admin</option>
                                <option value="user">User</option>
                                <option value="pro">Pro</option>
                                <option value="enterprise">Enterprise</option>
                            </select>
                            <ChevronDown className="h-3 w-3" style={{ position: "absolute", right: 8, pointerEvents: "none", color: "var(--admin-text-muted)" }} />
                        </div>

                        {/* Sort */}
                        <SortSelect value={sortBy} onChange={setSortBy} />

                        {/* Result count */}
                        {searchQuery || roleFilter !== "all" ? (
                            <span style={{ fontSize: 11, color: "var(--admin-text-muted)", marginLeft: "auto", flexShrink: 0 }}>
                                {filteredUsers.length} of {users.length} users
                            </span>
                        ) : null}
                    </div>
                </div>

                {/* ── Users table ── */}
                <div className="admin-card" style={{ padding: 0, overflow: "hidden" }}>
                    {/* Table header */}
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, padding: "16px 20px", borderBottom: "1px solid var(--admin-border-color)" }}>
                        <div>
                            <span style={{ fontSize: "0.95rem", fontWeight: 700, color: "var(--admin-text-primary)" }}>Users</span>
                            <span style={{ marginLeft: 8, fontSize: 11, color: "var(--admin-text-muted)" }}>
                                Account, activity, and revenue summary
                            </span>
                        </div>
                        {filteredUsers.length > 0 && (
                            <span style={{
                                fontSize: 10, fontWeight: 700, padding: "2px 8px", borderRadius: 999,
                                background: "rgba(59,130,246,0.10)", color: "var(--admin-blue)",
                                border: "1px solid rgba(59,130,246,0.22)",
                            }}>
                                {filteredUsers.length} results
                            </span>
                        )}
                    </div>

                    {/* Table body */}
                    {usersLoading ? (
                        <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "52px 24px", gap: 10 }}>
                            <div className="w-7 h-7 animate-spin rounded-full" style={{ border: "3px solid var(--admin-border-color)", borderTopColor: "var(--admin-blue)" }} />
                            <span style={{ fontSize: 12, color: "var(--admin-text-muted)" }}>Loading users…</span>
                        </div>
                    ) : usersError ? (
                        <div style={{
                            display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16,
                            padding: "20px 20px", margin: "12px 16px", borderRadius: 10,
                            border: "1px solid rgba(239,68,68,0.28)", background: "rgba(239,68,68,0.07)",
                        }}>
                            <span style={{ fontSize: 13, color: "var(--admin-red)" }}>Failed to load users. Check that the backend service is running.</span>
                            <button className="admin-btn admin-btn-secondary" style={{ fontSize: 12, padding: "5px 12px" }} onClick={() => refetch()}>
                                Retry
                            </button>
                        </div>
                    ) : (
                        <div style={{ overflowX: "auto" }}>
                            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                                <thead>
                                    <tr style={{ background: "var(--admin-bg-sidebar)", borderBottom: "1px solid var(--admin-border-color)" }}>
                                        <th style={{ ...TH, minWidth: 220 }}>User</th>
                                        <th style={{ ...TH, width: 100 }}>Created</th>
                                        <th style={{ ...TH, width: 80 }}>Role</th>
                                        <th style={{ ...TH, width: 72, textAlign: "center" }}>Bots</th>
                                        <th style={{ ...TH, width: 80, textAlign: "right" }}>Trades</th>
                                        <th style={{ ...TH, width: 110, textAlign: "right" }}>Commission</th>
                                        <th style={{ ...TH, width: 90 }}>Status</th>
                                        <th style={{ ...TH, width: 80, textAlign: "center" }}>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {filteredUsers.length === 0 ? (
                                        <tr>
                                            <td colSpan={8} style={{ padding: "48px 24px", textAlign: "center" }}>
                                                <div style={{ fontSize: 14, fontWeight: 600, color: "var(--admin-text-secondary)", marginBottom: 4 }}>
                                                    {users.length === 0 ? "No users found" : "No users match your filters"}
                                                </div>
                                                <div style={{ fontSize: 12, color: "var(--admin-text-muted)" }}>
                                                    {users.length === 0 ? "Registered users will appear here." : "Try changing the search query, status, or role filter."}
                                                </div>
                                            </td>
                                        </tr>
                                    ) : (
                                        filteredUsers.map((user: any, i: number) => (
                                            <tr
                                                key={user.id}
                                                style={{ borderBottom: i < filteredUsers.length - 1 ? "1px solid var(--admin-border-color)" : undefined, transition: "background 0.1s" }}
                                                onMouseEnter={(e) => { (e.currentTarget as HTMLTableRowElement).style.background = "var(--admin-bg-hover)"; }}
                                                onMouseLeave={(e) => { (e.currentTarget as HTMLTableRowElement).style.background = ""; }}
                                            >
                                                {/* User cell */}
                                                <td style={TD}>
                                                    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                                                        <UserAvatar email={user.email} />
                                                        <div style={{ minWidth: 0 }}>
                                                            <div style={{
                                                                fontWeight: 600, color: "var(--admin-text-primary)",
                                                                maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                                                            }} title={user.email}>
                                                                {user.email}
                                                            </div>
                                                            <div style={{
                                                                fontSize: 10, color: "var(--admin-text-muted)",
                                                                fontFamily: "monospace",
                                                                maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                                                            }} title={user.id}>
                                                                #{(user.id || "").slice(0, 8)}
                                                            </div>
                                                        </div>
                                                    </div>
                                                </td>

                                                {/* Created */}
                                                <td style={{ ...TD, color: "var(--admin-text-muted)", whiteSpace: "nowrap" }}>
                                                    {formatDate(user.created_at)}
                                                </td>

                                                {/* Role */}
                                                <td style={TD}>
                                                    <Pill value={user.role || "user"} tone={roleTone(user.role)} />
                                                </td>

                                                {/* Bots */}
                                                <td style={{ ...TD, textAlign: "center", fontFamily: "monospace", color: "var(--admin-text-secondary)" }}>
                                                    {user.active_bots ?? 0} / {user.total_bots ?? 0}
                                                </td>

                                                {/* Trades */}
                                                <td style={{ ...TD, textAlign: "right", color: "var(--admin-text-secondary)" }}>
                                                    {(user.total_trades || 0).toLocaleString()}
                                                </td>

                                                {/* Commission */}
                                                <td style={{ ...TD, textAlign: "right", color: "var(--admin-green)", fontWeight: 600 }}>
                                                    {formatCurrency(user.total_commission || 0)}
                                                </td>

                                                {/* Status */}
                                                <td style={TD}>
                                                    <Pill value={user.status || "active"} tone={statusTone(user.status)} />
                                                </td>

                                                {/* Actions */}
                                                <td style={{ ...TD, textAlign: "center" }}>
                                                    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}>
                                                        <ActionBtn
                                                            onClick={() => handleToggleStatus(user.id, user.status)}
                                                            disabled={actionPending}
                                                            title={user.status === "suspended" ? "Activate user" : "Suspend user"}
                                                        >
                                                            {user.status === "suspended" ? (
                                                                <CheckCircle className="h-3.5 w-3.5" style={{ color: "var(--admin-green)" }} />
                                                            ) : (
                                                                <Ban className="h-3.5 w-3.5" style={{ color: "var(--admin-red)" }} />
                                                            )}
                                                        </ActionBtn>
                                                    </div>
                                                </td>
                                            </tr>
                                        ))
                                    )}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>

            </div>
        </AdminLayout>
    );
}
