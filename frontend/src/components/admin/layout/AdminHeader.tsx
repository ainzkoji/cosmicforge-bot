import { Search, Bell, User } from "lucide-react";
import { useState } from "react";

export function AdminHeader() {
    const [searchQuery, setSearchQuery] = useState("");

    return (
        <div className="admin-header">
            <div className="flex items-center justify-between">
                {/* Search */}
                <div className="flex-1 max-w-md">
                    <div className="relative">
                        <Search
                            className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4"
                            style={{ color: 'var(--admin-text-muted)' }}
                        />
                        <input
                            type="text"
                            placeholder="Search users, transactions, etc..."
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            className="admin-input pl-10"
                            style={{ maxWidth: '400px' }}
                        />
                    </div>
                </div>

                {/* Right Section */}
                <div className="flex items-center gap-4">
                    {/* Notifications */}
                    <button className="relative p-2 rounded-lg hover:bg-opacity-10 transition-colors" style={{ background: 'var(--admin-bg-hover)' }}>
                        <Bell className="w-5 h-5" style={{ color: 'var(--admin-text-secondary)' }} />
                        <span className="absolute top-1 right-1 w-2 h-2 rounded-full" style={{ background: 'var(--admin-red)' }} />
                    </button>

                    {/* Admin Profile */}
                    <div className="flex items-center gap-3 px-3 py-2 rounded-lg cursor-pointer hover:bg-opacity-10 transition-colors" style={{ background: 'var(--admin-bg-hover)' }}>
                        <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'var(--admin-blue)' }}>
                            <User className="w-4 h-4 text-white" />
                        </div>
                        <div className="text-sm">
                            <div className="font-medium" style={{ color: 'var(--admin-text-primary)' }}>
                                Admin User
                            </div>
                            <div className="text-xs" style={{ color: 'var(--admin-text-muted)' }}>
                                Super Admin
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
