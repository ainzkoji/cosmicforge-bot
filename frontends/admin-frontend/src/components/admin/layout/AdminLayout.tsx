import { ReactNode } from "react";
import { AdminSidebar } from "./AdminSidebar";
import { AdminHeader } from "./AdminHeader";
import { EventBlackoutBanner } from "@/components/admin/events/EventBlackoutBanner";
import "../../../styles/admin-theme.css";

interface AdminLayoutProps {
    children: ReactNode;
}

export function AdminLayout({ children }: AdminLayoutProps) {
    return (
        <div className="admin-layout">
            <AdminSidebar />
            <div className="admin-main">
                <AdminHeader />
                <div className="admin-content">
                    <EventBlackoutBanner />
                    {children}
                </div>
            </div>
        </div>
    );
}
