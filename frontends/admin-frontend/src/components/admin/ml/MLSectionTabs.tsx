import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";

export interface MLSectionTabItem {
    key: string;
    label: string;
    icon?: ReactNode;
    to?: string;
}

interface MLSectionTabsProps {
    items: MLSectionTabItem[];
    activeKey: string;
    onChange: (key: string) => void;
}

export function MLSectionTabs({ items, activeKey, onChange }: MLSectionTabsProps) {
    return (
        <div className="admin-ml-tabs" role="tablist" aria-label="ML monitoring sections">
            {items.map((item) => {
                const isActive = item.key === activeKey;
                if (item.to) {
                    return (
                        <NavLink
                            key={item.key}
                            to={item.to}
                            role="tab"
                            aria-selected={isActive}
                            className={({ isActive: routeActive }) => `admin-ml-tab ${routeActive ? "active" : ""}`}
                        >
                            {item.icon ? <span className="admin-ml-tab-icon">{item.icon}</span> : null}
                            <span>{item.label}</span>
                        </NavLink>
                    );
                }

                return (
                    <button
                        key={item.key}
                        type="button"
                        role="tab"
                        aria-selected={isActive}
                        className={`admin-ml-tab ${isActive ? "active" : ""}`}
                        onClick={() => onChange(item.key)}
                    >
                        {item.icon ? <span className="admin-ml-tab-icon">{item.icon}</span> : null}
                        <span>{item.label}</span>
                    </button>
                );
            })}
        </div>
    );
}
