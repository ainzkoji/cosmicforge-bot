import React from "react";
import { BarChart3, Clock, Zap, Database, Globe, Shield } from "lucide-react";

export type TabId = "overview" | "period" | "extremes" | "sizing" | "symbols" | "risk";

interface Tab { id: TabId; label: string; icon: React.ReactNode; }

const TABS: Tab[] = [
    { id: "overview",  label: "Overview",           icon: <BarChart3  className="w-3.5 h-3.5" /> },
    { id: "period",    label: "Period Performance",  icon: <Clock      className="w-3.5 h-3.5" /> },
    { id: "extremes",  label: "Trade Extremes",      icon: <Zap        className="w-3.5 h-3.5" /> },
    { id: "sizing",    label: "Sizing Evidence",     icon: <Database   className="w-3.5 h-3.5" /> },
    { id: "symbols",   label: "Symbol Performance",  icon: <Globe      className="w-3.5 h-3.5" /> },
    { id: "risk",      label: "Risk Quality",        icon: <Shield     className="w-3.5 h-3.5" /> },
];

interface Props { active: TabId; onChange: (tab: TabId) => void; }

export function ProfitabilityTabs({ active, onChange }: Props) {
    return (
        <div style={{
            display: "flex", gap: 2,
            borderBottom: "1px solid var(--admin-border-color)",
            overflowX: "auto", scrollbarWidth: "none",
            msOverflowStyle: "none",
        }}>
            {TABS.map((tab) => {
                const isActive = tab.id === active;
                return (
                    <button
                        key={tab.id}
                        onClick={() => onChange(tab.id)}
                        style={{
                            display: "flex", alignItems: "center", gap: 6,
                            padding: "9px 14px",
                            border: "none",
                            borderBottom: isActive
                                ? "2px solid rgba(139,92,246,0.9)"
                                : "2px solid transparent",
                            background: isActive ? "rgba(139,92,246,0.06)" : "transparent",
                            color: isActive ? "var(--admin-text-primary)" : "var(--admin-text-muted)",
                            fontSize: 11, fontWeight: isActive ? 700 : 500,
                            cursor: "pointer", whiteSpace: "nowrap",
                            borderRadius: "6px 6px 0 0",
                            transition: "all 0.15s", flexShrink: 0,
                            marginBottom: -1,
                        }}
                    >
                        <span style={{ opacity: isActive ? 1 : 0.6, color: isActive ? "rgba(139,92,246,0.9)" : "inherit", display: "flex" }}>
                            {tab.icon}
                        </span>
                        {tab.label}
                    </button>
                );
            })}
        </div>
    );
}
