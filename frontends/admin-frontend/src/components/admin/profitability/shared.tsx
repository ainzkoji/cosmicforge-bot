import React from "react";
import { CheckCircle2, AlertTriangle } from "lucide-react";
import type { ProfitabilityReportResponse } from "@/api/admin";

// ─── Formatters ───────────────────────────────────────────────────────────────

export function formatNumber(value: number | string | null | undefined, digits = 2): string {
    if (value === null || value === undefined) return "n/a";
    if (typeof value === "string") return value;
    return new Intl.NumberFormat("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    }).format(value);
}
export function formatPct(value: number | null | undefined): string {
    if (value === null || value === undefined) return "n/a";
    return `${formatNumber(value, 2)}%`;
}
export function formatPnl(value: number | null | undefined): string {
    if (value === null || value === undefined) return "n/a";
    return `${value > 0 ? "+" : ""}${formatNumber(value, 2)}`;
}
export function formatDate(iso: string | null | undefined): string {
    if (!iso) return "n/a";
    try {
        const d = new Date(iso);
        return d.toLocaleDateString("en-US", {
            month: "short", day: "numeric", year: "numeric",
            hour: "2-digit", minute: "2-digit", timeZone: "UTC", hour12: false,
        }).replace(",", " ·") + " UTC";
    } catch { return iso; }
}
export function pnlColor(v: number | null | undefined): string {
    if (v == null || v === 0) return "var(--admin-text-secondary)";
    return v > 0 ? "var(--admin-green)" : "var(--admin-red)";
}
export function truncate(str: string | null | undefined, max: number): string {
    if (!str) return "n/a";
    return str.length > max ? str.slice(0, max) + "…" : str;
}
export function exportRows(report?: ProfitabilityReportResponse) {
    if (!report) return [];
    return report.per_symbol.map((r) => ({
        symbol: r.symbol, trades: r.trades, win_rate_pct: r.win_rate_pct,
        total_pnl: r.total_pnl, average_pnl: r.average_pnl,
        average_r_multiple: r.average_r_multiple,
        sl_count: r.sl_count, tp_count: r.tp_count,
        time_exit_count: r.time_exit_count, other_count: r.other_count,
    }));
}

// ─── Table base styles ────────────────────────────────────────────────────────

export const TH: React.CSSProperties = {
    padding: "8px 12px",
    textAlign: "left",
    fontSize: 9,
    fontWeight: 700,
    textTransform: "uppercase",
    letterSpacing: "0.09em",
    color: "var(--admin-text-muted)",
    whiteSpace: "nowrap",
    background: "rgba(0,0,0,0.25)",
    borderBottom: "1px solid rgba(255,255,255,0.05)",
    position: "sticky",
    top: 0,
    zIndex: 2,
};
export const TD: React.CSSProperties = {
    padding: "9px 12px",
    fontSize: 11,
    color: "var(--admin-text-primary)",
    borderBottom: "1px solid rgba(255,255,255,0.025)",
    whiteSpace: "nowrap",
};

// ─── Tone ────────────────────────────────────────────────────────────────────

export type Tone = "positive" | "negative" | "info" | "neutral";
export const TONE_MAP: Record<Tone, { accent: string; borderAlpha: string; bgAlpha: string }> = {
    positive: { accent: "#10B981", borderAlpha: "rgba(16,185,129,0.20)", bgAlpha: "rgba(16,185,129,0.025)" },
    negative: { accent: "#EF4444", borderAlpha: "rgba(239,68,68,0.20)",  bgAlpha: "rgba(239,68,68,0.025)"  },
    info:     { accent: "#06B6D4", borderAlpha: "rgba(6,182,212,0.20)",  bgAlpha: "rgba(6,182,212,0.025)"  },
    neutral:  { accent: "var(--admin-text-secondary)", borderAlpha: "var(--admin-border-color)", bgAlpha: "transparent" },
};

// ─── KPI Card ─────────────────────────────────────────────────────────────────

interface KPICardProps {
    label: string;
    value: string | number;
    icon: React.ReactNode;
    supportLabel: string;
    supportValue: string;
    tone?: Tone;
}
export function KPICard({ label, value, icon, supportLabel, supportValue, tone = "neutral" }: KPICardProps) {
    const { accent, borderAlpha, bgAlpha } = TONE_MAP[tone];
    return (
        <div style={{
            background: `linear-gradient(160deg, ${bgAlpha} 0%, transparent 60%), var(--admin-bg-secondary)`,
            border: `1px solid ${borderAlpha}`,
            borderTop: `2px solid ${accent}`,
            borderRadius: 10, padding: "14px 16px",
            display: "flex", flexDirection: "column", gap: 0,
            boxShadow: "0 2px 12px rgba(0,0,0,0.18)", minWidth: 0,
        }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
                <span style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)" }}>
                    {label}
                </span>
                <div style={{
                    width: 24, height: 24, borderRadius: 6,
                    background: `color-mix(in srgb, ${accent} 10%, transparent)`,
                    border: `1px solid color-mix(in srgb, ${accent} 20%, transparent)`,
                    display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
                }}>{icon}</div>
            </div>
            <div style={{
                fontSize: 26, fontWeight: 800, fontFamily: "monospace",
                color: accent, letterSpacing: "-0.025em", lineHeight: 1.05, marginBottom: 10,
            }}>{value}</div>
            <div style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                paddingTop: 8, borderTop: "1px solid rgba(255,255,255,0.035)", fontSize: 10,
            }}>
                <span style={{ color: "var(--admin-text-muted)" }}>{supportLabel}</span>
                <span style={{ fontWeight: 700, color: "var(--admin-text-secondary)", fontFamily: "monospace" }}>{supportValue}</span>
            </div>
        </div>
    );
}

// ─── Win Rate Badge ───────────────────────────────────────────────────────────

export function WinRateBadge({ pct }: { pct: number | null | undefined }) {
    if (pct == null) return <span style={{ color: "var(--admin-text-muted)", fontSize: 11, fontFamily: "monospace" }}>n/a</span>;
    const good = pct >= 55, ok = pct >= 45;
    return (
        <span style={{
            display: "inline-flex", alignItems: "center", padding: "1px 6px", borderRadius: 4,
            fontSize: 10, fontWeight: 700, fontFamily: "monospace",
            background: good ? "rgba(16,185,129,0.07)" : ok ? "rgba(245,158,11,0.07)" : "rgba(239,68,68,0.07)",
            color:      good ? "var(--admin-green)"    : ok ? "var(--admin-yellow)"    : "var(--admin-red)",
            border: `1px solid ${good ? "rgba(16,185,129,0.15)" : ok ? "rgba(245,158,11,0.15)" : "rgba(239,68,68,0.15)"}`,
        }}>{formatPct(pct)}</span>
    );
}

// ─── Diagnostic card ──────────────────────────────────────────────────────────

export type DiagSeverity = "healthy" | "warning" | "critical";

const DIAG: Record<DiagSeverity, { accent: string; bg: string; border: string; icon: string }> = {
    healthy:  { accent: "#10B981", bg: "rgba(16,185,129,0.03)", border: "rgba(16,185,129,0.12)", icon: "✓" },
    warning:  { accent: "#F59E0B", bg: "rgba(245,158,11,0.04)", border: "rgba(245,158,11,0.15)", icon: "△" },
    critical: { accent: "#EF4444", bg: "rgba(239,68,68,0.04)",  border: "rgba(239,68,68,0.15)",  icon: "✕" },
};

export function RiskDiagCard({ label, value, hint, severity }: {
    label: string; value: string | number; hint: string; severity: DiagSeverity;
}) {
    const c = DIAG[severity];
    return (
        <div style={{
            background: c.bg, border: `1px solid ${c.border}`,
            borderLeft: `3px solid ${c.accent}`, borderRadius: 8,
            padding: "12px 14px", display: "flex", flexDirection: "column", gap: 6,
        }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
                <span style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "var(--admin-text-muted)" }}>
                    {label}
                </span>
                <span style={{ fontSize: 10, color: c.accent, fontWeight: 800 }}>{c.icon}</span>
            </div>
            <div style={{ fontSize: 20, fontWeight: 800, fontFamily: "monospace", color: c.accent, letterSpacing: "-0.02em" }}>
                {value}
            </div>
            <div style={{ fontSize: 10, color: "var(--admin-text-muted)", lineHeight: 1.45 }}>{hint}</div>
        </div>
    );
}

// ─── Snap stat (overview inline) ──────────────────────────────────────────────

export function SnapStat({ label, value, ok }: { label: string; value: string | number; ok: boolean }) {
    const color = ok ? "var(--admin-green)" : "var(--admin-yellow)";
    return (
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--admin-text-muted)", fontWeight: 700 }}>
                {label}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                {ok
                    ? <CheckCircle2 className="w-3 h-3" style={{ color: "var(--admin-green)", opacity: 0.7 }} />
                    : <AlertTriangle className="w-3 h-3" style={{ color: "var(--admin-yellow)", opacity: 0.7 }} />
                }
                <span style={{ fontSize: 14, fontWeight: 700, fontFamily: "monospace", color }}>{value}</span>
            </div>
        </div>
    );
}
