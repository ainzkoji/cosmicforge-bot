import React from "react";
import type { ProfitabilityReportResponse, ProfitabilityRecentWindow } from "@/api/admin";
import { formatNumber, formatPct, formatPnl, pnlColor } from "./shared";

interface Props { report: ProfitabilityReportResponse; }

function PeriodCard({ label, data }: { label: string; data: ProfitabilityRecentWindow }) {
    const pnl  = data.total_realized_pnl ?? 0;
    const pf   = Number(data.profit_factor ?? 0);
    const pfOk = pf >= 1;
    const winOk = (data.win_rate_pct ?? 0) >= 50;

    return (
        <div style={{
            background: "var(--admin-bg-secondary)",
            border: "1px solid var(--admin-border-color)",
            borderRadius: 10, padding: "20px 22px",
            boxShadow: "0 2px 10px rgba(0,0,0,0.14)",
        }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                <span style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-primary)" }}>{label}</span>
                <span style={{
                    fontSize: 8, fontWeight: 700, padding: "1px 5px", borderRadius: 3,
                    background: "rgba(255,255,255,0.03)", color: "var(--admin-text-muted)",
                    border: "1px solid rgba(255,255,255,0.06)",
                    textTransform: "uppercase", letterSpacing: "0.04em",
                }}>SNAPSHOT</span>
            </div>

            <div style={{
                fontSize: 28, fontWeight: 800, fontFamily: "monospace",
                color: pnlColor(pnl), letterSpacing: "-0.02em", marginBottom: 16,
            }}>
                {formatPnl(pnl)}
                <span style={{ fontSize: 11, fontWeight: 400, color: "var(--admin-text-muted)", marginLeft: 6, fontFamily: "sans-serif" }}>
                    USDT
                </span>
            </div>

            <div style={{
                display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px 16px",
                paddingTop: 14, borderTop: "1px solid rgba(255,255,255,0.035)",
            }}>
                {[
                    { label: "Closed Trades", value: String(data.closed_trades ?? "n/a"), color: "var(--admin-text-primary)" },
                    { label: "Win Rate",       value: formatPct(data.win_rate_pct),        color: winOk ? "var(--admin-green)" : "var(--admin-text-secondary)" },
                    { label: "Avg PnL",        value: formatPnl(data.average_pnl),         color: pnlColor(data.average_pnl) },
                    { label: "Profit Factor",  value: formatNumber(data.profit_factor, 2),
                        color: data.profit_factor == null ? "var(--admin-text-muted)" : pfOk ? "var(--admin-green)" : "var(--admin-red)" },
                ].map(({ label: sl, value: sv, color }) => (
                    <div key={sl}>
                        <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.05em", color: "var(--admin-text-muted)", marginBottom: 2, fontWeight: 700 }}>
                            {sl}
                        </div>
                        <div style={{ fontSize: 13, fontWeight: 700, fontFamily: "monospace", color }}>{sv}</div>
                    </div>
                ))}
            </div>
        </div>
    );
}

export function PeriodPerformanceTab({ report }: Props) {
    return (
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            <p style={{ margin: 0, fontSize: 12, color: "var(--admin-text-muted)", lineHeight: 1.55 }}>
                Rolling window performance across closed filled trades. Compares the last 24 h, 48 h, and 7 days of bot activity.
            </p>
            <div style={{ display: "grid", gap: 14, gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))" }}>
                <PeriodCard label="Last 24 Hours" data={report.recent.last_24h} />
                <PeriodCard label="Last 48 Hours" data={report.recent.last_48h} />
                <PeriodCard label="Last 7 Days"   data={report.recent.last_7d}  />
            </div>
        </div>
    );
}
