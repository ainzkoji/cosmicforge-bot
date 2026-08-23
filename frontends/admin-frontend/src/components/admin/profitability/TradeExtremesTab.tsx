import React from "react";
import { TrendingDown, TrendingUp } from "lucide-react";
import type { ProfitabilityReportResponse, ProfitabilityTrade } from "@/api/admin";
import { formatDate, formatNumber, formatPnl } from "./shared";

function TradeCard({ title, trade, tone }: {
    title: string; trade: ProfitabilityTrade | null; tone: "good" | "bad";
}) {
    const accentHex = tone === "good" ? "#10B981" : "#EF4444";
    const accent    = tone === "good" ? "var(--admin-green)" : "var(--admin-red)";
    const sideBg    = (s?: string | null) => s?.toLowerCase() === "long" ? "rgba(59,130,246,0.08)" : "rgba(245,158,11,0.08)";
    const sideColor = (s?: string | null) => s?.toLowerCase() === "long" ? "var(--admin-blue)"    : "var(--admin-yellow)";
    const sideBdr   = (s?: string | null) => s?.toLowerCase() === "long" ? "rgba(59,130,246,0.18)" : "rgba(245,158,11,0.18)";

    return (
        <div style={{
            background: "var(--admin-bg-secondary)",
            border: "1px solid var(--admin-border-color)",
            borderLeft: `3px solid ${accentHex}`,
            borderRadius: 10, padding: "18px 20px",
            boxShadow: `0 2px 12px rgba(0,0,0,0.15), -6px 0 20px -8px ${accentHex}22`,
            display: "flex", flexDirection: "column",
        }}>
            {/* Header */}
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                    <div style={{
                        width: 24, height: 24, borderRadius: 6,
                        background: `color-mix(in srgb, ${accentHex} 8%, transparent)`,
                        display: "flex", alignItems: "center", justifyContent: "center",
                    }}>
                        {tone === "good"
                            ? <TrendingUp  className="w-3.5 h-3.5" style={{ color: accent }} />
                            : <TrendingDown className="w-3.5 h-3.5" style={{ color: accent }} />
                        }
                    </div>
                    <span style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "var(--admin-text-primary)" }}>
                        {title}
                    </span>
                </div>
                {trade && (
                    <span style={{
                        fontSize: 9, fontWeight: 800, padding: "2px 7px", borderRadius: 999,
                        background: sideBg(trade.side), color: sideColor(trade.side),
                        border: `1px solid ${sideBdr(trade.side)}`,
                        textTransform: "uppercase", letterSpacing: "0.06em",
                    }}>
                        {trade.side || "n/a"}
                    </span>
                )}
            </div>

            {trade ? (
                <>
                    {/* Symbol + PnL */}
                    <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 10, marginBottom: 16 }}>
                        <div>
                            <div style={{ fontSize: 24, fontWeight: 800, fontFamily: "monospace", color: "var(--admin-text-primary)", letterSpacing: "-0.02em", lineHeight: 1.1 }}>
                                {trade.symbol || "UNKNOWN"}
                            </div>
                            <div style={{ fontSize: 11, color: "var(--admin-text-muted)", marginTop: 4, maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {trade.exit_reason || trade.trigger_source || "No exit reason"}
                            </div>
                        </div>
                        <div style={{ textAlign: "right", flexShrink: 0 }}>
                            <div style={{ fontSize: 24, fontWeight: 800, fontFamily: "monospace", color: accent, letterSpacing: "-0.02em" }}>
                                {formatPnl(trade.realized_pnl)}
                            </div>
                            <div style={{ fontSize: 9, color: "var(--admin-text-muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginTop: 2 }}>USDT</div>
                        </div>
                    </div>

                    {/* Stat tiles */}
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                        {[
                            { label: "R Multiple", value: formatNumber(trade.r_multiple, 2) },
                            { label: "Position",   value: trade.position_id ? `…${trade.position_id.slice(-8)}` : "n/a" },
                            { label: "Closed At",  value: formatDate(trade.timestamp_utc) },
                            { label: "Trade ID",   value: trade.id != null ? `…${String(trade.id).slice(-8)}` : "n/a" },
                        ].map(({ label: sl, value: sv }) => (
                            <div key={sl} style={{
                                background: "rgba(255,255,255,0.015)",
                                border: "1px solid rgba(255,255,255,0.04)",
                                borderRadius: 7, padding: "8px 10px",
                            }}>
                                <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--admin-text-muted)", marginBottom: 3, fontWeight: 700 }}>{sl}</div>
                                <div style={{ fontSize: 11, fontWeight: 700, fontFamily: "monospace", color: "var(--admin-text-secondary)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{sv}</div>
                            </div>
                        ))}
                    </div>
                </>
            ) : (
                <div style={{ padding: "40px 0", textAlign: "center", color: "var(--admin-text-muted)", fontSize: 12 }}>
                    No closed trade available.
                </div>
            )}
        </div>
    );
}

interface Props { report: ProfitabilityReportResponse; }

export function TradeExtremesTab({ report }: Props) {
    return (
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            <p style={{ margin: 0, fontSize: 12, color: "var(--admin-text-muted)", lineHeight: 1.55 }}>
                The single best and worst executed trades across all closed fills, ranked by realized PnL.
            </p>
            <div style={{ display: "grid", gap: 14, gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))" }}>
                <TradeCard title="Peak Trade — Best"    trade={report.overall.best_trade}  tone="good" />
                <TradeCard title="Trough Trade — Worst" trade={report.overall.worst_trade} tone="bad"  />
            </div>
        </div>
    );
}
