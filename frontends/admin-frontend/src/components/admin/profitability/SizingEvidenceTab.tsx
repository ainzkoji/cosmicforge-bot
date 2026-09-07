import React, { useState } from "react";
import { Search } from "lucide-react";
import type { ProfitabilityReportResponse, SizingCapEvent } from "@/api/admin";
import { TH, TD, formatNumber, formatPct, truncate } from "./shared";

const ctrlBase: React.CSSProperties = {
    background: "var(--admin-bg-secondary)",
    border: "1px solid var(--admin-border-color)",
    color: "var(--admin-text-primary)",
    borderRadius: 6, fontSize: 11, outline: "none", height: 28,
};

function SizingCapRow({ event, isExpanded, onToggle, idx }: {
    event: SizingCapEvent; isExpanded: boolean; onToggle: () => void; idx: number;
}) {
    const explanation = event.admin_message || event.cap_reason || "Fixed allocation sizing evidence";
    const riskLevel   = event.risk_level_label || event.risk_level || "n/a";
    const isHigh      = typeof riskLevel === "string" && riskLevel.toLowerCase().includes("high");
    const zebra: React.CSSProperties = idx % 2 === 0 ? {} : { background: "rgba(255,255,255,0.008)" };

    return (
        <>
            <tr onClick={onToggle} style={{ cursor: "pointer", transition: "background 0.12s", ...zebra }}>
                <td style={{ ...TD, fontWeight: 700, fontFamily: "monospace" }}>{event.symbol || "UNKNOWN"}</td>
                <td style={TD}>
                    <span style={{
                        display: "inline-flex", alignItems: "center", padding: "1px 5px", borderRadius: 4,
                        fontSize: 9, fontWeight: 700, whiteSpace: "nowrap",
                        background: isHigh ? "rgba(239,68,68,0.07)"  : "rgba(245,158,11,0.07)",
                        color:      isHigh ? "var(--admin-red)"       : "var(--admin-yellow)",
                        border:     `1px solid ${isHigh ? "rgba(239,68,68,0.15)" : "rgba(245,158,11,0.15)"}`,
                    }}>{riskLevel}</span>
                </td>
                <td style={{ ...TD, textAlign: "right", fontFamily: "monospace" }}>{formatNumber(event.base_margin_usdt, 2)}</td>
                <td style={{ ...TD, textAlign: "right", fontFamily: "monospace" }}>{formatNumber(event.final_margin_usdt, 2)}</td>
                <td style={{ ...TD, textAlign: "right", fontFamily: "monospace" }}>{formatNumber(event.base_notional_usdt, 2)}</td>
                <td style={{ ...TD, textAlign: "right", fontFamily: "monospace" }}>{formatNumber(event.final_notional_usdt, 2)}</td>
                <td style={{ ...TD, textAlign: "right", fontFamily: "monospace" }}>{formatNumber(event.leverage, 0)}x</td>
                <td style={{ ...TD, textAlign: "right" }}>{formatPct(event.account_risk_pct)}</td>
                <td style={{ ...TD, textAlign: "right" }}>{formatPct(event.stop_distance_pct)}</td>
                <td style={{ ...TD, maxWidth: 200 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <span style={{ fontSize: 11, color: "var(--admin-text-secondary)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {truncate(explanation, 36)}
                        </span>
                        <span style={{ fontSize: 9, color: "var(--admin-purple)", fontWeight: 700, flexShrink: 0 }}>
                            {isExpanded ? "▲" : "▼"}
                        </span>
                    </div>
                </td>
            </tr>
            {isExpanded && (
                <tr>
                    <td colSpan={10} style={{
                        ...TD,
                        background: "rgba(139,92,246,0.03)",
                        borderLeft: "3px solid rgba(139,92,246,0.25)",
                        padding: "10px 16px",
                        fontSize: 11, lineHeight: 1.55, color: "var(--admin-text-secondary)", whiteSpace: "normal",
                    }}>
                        <span style={{ fontWeight: 700, color: "var(--admin-text-muted)", marginRight: 6 }}>Sizing Evidence:</span>
                        {explanation}
                        {event.trace_id && (
                            <span style={{ marginLeft: 10, fontSize: 10, color: "var(--admin-text-muted)", fontFamily: "monospace" }}>
                                · {event.trace_id}
                            </span>
                        )}
                    </td>
                </tr>
            )}
        </>
    );
}

interface Props { report: ProfitabilityReportResponse; }

export function SizingEvidenceTab({ report }: Props) {
    const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
    const [symbolFilter, setSymbolFilter] = useState("");
    const [riskFilter, setRiskFilter]     = useState("all");

    const toggleRow = (id: string) => setExpandedRows((prev) => ({ ...prev, [id]: !prev[id] }));

    const allRiskLevels = Array.from(new Set(
        report.sizing_cap_events.map((e) => e.risk_level_label || e.risk_level || "n/a")
    )).filter(Boolean) as string[];

    const filtered = report.sizing_cap_events.filter((e) => {
        const sym = symbolFilter.trim().toLowerCase();
        if (sym && !(e.symbol || "").toLowerCase().includes(sym)) return false;
        if (riskFilter !== "all") {
            const rl = e.risk_level_label || e.risk_level || "n/a";
            if (rl !== riskFilter) return false;
        }
        return true;
    });

    return (
        <div className="admin-card" style={{ padding: "20px 22px" }}>
            {/* Header */}
            <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 10, marginBottom: 16 }}>
                <div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-primary)" }}>
                            Fixed Allocation Sizing Evidence
                        </span>
                        <span style={{
                            fontSize: 8, fontWeight: 800, padding: "1px 6px", borderRadius: 4,
                            background: "rgba(245,158,11,0.07)", color: "var(--admin-yellow)",
                            border: "1px solid rgba(245,158,11,0.15)",
                            textTransform: "uppercase", letterSpacing: "0.06em",
                        }}>
                            {filtered.length} of {report.sizing_cap_events.length} events
                        </span>
                    </div>
                    <p style={{ margin: "4px 0 0", fontSize: 10, color: "var(--admin-text-muted)" }}>
                        Exchange-facing margin evidence, ATR distance metrics, and stop-loss constraints. Click any row to expand.
                    </p>
                </div>
                <div style={{ display: "flex", gap: 7, flexWrap: "wrap", alignItems: "center" }}>
                    <div style={{ position: "relative" }}>
                        <Search style={{ position: "absolute", left: 8, top: "50%", transform: "translateY(-50%)", width: 11, height: 11, color: "var(--admin-text-muted)", pointerEvents: "none" }} />
                        <input
                            type="text" placeholder="Symbol…"
                            value={symbolFilter} onChange={(e) => setSymbolFilter(e.target.value)}
                            style={{ ...ctrlBase, padding: "0 8px 0 24px", width: 110 }}
                        />
                    </div>
                    <select
                        value={riskFilter} onChange={(e) => setRiskFilter(e.target.value)}
                        style={{ ...ctrlBase, padding: "0 10px", cursor: "pointer", width: 140 }}
                    >
                        <option value="all">All Risk Levels</option>
                        {allRiskLevels.map((rl) => <option key={rl} value={rl}>{rl}</option>)}
                    </select>
                </div>
            </div>

            {/* Table */}
            <div style={{ maxHeight: 500, overflowY: "auto", overflowX: "auto", border: "1px solid rgba(255,255,255,0.05)", borderRadius: 7, background: "rgba(0,0,0,0.15)" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                    <thead>
                        <tr>
                            {["Symbol","Risk Level","Base Margin","Final Margin","Base Notional","Final Notional","Leverage","Risk Cap","ATR Stop","Action / Reason"].map((h) => (
                                <th key={h} style={TH}>{h}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {filtered.length > 0 ? (
                            filtered.map((event, idx) => {
                                const rowId = event.trace_id || `${event.symbol}-${event.timestamp_utc}-${idx}`;
                                return (
                                    <SizingCapRow
                                        key={rowId} event={event} idx={idx}
                                        isExpanded={!!expandedRows[rowId]}
                                        onToggle={() => toggleRow(rowId)}
                                    />
                                );
                            })
                        ) : (
                            <tr>
                                <td colSpan={10} style={{ padding: "40px 12px", textAlign: "center", color: "var(--admin-text-muted)", fontSize: 12 }}>
                                    {report.sizing_cap_events.length === 0
                                        ? "No fixed-allocation sizing evidence recorded yet."
                                        : "No events matching current filters."}
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
