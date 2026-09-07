import React, { useMemo, useState } from "react";
import { ChevronDown, Search } from "lucide-react";
import type { ProfitabilityReportResponse, ProfitabilitySymbolRow } from "@/api/admin";
import { TH, TD, WinRateBadge, formatNumber, formatPnl, pnlColor } from "./shared";

type SortKey = "symbol" | "trades" | "total_pnl" | "avg_pnl" | "win_rate";

const ctrlBase: React.CSSProperties = {
    background: "var(--admin-bg-secondary)",
    border: "1px solid var(--admin-border-color)",
    color: "var(--admin-text-primary)",
    borderRadius: 6, fontSize: 11, outline: "none", height: 28,
};

function SymbolRow({ row, idx }: { row: ProfitabilitySymbolRow; idx: number }) {
    const zebra: React.CSSProperties = idx % 2 === 0 ? {} : { background: "rgba(255,255,255,0.008)" };
    return (
        <tr style={{ transition: "background 0.12s", ...zebra }}>
            <td style={{ ...TD, fontWeight: 700, fontFamily: "monospace", fontSize: 12 }}>{row.symbol}</td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", fontWeight: 600 }}>{row.trades}</td>
            <td style={TD}><WinRateBadge pct={row.win_rate_pct} /></td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", fontWeight: 700, color: pnlColor(row.total_pnl) }}>{formatPnl(row.total_pnl)}</td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", color: pnlColor(row.average_pnl) }}>{formatPnl(row.average_pnl)}</td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", color: "var(--admin-text-secondary)" }}>{formatNumber(row.average_r_multiple, 2)}</td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", color: "var(--admin-text-muted)" }}>{row.sl_count}</td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", color: "var(--admin-text-muted)" }}>{row.tp_count}</td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", color: "var(--admin-text-muted)" }}>{row.time_exit_count}</td>
            <td style={{ ...TD, textAlign: "right", fontFamily: "monospace", color: "var(--admin-text-muted)" }}>{row.other_count}</td>
        </tr>
    );
}

interface Props { report: ProfitabilityReportResponse; }

export function SymbolPerformanceTab({ report }: Props) {
    const [symbolSearch, setSymbolSearch] = useState("");
    const [symbolSort, setSymbolSort]     = useState<SortKey>("total_pnl");

    const filtered = useMemo(() => {
        const rows = [...report.per_symbol];
        if (symbolSearch.trim()) {
            const q = symbolSearch.trim().toLowerCase();
            rows.splice(0, rows.length, ...rows.filter((r) => r.symbol.toLowerCase().includes(q)));
        }
        rows.sort((a, b) => {
            switch (symbolSort) {
                case "symbol":   return a.symbol.localeCompare(b.symbol);
                case "trades":   return (b.trades ?? 0) - (a.trades ?? 0);
                case "avg_pnl":  return (b.average_pnl ?? 0) - (a.average_pnl ?? 0);
                case "win_rate": return (b.win_rate_pct ?? 0) - (a.win_rate_pct ?? 0);
                default:         return b.total_pnl - a.total_pnl;
            }
        });
        return rows;
    }, [report.per_symbol, symbolSearch, symbolSort]);

    const profitable = report.per_symbol.filter((r) => r.total_pnl > 0).length;
    const losing     = report.per_symbol.filter((r) => r.total_pnl < 0).length;
    const bestSym    = report.per_symbol.length > 0
        ? report.per_symbol.reduce((a, b) => a.total_pnl > b.total_pnl ? a : b) : null;
    const worstSym   = report.per_symbol.length > 0
        ? report.per_symbol.reduce((a, b) => a.total_pnl < b.total_pnl ? a : b) : null;

    return (
        <div className="admin-card" style={{ padding: "20px 22px" }}>
            {/* Summary chips */}
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
                {[
                    { label: "Total Symbols", value: report.per_symbol.length, color: "var(--admin-text-secondary)" },
                    { label: "Profitable",    value: profitable,               color: "var(--admin-green)" },
                    { label: "Losing",        value: losing,                   color: "var(--admin-red)"   },
                    { label: "Best",          value: bestSym?.symbol  || "—",  color: "var(--admin-green)" },
                    { label: "Worst",         value: worstSym?.symbol || "—",  color: "var(--admin-red)"   },
                ].map(({ label, value, color }) => (
                    <div key={label} style={{
                        background: "rgba(255,255,255,0.03)",
                        border: "1px solid rgba(255,255,255,0.07)",
                        borderRadius: 6, padding: "4px 10px",
                        display: "flex", gap: 6, alignItems: "center",
                    }}>
                        <span style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.07em", color: "var(--admin-text-muted)", fontWeight: 700 }}>{label}</span>
                        <span style={{ fontSize: 11, fontWeight: 800, fontFamily: "monospace", color }}>{value}</span>
                    </div>
                ))}
            </div>

            {/* Controls row */}
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 10, marginBottom: 14 }}>
                <div>
                    <span style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-primary)" }}>Per-Symbol Performance</span>
                    <p style={{ margin: "3px 0 0", fontSize: 10, color: "var(--admin-text-muted)" }}>
                        Closed fills grouped by ticker — {filtered.length} of {report.per_symbol.length} shown
                    </p>
                </div>
                <div style={{ display: "flex", gap: 7 }}>
                    <div style={{ position: "relative" }}>
                        <Search style={{ position: "absolute", left: 8, top: "50%", transform: "translateY(-50%)", width: 11, height: 11, color: "var(--admin-text-muted)", pointerEvents: "none" }} />
                        <input
                            type="text" placeholder="Filter…"
                            value={symbolSearch} onChange={(e) => setSymbolSearch(e.target.value)}
                            style={{ ...ctrlBase, padding: "0 8px 0 24px", width: 130 }}
                        />
                    </div>
                    <div style={{ position: "relative" }}>
                        <select
                            value={symbolSort}
                            onChange={(e) => setSymbolSort(e.target.value as SortKey)}
                            style={{ ...ctrlBase, padding: "0 28px 0 10px", cursor: "pointer", width: 150, appearance: "none" }}
                        >
                            <option value="total_pnl">Total PnL ↓</option>
                            <option value="trades">Trades ↓</option>
                            <option value="win_rate">Win Rate ↓</option>
                            <option value="avg_pnl">Avg PnL ↓</option>
                            <option value="symbol">Symbol A–Z</option>
                        </select>
                        <ChevronDown style={{ position: "absolute", right: 7, top: "50%", transform: "translateY(-50%)", width: 10, height: 10, color: "var(--admin-text-muted)", pointerEvents: "none" }} />
                    </div>
                </div>
            </div>

            {/* Table */}
            <div style={{ maxHeight: 500, overflowY: "auto", overflowX: "auto", border: "1px solid rgba(255,255,255,0.05)", borderRadius: 7, background: "rgba(0,0,0,0.15)" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                    <thead>
                        <tr>
                            {[
                                { label: "Symbol",    align: "left"  },
                                { label: "Trades",    align: "right" },
                                { label: "Win Rate",  align: "left"  },
                                { label: "Total PnL", align: "right" },
                                { label: "Avg PnL",   align: "right" },
                                { label: "Avg R",     align: "right" },
                                { label: "SL",        align: "right" },
                                { label: "TP",        align: "right" },
                                { label: "Time Exit", align: "right" },
                                { label: "Other",     align: "right" },
                            ].map(({ label, align }) => (
                                <th key={label} style={{ ...TH, textAlign: align as React.CSSProperties["textAlign"] }}>{label}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {filtered.length > 0 ? (
                            filtered.map((row, idx) => <SymbolRow key={row.symbol} row={row} idx={idx} />)
                        ) : (
                            <tr>
                                <td colSpan={10} style={{ padding: "40px 12px", textAlign: "center", color: "var(--admin-text-muted)", fontSize: 12 }}>
                                    {symbolSearch.trim() ? `No symbols matching "${symbolSearch}"` : "No closed executed trades found."}
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
