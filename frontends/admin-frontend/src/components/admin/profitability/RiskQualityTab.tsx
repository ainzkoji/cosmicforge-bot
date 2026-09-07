import React from "react";
import type { ProfitabilityReportResponse } from "@/api/admin";
import { RiskDiagCard, type DiagSeverity, formatNumber } from "./shared";

interface Props { report: ProfitabilityReportResponse; }

function countSev(n: number, warnAt = 1, critAt = 3): DiagSeverity {
    if (n === 0)        return "healthy";
    if (n < critAt)     return "warning";
    return "critical";
}
function slipSev(n: number | null): DiagSeverity {
    if (n == null || n < 0.01) return "healthy";
    if (n < 0.05)              return "warning";
    return "critical";
}

export function RiskQualityTab({ report }: Props) {
    const risk = report.risk_execution_quality;

    const groups: Array<{
        group: string;
        items: Array<{ label: string; value: string | number; hint: string; severity: DiagSeverity }>;
    }> = [
        {
            group: "Duplicate Detection",
            items: [
                {
                    label: "Duplicate Order Groups",
                    value: risk.duplicate_order_id_action_symbol_groups,
                    hint: "Order ID + action + symbol groups appearing more than once. Non-zero indicates potential double-fill.",
                    severity: countSev(risk.duplicate_order_id_action_symbol_groups),
                },
                {
                    label: "Duplicate Exact Fill Groups",
                    value: risk.duplicate_exact_fill_groups,
                    hint: "Fills with identical position, action, symbol, side, timestamp, qty, and price — likely exact duplicates.",
                    severity: countSev(risk.duplicate_exact_fill_groups),
                },
            ],
        },
        {
            group: "Linkage Integrity",
            items: [
                {
                    label: "Missing Run ID",
                    value: risk.missing_run_id_count,
                    hint: "Fills with no linked run_id. Prevents tracing a trade back to a bot session.",
                    severity: countSev(risk.missing_run_id_count, 1, 5),
                },
                {
                    label: "Missing Position ID",
                    value: risk.missing_position_id_count,
                    hint: "Fills with no position_id. Open/close pairs cannot be matched without this field.",
                    severity: countSev(risk.missing_position_id_count, 1, 5),
                },
            ],
        },
        {
            group: "Close Fill Quality",
            items: [
                {
                    label: "Null Exit Reason (CLOSE fills)",
                    value: risk.closed_fills_null_exit_reason,
                    hint: "CLOSE fills missing an exit_reason. Indicates a silent or force-close that bypassed normal exit logic.",
                    severity: countSev(risk.closed_fills_null_exit_reason, 1, 5),
                },
                {
                    label: "Null R Multiple (CLOSE fills)",
                    value: risk.closed_fills_null_r_multiple,
                    hint: "CLOSE fills where R-multiple was not recorded. R-multiple is required for performance scoring.",
                    severity: countSev(risk.closed_fills_null_r_multiple, 1, 5),
                },
            ],
        },
        {
            group: "Slippage",
            items: [
                {
                    label: "Avg Slippage %",
                    value: `${formatNumber(risk.average_slippage_pct, 4)}%`,
                    hint: "Average slippage across all fills. Target: below 0.01% on liquid pairs.",
                    severity: slipSev(risk.average_slippage_pct),
                },
                {
                    label: "Max Abs Slippage %",
                    value: `${formatNumber(risk.biggest_abs_slippage_pct, 4)}%`,
                    hint: "Largest single slippage event recorded. Outliers above 0.05% warrant investigation.",
                    severity: slipSev(risk.biggest_abs_slippage_pct),
                },
            ],
        },
    ];

    const allItems  = groups.flatMap((g) => g.items);
    const critical  = allItems.filter((i) => i.severity === "critical").length;
    const warning   = allItems.filter((i) => i.severity === "warning").length;
    const healthy   = allItems.filter((i) => i.severity === "healthy").length;

    return (
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
            {/* Summary banner */}
            <div style={{
                display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center",
                padding: "12px 16px",
                background: "var(--admin-bg-secondary)",
                border: "1px solid var(--admin-border-color)",
                borderRadius: 10,
            }}>
                <span style={{ fontSize: 11, color: "var(--admin-text-muted)", fontWeight: 600 }}>Diagnostic summary:</span>
                {critical > 0 && (
                    <span style={{ fontSize: 11, fontWeight: 700, color: "#EF4444", background: "rgba(239,68,68,0.08)", padding: "2px 8px", borderRadius: 4, border: "1px solid rgba(239,68,68,0.15)" }}>
                        {critical} Critical
                    </span>
                )}
                {warning > 0 && (
                    <span style={{ fontSize: 11, fontWeight: 700, color: "#F59E0B", background: "rgba(245,158,11,0.08)", padding: "2px 8px", borderRadius: 4, border: "1px solid rgba(245,158,11,0.15)" }}>
                        {warning} Warning
                    </span>
                )}
                {healthy > 0 && (
                    <span style={{ fontSize: 11, fontWeight: 700, color: "#10B981", background: "rgba(16,185,129,0.08)", padding: "2px 8px", borderRadius: 4, border: "1px solid rgba(16,185,129,0.15)" }}>
                        {healthy} Healthy
                    </span>
                )}
            </div>

            {/* Grouped diagnostics */}
            {groups.map(({ group, items }) => (
                <div key={group}>
                    <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)", marginBottom: 10 }}>
                        {group}
                    </div>
                    <div style={{ display: "grid", gap: 10, gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))" }}>
                        {items.map((item) => (
                            <RiskDiagCard
                                key={item.label}
                                label={item.label}
                                value={item.value}
                                hint={item.hint}
                                severity={item.severity}
                            />
                        ))}
                    </div>
                </div>
            ))}
        </div>
    );
}
