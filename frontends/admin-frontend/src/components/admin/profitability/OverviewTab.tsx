import React from "react";
import {
    Activity, BarChart3, CheckCircle2, AlertTriangle,
    Percent, TrendingDown, TrendingUp, Wallet,
} from "lucide-react";
import type { ProfitabilityReportResponse } from "@/api/admin";
import { KPICard, SnapStat, formatNumber, formatPct, formatPnl, pnlColor } from "./shared";

interface Props { report: ProfitabilityReportResponse; }

function MiniStat({ label, icon, accent, value, detail }: {
    label: string; icon: React.ReactNode; accent: string; value: string; detail: string;
}) {
    return (
        <div style={{
            background: "var(--admin-bg-secondary)",
            border: "1px solid var(--admin-border-color)",
            borderLeft: `3px solid ${accent}`,
            borderRadius: 8, padding: "11px 13px",
        }}>
            <div style={{ display: "flex", alignItems: "center", gap: 5, marginBottom: 5 }}>
                <span style={{ color: accent, display: "flex" }}>{icon}</span>
                <span style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "var(--admin-text-muted)" }}>
                    {label}
                </span>
            </div>
            <div style={{ fontSize: 15, fontWeight: 800, fontFamily: "monospace", color: "var(--admin-text-primary)", marginBottom: 3 }}>
                {value}
            </div>
            <div style={{ fontSize: 10, color: "var(--admin-text-muted)", fontFamily: "monospace" }}>{detail}</div>
        </div>
    );
}

export function OverviewTab({ report }: Props) {
    const overall = report.overall;
    const risk    = report.risk_execution_quality;

    const bestSymbol  = report.per_symbol.length > 0
        ? report.per_symbol.reduce((a, b) => a.total_pnl > b.total_pnl ? a : b)
        : null;
    const worstSymbol = report.per_symbol.length > 0
        ? report.per_symbol.reduce((a, b) => a.total_pnl < b.total_pnl ? a : b)
        : null;

    const dupCount     = risk.duplicate_order_id_action_symbol_groups + risk.duplicate_exact_fill_groups;
    const missingCount = risk.missing_run_id_count + risk.missing_position_id_count;
    const nullCount    = risk.closed_fills_null_exit_reason + risk.closed_fills_null_r_multiple;

    return (
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>

            {/* Primary KPIs */}
            <div style={{ display: "grid", gap: 12, gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))" }}>
                <KPICard
                    label="Total Realized PnL"
                    value={formatPnl(overall.total_realized_pnl)}
                    icon={<Wallet className="w-3.5 h-3.5" style={{ color: pnlColor(overall.total_realized_pnl) }} />}
                    supportLabel="Closed execution trades"
                    supportValue={`${overall.closed_trades ?? 0}`}
                    tone={overall.total_realized_pnl >= 0 ? "positive" : "negative"}
                />
                <KPICard
                    label="Win Rate"
                    value={formatPct(overall.win_rate_pct)}
                    icon={<Percent className="w-3.5 h-3.5" style={{ color: (overall.win_rate_pct ?? 0) >= 50 ? "var(--admin-green)" : "var(--admin-text-muted)" }} />}
                    supportLabel="Total fills counted"
                    supportValue={`${overall.total_trades ?? 0} fills`}
                    tone={(overall.win_rate_pct ?? 0) >= 50 ? "positive" : "neutral"}
                />
                <KPICard
                    label="Profit Factor"
                    value={formatNumber(overall.profit_factor, 2)}
                    icon={<BarChart3 className="w-3.5 h-3.5" style={{ color: Number(overall.profit_factor ?? 0) >= 1 ? "var(--admin-green)" : "var(--admin-yellow)" }} />}
                    supportLabel="Avg R-multiple"
                    supportValue={`R ${formatNumber(overall.average_r_multiple, 2)}`}
                    tone={Number(overall.profit_factor ?? 0) >= 1 ? "positive" : "negative"}
                />
                <KPICard
                    label="Open Trades"
                    value={overall.open_trades}
                    icon={<Activity className="w-3.5 h-3.5" style={{ color: "#06B6D4" }} />}
                    supportLabel="Raw open fills"
                    supportValue={`${overall.raw_open_fills ?? 0} fills`}
                    tone="info"
                />
            </div>

            {/* Mini extremes row */}
            <div style={{ display: "grid", gap: 10, gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))" }}>
                <MiniStat
                    label="Best Symbol" icon={<TrendingUp className="w-3 h-3" />} accent="#10B981"
                    value={bestSymbol?.symbol || "—"}
                    detail={bestSymbol ? `${formatPnl(bestSymbol.total_pnl)} USDT · ${formatPct(bestSymbol.win_rate_pct)} WR` : "No data"}
                />
                <MiniStat
                    label="Worst Symbol" icon={<TrendingDown className="w-3 h-3" />} accent="#EF4444"
                    value={worstSymbol?.symbol || "—"}
                    detail={worstSymbol ? `${formatPnl(worstSymbol.total_pnl)} USDT · ${formatPct(worstSymbol.win_rate_pct)} WR` : "No data"}
                />
                <MiniStat
                    label="Best Trade" icon={<TrendingUp className="w-3 h-3" />} accent="#10B981"
                    value={overall.best_trade?.symbol || "—"}
                    detail={overall.best_trade ? `${formatPnl(overall.best_trade.realized_pnl)} USDT · R ${formatNumber(overall.best_trade.r_multiple, 2)}` : "No data"}
                />
                <MiniStat
                    label="Worst Trade" icon={<TrendingDown className="w-3 h-3" />} accent="#EF4444"
                    value={overall.worst_trade?.symbol || "—"}
                    detail={overall.worst_trade ? `${formatPnl(overall.worst_trade.realized_pnl)} USDT · R ${formatNumber(overall.worst_trade.r_multiple, 2)}` : "No data"}
                />
            </div>

            {/* Execution quality snapshot */}
            <div style={{
                background: "var(--admin-bg-secondary)",
                border: "1px solid var(--admin-border-color)",
                borderRadius: 10, padding: "14px 16px",
            }}>
                <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--admin-text-muted)", marginBottom: 12 }}>
                    Execution Quality Snapshot
                </div>
                <div style={{ display: "grid", gap: 14, gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))" }}>
                    <SnapStat label="Duplicate Groups"   value={dupCount}     ok={dupCount === 0} />
                    <SnapStat label="Missing Run/Pos ID" value={missingCount} ok={missingCount === 0} />
                    <SnapStat label="Null Exit/R Reason" value={nullCount}    ok={nullCount === 0} />
                    <SnapStat label="Avg Slippage"       value={`${formatNumber(risk.average_slippage_pct, 4)}%`} ok />
                </div>
            </div>
        </div>
    );
}
