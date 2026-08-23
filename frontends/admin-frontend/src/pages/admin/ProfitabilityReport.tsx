import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Activity, AlertTriangle, Loader2 } from "lucide-react";
import { getProfitabilityReport, type ProfitabilityReportResponse } from "@/api/admin";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { ExportButton } from "@/components/admin/common/ExportButton";
import { exportRows, formatDate } from "@/components/admin/profitability/shared";
import { ProfitabilityTabs, type TabId } from "@/components/admin/profitability/ProfitabilityTabs";
import { OverviewTab }          from "@/components/admin/profitability/OverviewTab";
import { PeriodPerformanceTab } from "@/components/admin/profitability/PeriodPerformanceTab";
import { TradeExtremesTab }     from "@/components/admin/profitability/TradeExtremesTab";
import { SizingEvidenceTab }    from "@/components/admin/profitability/SizingEvidenceTab";
import { SymbolPerformanceTab } from "@/components/admin/profitability/SymbolPerformanceTab";
import { RiskQualityTab }       from "@/components/admin/profitability/RiskQualityTab";

function renderTab(tab: TabId, report: ProfitabilityReportResponse) {
    switch (tab) {
        case "overview":  return <OverviewTab          report={report} />;
        case "period":    return <PeriodPerformanceTab report={report} />;
        case "extremes":  return <TradeExtremesTab     report={report} />;
        case "sizing":    return <SizingEvidenceTab    report={report} />;
        case "symbols":   return <SymbolPerformanceTab report={report} />;
        case "risk":      return <RiskQualityTab       report={report} />;
    }
}

export default function ProfitabilityReport() {
    const [activeTab, setActiveTab] = useState<TabId>("overview");

    const { data: report, isLoading, isError, refetch, isFetching } = useQuery({
        queryKey: ["adminProfitabilityReport"],
        queryFn: getProfitabilityReport,
        refetchInterval: 60_000,
    });

    const ready = !isLoading && !isError && report?.overall && report?.risk_execution_quality;

    return (
        <AdminLayout>
            <div style={{ display: "flex", flexDirection: "column", gap: 0, maxWidth: 1600, margin: "0 auto", paddingBottom: 32 }}>

                {/* ── Header ─────────────────────────────────────────────────── */}
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "flex-start", justifyContent: "space-between", gap: 12, marginBottom: 20 }}>
                    <div>
                        <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 7, marginBottom: 5 }}>
                            <h1 style={{ margin: 0, fontSize: "1.45rem", fontWeight: 800, letterSpacing: "-0.025em", color: "var(--admin-text-primary)", lineHeight: 1.15 }}>
                                Profitability Report
                            </h1>
                            {(["Admin Portal", report ? "Live" : null] as (string | null)[]).filter(Boolean).map((badge, i) => (
                                <span key={badge!} style={{
                                    fontSize: 8, fontWeight: 800, padding: "2px 7px", borderRadius: 4,
                                    background: i === 0 ? "rgba(59,130,246,0.07)" : "rgba(16,185,129,0.07)",
                                    color:      i === 0 ? "var(--admin-blue)"      : "var(--admin-green)",
                                    border:     i === 0 ? "1px solid rgba(59,130,246,0.15)" : "1px solid rgba(16,185,129,0.15)",
                                    textTransform: "uppercase" as const, letterSpacing: "0.08em",
                                }}>{badge}</span>
                            ))}
                        </div>
                        <p style={{ margin: 0, fontSize: 12, color: "var(--admin-text-secondary)", lineHeight: 1.45 }}>
                            Executed-trade performance, symbol profitability, sizing evidence, and execution-quality diagnostics.
                        </p>
                        {report && (
                            <p style={{ margin: "5px 0 0", fontSize: 10, color: "var(--admin-text-muted)", letterSpacing: "0.01em", lineHeight: 1.6 }}>
                                Generated {formatDate(report.generated_at)}
                                <span style={{ margin: "0 5px", opacity: 0.35 }}>·</span>
                                Source: <span style={{ fontFamily: "monospace" }}>trade_fills</span> only
                                <span style={{ margin: "0 5px", opacity: 0.35 }}>·</span>
                                Excludes backfill + SHADOW
                            </p>
                        )}
                    </div>
                    <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
                        <button
                            className="admin-btn admin-btn-secondary"
                            onClick={() => refetch()}
                            disabled={isFetching}
                            style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, height: 30, padding: "0 10px" }}
                        >
                            {isFetching ? <Loader2 className="w-3 h-3 animate-spin" /> : <Activity className="w-3 h-3" />}
                            Refresh
                        </button>
                        <ExportButton data={exportRows(report)} filename="profitability_report" label="Export Symbols" />
                    </div>
                </div>

                {/* ── Tab bar ────────────────────────────────────────────────── */}
                <div style={{ marginBottom: 24 }}>
                    <ProfitabilityTabs active={activeTab} onChange={setActiveTab} />
                </div>

                {/* ── Content ────────────────────────────────────────────────── */}
                {isLoading ? (
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", padding: "80px 0" }}>
                        <Loader2 className="w-7 h-7 animate-spin" style={{ color: "var(--admin-blue)" }} />
                    </div>
                ) : isError || !ready ? (
                    <div className="admin-card" style={{ display: "flex", alignItems: "center", gap: 10 }}>
                        <AlertTriangle className="w-4 h-4 flex-shrink-0" style={{ color: "var(--admin-red)" }} />
                        <div>
                            <div style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-red)" }}>Unable to load profitability report</div>
                            <div style={{ fontSize: 11, color: "var(--admin-text-muted)", marginTop: 2 }}>Check backend connectivity and try refreshing.</div>
                        </div>
                        <button className="admin-btn admin-btn-secondary" style={{ marginLeft: "auto" }} onClick={() => refetch()}>Retry</button>
                    </div>
                ) : (
                    renderTab(activeTab, report!)
                )}
            </div>
        </AdminLayout>
    );
}
