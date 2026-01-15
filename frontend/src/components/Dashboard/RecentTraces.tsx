import { useState } from "react";
import { Activity, ChevronDown, ChevronRight, CheckCircle, XCircle, Shield, Zap } from "lucide-react";
import { Trace } from "@/api/client";

export function RecentTraces({ traces }: { traces: Trace[] }) {
    if (!traces.length) return <div className="text-muted-foreground">No recent activity.</div>;

    return (
        <div className="space-y-4">
            <h2 className="text-xl font-bold flex items-center gap-2">
                <Activity className="w-5 h-5 text-blue-500" />
                Decision Stream
            </h2>
            <div className="space-y-2">
                {traces.map((trace) => (
                    <TraceItem key={trace.trace_id} trace={trace} />
                ))}
            </div>
        </div>
    );
}

function TraceItem({ trace }: { trace: Trace }) {
    const [expanded, setExpanded] = useState(false);

    return (
        <div className="rounded-lg border bg-card text-card-foreground shadow-sm overflow-hidden">
            <div
                className="p-3 flex items-center justify-between hover:bg-accent/50 transition-colors cursor-pointer"
                onClick={() => setExpanded(!expanded)}
            >
                <div className="flex items-center gap-3">
                    {expanded ? <ChevronDown className="w-4 h-4 text-muted-foreground" /> : <ChevronRight className="w-4 h-4 text-muted-foreground" />}
                    <div className="flex flex-col">
                        <span className="font-mono font-bold text-sm flex items-center gap-2">
                            {trace.symbol}
                            <span className="text-[10px] font-normal px-1.5 py-0.5 rounded-full bg-muted text-muted-foreground border">
                                {trace.timeframe}
                            </span>
                        </span>
                        <div className="flex items-center gap-2 text-xs text-muted-foreground">
                            <span>{new Date(trace.ts).toLocaleTimeString()}</span>
                            <span>•</span>
                            <span>${trace.last_price?.toFixed(2)}</span>
                        </div>
                    </div>
                </div>

                <div className="flex flex-col items-end">
                    <Badge signal={trace.signal} />
                    <span className="text-xs text-muted-foreground mt-1">{trace.execution_status || "Evaluated"}</span>
                </div>
            </div>

            {expanded && (
                <div className="px-3 pb-3 pt-0 border-t bg-muted/20">
                    <TraceDetails trace={trace} />
                </div>
            )}
        </div>
    );
}

function TraceDetails({ trace }: { trace: Trace }) {
    // Parse JSON details safely
    const strategies = tryParse(trace.strategy_signals_json);
    const gateDetails = tryParse(trace.gate_details_json);

    return (
        <div className="grid gap-4 py-3 text-sm">
            {/* Strategy Section */}
            <div className="grid gap-2">
                <h4 className="font-semibold flex items-center gap-2 text-muted-foreground">
                    <Zap className="w-3 h-3" /> Strategy
                </h4>
                <div className="bg-background rounded border p-2 space-y-2">
                    <div className="flex justify-between items-center">
                        <span className="text-muted-foreground">Final Signal:</span>
                        <span className="font-medium">{trace.signal}</span>
                    </div>
                    {/* Render detailed strategy signals if available */}
                    {Array.isArray(strategies) && strategies.length > 0 && (
                        <div className="text-xs space-y-1 border-t pt-2">
                            {strategies.map((s: any, idx: number) => (
                                <div key={idx} className="flex justify-between">
                                    <span className="text-muted-foreground">{s.strategy || "Valid"}</span>
                                    <span className="font-mono">{s.signal} ({s.confidence?.toFixed(2)})</span>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* Risk Gate Section */}
            <div className="grid gap-2">
                <h4 className="font-semibold flex items-center gap-2 text-muted-foreground">
                    <Shield className="w-3 h-3" /> Risk Gate
                </h4>
                <div className={`rounded border p-2 space-y-2 ${trace.gate_allowed ? 'bg-green-500/10 border-green-500/20' : 'bg-red-500/10 border-red-500/20'}`}>
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                            {trace.gate_allowed ? <CheckCircle className="w-4 h-4 text-green-500" /> : <XCircle className="w-4 h-4 text-red-500" />}
                            <span className={trace.gate_allowed ? "text-green-700 dark:text-green-400 font-medium" : "text-red-700 dark:text-red-400 font-medium"}>
                                {trace.gate_allowed ? "Approved" : "Blocked"}
                            </span>
                        </div>
                        {!trace.gate_allowed && (
                            <span className="text-xs font-mono text-red-600 dark:text-red-400">{trace.gate_reason}</span>
                        )}
                    </div>

                    {/* Gate Details Expansion */}
                    {gateDetails && typeof gateDetails === 'object' && (
                        <div className="text-xs border-t border-black/5 dark:border-white/5 pt-2 grid grid-cols-2 gap-1">
                            {Object.entries(gateDetails).map(([k, v]) => (
                                <div key={k} className="flex justify-between">
                                    <span className="text-muted-foreground capitalize">{k}:</span>
                                    <span className="font-mono truncate ml-2">{String(v)}</span>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* System & Risk State Section */}
            <div className="grid gap-2">
                <h4 className="font-semibold flex items-center gap-2 text-muted-foreground">
                    <Activity className="w-3 h-3" /> System State
                </h4>
                <div className="bg-background rounded border p-2 grid grid-cols-2 gap-2 text-xs">
                    <div>
                        <span className="text-muted-foreground block">Kill Switch</span>
                        <span className={`font-mono font-bold ${trace.kill_switch_state === 'HARD_KILL' ? 'text-red-500' : 'text-green-500'}`}>
                            {trace.kill_switch_state || 'NORMAL'}
                        </span>
                    </div>
                    <div>
                        <span className="text-muted-foreground block">Freeze</span>
                        <span className={`font-mono ${trace.exposure_freeze ? 'text-orange-500' : 'text-muted-foreground'}`}>
                            {trace.exposure_freeze ? 'ACTIVE' : 'OFF'}
                        </span>
                    </div>
                    <div>
                        <span className="text-muted-foreground block">Regime</span>
                        <span className="font-mono">{trace.regime_state || 'STD'} ({(trace.regime_confidence || 0).toFixed(2)})</span>
                    </div>
                    <div>
                        <span className="text-muted-foreground block">Risk Budget</span>
                        <span className="font-mono">
                            ${trace.portfolio_risk_used?.toFixed(0)} / ${trace.portfolio_risk_budget?.toFixed(0)}
                        </span>
                    </div>
                </div>
            </div>

            {/* Execution Details (if applicable) */}
            {(trace.execution_status !== "None" && trace.execution_status !== "Evaluated") && (
                <div className="grid gap-2">
                    <h4 className="font-semibold flex items-center gap-2 text-muted-foreground">Execution</h4>
                    <div className="bg-background rounded border p-2 grid grid-cols-2 gap-2 text-xs">
                        <div>
                            <span className="text-muted-foreground block">Action</span>
                            <span className="font-mono">{trace.intended_action}</span>
                        </div>
                        <div>
                            <span className="text-muted-foreground block">Status</span>
                            <span className="font-mono">{trace.execution_status}</span>
                        </div>
                        {trace.order_id && (
                            <div className="col-span-2">
                                <span className="text-muted-foreground block">Order ID</span>
                                <span className="font-mono text-[10px] break-all">{trace.order_id}</span>
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}

function tryParse(jsonStr?: string) {
    if (!jsonStr) return null;
    try {
        return JSON.parse(jsonStr);
    } catch {
        return null;
    }
}

function Badge({ signal }: { signal: string }) {
    const color =
        signal === "BUY" ? "bg-green-500/15 text-green-500 border-green-500/30" :
            signal === "SELL" ? "bg-red-500/15 text-red-500 border-red-500/30" :
                "bg-gray-500/15 text-gray-500 border-gray-500/30";

    return (
        <span className={`px-2 py-0.5 rounded text-xs font-bold border ${color}`}>
            {signal}
        </span>
    );
}
