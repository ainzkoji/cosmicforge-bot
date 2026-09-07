import { Trace } from "@/api/client";
import { TrendingUp, Wallet, AlertOctagon } from "lucide-react";

export function RiskPanel({ latestTrace }: { latestTrace?: Trace }) {
    if (!latestTrace) return null;

    return (
        <div className="grid gap-4 md:grid-cols-3 mb-8">
            <Card
                label="Total Equity"
                value={`$${latestTrace.equity.toFixed(2)}`}
                icon={Wallet}
                trend="+0.0%"
            />
            <Card
                label="Active Positions"
                value={latestTrace.open_positions_count.toString()}
                icon={TrendingUp}
                subtext="Across all symbols"
            />
            <Card
                label="Margin Level"
                value={latestTrace.margin_level && latestTrace.margin_level > 900 ? "> 999%" : `${latestTrace.margin_level?.toFixed(1) || '0.0'}%`}
                icon={AlertOctagon}
                subtext={latestTrace.margin_used && latestTrace.margin_used > 0 ? `Used: $${latestTrace.margin_used.toFixed(2)}` : "Healthy > 200%"}
                alert={latestTrace.margin_level !== undefined && latestTrace.margin_level < 200 && latestTrace.margin_level > 0}
            />
        </div>
    );
}

function Card({ label, value, icon: Icon, trend, subtext, alert }: any) {
    return (
        <div className={`p-6 rounded-xl border bg-card text-card-foreground shadow-sm ${alert ? 'border-red-500/50 bg-red-500/5' : ''}`}>
            <div className="flex items-center justify-between mb-4">
                <span className="text-sm font-medium text-muted-foreground">{label}</span>
                <Icon className={`w-4 h-4 ${alert ? 'text-red-500' : 'text-muted-foreground'}`} />
            </div>
            <div className="text-2xl font-bold">{value}</div>
            {(trend || subtext) && (
                <p className="text-xs text-muted-foreground mt-1">
                    {trend && <span className="text-green-500 mr-1">{trend}</span>}
                    {subtext}
                </p>
            )}
        </div>
    );
}
