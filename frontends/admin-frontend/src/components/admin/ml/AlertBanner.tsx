import { AlertTriangle, Info } from "lucide-react";

interface AlertBannerProps {
    title: string;
    body: string;
    tone?: "warning" | "danger" | "info";
    compact?: boolean;
}

function toneColors(tone: AlertBannerProps["tone"]) {
    switch (tone) {
        case "danger":
            return {
                border: "rgba(239, 68, 68, 0.45)",
                background: "rgba(239, 68, 68, 0.12)",
                color: "var(--admin-red)",
            };
        case "info":
            return {
                border: "rgba(59, 130, 246, 0.45)",
                background: "rgba(59, 130, 246, 0.12)",
                color: "var(--admin-blue)",
            };
        default:
            return {
                border: "rgba(245, 158, 11, 0.45)",
                background: "rgba(245, 158, 11, 0.12)",
                color: "var(--admin-yellow)",
            };
    }
}

export function AlertBanner({ title, body, tone = "warning", compact = false }: AlertBannerProps) {
    const colors = toneColors(tone);
    const Icon = tone === "info" ? Info : AlertTriangle;

    return (
        <div
            className={`admin-alert-banner ${compact ? "compact" : ""}`}
            style={{
                borderColor: colors.border,
                background: colors.background,
            }}
        >
            <Icon className="w-5 h-5 flex-shrink-0" style={{ color: colors.color }} />
            <div>
                <div className="text-sm font-semibold" style={{ color: colors.color }}>
                    {title}
                </div>
                <div className={compact ? "text-xs mt-1" : "text-sm mt-1"} style={{ color: "var(--admin-text-secondary)" }}>
                    {body}
                </div>
            </div>
        </div>
    );
}
