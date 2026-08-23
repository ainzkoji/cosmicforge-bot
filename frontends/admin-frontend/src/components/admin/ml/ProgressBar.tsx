interface ProgressBarProps {
    label: string;
    value: number;
    max: number;
    tone?: "success" | "warning" | "danger" | "info";
    helper?: string;
}

function toneColor(tone: ProgressBarProps["tone"]) {
    switch (tone) {
        case "success":
            return "var(--admin-green)";
        case "warning":
            return "var(--admin-yellow)";
        case "danger":
            return "var(--admin-red)";
        default:
            return "var(--admin-blue)";
    }
}

export function ProgressBar({ label, value, max, tone = "info", helper }: ProgressBarProps) {
    const safeMax = max > 0 ? max : 1;
    const percent = Math.min((value / safeMax) * 100, 100);

    return (
        <div className="space-y-2">
            <div className="flex items-center justify-between gap-3">
                <div>
                    <div className="text-sm font-semibold" style={{ color: "var(--admin-text-primary)" }}>
                        {label}
                    </div>
                    {helper ? (
                        <div className="text-xs" style={{ color: "var(--admin-text-muted)" }}>
                            {helper}
                        </div>
                    ) : null}
                </div>
                <div className="text-sm font-semibold" style={{ color: "var(--admin-text-secondary)" }}>
                    {value.toLocaleString()} / {max.toLocaleString()}
                </div>
            </div>
            <div className="admin-progress-track">
                <div
                    className="admin-progress-fill"
                    style={{
                        width: `${percent}%`,
                        background: toneColor(tone),
                    }}
                />
            </div>
        </div>
    );
}
