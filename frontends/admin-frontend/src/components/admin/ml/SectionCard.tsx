import type { ReactNode } from "react";

interface SectionCardProps {
    title: string;
    subtitle?: string;
    action?: ReactNode;
    children: ReactNode;
}

export function SectionCard({ title, subtitle, action, children }: SectionCardProps) {
    return (
        <section className="admin-card admin-ml-section-card space-y-4">
            <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                <div>
                    <h2 className="text-lg font-semibold tracking-tight" style={{ color: "var(--admin-text-primary)" }}>
                        {title}
                    </h2>
                    {subtitle ? (
                        <p className="mt-1 text-sm leading-6" style={{ color: "var(--admin-text-secondary)" }}>
                            {subtitle}
                        </p>
                    ) : null}
                </div>
                {action ? <div>{action}</div> : null}
            </div>
            {children}
        </section>
    );
}
