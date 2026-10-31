import { GraduationCap } from "lucide-react";

/**
 * Learning content is not available yet. This page intentionally shows no courses, authors, progress or
 * certification until real content exists. Whether to keep this page or remove it is an owner decision
 * (Section L-05 in docs/implementation/MASTER_PLAN_I_L_COMPLIANCE.md).
 */
export default function Academy() {
    return (
        <div className="max-w-4xl mx-auto space-y-8 animate-in fade-in">
            <div>
                <h1 className="text-3xl font-bold mb-2">Academy</h1>
                <p className="text-muted-foreground">Learning resources about automated trading.</p>
            </div>

            <div role="status" className="bg-card border border-border rounded-xl p-10 text-center">
                <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center mx-auto mb-4 text-muted-foreground">
                    <GraduationCap className="w-6 h-6" />
                </div>
                <h2 className="font-bold text-lg mb-2">Not available yet</h2>
                <p className="text-sm text-muted-foreground max-w-md mx-auto">
                    CosmicForge learning content is not available yet. There are no courses, progress tracking or
                    certifications on this page.
                </p>
            </div>
        </div>
    );
}
