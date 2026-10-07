import { Key } from "lucide-react";

/**
 * Programmatic API access is not available yet. This page intentionally shows
 * no keys, usage figures or controls until the feature exists.
 */
export default function DeveloperSettings() {
    return (
        <div className="max-w-4xl mx-auto space-y-8 animate-in fade-in">
            <div>
                <h1 className="text-3xl font-bold mb-2">My API Keys</h1>
                <p className="text-muted-foreground">Programmatic access to the CosmicForge platform.</p>
            </div>

            <div role="status" className="bg-card border border-border rounded-xl p-10 text-center">
                <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center mx-auto mb-4 text-muted-foreground">
                    <Key className="w-6 h-6" />
                </div>
                <h2 className="font-bold text-lg mb-2">Not available yet</h2>
                <p className="text-sm text-muted-foreground max-w-md mx-auto">
                    CosmicForge API keys and developer documentation are not available yet. You have no API keys,
                    and none can be created from this page.
                </p>
            </div>
        </div>
    );
}
