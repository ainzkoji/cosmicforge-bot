import { useState } from "react";
import { Key, Copy, RefreshCw, BarChart2, Check } from "lucide-react";

export default function DeveloperSettings() {
    const [keys, setKeys] = useState([
        { id: "pk_live_...", name: "Production Key", created: "2023-10-15", lastUsed: "2 mins ago" },
        { id: "pk_test_...", name: "Test Key", created: "2023-11-02", lastUsed: "1 day ago" }
    ]);
    const [copied, setCopied] = useState("");

    const handleCopy = (keyId: string) => {
        setCopied(keyId);
        navigator.clipboard.writeText(keyId);
        setTimeout(() => setCopied(""), 2000);
    };

    return (
        <div className="max-w-4xl mx-auto space-y-8 animate-in fade-in">
            <div>
                <h1 className="text-3xl font-bold mb-2">My API Keys</h1>
                <p className="text-muted-foreground">Manage your API keys for programmatic access to the CosmicForge platform.</p>
            </div>

            {/* Key List */}
            <div className="bg-card border border-border rounded-xl overflow-hidden">
                <div className="p-6 border-b border-border flex justify-between items-center">
                    <h3 className="font-bold text-lg">Active Keys</h3>
                    <button className="px-4 py-2 bg-primary text-primary-foreground rounded-lg text-sm font-bold hover:bg-primary/90 transition-colors flex items-center gap-2">
                        <Key className="w-4 h-4" /> Generate New Key
                    </button>
                </div>
                <div className="divide-y divide-border">
                    {keys.map((key) => (
                        <div key={key.id} className="p-6 flex flex-col md:flex-row items-center justify-between gap-4">
                            <div>
                                <div className="font-bold text-lg mb-1">{key.name}</div>
                                <div className="font-mono text-sm text-muted-foreground bg-muted/50 px-2 py-1 rounded w-fit">
                                    {key.id}••••••••••••
                                </div>
                            </div>
                            <div className="flex items-center gap-6 text-sm text-muted-foreground">
                                <div>Created: {key.created}</div>
                                <div>Last used: {key.lastUsed}</div>
                                <button
                                    onClick={() => handleCopy(key.id)}
                                    className="p-2 hover:bg-muted rounded transition-colors text-foreground"
                                    title="Copy Key ID"
                                >
                                    {copied === key.id ? <Check className="w-4 h-4 text-green-500" /> : <Copy className="w-4 h-4" />}
                                </button>
                                <button className="p-2 hover:bg-muted rounded transition-colors text-red-500 hover:bg-red-500/10">
                                    Revoke
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {/* Stats */}
            <div className="bg-card border border-border rounded-xl p-6">
                <h3 className="font-bold text-lg mb-6 flex items-center gap-2">
                    <BarChart2 className="w-5 h-5" /> API Usage
                </h3>
                <div className="h-64 flex items-end justify-between gap-2">
                    {[45, 60, 30, 80, 55, 90, 40, 70, 50, 65, 85, 95].map((h, i) => (
                        <div key={i} className="w-full bg-primary/20 hover:bg-primary/40 rounded-t-sm relative group transition-colors" style={{ height: `${h}%` }}>
                            <div className="absolute bottom-full mb-2 left-1/2 -translate-x-1/2 bg-popover text-popover-foreground text-xs px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity">
                                {h * 100} reqs
                            </div>
                        </div>
                    ))}
                </div>
                <div className="flex justify-between text-xs text-muted-foreground mt-2">
                    <span>00:00</span>
                    <span>12:00</span>
                    <span>23:59</span>
                </div>
            </div>

            {/* Docs Link */}
            <div className="bg-muted/30 border border-border rounded-xl p-6 flex flex-col md:flex-row justify-between items-center gap-4">
                <div>
                    <h3 className="font-bold">Developer Documentation</h3>
                    <p className="text-sm text-muted-foreground">Read our guides and API reference to build your integration.</p>
                </div>
                <button className="px-6 py-2 border border-border rounded-lg hover:bg-muted transition-colors">
                    View Docs
                </button>
            </div>
        </div>
    );
}
