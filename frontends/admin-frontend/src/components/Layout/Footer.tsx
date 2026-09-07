import { ExternalLink, RefreshCw } from "lucide-react";

export function Footer() {
    return (
        <footer className="border-t bg-card/30 backdrop-blur-sm mt-auto">
            <div className="container mx-auto px-4 py-8">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                    {/* Brand & Copyright */}
                    <div className="flex flex-col gap-4">
                        <div className="flex items-center gap-2">
                            <img
                                src="/src/assets/logo.png"
                                alt="CosmicForge"
                                className="w-8 h-8 object-contain"
                            />
                            <span className="font-bold tracking-tight text-lg">CosmicForge Stratos</span>
                        </div>
                        <p className="text-sm text-muted-foreground">
                            Advanced market analysis and automated execution system with deterministic monitoring.
                        </p>
                        <p className="text-xs text-muted-foreground mt-2">
                            © 2026 CosmicForge AI. All rights reserved.
                        </p>
                    </div>

                    {/* Resources */}
                    <div className="flex flex-col gap-4">
                        <h3 className="font-semibold text-sm tracking-wide uppercase text-muted-foreground">Resources</h3>
                        <ul className="flex flex-col gap-2 text-sm">
                            <li>
                                <a href="#" className="flex items-center gap-2 hover:text-primary transition-colors text-muted-foreground">
                                    <ExternalLink className="w-3 h-3" /> API Documentation
                                </a>
                            </li>
                            <li>
                                <a href="#" className="flex items-center gap-2 hover:text-primary transition-colors text-muted-foreground">
                                    <ExternalLink className="w-3 h-3" /> System Status
                                </a>
                            </li>
                        </ul>
                    </div>

                    {/* System Info */}
                    <div className="flex flex-col gap-4">
                        <h3 className="font-semibold text-sm tracking-wide uppercase text-muted-foreground">System</h3>
                        <div className="flex flex-col gap-2 text-sm text-muted-foreground">
                            <div className="flex justify-between items-center border-b border-border/50 pb-2">
                                <span>Version</span>
                                <span className="font-mono text-xs bg-muted px-2 py-0.5 rounded">v2.0.0-rc1</span>
                            </div>
                            <div className="flex justify-between items-center border-b border-border/50 pb-2">
                                <span>Environment</span>
                                <span className="font-mono text-xs text-green-500">Production</span>
                            </div>
                            <div className="flex justify-between items-center pt-1">
                                <span>Last Sync</span>
                                <div className="flex items-center gap-1.5 text-xs">
                                    <RefreshCw className="w-3 h-3 animate-[spin_10s_linear_infinite] opacity-50" />
                                    <span>Auto (100ms)</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </footer>
    );
}
