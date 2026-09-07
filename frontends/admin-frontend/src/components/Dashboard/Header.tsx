import { ShieldCheck, Server } from "lucide-react";

export function Header() {
    const online = true; // Ideally synced with query status

    return (
        <header className="border-b bg-card/50 backdrop-blur-sm sticky top-0 z-10">
            <div className="container mx-auto px-4 h-16 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <ShieldCheck className="w-6 h-6 text-primary" />
                    <h1 className="text-xl font-bold tracking-tight">CosmicForg <span className="text-primary">Monitor</span></h1>
                </div>

                <div className="flex items-center gap-4 text-sm">
                    <div className="flex items-center gap-2 text-muted-foreground">
                        <Server className="w-4 h-4" />
                        <span>Port 8000</span>
                    </div>
                    <div className={`flex items-center gap-1.5 px-2 py-1 rounded-full text-xs font-medium ${online ? 'bg-green-500/15 text-green-500' : 'bg-red-500/15 text-red-500'}`}>
                        <span className={`w-2 h-2 rounded-full ${online ? 'bg-green-500' : 'bg-red-500'}`} />
                        {online ? 'System Active' : 'Disconnected'}
                    </div>
                </div>
            </div>
        </header>
    );
}
