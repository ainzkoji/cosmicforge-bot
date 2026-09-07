import { useState } from "react";
import { AdvancedDashboard } from "@/components/Dashboard/AdvancedDashboard";
import { BeginnerDashboard } from "@/components/Dashboard/BeginnerDashboard";
import { Settings2, User, Zap } from "lucide-react";

export default function Home() {
    const [viewMode, setViewMode] = useState<'beginner' | 'advanced'>('advanced');

    return (
        <div className="max-w-[1600px] mx-auto">
            {/* View Switcher Control - For Demo/Dev purposes */}
            <div className="flex justify-end mb-6">
                <div className="bg-muted/50 p-1 rounded-lg flex items-center gap-1">
                    <button
                        onClick={() => setViewMode('beginner')}
                        className={`px-3 py-1.5 rounded-md text-xs font-medium flex items-center gap-2 transition-all ${viewMode === 'beginner'
                            ? 'bg-background shadow text-foreground'
                            : 'text-muted-foreground hover:text-foreground'
                            }`}
                    >
                        <User className="w-3 h-3" /> Beginner
                    </button>
                    <button
                        onClick={() => setViewMode('advanced')}
                        className={`px-3 py-1.5 rounded-md text-xs font-medium flex items-center gap-2 transition-all ${viewMode === 'advanced'
                            ? 'bg-background shadow text-foreground'
                            : 'text-muted-foreground hover:text-foreground'
                            }`}
                    >
                        <Zap className="w-3 h-3" /> Pro
                    </button>
                </div>
            </div>

            {viewMode === 'beginner' ? <BeginnerDashboard /> : <AdvancedDashboard />}
        </div>
    );
}
