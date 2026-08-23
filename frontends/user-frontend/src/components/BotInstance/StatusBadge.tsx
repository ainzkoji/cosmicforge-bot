
import { Activity, Pause, Square, AlertTriangle } from 'lucide-react';
import { cn } from '@/lib/utils';

type Status = 'active' | 'paused' | 'stopped' | 'error';

interface StatusBadgeProps {
    status: Status | string;
    className?: string;
    showIcon?: boolean;
}

export const StatusBadge = ({ status, className, showIcon = true }: StatusBadgeProps) => {
    const statusMap: Record<string, { color: string; label: string; icon: any }> = {
        active: { color: 'bg-green-500/10 text-green-500 border-green-500/20', label: 'Active', icon: Activity },
        paused: { color: 'bg-amber-500/10 text-amber-500 border-amber-500/20', label: 'Paused', icon: Pause },
        stopped: { color: 'bg-muted text-muted-foreground border-border', label: 'Stopped', icon: Square },
        error: { color: 'bg-red-500/10 text-red-500 border-red-500/20', label: 'Error', icon: AlertTriangle },
    };

    const config = statusMap[status] || statusMap.stopped;
    const Icon = config.icon;

    return (
        <div className={cn(
            "flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border transition-all",
            config.color,
            status === 'active' && "animate-pulse-subtle shadow-[0_0_10px_-4px_rgba(34,197,94,0.5)]",
            className
        )}>
            {showIcon && <Icon className="w-3.5 h-3.5" />}
            <span className="capitalize">{config.label}</span>
        </div>
    );
};
