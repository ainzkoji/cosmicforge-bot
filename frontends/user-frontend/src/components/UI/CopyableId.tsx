import { useState } from 'react';
import { Copy, Check } from 'lucide-react';

interface CopyableIdProps {
    id: string;
    label?: string;
    className?: string;
    maxLength?: number;
}

export function CopyableId({ id, label, className = "", maxLength = 12 }: CopyableIdProps) {
    const [copied, setCopied] = useState(false);

    const handleCopy = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        navigator.clipboard.writeText(id);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    const displayId = id.length > maxLength 
        ? `${id.substring(0, Math.floor(maxLength / 2))}...${id.substring(id.length - Math.floor(maxLength / 2))}`
        : id;

    return (
        <div 
            className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-muted/40 border border-border/50 group/copyable tooltip-trigger ${className}`}
            title={`${label ? label + ': ' : ''}${id}`}
            onClick={handleCopy}
            style={{ cursor: 'pointer' }}
        >
            {label && <span className="text-muted-foreground text-xs font-medium">{label}:</span>}
            <span className="font-mono text-xs text-foreground/80">{displayId}</span>
            <button 
                className="text-muted-foreground hover:text-foreground transition-colors ml-0.5 focus:outline-none flex items-center justify-center w-3 h-3"
                aria-label="Copy to clipboard"
            >
                {copied ? <Check className="w-3 h-3 text-green-500" /> : <Copy className="w-3 h-3 opacity-50 group-hover/copyable:opacity-100" />}
            </button>
            {copied && <span className="text-[10px] text-green-500 absolute -translate-y-6 bg-green-500/10 px-1 rounded backdrop-blur-sm pointer-events-none transition-opacity duration-200">Copied!</span>}
        </div>
    );
}
