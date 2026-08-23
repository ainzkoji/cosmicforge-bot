import { Download } from "lucide-react";
import { exportToCSV, exportToJSON, createTimestampedFilename } from "@/utils/export";

interface ExportButtonProps {
    data: any[];
    filename: string;
    label?: string;
    className?: string;
}

export function ExportButton({ data, filename, label = "Export", className = "" }: ExportButtonProps) {
    const handleExport = (format: 'csv' | 'json') => {
        const timestampedFilename = createTimestampedFilename(filename);

        if (format === 'csv') {
            exportToCSV(data, timestampedFilename);
        } else {
            exportToJSON(data, timestampedFilename);
        }
    };

    return (
        <div className="relative group">
            <button
                className={`admin-btn admin-btn-secondary ${className}`}
                onClick={() => handleExport('csv')}
            >
                <Download className="w-4 h-4" />
                {label}
            </button>

            {/* Dropdown for format selection (hover) */}
            <div className="absolute right-0 mt-1 hidden group-hover:block z-10 min-w-[120px]">
                <div className="admin-card py-1 shadow-lg">
                    <button
                        onClick={() => handleExport('csv')}
                        className="w-full text-left px-4 py-2 text-sm hover:bg-opacity-10 hover:bg-white transition-colors"
                        style={{ color: 'var(--admin-text-primary)' }}
                    >
                        Export as CSV
                    </button>
                    <button
                        onClick={() => handleExport('json')}
                        className="w-full text-left px-4 py-2 text-sm hover:bg-opacity-10 hover:bg-white transition-colors"
                        style={{ color: 'var(--admin-text-primary)' }}
                    >
                        Export as JSON
                    </button>
                </div>
            </div>
        </div>
    );
}
