import React, { useState } from 'react';
import { ReportsAPI } from '@/api/reports';
import { Download, FileText, AlertTriangle } from 'lucide-react';

interface TaxReportExportProps {
    brokerAccountId?: string;
}

export const TaxReportExport: React.FC<TaxReportExportProps> = ({ brokerAccountId }) => {
    const currentYear = new Date().getFullYear();
    const [selectedYear, setSelectedYear] = useState(currentYear - 1); // Default to previous year

    const [exporting, setExporting] = useState<'csv' | 'pdf' | null>(null);
    const [exportError, setExportError] = useState<string | null>(null);

    // The export endpoints require the Authorization header, so the file is
    // fetched with the session and saved from a Blob instead of opening a URL.
    const handleExport = async (format: 'csv' | 'pdf') => {
        setExporting(format);
        setExportError(null);
        try {
            await ReportsAPI.downloadTaxReport(selectedYear, format, brokerAccountId);
        } catch (err) {
            setExportError(err instanceof Error ? err.message : 'Export failed');
        } finally {
            setExporting(null);
        }
    };

    return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-sm p-6">
            <div className="flex items-start justify-between mb-4">
                <div>
                    <h3 className="text-lg font-semibold text-gray-900">Tax Reports</h3>
                    <p className="text-sm text-gray-500 mt-1">
                        Download execution-based trade reports for tax purposes.
                    </p>
                </div>
                <div className="p-2 bg-blue-50 text-blue-600 rounded-lg">
                    <FileText className="h-6 w-6" />
                </div>
            </div>

            <div className="bg-yellow-50 border border-yellow-200 rounded-md p-3 mb-6">
                <div className="flex gap-2 text-yellow-800 text-sm">
                    <AlertTriangle className="h-5 w-5 flex-shrink-0" />
                    <p>
                        <strong>Disclaimer:</strong> This is an execution-based report. It does NOT use FIFO/LIFO matching.
                        Consult a tax professional for accurate tax filing.
                    </p>
                </div>
            </div>

            <div className="flex items-end gap-4">
                <div className="flex-1">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Tax Year</label>
                    <select
                        value={selectedYear}
                        onChange={(e) => setSelectedYear(Number(e.target.value))}
                        className="block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm px-3 py-2 border"
                    >
                        {[currentYear, currentYear - 1, currentYear - 2].map(year => (
                            <option key={year} value={year}>{year}</option>
                        ))}
                    </select>
                </div>
                <div className="flex gap-2">
                    <button
                        onClick={() => handleExport('csv')}
                        disabled={exporting !== null}
                        className="flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition disabled:opacity-50"
                    >
                        <Download className="h-4 w-4" />
                        {exporting === 'csv' ? 'Preparing…' : 'Export CSV'}
                    </button>
                    <button
                        onClick={() => handleExport('pdf')}
                        disabled={exporting !== null}
                        className="flex items-center justify-center gap-2 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition disabled:opacity-50"
                    >
                        <FileText className="h-4 w-4" />
                        {exporting === 'pdf' ? 'Preparing…' : 'Export PDF'}
                    </button>
                </div>
            </div>

            {exportError && (
                <p role="alert" className="mt-4 text-sm text-red-600">
                    Export failed: {exportError}
                </p>
            )}
        </div>
    );
};
