import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportsAPI } from '@/api/reports';
import { Download, FileText, AlertTriangle } from 'lucide-react';
import { apiClient } from '@/api/client';

interface TaxReportExportProps {
    brokerAccountId?: string;
}

export const TaxReportExport: React.FC<TaxReportExportProps> = ({ brokerAccountId }) => {
    const currentYear = new Date().getFullYear();
    const [selectedYear, setSelectedYear] = useState(currentYear - 1); // Default to previous year

    const handleExport = (format: 'csv' | 'json' | 'pdf') => {
        const token = localStorage.getItem('access_token') || '';
        if (format === 'csv') {
            // Direct download via URL for CSV
            const url = ReportsAPI.exportTaxReportCsvUrl(selectedYear, token, brokerAccountId);
            window.open(url, '_blank');
        } else if (format === 'pdf') {
            const url = ReportsAPI.exportTaxReportPdfUrl(selectedYear, token, brokerAccountId);
            window.open(url, '_blank');
        } else {
            // For JSON, we could fetch and download blob, but let's stick to CSV as primary for now
            // Or implement JSON download logic here
            alert("JSON export via UI coming soon. Please use CSV for now.");
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
                        className="flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition"
                    >
                        <Download className="h-4 w-4" />
                        Export CSV
                    </button>
                    <button
                        onClick={() => handleExport('pdf')}
                        className="flex items-center justify-center gap-2 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition"
                    >
                        <FileText className="h-4 w-4" />
                        Export PDF
                    </button>
                </div>
            </div>
        </div>
    );
};
