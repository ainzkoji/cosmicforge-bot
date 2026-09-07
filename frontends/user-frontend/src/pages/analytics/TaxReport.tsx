import React from 'react';
import { TaxReportExport } from '@/components/reports/TaxReportExport';

export function TaxReport() {
    return (
        <div className="space-y-6">
            <div className="max-w-4xl mx-auto">
                <TaxReportExport />
            </div>
        </div>
    );
}
