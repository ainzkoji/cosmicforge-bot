/**
 * Utility functions for exporting admin data to CSV and JSON formats
 */

/**
 * Convert array of objects to CSV format
 */
export function exportToCSV(data: any[], filename: string = 'export') {
    if (!data || data.length === 0) {
        console.warn('No data to export');
        return;
    }

    // Get all unique keys from all objects
    const allKeys = Array.from(
        new Set(data.flatMap(obj => Object.keys(obj)))
    );

    // Create CSV header
    const headers = allKeys.join(',');

    // Create CSV rows
    const rows = data.map(obj => {
        return allKeys.map(key => {
            const value = obj[key];

            // Handle different data types
            if (value === null || value === undefined) {
                return '';
            }

            // Convert to string and escape quotes
            const stringValue = String(value).replace(/"/g, '""');

            // Wrap in quotes if contains comma, newline, or quote
            if (stringValue.includes(',') || stringValue.includes('\n') || stringValue.includes('"')) {
                return `"${stringValue}"`;
            }

            return stringValue;
        }).join(',');
    });

    // Combine header and rows
    const csv = [headers, ...rows].join('\n');

    // Create blob and download
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    downloadBlob(blob, `${filename}.csv`);
}

/**
 * Export data as JSON file
 */
export function exportToJSON(data: any[], filename: string = 'export') {
    if (!data || data.length === 0) {
        console.warn('No data to export');
        return;
    }

    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: 'application/json;charset=utf-8;' });
    downloadBlob(blob, `${filename}.json`);
}

/**
 * Helper function to trigger file download
 */
function downloadBlob(blob: Blob, filename: string) {
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
}

/**
 * Format date for filename (YYYY-MM-DD)
 */
export function getDateForFilename(): string {
    const now = new Date();
    return now.toISOString().split('T')[0];
}

/**
 * Create timestamped filename
 */
export function createTimestampedFilename(base: string): string {
    return `${base}_${getDateForFilename()}`;
}
