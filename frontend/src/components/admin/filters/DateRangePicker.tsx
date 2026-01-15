import { useState } from "react";
import { Calendar } from "lucide-react";

interface DateRangePickerProps {
    onDateRangeChange: (startDate: string, endDate: string) => void;
    className?: string;
}

export function DateRangePicker({ onDateRangeChange, className = "" }: DateRangePickerProps) {
    const [startDate, setStartDate] = useState("");
    const [endDate, setEndDate] = useState("");

    const handleStartDateChange = (value: string) => {
        setStartDate(value);
        if (value && endDate) {
            onDateRangeChange(value, endDate);
        }
    };

    const handleEndDateChange = (value: string) => {
        setEndDate(value);
        if (startDate && value) {
            onDateRangeChange(startDate, value);
        }
    };

    const handlePresetRange = (days: number) => {
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - days);

        const startStr = start.toISOString().split('T')[0];
        const endStr = end.toISOString().split('T')[0];

        setStartDate(startStr);
        setEndDate(endStr);
        onDateRangeChange(startStr, endStr);
    };

    const clearDates = () => {
        setStartDate("");
        setEndDate("");
        onDateRangeChange("", "");
    };

    return (
        <div className={`flex items-center gap-3 ${className}`}>
            <div className="flex items-center gap-2">
                <Calendar className="w-4 h-4" style={{ color: 'var(--admin-text-secondary)' }} />
                <input
                    type="date"
                    value={startDate}
                    onChange={(e) => handleStartDateChange(e.target.value)}
                    className="admin-input"
                    style={{ width: '150px' }}
                    placeholder="Start Date"
                />
                <span style={{ color: 'var(--admin-text-secondary)' }}>to</span>
                <input
                    type="date"
                    value={endDate}
                    onChange={(e) => handleEndDateChange(e.target.value)}
                    className="admin-input"
                    style={{ width: '150px' }}
                    placeholder="End Date"
                />
            </div>

            {/* Quick Presets */}
            <div className="flex gap-2">
                <button
                    onClick={() => handlePresetRange(7)}
                    className="admin-btn admin-btn-secondary px-3 py-1 text-xs"
                >
                    Last 7 Days
                </button>
                <button
                    onClick={() => handlePresetRange(30)}
                    className="admin-btn admin-btn-secondary px-3 py-1 text-xs"
                >
                    Last 30 Days
                </button>
                {(startDate || endDate) && (
                    <button
                        onClick={clearDates}
                        className="admin-btn admin-btn-secondary px-3 py-1 text-xs"
                    >
                        Clear
                    </button>
                )}
            </div>
        </div>
    );
}
