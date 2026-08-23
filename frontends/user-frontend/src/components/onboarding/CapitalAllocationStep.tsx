import { useState } from 'react';
import { DollarSign, Percent, AlertCircle, Info } from 'lucide-react';

interface CapitalAllocationStepProps {
    onNext: (data: { capital_allocation: number, allocation_type: 'fixed_amount' | 'percent_balance', allocation_value: number }) => void;
    isLoading: boolean;
    defaultValue?: { capital_allocation?: number, allocation_value?: number, allocation_type?: 'fixed_amount' | 'percent_balance' };
    riskProfile?: string; // Passed to handle validation
}

export function CapitalAllocationStep({ onNext, isLoading, defaultValue, riskProfile }: CapitalAllocationStepProps) {
    // 1. Global Capital (Budget)
    const [globalCapital, setGlobalCapital] = useState<string>(defaultValue?.capital_allocation?.toString() || '');

    // 2. Trade Amount
    const [type, setType] = useState<'fixed_amount' | 'percent_balance'>(defaultValue?.allocation_type || 'fixed_amount');
    const [amount, setAmount] = useState<string>(defaultValue?.allocation_value?.toString() || (defaultValue?.allocation_type === 'percent_balance' ? "5" : "100"));
    const [error, setError] = useState<string | null>(null);

    // Validation logic
    const validate = (globalCap: string, tradeVal: string, tradeType: 'fixed_amount' | 'percent_balance') => {
        const gCap = parseFloat(globalCap);
        const tVal = parseFloat(tradeVal);

        // Global Capital Validation
        if (!globalCap || isNaN(gCap) || gCap <= 0) return "Please enter a valid total capital amount";
        if (gCap < 50) return "Total capital should be at least $50";

        // Trade Amount Validation
        if (isNaN(tVal) || tVal <= 0) return "Please enter a valid positive trade amount";

        if (tradeType === 'percent_balance') {
            if (tVal < 0.1) return "Percentage must be at least 0.1%";
            if (tVal > 100) return "Percentage cannot exceed 100%";
            if (riskProfile === 'low' && tVal > 20) return "Low risk profile suggests typical trade sizes under 20%";
        } else {
            if (tVal < 5) return "Minimum trade amount is usually $5 (exchange requirement)";
            if (tVal > gCap) return "Trade amount cannot exceed total capital";
        }

        return null;
    };

    const handleNext = () => {
        const err = validate(globalCapital, amount, type);
        if (err) {
            setError(err);
            return;
        }

        onNext({
            capital_allocation: parseFloat(globalCapital),
            allocation_type: type,
            allocation_value: parseFloat(amount)
        });
    };

    const exampleBalance = parseFloat(globalCapital) || 1000;
    const calculatedValue = type === 'percent_balance' ? (parseFloat(amount || '0') / 100) * exampleBalance : parseFloat(amount || '0');

    return (
        <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-500 max-w-lg mx-auto">
            <div className="text-center space-y-4">
                <h2 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-400">
                    Capital Deployment
                </h2>
                <p className="text-gray-400">
                    Configure your total budget and individual trade sizing.
                </p>
            </div>

            <div className="space-y-6 bg-white/5 p-8 rounded-2xl border border-white/10">

                {/* 1. Global Capital Allocation */}
                <div className="space-y-3 pb-6 border-b border-white/5">
                    <label className="text-sm font-medium text-gray-300 uppercase tracking-wider">Total Capital Budget</label>
                    <div className="relative">
                        <input
                            type="number"
                            value={globalCapital}
                            onChange={(e) => {
                                setGlobalCapital(e.target.value);
                                if (error) setError(null);
                            }}
                            className="w-full bg-black/20 border border-white/10 rounded-xl pl-4 pr-12 py-3 text-lg font-mono focus:outline-none focus:border-blue-500 transition-colors"
                            placeholder="e.g. 1000"
                            min="0"
                        />
                        <div className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500 font-bold">
                            USDT
                        </div>
                    </div>
                    <p className="text-xs text-gray-500">The maximum amount of capital this bot can manage.</p>
                </div>

                {/* 2. Trade Amount Type */}
                <div className="space-y-3">
                    <label className="text-sm font-medium text-gray-300 uppercase tracking-wider">Trade Amount Per Position</label>

                    {/* Toggle */}
                    <div className="flex p-1 bg-black/40 rounded-lg">
                        <button
                            onClick={() => { setType('fixed_amount'); setAmount('100'); setError(null); }}
                            className={`
                                flex-1 py-2 px-4 rounded-md text-sm font-medium transition-all
                                ${type === 'fixed_amount' ? 'bg-blue-600 text-white shadow-lg' : 'text-gray-400 hover:text-white'}
                            `}
                        >
                            Fixed Amount
                        </button>
                        <button
                            onClick={() => { setType('percent_balance'); setAmount('5'); setError(null); }}
                            className={`
                                flex-1 py-2 px-4 rounded-md text-sm font-medium transition-all
                                ${type === 'percent_balance' ? 'bg-blue-600 text-white shadow-lg' : 'text-gray-400 hover:text-white'}
                            `}
                        >
                            % of Balance
                        </button>
                    </div>

                    {/* Input */}
                    <div className="relative">
                        <input
                            type="number"
                            value={amount}
                            onChange={(e) => {
                                setAmount(e.target.value);
                                if (error) setError(null);
                            }}
                            className="w-full bg-black/20 border border-white/10 rounded-xl pl-4 pr-12 py-3 text-lg font-mono focus:outline-none focus:border-blue-500 transition-colors"
                            placeholder={type === 'fixed_amount' ? "100" : "5"}
                            step={type === 'percent_balance' ? "0.1" : "1"}
                            min="0"
                        />
                        <div className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500">
                            {type === 'fixed_amount' ? <DollarSign className="w-5 h-5" /> : <Percent className="w-5 h-5" />}
                        </div>
                    </div>
                </div>

                {/* Dynamic Preview / Info */}
                <div className="bg-blue-500/10 border border-blue-500/20 rounded-xl p-4 flex items-start gap-3">
                    <Info className="w-5 h-5 text-blue-400 flex-shrink-0 mt-0.5" />
                    <div className="text-sm">
                        <p className="text-blue-200 font-medium mb-1">
                            {type === 'fixed_amount'
                                ? `Each trade will use exactly $${parseFloat(amount) || 0}.`
                                : `Each trade will use ${amount}% of your available balance.`
                            }
                        </p>
                        {type === 'percent_balance' && globalCapital && (
                            <p className="text-blue-300/70 text-xs">
                                Based on your budget of ${globalCapital}, initial trades would be ~<span className="text-white font-mono font-bold">${((parseFloat(amount) / 100) * parseFloat(globalCapital)).toFixed(2)}</span>.
                            </p>
                        )}
                        {type === 'fixed_amount' && globalCapital && (
                            <p className="text-blue-300/70 text-xs">
                                With a budget of ${globalCapital}, you can open approx <span className="text-white font-mono font-bold">{Math.floor(parseFloat(globalCapital) / (parseFloat(amount) || 1))}</span> concurrent trades.
                            </p>
                        )}
                    </div>
                </div>

                {/* Validation Warning */}
                {error && (
                    <div className="flex items-start gap-2 text-red-400 text-sm bg-red-500/10 p-3 rounded-lg border border-red-500/20">
                        <AlertCircle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                        <span>{error}</span>
                    </div>
                )}

                <button
                    onClick={handleNext}
                    disabled={isLoading}
                    className="w-full py-4 bg-blue-600 hover:bg-blue-500 text-white rounded-xl font-bold transition-all shadow-[0_0_20px_rgba(37,99,235,0.3)] disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    {isLoading ? "Saving..." : "Continue"}
                </button>
            </div>
        </div>
    );
}

