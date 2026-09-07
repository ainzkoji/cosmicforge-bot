import { useState } from 'react';
import { AlertTriangle, TrendingUp, Anchor, Check } from 'lucide-react';

interface RiskToleranceStepProps {
    onNext: (data: { risk_tolerance: string }) => void;
    isLoading: boolean;
    defaultValue?: string;
}

export function RiskToleranceStep({ onNext, isLoading, defaultValue }: RiskToleranceStepProps) {
    const [selected, setSelected] = useState<string>(defaultValue || '');

    const handleSelect = (val: string) => {
        setSelected(val);
        onNext({ risk_tolerance: val });
    };

    return (
        <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-500">
            <div className="text-center space-y-4">
                <h2 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-400">
                    Define Your Risk Appetite
                </h2>
                <p className="text-gray-400 max-w-lg mx-auto">
                    This setting controls your position sizes, stop-losses, and leverage limits.
                </p>
            </div>

            <div className="space-y-4">
                <RiskCard
                    value="low"
                    title="Low Risk (Conservative)"
                    description="Max drawdown ~5%. Capital preservation is the priority. Small position sizes."
                    icon={Anchor}
                    color="text-emerald-400"
                    accentColor="emerald"
                    onClick={() => handleSelect('low')}
                    isSelected={selected === 'low'}
                    isLoading={isLoading}
                />
                <RiskCard
                    value="medium"
                    title="Medium Risk (Balanced)"
                    description="Max drawdown ~15%. Balanced approach aiming for steady growth with moderate volatility."
                    icon={TrendingUp}
                    color="text-blue-400"
                    accentColor="blue"
                    onClick={() => handleSelect('medium')}
                    isSelected={selected === 'medium'}
                    isLoading={isLoading}
                />
                <RiskCard
                    value="high"
                    title="High Risk (Aggressive)"
                    description="Max drawdown ~30%. Targeting high returns. Significant volatility expected."
                    icon={AlertTriangle}
                    color="text-rose-400"
                    accentColor="rose"
                    onClick={() => handleSelect('high')}
                    isSelected={selected === 'high'}
                    isLoading={isLoading}
                />
            </div>

            <div className="flex items-center gap-3 p-4 bg-yellow-500/10 border border-yellow-500/20 rounded-lg text-sm text-yellow-200/80">
                <AlertTriangle className="w-5 h-5 flex-shrink-0 text-yellow-500" />
                <p>
                    Regardless of your choice, the bot will never exceed the absolute Hard Check limits defined by your exchange account.
                </p>
            </div>
        </div>
    );
}

function RiskCard({ value, title, description, icon: Icon, color, accentColor, onClick, isSelected, isLoading }: any) {
    // accentColor is used for dynamic class names, but Tailwind needs full class names to scan.
    // We'll map them explicitly or assume they are safe-listed. safest is to use standard colors or style objects.
    // For simplicity here, I'll use inline styles or specific mappings if I knew the full tailwind config, but let's stick to known classes.
    // I will use a mapping for the selected border/bg.

    const getSelectedClasses = () => {
        switch (value) {
            case 'low': return 'border-emerald-500 bg-emerald-500/10';
            case 'medium': return 'border-blue-500 bg-blue-500/10';
            case 'high': return 'border-rose-500 bg-rose-500/10';
            default: return '';
        }
    };

    return (
        <button
            onClick={onClick}
            disabled={isLoading}
            className={`
            w-full flex items-center gap-6 p-6 rounded-xl border transition-all text-left group
            ${isSelected
                    ? `${getSelectedClasses()} shadow-lg`
                    : 'border-white/10 bg-white/5 hover:bg-white/10 hover:border-white/20'
                }
        `}
        >
            <div className={`p-4 rounded-full bg-white/5 ${color} group-hover:scale-110 transition-transform`}>
                <Icon className="w-6 h-6" />
            </div>

            <div className="flex-1">
                <h3 className="text-lg font-bold text-white mb-1 flex items-center gap-3">
                    {title}
                    {isSelected && <Check className="w-5 h-5" />}
                </h3>
                <p className="text-sm text-gray-400">{description}</p>
            </div>
        </button>
    );
}
