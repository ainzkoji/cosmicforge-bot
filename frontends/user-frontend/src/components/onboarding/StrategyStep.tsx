import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { onboardingApi, Strategy } from '../../api/onboarding';
import { Layers, Activity, Info, Check } from 'lucide-react';

interface StrategyStepProps {
    onNext: (data: { strategy_preference: string }) => void;
    isLoading: boolean;
    defaultValue?: string;
}

export function StrategyStep({ onNext, isLoading, defaultValue }: StrategyStepProps) {
    const { data, isLoading: isStrategiesLoading, isError } = useQuery({
        queryKey: ['strategies'],
        queryFn: onboardingApi.getStrategies,
    });

    const [selectedId, setSelectedId] = useState<string>(defaultValue || '');

    const handleSelect = (id: string) => {
        setSelectedId(id);
        onNext({ strategy_preference: id });
    };

    if (isStrategiesLoading) {
        return <div className="flex justify-center py-20"><div className="animate-spin w-8 h-8 border-2 border-blue-500 rounded-full border-t-transparent" /></div>;
    }

    if (isError) {
        return <div className="text-center text-red-400 py-10">Failed to load strategies. Please try again.</div>;
    }

    const strategies = data?.strategies || [];

    return (
        <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-500">
            <div className="text-center space-y-4">
                <h2 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-400">
                    Choose Your Strategy
                </h2>
                <p className="text-gray-400 max-w-lg mx-auto">
                    Select the trading logic that best fits your goals. You can change this later.
                </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {strategies.map((strategy) => (
                    <StrategyCard
                        key={strategy.id}
                        strategy={strategy}
                        isSelected={selectedId === strategy.id}
                        onSelect={() => handleSelect(strategy.id)}
                        isLoading={isLoading}
                    />
                ))}
            </div>
        </div>
    );
}

function StrategyCard({ strategy, isSelected, onSelect, isLoading }: { strategy: Strategy, isSelected: boolean, onSelect: () => void, isLoading: boolean }) {
    return (
        <button
            onClick={onSelect}
            disabled={isLoading}
            className={`
                relative flex flex-col items-start p-6 rounded-2xl border transition-all text-left h-full group
                ${isSelected
                    ? 'border-blue-500 bg-blue-500/10 shadow-[0_0_20px_rgba(59,130,246,0.2)]'
                    : 'border-white/10 bg-white/5 hover:bg-white/10 hover:border-white/20'
                }
            `}
        >
            <div className="flex items-center justify-between w-full mb-4">
                <div className={`p-2 rounded-lg ${isSelected ? 'bg-blue-500/20 text-blue-400' : 'bg-white/10 text-gray-400'}`}>
                    <Layers className="w-6 h-6" />
                </div>
                {isSelected && <Check className="w-6 h-6 text-blue-500" />}
            </div>

            <h3 className="text-xl font-bold text-white mb-2">{strategy.name}</h3>
            <p className="text-sm text-gray-400 mb-6 flex-1">{strategy.description}</p>

            <div className="flex flex-wrap gap-2 mt-auto">
                {strategy.tags.map(tag => (
                    <span key={tag} className="text-xs px-2 py-1 rounded-md bg-white/5 border border-white/5 text-gray-300">
                        {tag}
                    </span>
                ))}
            </div>

            {/* Placeholder for performance chart if available */}
            {strategy.performance_chart_url && (
                <div className="mt-4 w-full h-16 bg-white/5 rounded-lg flex items-center justify-center text-xs text-gray-600">
                    <Activity className="w-4 h-4 mr-1" /> Performance Chart
                </div>
            )}
        </button>
    );
}
