import React, { useState, useEffect } from 'react';
import { api } from '../../api/client';
import {
    CheckCircle,
    ChevronRight,
    ShieldAlert,
    SlidersHorizontal
} from 'lucide-react';

interface ConfigWizardProps {
    accountId: string;
    onClose: () => void;
    onComplete: () => void;
}

const STEPS = [
    { id: 'strategy', title: 'Strategy Selection' },
    { id: 'risk', title: 'Risk Profile' },
    { id: 'review', title: 'Review & Confirm' }
];

export const ConfigWizard: React.FC<ConfigWizardProps> = ({ accountId, onClose, onComplete }) => {
    const [currentStep, setCurrentStep] = useState(0);
    const [strategies, setStrategies] = useState<any[]>([]);
    const [riskTemplates, setRiskTemplates] = useState<any>({});
    const [selectedStrategy, setSelectedStrategy] = useState<string>('');
    const [configName, setConfigName] = useState('');
    const [selectedRiskProfile, setSelectedRiskProfile] = useState<string>('balanced');
    const [customRiskParams, setCustomRiskParams] = useState<any>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        try {
            const [stratsRes, templatesRes] = await Promise.all([
                api.getStrategyCatalog(),
                api.getRiskTemplates()
            ]);
            setStrategies(stratsRes.strategies || []);
            setRiskTemplates(templatesRes.profiles || {});
        } catch (err) {
            console.error(err);
            setError("Failed to load initial data");
        }
    };

    const handleNext = () => {
        if (currentStep < STEPS.length - 1) {
            setCurrentStep(curr => curr + 1);
        } else {
            handleSubmit();
        }
    };

    const handleSubmit = async () => {
        setLoading(true);
        setError(null);
        try {
            // Construct payload
            const riskSettings = customRiskParams || riskTemplates[selectedRiskProfile];

            const payload = {
                broker_account_id: accountId,
                strategy_id: selectedStrategy,
                name: configName,
                risk_parameters: {
                    ...riskSettings,
                    risk_profile: selectedRiskProfile
                }
            };

            await api.createStrategyConfig(payload);
            onComplete();
        } catch (err: any) {
            setError(err.message || "Failed to create configuration");
            setLoading(false);
        }
    };

    const renderStepContent = () => {
        switch (currentStep) {
            case 0:
                return (
                    <div className="space-y-6">
                        <div>
                            <label className="block text-sm font-medium text-gray-400 mb-2">Configuration Name</label>
                            <input
                                type="text"
                                value={configName}
                                onChange={e => setConfigName(e.target.value)}
                                className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-3 text-white focus:border-blue-500 outline-none"
                                placeholder="e.g., BTC Trend Follower - Aggressive"
                            />
                        </div>
                        <div>
                            <label className="block text-sm font-medium text-gray-400 mb-2">Select Strategy</label>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                {strategies.map(strat => (
                                    <button
                                        key={strat.id}
                                        onClick={() => setSelectedStrategy(strat.id)}
                                        className={`p-4 rounded-xl border text-left transition-all ${selectedStrategy === strat.id
                                                ? 'bg-blue-900/30 border-blue-500 ring-1 ring-blue-500'
                                                : 'bg-gray-900 border-gray-700 hover:border-gray-500'
                                            }`}
                                    >
                                        <div className="font-bold text-lg mb-1">{strat.name}</div>
                                        <div className="text-sm text-gray-400 line-clamp-2">{strat.description}</div>
                                        <div className="mt-3 flex gap-2">
                                            {(strat.tags || []).map((tag: string) => (
                                                <span key={tag} className="text-xs bg-gray-800 px-2 py-1 rounded">
                                                    {tag}
                                                </span>
                                            ))}
                                        </div>
                                    </button>
                                ))}
                            </div>
                        </div>
                    </div>
                );
            case 1:
                return (
                    <div className="space-y-6">
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            {['conservative', 'balanced', 'aggressive'].map(profile => {
                                const template = riskTemplates[profile];
                                if (!template) return null;

                                const isSelected = selectedRiskProfile === profile;

                                return (
                                    <button
                                        key={profile}
                                        onClick={() => {
                                            setSelectedRiskProfile(profile);
                                            setCustomRiskParams(null);
                                        }}
                                        className={`p-5 rounded-xl border text-left transition-all relative overflow-hidden ${isSelected
                                                ? profile === 'conservative' ? 'bg-blue-900/30 border-blue-500' :
                                                    profile === 'balanced' ? 'bg-yellow-900/30 border-yellow-500' :
                                                        'bg-red-900/30 border-red-500'
                                                : 'bg-gray-900 border-gray-700 hover:bg-gray-800'
                                            }`}
                                    >
                                        <div className="font-bold text-lg capitalize mb-4 flex items-center justify-between">
                                            {profile}
                                            {isSelected && <CheckCircle className="w-6 h-6" />}
                                        </div>
                                        <div className="space-y-3 text-sm text-gray-300">
                                            <div className="flex justify-between">
                                                <span>Risk / Trade</span>
                                                <span className="font-mono">{(template.per_trade_risk_pct * 100).toFixed(1)}%</span>
                                            </div>
                                            <div className="flex justify-between">
                                                <span>Max Drawdown</span>
                                                <span className="font-mono">{(template.max_drawdown_pct * 100).toFixed(0)}%</span>
                                            </div>
                                            <div className="flex justify-between">
                                                <span>Stop Loss</span>
                                                <span className="font-mono">{template.stop_loss_multiplier}x ATR</span>
                                            </div>
                                            <div className="flex justify-between">
                                                <span>Daily Limit</span>
                                                <span className="font-mono">{(template.daily_loss_limit_pct * 100).toFixed(0)}%</span>
                                            </div>
                                        </div>
                                    </button>
                                );
                            })}
                        </div>

                        <div className="bg-gray-900/50 p-4 rounded-lg border border-gray-700">
                            <h4 className="font-bold mb-2 flex items-center gap-2">
                                <ShieldAlert className="w-5 h-5 text-yellow-500" />
                                Safety System Note
                            </h4>
                            <p className="text-sm text-gray-400">
                                Even if you select "Aggressive", the System Governance layer will clamp values to absolute maximums (e.g., max 10% daily loss, max 20x leverage).
                            </p>
                        </div>
                    </div>
                );
            case 2:
                const risk = customRiskParams || riskTemplates[selectedRiskProfile] || {};
                return (
                    <div className="space-y-6">
                        <div className="bg-gradient-to-br from-gray-800 to-gray-900 p-6 rounded-xl border border-gray-700">
                            <h3 className="text-xl font-bold mb-6">Confirm Configuration</h3>

                            <div className="grid grid-cols-2 gap-y-6 gap-x-12">
                                <div>
                                    <div className="text-sm text-gray-500 mb-1">Name</div>
                                    <div className="font-medium text-lg">{configName}</div>
                                </div>
                                <div>
                                    <div className="text-sm text-gray-500 mb-1">Strategy</div>
                                    <div className="font-medium text-lg">
                                        {strategies.find(s => s.id === selectedStrategy)?.name || selectedStrategy}
                                    </div>
                                </div>
                                <div>
                                    <div className="text-sm text-gray-500 mb-1">Risk Profile</div>
                                    <div className="font-medium text-lg capitalize">{selectedRiskProfile}</div>
                                </div>
                                <div>
                                    <div className="text-sm text-gray-500 mb-1">Daily Loss Limit</div>
                                    <div className="font-medium text-lg text-red-400">
                                        {(risk.daily_loss_limit_pct * 100).toFixed(1)}%
                                    </div>
                                </div>
                            </div>

                            <div className="mt-8 pt-6 border-t border-gray-700">
                                <h4 className="font-semibold mb-4 text-gray-300">Protection Layers Active</h4>
                                <div className="grid grid-cols-2 gap-3 text-sm">
                                    {['Pre-Trade Gating', 'Sizing Controls', 'Protective Orders', 'Post-Trade Monitoring'].map(layer => (
                                        <div key={layer} className="flex items-center gap-2 text-emerald-400">
                                            <CheckCircle className="w-4 h-4" />
                                            {layer}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>
                );
            default:
                return null;
        }
    };

    return (
        <div className="flex flex-col h-full max-h-[80vh]">
            <div className="p-6 border-b border-gray-700">
                <div className="flex justify-between mb-8">
                    <h2 className="text-2xl font-bold">New Configuration</h2>
                    <button onClick={onClose} className="text-gray-400 hover:text-white">✕</button>
                </div>

                {/* Progress Steps */}
                <div className="flex items-center">
                    {STEPS.map((step, idx) => (
                        <React.Fragment key={step.id}>
                            <div className={`flex items-center gap-2 ${idx <= currentStep ? 'text-blue-400' : 'text-gray-600'}`}>
                                <div className={`w-8 h-8 rounded-full flex items-center justify-center border-2 ${idx < currentStep ? 'bg-blue-500 border-blue-500 text-black' :
                                        idx === currentStep ? 'border-blue-400 text-blue-400' :
                                            'border-gray-600 text-gray-600'
                                    }`}>
                                    {idx < currentStep ? <CheckCircle className="w-5 h-5" /> : idx + 1}
                                </div>
                                <span className="font-medium whitespace-nowrap">{step.title}</span>
                            </div>
                            {idx < STEPS.length - 1 && (
                                <div className={`h-0.5 w-12 mx-4 ${idx < currentStep ? 'bg-blue-500' : 'bg-gray-700'}`} />
                            )}
                        </React.Fragment>
                    ))}
                </div>
            </div>

            <div className="p-8 flex-1 overflow-y-auto">
                {renderStepContent()}
                {error && (
                    <div className="mt-4 p-4 bg-red-900/30 border border-red-500 rounded-lg text-red-200">
                        {error}
                    </div>
                )}
            </div>

            <div className="p-6 border-t border-gray-700 bg-gray-800/50 flex justify-between">
                <button
                    onClick={() => setCurrentStep(curr => Math.max(0, curr - 1))}
                    disabled={currentStep === 0}
                    className={`px-6 py-2 rounded-lg font-medium transition-colors ${currentStep === 0
                            ? 'text-gray-600 cursor-not-allowed'
                            : 'text-gray-300 hover:text-white hover:bg-gray-700'
                        }`}
                >
                    Back
                </button>
                <button
                    onClick={handleNext}
                    disabled={
                        (currentStep === 0 && (!selectedStrategy || !configName)) ||
                        loading
                    }
                    className={`px-8 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg font-medium transition-colors flex items-center gap-2 ${loading ? 'opacity-50 cursor-wait' : ''
                        }`}
                >
                    {loading ? 'Processing...' : currentStep === STEPS.length - 1 ? 'Create Configuration' : 'Next Step'}
                    {!loading && <ChevronRight className="w-4 h-4" />}
                </button>
            </div>
        </div>
    );
};
