import React from 'react';
import { CheckCircle, Shield, AlertTriangle } from 'lucide-react';

interface RiskTemplate {
    risk_profile: string;
    per_trade_risk_pct: number;
    max_drawdown_pct: number;
    daily_loss_limit_pct: number;
    stop_loss_multiplier: number;
}

interface RiskProfileSelectorProps {
    value: string;
    onChange: (profile: string) => void;
    templates: Record<string, RiskTemplate>;
}

export const RiskProfileSelector: React.FC<RiskProfileSelectorProps> = ({ value, onChange, templates }) => {

    // System Safety Limits (Hardcoded from bot-backend/app/risk/risk_policy.py)
    const SAFETY_LIMITS: Record<string, { max_sl: string; max_risk: string }> = {
        conservative: { max_sl: "1.5%", max_risk: "15%" },
        balanced: { max_sl: "2.25%", max_risk: "22.5%" },
        aggressive: { max_sl: "3.0%", max_risk: "30%" }
    };

    return (
        <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {['conservative', 'balanced', 'aggressive'].map(profile => {
                    const template = templates[profile];
                    const safety = SAFETY_LIMITS[profile];
                    const isSelected = value === profile;

                    if (!template) return null;

                    return (
                        <button
                            key={profile}
                            onClick={() => onChange(profile)}
                            className={`p-5 rounded-xl border text-left transition-all relative overflow-hidden group ${isSelected
                                    ? profile === 'conservative' ? 'bg-blue-900/30 border-blue-500 ring-1 ring-blue-500' :
                                        profile === 'balanced' ? 'bg-yellow-900/30 border-yellow-500 ring-1 ring-yellow-500' :
                                            'bg-red-900/30 border-red-500 ring-1 ring-red-500'
                                    : 'bg-gray-900 border-gray-700 hover:bg-gray-800 hover:border-gray-600'
                                }`}
                        >
                            <div className="flex items-center justify-between mb-4">
                                <div className="font-bold text-lg capitalize flex items-center gap-2">
                                    {profile}
                                </div>
                                {isSelected && <CheckCircle className="w-6 h-6 text-white" />}
                            </div>

                            <div className="space-y-3 text-sm text-gray-300">
                                {/* Operating Defaults */}
                                <div className="flex justify-between">
                                    <span className="text-gray-400">Risk / Trade</span>
                                    <span className="font-mono font-medium">{(template.per_trade_risk_pct * 100).toFixed(1)}%</span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-gray-400">Daily Limit</span>
                                    <span className="font-mono font-medium">{(template.daily_loss_limit_pct * 100).toFixed(0)}%</span>
                                </div>

                                {/* Divider */}
                                <div className="h-px bg-gray-700/50 my-2" />

                                {/* System Safety Limits */}
                                {safety && (
                                    <>
                                        <div className="flex justify-between text-xs text-emerald-400 font-medium relative tooltip-container">
                                            <span className="flex items-center gap-1">
                                                <Shield className="w-3 h-3" />
                                                Safety Max SL
                                            </span>
                                            <span className="font-mono">{safety.max_sl}</span>
                                        </div>
                                        <div className="flex justify-between text-xs text-gray-500">
                                            <span>Max Compound Risk</span>
                                            <span className="font-mono">{safety.max_risk}</span>
                                        </div>
                                    </>
                                )}
                            </div>

                            {/* Selection Effect overlay */}
                            <div className={`absolute inset-0 pointer-events-none transition-opacity duration-300 ${isSelected ? 'opacity-10' : 'opacity-0'} ${profile === 'conservative' ? 'bg-blue-500' :
                                    profile === 'balanced' ? 'bg-yellow-500' :
                                        'bg-red-500'
                                }`} />
                        </button>
                    );
                })}
            </div>

            <div className="bg-gray-900/50 p-4 rounded-lg border border-gray-700">
                <h4 className="font-bold mb-2 flex items-center gap-2 text-sm text-white">
                    <AlertTriangle className="w-4 h-4 text-yellow-500" />
                    System Safety Policy
                </h4>
                <p className="text-xs text-gray-400 leading-relaxed">
                    The <strong>Safety Max SL</strong> is the absolute maximum stop loss distance allowed at 10x leverage.
                    If a strategy attempts to set a wider stop loss, the system will automatically <strong>clamp</strong> it to this limit to prevent excessive risk.
                </p>
            </div>
        </div>
    );
};
