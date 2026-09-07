import { useQuery } from '@tanstack/react-query';
import { onboardingApi } from '../../api/onboarding';
import { CheckCircle2, AlertTriangle, ExternalLink, ArrowRight } from 'lucide-react';
import { Link } from 'react-router-dom';

interface SummaryStepProps {
    onComplete: () => void;
    isLoading: boolean; // For the completion mutation
    onboardingState: any; // Passed to show summary
}

export function SummaryStep({ onComplete, isLoading, onboardingState }: SummaryStepProps) {
    const { data: nextSteps, isLoading: isCheckLoading } = useQuery({
        queryKey: ['onboardingNextSteps'],
        queryFn: onboardingApi.getNextSteps,
    });

    const choiceData = onboardingState?.data || {};

    return (
        <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-500 max-w-2xl mx-auto">
            <div className="text-center space-y-4">
                <div className="w-20 h-20 bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-6">
                    <CheckCircle2 className="w-10 h-10 text-green-500" />
                </div>
                <h2 className="text-3xl font-bold text-white">
                    Setup Complete!
                </h2>
                <p className="text-gray-400">
                    Here is a summary of your configuration.
                </p>
            </div>

            {/* Summary Cards */}
            <div className="bg-white/5 border border-white/10 rounded-2xl overflow-hidden divide-y divide-white/10">
                <SummaryItem label="Experience Level" value={choiceData.experience_level} />
                <SummaryItem label="Risk Tolerance" value={choiceData.risk_tolerance} />
                <SummaryItem label="Strategy" value={choiceData.strategy_preference} />
                <SummaryItem
                    label="Capital Allocation"
                    value={`${choiceData.capital_allocation} ${choiceData.allocation_model === 'percentage' ? '%' : 'USDT'}`}
                />
            </div>

            {/* Readiness/Blockers */}
            {!isCheckLoading && nextSteps && (!nextSteps.ready_for_live || nextSteps.blockers?.length > 0) && (
                <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-2xl p-6 space-y-4">
                    <h3 className="text-yellow-400 font-semibold flex items-center gap-2">
                        <AlertTriangle className="w-5 h-5" /> Action Required
                    </h3>
                    <div className="space-y-2">
                        {nextSteps.blockers.map((blocker: string) => (
                            <div key={blocker} className="flex items-center justify-between p-3 bg-black/20 rounded-lg">
                                <span className="text-gray-300 text-sm font-medium">
                                    {blocker === 'NO_BROKER' ? 'Connect a Broker' :
                                        blocker === 'KYC_REQUIRED' ? 'Complete Identity Verification' : blocker}
                                </span>
                                {blocker === 'NO_BROKER' && (
                                    <Link to="/features/brokers" className="text-blue-400 hover:text-blue-300 text-xs flex items-center gap-1">
                                        Connect <ExternalLink className="w-3 h-3" />
                                    </Link>
                                )}
                                {blocker === 'KYC_REQUIRED' && (
                                    <Link to="/kyc" className="text-blue-400 hover:text-blue-300 text-xs flex items-center gap-1">
                                        Verify <ExternalLink className="w-3 h-3" />
                                    </Link>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            )}

            <button
                onClick={onComplete}
                disabled={isLoading}
                className="w-full py-4 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 text-white rounded-xl font-bold transition-all shadow-[0_0_20px_rgba(37,99,235,0.3)] flex items-center justify-center gap-2"
            >
                {isLoading ? <span className="animate-pulse">Finalizing...</span> : (
                    <>
                        Go to Dashboard <ArrowRight className="w-5 h-5" />
                    </>
                )}
            </button>
        </div>
    );
}

function SummaryItem({ label, value }: { label: string, value: string }) {
    return (
        <div className="flex items-center justify-between p-4">
            <span className="text-gray-400">{label}</span>
            <span className="font-semibold text-white capitalize">{value?.replace(/_/g, ' ')}</span>
        </div>
    );
}
