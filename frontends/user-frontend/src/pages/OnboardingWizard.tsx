import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { motion, AnimatePresence } from 'framer-motion';
import { onboardingApi } from '../api/onboarding';

// Step Components
import { WelcomeStep } from '../components/onboarding/WelcomeStep';
import { ExperienceStep } from '../components/onboarding/ExperienceStep';
import { RiskToleranceStep } from '../components/onboarding/RiskToleranceStep';
import { StrategyStep } from '../components/onboarding/StrategyStep';
import { CapitalAllocationStep } from '../components/onboarding/CapitalAllocationStep';
import { SummaryStep } from '../components/onboarding/SummaryStep';

export default function OnboardingWizard() {
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const [currentStep, setCurrentStep] = useState<string>('loading');

    // Fetch initial state
    const { data: onboardingState, isLoading: isStateLoading } = useQuery({
        queryKey: ['onboardingState'],
        queryFn: onboardingApi.getState,
        retry: false,
    });

    // Sync state when loaded
    useEffect(() => {
        if (onboardingState) {
            if (onboardingState.status === 'completed') {
                navigate('/dashboard');
            } else if (onboardingState.status === 'in_progress' && onboardingState.current_step) {
                setCurrentStep(onboardingState.current_step);
            } else {
                // Default to welcome if not started or no step returned
                setCurrentStep(onboardingState.current_step || 'welcome');
            }
        } else if (!isStateLoading && !onboardingState) {
            // Fallback if API returns nothing (e.g., auth error handled elsewhere, or empty)
            setCurrentStep('welcome');
        }
    }, [onboardingState, isStateLoading, navigate]);

    // Mutation for submitting steps
    const submitStepMutation = useMutation({
        mutationFn: onboardingApi.submitStep,
        onSuccess: (data) => {
            // Update local state and invalidate query
            queryClient.setQueryData(['onboardingState'], data);
            if (data.current_step) {
                setCurrentStep(data.current_step);
            }
        },
        onError: (error) => {
            console.error("Failed to submit step:", error);
            // Could add toast notification here
        }
    });

    const handleStart = () => {
        submitStepMutation.mutate({ step: 'welcome', data: {} });
    };

    const handleNext = (stepName: string, data: any) => {
        submitStepMutation.mutate({ step: stepName, data });
    };

    const handleComplete = async () => {
        try {
            await onboardingApi.complete();
            navigate('/dashboard');
        } catch (e) {
            console.error("Failed to complete:", e);
        }
    };

    // Render content based on currentStep
    const renderStep = () => {
        const commonProps = {
            isLoading: submitStepMutation.isPending
        };

        // Helper to get default value from state if available
        const getData = (key: string) => onboardingState?.data?.[key];

        switch (currentStep) {
            case 'welcome':
                return <WelcomeStep onStart={handleStart} {...commonProps} />;
            case 'experience_level':
                return <ExperienceStep
                    onNext={(data) => handleNext('experience_level', data)}
                    defaultValue={getData('experience_level')}
                    {...commonProps}
                />;
            case 'risk_tolerance':
                return <RiskToleranceStep
                    onNext={(data) => handleNext('risk_tolerance', data)}
                    defaultValue={getData('risk_tolerance')}
                    {...commonProps}
                />;
            case 'strategy_preference':
                return <StrategyStep
                    onNext={(data) => handleNext('strategy_preference', data)}
                    defaultValue={getData('strategy_preference')}
                    {...commonProps}
                />;
            case 'capital_allocation':
                return <CapitalAllocationStep
                    onNext={(data) => handleNext('capital_allocation', data)}
                    defaultValue={{
                        capital_allocation: getData('capital_allocation'),
                        allocation_value: getData('allocation_value'),
                        allocation_type: getData('allocation_type')
                    }}
                    riskProfile={getData('risk_tolerance')}
                    {...commonProps}
                />;
            case 'summary':
                return <SummaryStep
                    onComplete={handleComplete}
                    isLoading={submitStepMutation.isPending}
                    onboardingState={onboardingState}
                />;
            case 'loading':
                return <div className="flex items-center justify-center min-h-[60vh]"><div className="w-8 h-8 border-2 border-blue-500 rounded-full animate-spin border-t-transparent" /></div>;
            default:
                // Fallback for Loading or Unknown
                if (isStateLoading) return <div className="flex items-center justify-center min-h-[60vh]"><div className="w-8 h-8 border-2 border-blue-500 rounded-full animate-spin border-t-transparent" /></div>;
                return <div className="text-center text-red-500">Unknown Step: {currentStep}</div>;
        }
    };

    return (
        <div className="min-h-screen w-full bg-[#0B0E14] text-white flex flex-col font-sans selection:bg-blue-500/30">
            {/* Header / Progress */}
            <div className="p-6 fixed top-0 left-0 w-full z-20 pointer-events-none">
                <div className="flex items-center gap-2 text-sm font-medium text-gray-500 pointer-events-auto">
                    <span className="text-blue-500">Setup</span>
                    <span>/</span>
                    <span className="capitalize text-gray-300">{currentStep.replace(/_/g, ' ')}</span>
                </div>
            </div>

            {/* Main Content Area */}
            <main className="flex-1 flex flex-col items-center justify-center relative overflow-hidden py-20 px-4">
                {/* Background Elements */}
                <div className="absolute top-0 left-0 w-full h-full overflow-hidden pointer-events-none z-0">
                    <div className="absolute top-[-20%] left-[-10%] w-[50%] h-[60%] bg-blue-600/10 rounded-full blur-[120px] opacity-60" />
                    <div className="absolute bottom-[-20%] right-[-10%] w-[50%] h-[60%] bg-purple-600/10 rounded-full blur-[120px] opacity-60" />
                </div>

                <div className="relative z-10 w-full max-w-5xl">
                    <AnimatePresence mode="wait">
                        <motion.div
                            key={currentStep}
                            initial={{ opacity: 0, x: 20, filter: 'blur(10px)' }}
                            animate={{ opacity: 1, x: 0, filter: 'blur(0px)' }}
                            exit={{ opacity: 0, x: -20, filter: 'blur(10px)' }}
                            transition={{ duration: 0.4, ease: "easeOut" }}
                        >
                            {renderStep()}
                        </motion.div>
                    </AnimatePresence>
                </div>
            </main>
        </div>
    );
}
