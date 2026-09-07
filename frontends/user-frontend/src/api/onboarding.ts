import { apiClient } from './client';

export interface OnboardingState {
    current_step: string;
    steps_completed: string[];
    data: Record<string, any>;
    status: 'not_started' | 'in_progress' | 'completed';
}

export interface StepSubmission {
    step: string;
    data: Record<string, any>;
}

export interface Strategy {
    id: string;
    name: string;
    description: string;
    tags: string[];
    performance_chart_url?: string; // Placeholder for sparkline data or image URL
    // Add other strategy fields as needed
}

export interface OnboardingNextSteps {
    blockers: string[];
    ready_for_live: boolean;
}

export const onboardingApi = {
    // Get current onboarding state
    getState: async (): Promise<OnboardingState> => {
        const res = await apiClient.get('/api/onboarding/state');
        return res.data;
    },

    // Submit a step
    submitStep: async (data: StepSubmission): Promise<OnboardingState> => {
        const res = await apiClient.post('/api/onboarding/step', data);
        return res.data;
    },

    // Get strategies for selection
    getStrategies: async (): Promise<{ strategies: Strategy[] }> => {
        const res = await apiClient.get('/api/onboarding/strategies');
        return res.data;
    },

    // Complete onboarding
    complete: async (): Promise<OnboardingNextSteps> => {
        const res = await apiClient.post('/api/onboarding/complete');
        return res.data;
    },

    // Check next steps (readiness)
    getNextSteps: async (): Promise<OnboardingNextSteps> => {
        const res = await apiClient.get('/api/onboarding/next-steps');
        return res.data;
    }
};
