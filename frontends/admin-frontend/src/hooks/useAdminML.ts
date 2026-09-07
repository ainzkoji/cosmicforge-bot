import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
    getMLActivity,
    getMLAlerts,
    getMLControlPanel,
    getMLDashboard,
    getMLDashboardSummary,
    getMLDatasetBuilderStatus,
    getMLDriftMonitoring,
    getMLFeatureCompleteness,
    getMLLinkageHealth,
    getMLOverview,
    getMLRuntimeStatus,
    getMLShadowPerformance,
    getMLTrainingGate,
    getMLValidationHistory,
    triggerMLAction,
    type GetMLActivityParams,
    type MLActionKey,
    type TriggerMLActionRequest,
} from "@/api/admin";

export function useMLDashboardSummaryQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "dashboard-summary"],
        queryFn: getMLDashboardSummary,
        staleTime: 60_000,
    });
}

export function useMLOverviewQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "overview"],
        queryFn: getMLOverview,
        staleTime: 60_000,
    });
}

export function useMLTrainingGateQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "training-gate"],
        queryFn: getMLTrainingGate,
        staleTime: 60_000,
    });
}

export function useMLFeatureCompletenessQuery(recentLimit: number = 500) {
    return useQuery({
        queryKey: ["admin", "ml", "feature-completeness", recentLimit],
        queryFn: () => getMLFeatureCompleteness(recentLimit),
        staleTime: 60_000,
    });
}

export function useMLLinkageHealthQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "linkage-health"],
        queryFn: getMLLinkageHealth,
        staleTime: 60_000,
    });
}

export function useMLActivityQuery(params: GetMLActivityParams = {}) {
    const days = params.days ?? 30;
    const page = params.page ?? 1;
    const pageSize = params.pageSize ?? 20;

    return useQuery({
        queryKey: ["admin", "ml", "activity", days, page, pageSize],
        queryFn: () => getMLActivity({ days, page, pageSize }),
        staleTime: 30_000,
        placeholderData: (previousData) => previousData,
    });
}

export function useMLShadowPerformanceQuery(days: number = 90) {
    return useQuery({
        queryKey: ["admin", "ml", "shadow-performance", days],
        queryFn: () => getMLShadowPerformance(days),
        staleTime: 60_000,
    });
}

export function useMLValidationHistoryQuery(limit: number = 10) {
    return useQuery({
        queryKey: ["admin", "ml", "validation-history", limit],
        queryFn: () => getMLValidationHistory(limit),
        staleTime: 60_000,
    });
}

export function useMLDatasetBuilderStatusQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "dataset-builder-status"],
        queryFn: getMLDatasetBuilderStatus,
        staleTime: 60_000,
    });
}

export function useMLAlertsQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "alerts"],
        queryFn: getMLAlerts,
        staleTime: 30_000,
    });
}

export function useMLControlPanelQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "control-panel"],
        queryFn: getMLControlPanel,
        staleTime: 15_000,
        refetchInterval: 15_000,
    });
}

export function useMLDriftMonitoringQuery(days: number = 30) {
    return useQuery({
        queryKey: ["admin", "ml", "drift-monitoring", days],
        queryFn: () => getMLDriftMonitoring(days),
        staleTime: 60_000,
    });
}

export function useMLDashboardQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "dashboard"],
        queryFn: getMLDashboard,
        staleTime: 60_000,
    });
}

export function useMLRuntimeStatusQuery() {
    return useQuery({
        queryKey: ["admin", "ml", "runtime-status"],
        queryFn: getMLRuntimeStatus,
        // Poll every 30 s — this is a safety-critical field visible at the top of the dashboard.
        staleTime: 30_000,
        refetchInterval: 30_000,
    });
}

export function useTriggerMLActionMutation() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: ({ actionKey, payload }: { actionKey: MLActionKey; payload: TriggerMLActionRequest }) =>
            triggerMLAction(actionKey, payload),
        onSuccess: async () => {
            await Promise.all([
                queryClient.invalidateQueries({ queryKey: ["admin", "ml", "dashboard"] }),
                queryClient.invalidateQueries({ queryKey: ["admin", "ml", "control-panel"] }),
                queryClient.invalidateQueries({ queryKey: ["admin", "ml", "dataset-builder-status"] }),
                queryClient.invalidateQueries({ queryKey: ["admin", "ml", "alerts"] }),
                queryClient.invalidateQueries({ queryKey: ["admin", "ml", "validation-history"] }),
            ]);
        },
    });
}
