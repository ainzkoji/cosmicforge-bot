// Admin API Client Functions
import { apiClient } from "./client";

// Dashboard Stats
export async function getAdminDashboardStats() {
    const response = await apiClient.get("/api/admin/dashboard/stats");
    return response.data;
}

export async function getRevenueOverview() {
    const response = await apiClient.get("/api/admin/dashboard/revenue-overview");
    return response.data;
}

// User Management
export async function listUsers(status?: string, limit: number = 50) {
    const response = await apiClient.get("/api/admin/users", {
        params: { status, limit }
    });
    return response.data;
}

export async function getUserDetails(userId: string) {
    const response = await apiClient.get(`/api/admin/users/${userId}`);
    return response.data;
}

export async function suspendUser(userId: string) {
    const response = await apiClient.post(`/api/admin/users/${userId}/suspend`);
    return response.data;
}

export async function activateUser(userId: string) {
    const response = await apiClient.post(`/api/admin/users/${userId}/activate`);
    return response.data;
}

// Revenue Analytics
export async function getRevenueAnalytics() {
    const response = await apiClient.get("/api/admin/revenue/overview");
    return response.data;
}

export async function getCommissionTiers() {
    const response = await apiClient.get("/api/admin/commissions/tiers");
    return response.data;
}

export async function updateCommissionTier(tierId: string, data: any) {
    const response = await apiClient.put(`/api/admin/commissions/tiers/${tierId}`, data);
    return response.data;
}

// Audit Logs
export async function getAuditLogs(eventType?: string, limit: number = 100) {
    const response = await apiClient.get("/api/admin/audit-logs", {
        params: { event_type: eventType, limit }
    });
    return response.data;
}

// Compliance
export async function getPendingKYC() {
    const response = await apiClient.get("/api/admin/compliance/kyc-pending");
    return response.data;
}

export async function getAMLFlags() {
    const response = await apiClient.get("/api/admin/compliance/aml-flags");
    return response.data;
}

export async function approveKYC(submissionId: string) {
    const response = await apiClient.post(`/api/admin/compliance/kyc/${submissionId}/approve`);
    return response.data;
}

export async function rejectKYC(submissionId: string, reason: string) {
    const response = await apiClient.post(`/api/admin/compliance/kyc/${submissionId}/reject`, { reason });
    return response.data;
}


