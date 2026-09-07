import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getSystemSettings, updateSystemSettings } from "@/api/admin";
import { Loader2, Save } from "lucide-react";
import { useState, useEffect } from "react";
// import { toast } from "sonner";

export default function PlatformSettings() {
    const queryClient = useQueryClient();
    const { data: settings, isLoading } = useQuery({
        queryKey: ["systemSettings"],
        queryFn: getSystemSettings,
    });

    const [formState, setFormState] = useState<any>({});

    useEffect(() => {
        if (settings) {
            setFormState(settings);
        }
    }, [settings]);

    const mutation = useMutation({
        mutationFn: updateSystemSettings,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["systemSettings"] });
            alert("Settings updated successfully"); // Simple alert for now
        },
        onError: (err) => {
            alert(`Failed to update settings: ${err}`);
        }
    });

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        if (window.confirm("Are you sure you want to update platform settings?")) {
            mutation.mutate(formState);
        }
    };

    const handleChange = (key: string, value: any) => {
        setFormState((prev: any) => ({ ...prev, [key]: value }));
    };

    if (isLoading) {
        return (
            <AdminLayout>
                <div className="flex items-center justify-center h-64">
                    <Loader2 className="w-8 h-8 animate-spin" style={{ color: 'var(--admin-blue)' }} />
                </div>
            </AdminLayout>
        );
    }

    return (
        <AdminLayout>
            <div className="space-y-6">
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                            Platform Settings
                        </h1>
                        <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                            Configure global system parameters
                        </p>
                    </div>
                </div>

                <div className="admin-card">
                    <form onSubmit={handleSubmit} className="space-y-6">

                        {/* Maintenance Mode */}
                        <div className="flex items-center justify-between p-4 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                            <div>
                                <h3 className="font-semibold" style={{ color: 'var(--admin-text-primary)' }}>Maintenance Mode</h3>
                                <p className="text-sm" style={{ color: 'var(--admin-text-secondary)' }}>Disable user access for maintenance</p>
                            </div>
                            <label className="relative inline-flex items-center cursor-pointer">
                                <input
                                    type="checkbox"
                                    className="sr-only peer"
                                    checked={formState.maintenance_mode === "true" || formState.maintenance_mode === true}
                                    onChange={(e) => handleChange("maintenance_mode", e.target.checked ? "true" : "false")}
                                />
                                <div className="w-11 h-6 bg-gray-600 rounded-full peer peer-checked:bg-red-600 peer-checked:after:translate-x-full after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all"></div>
                            </label>
                        </div>

                        {/* Signup Enabled */}
                        <div className="flex items-center justify-between p-4 rounded-lg" style={{ background: 'var(--admin-bg-hover)' }}>
                            <div>
                                <h3 className="font-semibold" style={{ color: 'var(--admin-text-primary)' }}>New Signups</h3>
                                <p className="text-sm" style={{ color: 'var(--admin-text-secondary)' }}>Allow new users to register</p>
                            </div>
                            <label className="relative inline-flex items-center cursor-pointer">
                                <input
                                    type="checkbox"
                                    className="sr-only peer"
                                    checked={formState.signup_enabled === "true" || formState.signup_enabled === true}
                                    onChange={(e) => handleChange("signup_enabled", e.target.checked ? "true" : "false")}
                                />
                                <div className="w-11 h-6 bg-gray-600 rounded-full peer peer-checked:bg-green-600 peer-checked:after:translate-x-full after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all"></div>
                            </label>
                        </div>

                        {/* Max Bots */}
                        <div>
                            <label className="block text-sm font-medium mb-2" style={{ color: 'var(--admin-text-secondary)' }}>
                                Max Bots Per User (Default)
                            </label>
                            <input
                                type="number"
                                className="admin-input"
                                value={formState.max_bots_per_user || 5}
                                onChange={(e) => handleChange("max_bots_per_user", e.target.value)}
                            />
                        </div>

                        {/* Default Risk Cap */}
                        <div>
                            <label className="block text-sm font-medium mb-2" style={{ color: 'var(--admin-text-secondary)' }}>
                                Default Risk Cap (0-1.0)
                            </label>
                            <input
                                type="number"
                                step="0.01"
                                className="admin-input"
                                value={formState.default_risk_cap || 0.05}
                                onChange={(e) => handleChange("default_risk_cap", e.target.value)}
                            />
                        </div>

                        <div className="pt-4">
                            <button
                                type="submit"
                                disabled={mutation.isPending}
                                className="admin-btn admin-btn-primary w-full flex justify-center items-center gap-2"
                            >
                                {mutation.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                                Save Changes
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </AdminLayout>
    );
}
