import { useNavigate, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/client";
import { useForm } from "react-hook-form";
import { ArrowLeft, Save, AlertTriangle } from "lucide-react";
import { useEffect } from "react";

interface EditBotForm {
    name: string;
    risk_level: string;
    allocation_type: string;
    allocation_value: number;
    capital_allocation: number;
    capital_allocation_type: string;
}

export default function EditBot() {
    const { id } = useParams<{ id: string }>();
    const navigate = useNavigate();
    const queryClient = useQueryClient();

    const { data: bot, isLoading: isBotLoading, error } = useQuery({
        queryKey: ['bot', id],
        queryFn: () => api.getBotDetails(id!),
        enabled: !!id
    });

    const { register, handleSubmit, setValue, watch, formState: { errors } } = useForm<EditBotForm>();

    // Watch allocation type to update helper text or validation label
    const allocationType = watch('allocation_type');
    const capitalAllocation = watch('capital_allocation');
    const capitalAllocationType = watch('capital_allocation_type');

    useEffect(() => {
        if (bot) {
            setValue('name', bot.name ?? '');
            // Default to 'balanced' if not present (risk profile mapping logic is complex, simplistic for now)
            setValue('risk_level', 'balanced');

            // Set Allocation Settings
            setValue('allocation_type', bot.allocation_type || 'fixed_amount');
            setValue('allocation_value', bot.allocation_value || 100);
            setValue('capital_allocation', bot.capital_allocation || 1000);
            setValue('capital_allocation_type', bot.capital_allocation_type || 'fixed_amount');
        }
    }, [bot, setValue]);

    const mutation = useMutation({
        mutationFn: (data: EditBotForm) => api.updateBotInstance(id!, {
            name: data.name,
            allocation_type: data.allocation_type,
            allocation_value: data.allocation_value,
            capital_allocation: data.capital_allocation,
            capital_allocation_type: data.capital_allocation_type,
            risk_profile_id: undefined // We are not updating risk profile explicitly via ID for now
        }),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['bot', id] });
            navigate(`/dashboard/bots/${id}`);
        }
    });

    const onSubmit = (data: EditBotForm) => {
        mutation.mutate(data);
    };

    if (isBotLoading) {
        return (
            <div className="flex items-center justify-center min-h-[50vh]">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-500" />
            </div>
        );
    }

    if (error || !bot) {
        return (
            <div className="text-center py-12">
                <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
                <h3 className="text-xl font-bold text-white mb-2">Bot not found</h3>
                <button
                    onClick={() => navigate('/dashboard/bots')}
                    className="mt-4 px-4 py-2 bg-white/10 rounded-lg text-white"
                >
                    Back to Dashboard
                </button>
            </div>
        );
    }

    return (
        <div className="max-w-2xl mx-auto space-y-6">
            <div className="flex items-center gap-4 mb-6">
                <button
                    onClick={() => navigate(`/dashboard/bots/${id}`)}
                    className="p-2 hover:bg-white/5 rounded-lg text-gray-400 hover:text-white transition-colors"
                >
                    <ArrowLeft className="w-5 h-5" />
                </button>
                <h1 className="text-2xl font-bold text-white">Edit Bot Configuration</h1>
            </div>

            <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
                {/* General Settings */}
                <div className="bg-[#111122] border border-white/5 rounded-xl p-6 space-y-4">
                    <h2 className="text-lg font-semibold text-white mb-4">General Settings</h2>

                    <div className="space-y-2">
                        <label className="text-sm text-gray-400">Bot Name</label>
                        <input
                            {...register('name', { required: "Name is required" })}
                            className="w-full bg-black/20 border border-white/10 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-purple-500/50"
                        />
                        {errors.name && <span className="text-red-500 text-xs">{errors.name.message}</span>}
                    </div>
                </div>

                {/* Capital Deployment & Trade Size */}
                <div className="bg-[#111122] border border-white/5 rounded-xl p-6 space-y-6">
                    <h2 className="text-lg font-semibold text-white">Capital & Sizing</h2>

                    {/* Total Budget */}
                    <div className="space-y-2">
                        <label className="text-sm text-gray-400">Total Capital Budget</label>
                        <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <select
                                    {...register('capital_allocation_type')}
                                    className="w-full bg-black/20 border border-white/10 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-purple-500/50"
                                >
                                    <option value="fixed_amount">Fixed Amount (USDT)</option>
                                    <option value="percent_balance">% of Balance</option>
                                </select>
                            </div>
                            <div className="relative">
                                <input
                                    type="number"
                                    step={capitalAllocationType === 'percent_balance' ? "1" : "10"}
                                    {...register('capital_allocation', {
                                        valueAsNumber: true,
                                        validate: (value) => {
                                            if (capitalAllocationType === 'percent_balance') {
                                                if (value <= 0 || value > 100) return "Must be between 0% and 100%";
                                            } else {
                                                if (value < 50) return "Minimum capital is $50";
                                            }
                                            return true;
                                        }
                                    })}
                                    className="w-full bg-black/20 border border-white/10 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-purple-500/50"
                                />
                                <span className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500 text-sm">
                                    {capitalAllocationType === 'percent_balance' ? '%' : 'USDT'}
                                </span>
                            </div>
                        </div>
                        {errors.capital_allocation && <span className="text-red-500 text-xs">{errors.capital_allocation.message}</span>}
                        <p className="text-xs text-gray-500">Maximum capital needed for this bot's positions.</p>
                    </div>

                    <div className="border-t border-white/5 pt-4">
                        <h3 className="text-md font-medium text-white mb-4">Trade Amount per Position</h3>
                        <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <label className="text-sm text-gray-400">Type</label>
                                <select
                                    {...register('allocation_type')}
                                    className="w-full bg-black/20 border border-white/10 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-purple-500/50"
                                >
                                    <option value="fixed_amount">Fixed Amount (USDT)</option>
                                    <option value="percent_balance">% of Balance</option>
                                </select>
                            </div>
                            <div className="space-y-2">
                                <label className="text-sm text-gray-400">Value</label>
                                <div className="relative">
                                    <input
                                        type="number"
                                        step={allocationType === 'percent_balance' ? "0.1" : "1"}
                                        {...register('allocation_value', {
                                            valueAsNumber: true,
                                            min: { value: 0.1, message: "Must be greater than 0" }
                                        })}
                                        className="w-full bg-black/20 border border-white/10 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-purple-500/50"
                                    />
                                    <span className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500 text-sm">
                                        {allocationType === 'percent_balance' ? '%' : 'USDT'}
                                    </span>
                                </div>
                                {errors.allocation_value && <span className="text-red-500 text-xs">{errors.allocation_value.message}</span>}
                            </div>
                        </div>
                        {/* Dynamic Helper Text */}
                        <div className="mt-2 text-xs text-gray-500">
                            {/* Helpful text based on combinations */}
                            {capitalAllocationType === 'percent_balance' && (
                                <span className="text-blue-400 block mb-1">Bot budget tracks {capitalAllocation}% of your account balance.</span>
                            )}
                            {allocationType === 'fixed_amount' && capitalAllocationType === 'fixed_amount' && capitalAllocation > 0 && (
                                <span>~{Math.floor(capitalAllocation / (watch('allocation_value') || 1))} concurrent trades max.</span>
                            )}
                        </div>
                    </div>
                </div>

                <div className="flex justify-end gap-3 pt-4">
                    <button
                        type="button"
                        onClick={() => navigate(`/dashboard/bots/${id}`)}
                        className="px-6 py-2 bg-transparent text-gray-400 hover:text-white transition-colors"
                    >
                        Cancel
                    </button>
                    <button
                        type="submit"
                        disabled={mutation.isPending}
                        className="flex items-center gap-2 px-6 py-2 bg-purple-500 text-white rounded-lg hover:bg-purple-600 disabled:opacity-50 transition-all font-medium"
                    >
                        {mutation.isPending ? (
                            <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                        ) : (
                            <Save className="w-4 h-4" />
                        )}
                        Save Changes
                    </button>
                </div>
            </form>
        </div>
    );
}
