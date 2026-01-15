import { useState, useEffect } from "react";
import { useAuth } from "@/auth/AuthContext";
import { api } from "@/api/client";
import { User as UserIcon, Mail, Loader2 } from "lucide-react";

export default function Profile() {
    const { userEmail, userName, refreshUser } = useAuth();
    const [name, setName] = useState(userName || "");
    const [isEditing, setIsEditing] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [success, setSuccess] = useState<string | null>(null);

    // Update local state when context updates (e.g. on initial load)
    useEffect(() => {
        if (userName) setName(userName);
    }, [userName]);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setIsLoading(true);
        setError(null);
        setSuccess(null);

        try {
            await api.updateProfile({ name });
            await refreshUser(); // Update context
            setSuccess("Profile updated successfully");
            setIsEditing(false);
        } catch (err: any) {
            setError(err.message || "Failed to update profile");
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="max-w-4xl mx-auto space-y-8">
            <h1 className="text-3xl font-bold text-[#1E1B4B]">My Profile</h1>

            <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden shadow-sm">
                <div className="h-32 bg-gradient-to-r from-[#2D3A8C] to-[#4752B3]" />
                <div className="px-8 pb-8">
                    <div className="relative -mt-16 mb-6">
                        <div className="w-32 h-32 rounded-2xl bg-white p-2 shadow-lg inline-block">
                            <div className="w-full h-full rounded-xl bg-gray-100 flex items-center justify-center text-[#2D3A8C]">
                                <UserIcon className="w-12 h-12" />
                            </div>
                        </div>
                    </div>

                    <div className="space-y-6">
                        <div className="grid gap-6 md:grid-cols-2">
                            {/* Email Field (Read-only) */}
                            <div className="space-y-2">
                                <label className="text-sm font-medium text-gray-500">Email Address</label>
                                <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg border border-gray-200 text-gray-700">
                                    <Mail className="w-5 h-5 text-gray-400" />
                                    <span>{userEmail}</span>
                                </div>
                            </div>

                            {/* Name Field (Editable) */}
                            <div className="space-y-2">
                                <label className="text-sm font-medium text-gray-500">Display Name</label>
                                {isEditing ? (
                                    <form onSubmit={handleSubmit} className="flex gap-2">
                                        <input
                                            type="text"
                                            value={name}
                                            onChange={(e) => setName(e.target.value)}
                                            className="flex-1 p-2.5 rounded-lg border border-gray-300 focus:ring-2 focus:ring-[#2D3A8C] focus:border-transparent outline-none"
                                            placeholder="Enter your name"
                                        />
                                        <div className="flex gap-2">
                                            <button
                                                type="submit"
                                                disabled={isLoading}
                                                className="px-4 py-2 bg-[#2D3A8C] text-white rounded-lg hover:bg-[#1E2660] disabled:opacity-50"
                                            >
                                                {isLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : "Save"}
                                            </button>
                                            <button
                                                type="button"
                                                onClick={() => { setIsEditing(false); setName(userName || ""); }}
                                                className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
                                            >
                                                Cancel
                                            </button>
                                        </div>
                                    </form>
                                ) : (
                                    <div className="flex items-center justify-between p-3 bg-white rounded-lg border border-gray-200">
                                        <span className="font-medium text-gray-900">{userName || "Not set"}</span>
                                        <button
                                            onClick={() => setIsEditing(true)}
                                            className="text-sm text-[#2D3A8C] font-medium hover:underline"
                                        >
                                            Edit
                                        </button>
                                    </div>
                                )}
                            </div>
                        </div>

                        {success && (
                            <div className="p-4 bg-green-50 text-green-700 rounded-lg border border-green-100 text-sm font-medium">
                                {success}
                            </div>
                        )}

                        {error && (
                            <div className="p-4 bg-red-50 text-red-700 rounded-lg border border-red-100 text-sm font-medium">
                                {error}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
