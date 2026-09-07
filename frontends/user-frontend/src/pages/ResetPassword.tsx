import { useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { api } from "@/api/client";
import { Loader2, ArrowLeft, Lock, KeyRound, Check, Eye, EyeOff } from "lucide-react";

export default function ResetPassword() {
    const [searchParams] = useSearchParams();
    const navigate = useNavigate();

    const [email, setEmail] = useState(searchParams.get("email") || "");
    const [code, setCode] = useState("");
    const [newPassword, setNewPassword] = useState("");
    const [confirmPassword, setConfirmPassword] = useState("");
    const [showPassword, setShowPassword] = useState(false);
    const [showConfirmPassword, setShowConfirmPassword] = useState(false);
    const [loading, setLoading] = useState(false);
    const [success, setSuccess] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Password validation
    const passwordRequirements = [
        { label: "At least 8 characters", test: (p: string) => p.length >= 8 },
        { label: "Contains uppercase letter", test: (p: string) => /[A-Z]/.test(p) },
        { label: "Contains lowercase letter", test: (p: string) => /[a-z]/.test(p) },
        { label: "Contains a number", test: (p: string) => /\d/.test(p) },
    ];

    const allRequirementsMet = passwordRequirements.every(req => req.test(newPassword));
    const passwordsMatch = newPassword === confirmPassword && confirmPassword.length > 0;

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();

        if (!allRequirementsMet) {
            setError("Password does not meet requirements");
            return;
        }

        if (!passwordsMatch) {
            setError("Passwords do not match");
            return;
        }

        setLoading(true);
        setError(null);

        try {
            await api.resetPassword(email, code, newPassword);
            setSuccess(true);
            // Redirect to login after 3 seconds
            setTimeout(() => navigate("/login"), 3000);
        } catch (err: any) {
            console.error(err);
            setError(err.message || "Failed to reset password. Please check your code and try again.");
        } finally {
            setLoading(false);
        }
    };

    if (success) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-gray-50 p-4">
                <div className="max-w-md w-full bg-white rounded-2xl shadow-sm border border-gray-100 p-8 text-center">
                    <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-6">
                        <Check className="w-8 h-8 text-green-600" />
                    </div>
                    <h2 className="text-2xl font-bold text-[#1E1B4B] mb-2">Password Reset!</h2>
                    <p className="text-gray-600 mb-8">
                        Your password has been successfully reset. Redirecting to login...
                    </p>
                    <Link to="/login" className="inline-flex items-center text-[#1E1B4B] font-semibold hover:underline">
                        <ArrowLeft className="w-4 h-4 mr-2" /> Go to Login
                    </Link>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen flex items-center justify-center bg-white p-4">
            <div className="w-full max-w-md">
                {/* Logo */}
                <div className="flex justify-center mb-8">
                    <img src="/src/assets/logo.png" alt="CosmicForge" className="h-12 w-12" />
                </div>

                <div className="text-center mb-8">
                    <h1 className="text-2xl font-bold text-[#1E1B4B] mb-2">Set New Password</h1>
                    <p className="text-gray-500">Enter the code from your email and your new password</p>
                </div>

                <form onSubmit={handleSubmit} className="space-y-5">
                    {error && (
                        <div className="p-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm text-center">
                            {error}
                        </div>
                    )}

                    {/* Email (pre-filled if passed via URL) */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Email Address</label>
                        <input
                            type="email"
                            required
                            value={email}
                            onChange={(e) => setEmail(e.target.value)}
                            className="w-full px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all"
                            placeholder="Enter your email"
                        />
                    </div>

                    {/* Reset Code */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Reset Code</label>
                        <div className="relative">
                            <KeyRound className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
                            <input
                                type="text"
                                required
                                value={code}
                                onChange={(e) => setCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
                                className="w-full pl-12 pr-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all text-center text-lg tracking-widest font-mono"
                                placeholder="000000"
                                maxLength={6}
                            />
                        </div>
                    </div>

                    {/* New Password */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">New Password</label>
                        <div className="relative">
                            <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
                            <input
                                type={showPassword ? "text" : "password"}
                                required
                                value={newPassword}
                                onChange={(e) => setNewPassword(e.target.value)}
                                className="w-full pl-12 pr-12 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all"
                                placeholder="Create a new password"
                            />
                            <button
                                type="button"
                                onClick={() => setShowPassword(!showPassword)}
                                className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                            >
                                {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                            </button>
                        </div>

                        {/* Password requirements */}
                        {newPassword && (
                            <div className="mt-2 space-y-1">
                                {passwordRequirements.map((req, idx) => (
                                    <div key={idx} className={`flex items-center text-xs ${req.test(newPassword) ? 'text-green-600' : 'text-gray-400'}`}>
                                        <Check className={`w-3 h-3 mr-1.5 ${req.test(newPassword) ? 'opacity-100' : 'opacity-40'}`} />
                                        {req.label}
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Confirm Password */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Confirm Password</label>
                        <div className="relative">
                            <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
                            <input
                                type={showConfirmPassword ? "text" : "password"}
                                required
                                value={confirmPassword}
                                onChange={(e) => setConfirmPassword(e.target.value)}
                                className={`w-full pl-12 pr-12 py-3 rounded-xl border bg-gray-50 focus:bg-white focus:ring-2 outline-none transition-all ${confirmPassword && !passwordsMatch
                                    ? 'border-red-300 focus:border-red-400 focus:ring-red-200'
                                    : confirmPassword && passwordsMatch
                                        ? 'border-green-300 focus:border-green-400 focus:ring-green-200'
                                        : 'border-gray-200 focus:border-[#1E1B4B] focus:ring-[#1E1B4B]/20'
                                    }`}
                                placeholder="Confirm your password"
                            />
                            <button
                                type="button"
                                onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                                className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                            >
                                {showConfirmPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                            </button>
                        </div>
                        {confirmPassword && !passwordsMatch && (
                            <p className="mt-1 text-xs text-red-500">Passwords do not match</p>
                        )}
                    </div>

                    <button
                        type="submit"
                        disabled={loading || !allRequirementsMet || !passwordsMatch || !code}
                        className="w-full py-3.5 rounded-xl bg-[#1E1B4B] text-white font-semibold hover:bg-[#2D2A5B] flex items-center justify-center gap-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : "Reset Password"}
                    </button>
                </form>

                <div className="text-center mt-8">
                    <Link to="/forgot-password" className="inline-flex items-center text-sm text-gray-500 hover:text-[#1E1B4B] transition-colors">
                        <ArrowLeft className="w-4 h-4 mr-2" /> Request new code
                    </Link>
                </div>
            </div>
        </div>
    );
}
