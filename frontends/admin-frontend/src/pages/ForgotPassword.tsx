import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "@/api/client";
import { Loader2, ArrowLeft, Mail, Check } from "lucide-react";

export default function ForgotPassword() {
    const [email, setEmail] = useState("");
    const [loading, setLoading] = useState(false);
    const [success, setSuccess] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        setError(null);
        try {
            await api.forgotPassword(email);
            setSuccess(true);
        } catch (err: any) {
            // We usually don't want to reveal if email exists, but for UX we might show generic error
            console.error(err);
            setError("Failed to process request. Please try again.");
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
                    <h2 className="text-2xl font-bold text-[#1E1B4B] mb-2">Check your email</h2>
                    <p className="text-gray-600 mb-6">
                        If an account exists for <span className="font-semibold text-[#1E1B4B]">{email}</span>, we've sent a reset code to your email.
                    </p>
                    <Link
                        to={`/reset-password?email=${encodeURIComponent(email)}`}
                        className="inline-block w-full py-3 rounded-xl bg-[#1E1B4B] text-white font-semibold hover:bg-[#2D2A5B] transition-all mb-4"
                    >
                        Enter Reset Code
                    </Link>
                    <Link to="/login" className="inline-flex items-center text-sm text-gray-500 hover:text-[#1E1B4B] transition-colors">
                        <ArrowLeft className="w-4 h-4 mr-2" /> Back to Login
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
                    <h1 className="text-2xl font-bold text-[#1E1B4B] mb-2">Reset Password</h1>
                    <p className="text-gray-500">Enter your email to receive reset instructions</p>
                </div>

                <form onSubmit={handleSubmit} className="space-y-6">
                    {error && (
                        <div className="p-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm text-center">
                            {error}
                        </div>
                    )}

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Email Address</label>
                        <div className="relative">
                            <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
                            <input
                                type="email"
                                required
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                className="w-full pl-12 pr-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all"
                                placeholder="Enter your email"
                            />
                        </div>
                    </div>

                    <button
                        type="submit"
                        disabled={loading}
                        className="w-full py-3.5 rounded-xl bg-[#1E1B4B] text-white font-semibold hover:bg-[#2D2A5B] flex items-center justify-center gap-2 transition-all disabled:opacity-50"
                    >
                        {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : "Send Reset Link"}
                    </button>
                </form>

                <div className="text-center mt-8">
                    <Link to="/login" className="inline-flex items-center text-sm text-gray-500 hover:text-[#1E1B4B] transition-colors">
                        <ArrowLeft className="w-4 h-4 mr-2" /> Back to Login
                    </Link>
                </div>
            </div>
        </div>
    );
}
