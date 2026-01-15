import { useState, useEffect } from "react";
import { Link, useNavigate, useLocation } from "react-router-dom";
import { Loader2, Mail, ArrowRight, RefreshCw } from "lucide-react";
import { api } from "@/api/client";

const RESEND_COOLDOWN_SECONDS = 90;

export default function VerifyEmail() {
    const navigate = useNavigate();
    const location = useLocation();
    const emailFromState = (location.state as any)?.email || "";

    const [email, setEmail] = useState(emailFromState);
    const [code, setCode] = useState("");
    const [loading, setLoading] = useState(false);
    const [resending, setResending] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [success, setSuccess] = useState<string | null>(null);
    const [cooldown, setCooldown] = useState(0);

    // Countdown timer for resend
    useEffect(() => {
        if (cooldown > 0) {
            const timer = setTimeout(() => setCooldown(cooldown - 1), 1000);
            return () => clearTimeout(timer);
        }
    }, [cooldown]);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        setError(null);
        try {
            await api.verifyEmail(email, code);
            setSuccess("Email verified! Redirecting to login...");
            setTimeout(() => navigate("/login"), 2000);
        } catch (err: any) {
            setError(err.message || "Verification failed");
        } finally {
            setLoading(false);
        }
    };

    const handleResend = async () => {
        if (!email) {
            setError("Please enter your email");
            return;
        }
        if (cooldown > 0) return;

        setResending(true);
        setError(null);
        setSuccess(null);
        try {
            await api.resendVerification(email);
            setSuccess("A new code has been sent to your email");
            setCooldown(RESEND_COOLDOWN_SECONDS); // Start countdown
        } catch (err: any) {
            setError(err.message || "Failed to resend code");
        } finally {
            setResending(false);
        }
    };

    const formatTime = (seconds: number) => {
        const mins = Math.floor(seconds / 60);
        const secs = seconds % 60;
        return `${mins}:${secs.toString().padStart(2, '0')}`;
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-white">
            <div className="w-full max-w-md px-8 py-12">
                {/* Logo */}
                <div className="flex justify-center mb-8">
                    <div className="w-16 h-16 rounded-full bg-[#2D3A8C] flex items-center justify-center">
                        <Mail className="w-8 h-8 text-white" />
                    </div>
                </div>

                {/* Title */}
                <h1 className="text-2xl font-bold text-center text-gray-900 mb-2">Verify Your Email</h1>
                <p className="text-center text-gray-500 text-sm mb-8">
                    Enter the 6-digit code sent to your email
                </p>

                <form onSubmit={handleSubmit} className="space-y-5">
                    {error && (
                        <div className="p-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm text-center">
                            {error}
                        </div>
                    )}
                    {success && (
                        <div className="p-3 rounded-lg bg-green-50 border border-green-200 text-green-600 text-sm text-center">
                            {success}
                        </div>
                    )}

                    {/* Email */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Email</label>
                        <input
                            type="email"
                            required
                            value={email}
                            onChange={(e) => setEmail(e.target.value)}
                            className="w-full px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#2D3A8C] focus:ring-2 focus:ring-[#2D3A8C]/20 outline-none transition-all text-gray-900"
                            placeholder="Enter your email"
                        />
                    </div>

                    {/* Code */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Verification Code</label>
                        <input
                            type="text"
                            required
                            maxLength={6}
                            value={code}
                            onChange={(e) => setCode(e.target.value.replace(/\D/g, ''))}
                            className="w-full px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#2D3A8C] focus:ring-2 focus:ring-[#2D3A8C]/20 outline-none transition-all text-gray-900 text-center text-2xl tracking-widest font-mono"
                            placeholder="000000"
                        />
                    </div>

                    {/* Submit */}
                    <button
                        type="submit"
                        disabled={loading || code.length !== 6}
                        className="w-full py-3.5 rounded-xl bg-[#2D3A8C] text-white font-semibold hover:bg-[#252f73] flex items-center justify-center gap-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-[#2D3A8C]/30"
                    >
                        {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : <>Verify <ArrowRight className="w-5 h-5" /></>}
                    </button>
                </form>

                {/* Resend with Timer */}
                <div className="mt-6 text-center">
                    <button
                        onClick={handleResend}
                        disabled={resending || cooldown > 0}
                        className="inline-flex items-center gap-2 text-[#2D3A8C] font-medium hover:underline disabled:opacity-50 disabled:cursor-not-allowed disabled:no-underline"
                    >
                        {resending ? (
                            <><Loader2 className="w-4 h-4 animate-spin" /> Sending...</>
                        ) : cooldown > 0 ? (
                            <><RefreshCw className="w-4 h-4" /> Resend in {formatTime(cooldown)}</>
                        ) : (
                            <><RefreshCw className="w-4 h-4" /> Didn't receive a code? Resend</>
                        )}
                    </button>
                </div>

                {/* Back to Login */}
                <p className="mt-6 text-center text-gray-500 text-sm">
                    <Link to="/login" className="text-[#2D3A8C] font-semibold hover:underline">Back to Login</Link>
                </p>
            </div>
        </div>
    );
}
