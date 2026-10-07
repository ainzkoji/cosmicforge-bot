import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "@/auth/AuthContext";
import { LoginError } from "@/api/client";
import { Loader2, Eye, EyeOff } from "lucide-react";
import logo from "../assets/logo.png";

export default function Login() {
    const { login } = useAuth();
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [showPassword, setShowPassword] = useState(false);
    // Shown once the backend answers TOTP_REQUIRED (account has 2FA enabled).
    const [totpRequired, setTotpRequired] = useState(false);
    const [totpCode, setTotpCode] = useState("");

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        setError(null);
        try {
            // Admins sign in on the separate admin portal; this app has no /admin route.
            await login({
                username: email,
                password,
                ...(totpRequired && totpCode ? { totp_code: totpCode } : {}),
            });
            navigate("/dashboard");
        } catch (err: any) {
            const code = err instanceof LoginError ? err.code : null;
            if (code === "TOTP_REQUIRED") {
                // Password accepted; ask for the authenticator code and resubmit.
                setTotpRequired(true);
                setTotpCode("");
                return;
            }
            if (code === "TOTP_INVALID") {
                setTotpRequired(true);
                setTotpCode("");
                setError("That code is not valid. Enter the current 6-digit code from your authenticator app.");
                return;
            }
            console.error(err);
            if (err.message === "User not verified") {
                navigate("/verify-email");
                return;
            }
            // Use the actual error message if available (e.g. from our debug work) or fallback
            setError(err.message || "Invalid credentials. Please try again.");
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-white">
            <div className="w-full max-w-md px-8 py-12">
                {/* Logo */}
                <div className="flex justify-center mb-8">
                    <img src={logo}alt="CosmicForge" className="h-16 w-16" />
                </div>

                {/* Title */}
                <h1 className="text-2xl font-bold text-center text-[#1E1B4B] mb-2">Welcome Back!</h1>
                <p className="text-center text-gray-500 text-sm mb-8">Sign in to continue trading</p>

                <form onSubmit={handleSubmit} className="space-y-5">
                    {error && (
                        <div className="p-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm text-center">
                            {error}
                        </div>
                    )}

                    {/* Email */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Email</label>
                        <input
                            type="email"
                            required
                            value={email}
                            onChange={(e) => { setEmail(e.target.value); setTotpRequired(false); setTotpCode(""); }}
                            className="w-full px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all text-gray-900 placeholder:text-gray-400"
                            placeholder="Enter your email"
                        />
                    </div>

                    {/* Password */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Password</label>
                        <div className="relative">
                            <input
                                type={showPassword ? "text" : "password"}
                                required
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                className="w-full px-4 py-3 pr-12 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all text-gray-900 placeholder:text-gray-400"
                                placeholder="Enter your password"
                            />
                            <button
                                type="button"
                                onClick={() => setShowPassword(!showPassword)}
                                className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 transition-colors"
                            >
                                {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                            </button>
                        </div>
                    </div>

                    {/* Authenticator code (accounts with 2FA) */}
                    {totpRequired && (
                        <div>
                            <label htmlFor="totp-code" className="block text-sm font-medium text-gray-700 mb-1.5">Authenticator code</label>
                            <input
                                id="totp-code"
                                type="text"
                                inputMode="numeric"
                                autoComplete="one-time-code"
                                pattern="[0-9]{6}"
                                maxLength={6}
                                required
                                autoFocus
                                value={totpCode}
                                onChange={(e) => setTotpCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
                                className="w-full px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all text-gray-900 placeholder:text-gray-400 text-center font-mono text-xl tracking-widest"
                                placeholder="000000"
                            />
                            <p className="mt-1.5 text-xs text-gray-500">Enter the 6-digit code from your authenticator app.</p>
                        </div>
                    )}

                    {/* Forgot Password */}
                    <div className="text-right">
                        <Link to="/forgot-password" className="text-sm text-[#1E1B4B] hover:underline">Forgot Password?</Link>
                    </div>

                    {/* Submit Button */}
                    <button
                        type="submit"
                        disabled={loading || (totpRequired && totpCode.length !== 6)}
                        className="w-full py-3.5 rounded-xl bg-[#1E1B4B] text-white font-semibold hover:bg-[#2D2A5B] flex items-center justify-center gap-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-[#1E1B4B]/30"
                    >
                        {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : totpRequired ? "Verify & Sign In" : "Sign In"}
                    </button>
                </form>

                {/* Divider */}
                <div className="flex items-center my-6">
                    <div className="flex-1 border-t border-gray-200" />
                    <span className="px-4 text-sm text-gray-400">Or continue with</span>
                    <div className="flex-1 border-t border-gray-200" />
                </div>

                {/* Social Login */}
                <div className="flex gap-4">
                    <button className="flex-1 py-3 rounded-xl border border-gray-200 bg-white hover:bg-gray-50 flex items-center justify-center gap-2 transition-colors">
                        <img src="https://www.google.com/favicon.ico" alt="Google" className="w-5 h-5" />
                        <span className="text-sm font-medium text-gray-700">Google</span>
                    </button>
                    <button className="flex-1 py-3 rounded-xl border border-gray-200 bg-white hover:bg-gray-50 flex items-center justify-center gap-2 transition-colors">
                        <svg className="w-5 h-5" fill="#1877F2" viewBox="0 0 24 24">
                            <path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z" />
                        </svg>
                        <span className="text-sm font-medium text-gray-700">Facebook</span>
                    </button>
                </div>

                {/* Sign Up Link */}
                <p className="mt-8 text-center text-gray-500 text-sm">
                    Don't have an account?{" "}
                    <Link to="/register" className="text-[#1E1B4B] font-semibold hover:underline">Sign Up</Link>
                </p>
            </div>
        </div>
    );
}
