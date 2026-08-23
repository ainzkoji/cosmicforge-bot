import { useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { useAuth } from "@/auth/AuthContext";
import { Mail, Lock, Github, CheckCircle, ArrowRight, ShieldCheck } from "lucide-react";

import { api } from "@/api/client";

export default function Register() {
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const refCode = searchParams.get("ref");

    const [isLoading, setIsLoading] = useState(false);
    const [formData, setFormData] = useState({
        email: "",
        password: "",
        confirmPassword: "",
        role: "trader" // or 'affiliate'
    });

    const { register } = useAuth();
    const [error, setError] = useState<string | null>(null);

    const handleRegister = async (e: React.FormEvent) => {
        e.preventDefault();
        setIsLoading(true);
        setError(null);

        if (formData.password !== formData.confirmPassword) {
            setError("Passwords do not match");
            setIsLoading(false);
            return;
        }

        try {
            await register({
                email: formData.email,
                password: formData.password,
                confirmed_password: formData.confirmPassword, // Updated backend expects this
            });
            // On success, backend usually returns user or token.
            // AuthContext.register typically auto-logs in or just returns.
            // Assuming we need to verify email next:
            navigate("/verify-email", { state: { email: formData.email } });
        } catch (err: any) {
            console.error("Registration error:", err);
            const errorMessage = err.message || "Registration failed. Please try again.";

            // Check if error is due to existing user
            if (errorMessage.toLowerCase().includes("already registered") || errorMessage.toLowerCase().includes("exists")) {
                try {
                    // Try to resend verification email
                    await api.resendVerification(formData.email);
                    navigate("/verify-email", {
                        state: {
                            email: formData.email,
                            message: "Account exists but is unverified. A new verification code has been sent."
                        }
                    });
                    return;
                } catch (resendErr) {
                    // If resend fails, likely user is already verified or other issue, show original error
                    console.error("Resend failed:", resendErr);
                    setError(errorMessage);
                }
            } else {
                setError(errorMessage);
            }
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-background flex flex-col md:flex-row">
            {/* Left Panel - Branding */}
            <div className="hidden md:flex flex-col justify-between w-1/2 lg:w-2/5 bg-black p-12 relative overflow-hidden">
                <div className="absolute inset-0 bg-grid-white/5 bg-[size:30px_30px]" />
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[500px] bg-primary/20 rounded-full blur-[100px]" />

                <div className="relative z-10">
                    <div className="flex items-center gap-2 text-primary font-bold text-xl mb-2">
                        <div className="w-8 h-8 rounded bg-primary flex items-center justify-center text-black">
                            <ShieldCheck className="w-5 h-5" />
                        </div>
                        CosmicForge
                    </div>
                </div>

                <div className="relative z-10 text-white space-y-6">
                    <h1 className="text-4xl font-bold tracking-tight leading-tight">
                        Automate your trading across Crypto, Forex, and Stocks.
                    </h1>
                    <div className="space-y-4">
                        <div className="flex items-center gap-3">
                            <CheckCircle className="w-5 h-5 text-green-500" />
                            <span className="text-gray-300">Institutional-grade execution</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <CheckCircle className="w-5 h-5 text-green-500" />
                            <span className="text-gray-300">End-to-End Encrypted Keys</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <CheckCircle className="w-5 h-5 text-green-500" />
                            <span className="text-gray-300">AI-Powered Risk Management</span>
                        </div>
                    </div>
                </div>

                <div className="relative z-10 text-gray-500 text-sm">
                    © 2026 CosmicForge Inc.
                </div>
            </div>

            {/* Right Panel - Form */}
            <div className="flex-1 flex items-center justify-center p-8 bg-background relative">
                {/* Mobile bg decor */}
                <div className="absolute top-0 right-0 w-64 h-64 bg-primary/5 rounded-full blur-3xl md:hidden" />

                <div className="w-full max-w-md space-y-8">
                    <div className="text-center md:text-left">
                        <h2 className="text-3xl font-bold tracking-tight">Create your account</h2>
                        <p className="text-muted-foreground mt-2">Start your automated trading journey today.</p>
                    </div>

                    {refCode && (
                        <div className="bg-primary/10 border border-primary/20 rounded-lg p-3 text-sm flex items-center gap-2 text-primary">
                            <CheckCircle className="w-4 h-4" />
                            <span>Referred by <b>{refCode}</b>. You've unlocked a 14-day Pro trial.</span>
                        </div>
                    )}

                    <div className="space-y-3">
                        <button className="w-full h-11 border border-border rounded-lg bg-card hover:bg-muted transition-colors flex items-center justify-center gap-2 font-medium">
                            <svg viewBox="0 0 24 24" className="w-5 h-5" aria-hidden="true"><g><path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"></path><path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"></path><path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"></path><path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"></path></g></svg>
                            Sign up with Google
                        </button>
                        <button className="w-full h-11 border border-border rounded-lg bg-card hover:bg-muted transition-colors flex items-center justify-center gap-2 font-medium">
                            <Github className="w-5 h-5" />
                            Sign up with GitHub
                        </button>
                    </div>

                    <div className="relative">
                        <div className="absolute inset-0 flex items-center">
                            <span className="w-full border-t border-border" />
                        </div>
                        <div className="relative flex justify-center text-xs uppercase">
                            <span className="bg-background px-2 text-muted-foreground">Or continue with</span>
                        </div>
                    </div>

                    <form onSubmit={handleRegister} className="space-y-4">
                        {error && (
                            <div className="p-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm text-center">
                                {error}
                            </div>
                        )}
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Email</label>
                            <div className="relative">
                                <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                                <input
                                    type="email"
                                    className="w-full h-10 pl-10 pr-3 rounded-lg border border-border bg-background focus:ring-2 focus:ring-primary focus:border-transparent outline-none transition-all"
                                    placeholder="name@example.com"
                                    required
                                    value={formData.email}
                                    onChange={e => setFormData({ ...formData, email: e.target.value })}
                                />
                            </div>
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Password</label>
                            <div className="relative">
                                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                                <input
                                    type="password"
                                    className="w-full h-10 pl-10 pr-3 rounded-lg border border-border bg-background focus:ring-2 focus:ring-primary focus:border-transparent outline-none transition-all"
                                    placeholder="Create a strong password"
                                    required
                                    value={formData.password}
                                    onChange={e => setFormData({ ...formData, password: e.target.value })}
                                />
                            </div>
                            <p className="text-xs text-muted-foreground">Must be at least 8 characters with 1 symbol.</p>
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Confirm Password</label>
                            <div className="relative">
                                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                                <input
                                    type="password"
                                    className="w-full h-10 pl-10 pr-3 rounded-lg border border-border bg-background focus:ring-2 focus:ring-primary focus:border-transparent outline-none transition-all"
                                    placeholder="Confirm your password"
                                    required
                                    value={formData.confirmPassword}
                                    onChange={e => setFormData({ ...formData, confirmPassword: e.target.value })}
                                />
                            </div>
                        </div>

                        <button
                            type="submit"
                            disabled={isLoading}
                            className="w-full h-11 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/25 disabled:opacity-50 flex items-center justify-center gap-2"
                        >
                            {isLoading ? "Creating Account..." : "Create Account"} <ArrowRight className="w-4 h-4" />
                        </button>
                    </form>

                    <p className="text-center text-sm text-muted-foreground">
                        Already have an account? <Link to="/login" className="text-primary hover:underline font-semibold">Sign in</Link>
                    </p>

                    {/* Broker Affiliate Links */}
                    <div className="pt-6 border-t border-border text-center space-y-3">
                        <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold">Don't have an exchange account?</p>
                        <div className="flex justify-center gap-4 text-xs font-medium">
                            <a href="https://accounts.binance.com/register?ref=PARTNER_CODE" target="_blank" rel="noreferrer" className="flex items-center gap-1.5 text-gray-500 hover:text-[#FCD535] transition-colors">
                                <div className="w-2 h-2 rounded-full bg-[#FCD535]" /> Binance (20% Off Fees)
                            </a>
                            <a href="https://bingx.com/invite/PARTNER_CODE" target="_blank" rel="noreferrer" className="flex items-center gap-1.5 text-gray-500 hover:text-blue-500 transition-colors">
                                <div className="w-2 h-2 rounded-full bg-blue-500" /> BingX ($5k Bonus)
                            </a>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
