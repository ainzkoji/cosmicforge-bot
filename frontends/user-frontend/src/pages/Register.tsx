import { useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { useAuth } from "@/auth/AuthContext";
import { Mail, Lock, CheckCircle, ArrowRight, ShieldCheck } from "lucide-react";

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
    // Must be ticked by the user; never pre-checked. This records ONLY the Risk
    // Disclosure acknowledgement: the Terms and Privacy pages are placeholders,
    // so no terms acceptance is claimed or recorded until real terms exist.
    const [acceptedRisk, setAcceptedRisk] = useState(false);

    const handleRegister = async (e: React.FormEvent) => {
        e.preventDefault();
        setIsLoading(true);
        setError(null);

        if (formData.password !== formData.confirmPassword) {
            setError("Passwords do not match");
            setIsLoading(false);
            return;
        }

        if (!acceptedRisk) {
            setError("Please confirm that you have read and understood the Risk Disclosure.");
            setIsLoading(false);
            return;
        }
        const acceptedAt = new Date().toISOString();

        try {
            await register({
                email: formData.email,
                password: formData.password,
                confirmed_password: formData.confirmPassword, // Updated backend expects this
                // terms_accepted_at is deliberately not sent (optional on the
                // backend): there are no published terms to accept yet.
                risk_disclaimer_accepted_at: acceptedAt,
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
                        Automated trading on your own exchange account.
                    </h1>
                    <div className="space-y-4">
                        <div className="flex items-center gap-3">
                            <CheckCircle className="w-5 h-5 text-green-500" />
                            <span className="text-gray-300">Rules-based execution with limits you set</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <CheckCircle className="w-5 h-5 text-green-500" />
                            <span className="text-gray-300">Broker credentials encrypted at rest</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <CheckCircle className="w-5 h-5 text-green-500" />
                            <span className="text-gray-300">Paper and demo trading before real money</span>
                        </div>
                    </div>
                    <p className="text-sm text-gray-400">
                        Trading leveraged products carries a high risk of loss. Past or simulated performance
                        does not indicate future results. Nothing here is financial advice.
                    </p>
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
                        <p className="text-muted-foreground mt-2">Set up your account to try automated trading in paper or demo mode.</p>
                    </div>

                    {refCode && (
                        <div className="bg-primary/10 border border-primary/20 rounded-lg p-3 text-sm flex items-center gap-2 text-primary">
                            <CheckCircle className="w-4 h-4" />
                            <span>Referred by <b>{refCode}</b>.</span>
                        </div>
                    )}

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
                            <p className="text-xs text-muted-foreground">Must be at least 8 characters.</p>
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

                        <label className="flex items-start gap-3 text-sm cursor-pointer">
                            <input
                                type="checkbox"
                                required
                                checked={acceptedRisk}
                                onChange={e => setAcceptedRisk(e.target.checked)}
                                className="mt-0.5 h-4 w-4 shrink-0"
                            />
                            <span>
                                I have read and understood the{" "}
                                <Link to="/risk-disclosure" target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-semibold">Risk Disclosure</Link>.
                            </span>
                        </label>

                        <button
                            type="submit"
                            disabled={isLoading || !acceptedRisk}
                            className="w-full h-11 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-all shadow-lg hover:shadow-primary/25 disabled:opacity-50 flex items-center justify-center gap-2"
                        >
                            {isLoading ? "Creating Account..." : "Create Account"} <ArrowRight className="w-4 h-4" />
                        </button>
                    </form>

                    <p className="text-center text-sm text-muted-foreground">
                        Already have an account? <Link to="/login" className="text-primary hover:underline font-semibold">Sign in</Link>
                    </p>

                </div>
            </div>
        </div>
    );
}
