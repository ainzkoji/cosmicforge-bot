import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { motion } from "framer-motion";
import { Shield, ShieldCheck, ArrowRight } from "lucide-react";
import { TwoFASetup } from "@/components/Auth/TwoFA";

export default function Setup2FA() {
    const navigate = useNavigate();
    // "setup" hands over to TwoFASetup, which asks the backend for a real
    // secret (POST /auth/2fa/setup) and only completes once the backend has
    // accepted a code from the authenticator app (POST /auth/2fa/verify).
    const [step, setStep] = useState<"intro" | "setup">("intro");

    return (
        <div className="min-h-screen bg-background flex flex-col items-center justify-center p-4">
            <div className="w-full max-w-lg">
                {/* Progress */}
                <div className="flex justify-center mb-8 gap-2">
                    <div className={`h-2 w-12 rounded-full transition-colors ${step === 'intro' ? 'bg-primary' : 'bg-primary/30'}`} />
                    <div className={`h-2 w-12 rounded-full transition-colors ${step === 'setup' ? 'bg-primary' : 'bg-primary/30'}`} />
                </div>

                <div className="bg-card border border-border rounded-2xl shadow-xl overflow-hidden">
                    <div className="p-8 text-center space-y-6">

                        <div className="mx-auto w-16 h-16 bg-primary/10 rounded-2xl flex items-center justify-center text-primary mb-4">
                            <ShieldCheck className="w-8 h-8" />
                        </div>

                        {step === "intro" && (
                            <motion.div
                                initial={{ opacity: 0, y: 10 }}
                                animate={{ opacity: 1, y: 0 }}
                                className="space-y-6"
                            >
                                <h2 className="text-2xl font-bold">Secure Your Account</h2>
                                <p className="text-muted-foreground text-lg">
                                    We recommend enabling Two-Factor Authentication (2FA) to help protect your account and API keys. It is optional and you can set it up later in Security settings.
                                </p>
                                <button
                                    onClick={() => setStep("setup")}
                                    className="w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-all flex items-center justify-center gap-2"
                                >
                                    Setup 2FA Now <ArrowRight className="w-4 h-4" />
                                </button>
                                <button onClick={() => navigate("/dashboard")} className="text-sm text-muted-foreground hover:text-foreground">
                                    Skip for now (Not Recommended)
                                </button>
                            </motion.div>
                        )}

                        {step === "setup" && (
                            <motion.div
                                initial={{ opacity: 0, x: 20 }}
                                animate={{ opacity: 1, x: 0 }}
                                className="space-y-6"
                            >
                                <TwoFASetup onComplete={() => navigate("/dashboard/subscription?plan_selection=true")} />
                                <button onClick={() => setStep("intro")} className="text-sm text-primary hover:underline">
                                    Back
                                </button>
                            </motion.div>
                        )}

                    </div>
                </div>

                <div className="text-center mt-8 flex items-center justify-center gap-2 text-muted-foreground text-sm">
                    <Shield className="w-4 h-4" />
                    <span>Your security is our top priority.</span>
                </div>
            </div>
        </div>
    );
}
