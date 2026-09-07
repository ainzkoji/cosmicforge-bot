import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { motion } from "framer-motion";
import { Shield, ShieldCheck, ArrowRight, Copy, Check } from "lucide-react";

export default function Setup2FA() {
    const navigate = useNavigate();
    const [step, setStep] = useState<"intro" | "scan" | "verify">("intro");
    const [code, setCode] = useState("");
    const [copied, setCopied] = useState(false);

    const MOCK_SECRET = "JBSWY3DPEHPK3PXP"; // Mock TOTP secret

    const handleCopy = () => {
        navigator.clipboard.writeText(MOCK_SECRET);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    const handleVerify = (e: React.FormEvent) => {
        e.preventDefault();
        // Mock verify
        if (code.length === 6) {
            navigate("/subscription?plan_selection=true"); // Proceed to subscription
        }
    };

    return (
        <div className="min-h-screen bg-background flex flex-col items-center justify-center p-4">
            <div className="w-full max-w-lg">
                {/* Progress */}
                <div className="flex justify-center mb-8 gap-2">
                    <div className={`h-2 w-12 rounded-full transition-colors ${step === 'intro' ? 'bg-primary' : 'bg-primary/30'}`} />
                    <div className={`h-2 w-12 rounded-full transition-colors ${step === 'scan' ? 'bg-primary' : 'bg-primary/30'}`} />
                    <div className={`h-2 w-12 rounded-full transition-colors ${step === 'verify' ? 'bg-primary' : 'bg-primary/30'}`} />
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
                                    We require Two-Factor Authentication (2FA) for all accounts to protect your assets and API keys.
                                </p>
                                <button
                                    onClick={() => setStep("scan")}
                                    className="w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-all flex items-center justify-center gap-2"
                                >
                                    Setup 2FA Now <ArrowRight className="w-4 h-4" />
                                </button>
                                <button onClick={() => navigate("/dashboard")} className="text-sm text-muted-foreground hover:text-foreground">
                                    Skip for now (Not Recommended)
                                </button>
                            </motion.div>
                        )}

                        {step === "scan" && (
                            <motion.div
                                initial={{ opacity: 0, x: 20 }}
                                animate={{ opacity: 1, x: 0 }}
                                className="space-y-6"
                            >
                                <h2 className="text-2xl font-bold">Scan QR Code</h2>
                                <p className="text-muted-foreground">
                                    Open your authenticator app (Google Auth, Authy, etc.) and scan this code.
                                </p>

                                <div className="bg-white p-4 rounded-xl w-48 h-48 mx-auto shadow-inner flex items-center justify-center">
                                    {/* Mock QR Placeholder */}
                                    <div className="w-full h-full bg-slate-900 pattern-isometric pattern-opacity-100 pattern-size-4" style={{ backgroundImage: 'radial-gradient(black 2px, transparent 2px)', backgroundSize: '10px 10px' }} />
                                </div>

                                <div className="bg-muted p-3 rounded-lg flex items-center justify-between gap-3">
                                    <code className="font-mono font-bold tracking-widest">{MOCK_SECRET}</code>
                                    <button onClick={handleCopy} className="p-2 hover:bg-background rounded-md transition-colors text-muted-foreground hover:text-primary">
                                        {copied ? <Check className="w-4 h-4" /> : <Copy className="w-4 h-4" />}
                                    </button>
                                </div>

                                <button
                                    onClick={() => setStep("verify")}
                                    className="w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-all"
                                >
                                    I've Scanned It →
                                </button>
                            </motion.div>
                        )}

                        {step === "verify" && (
                            <motion.div
                                initial={{ opacity: 0, x: 20 }}
                                animate={{ opacity: 1, x: 0 }}
                                className="space-y-6"
                            >
                                <h2 className="text-2xl font-bold">Verify Code</h2>
                                <p className="text-muted-foreground">
                                    Enter the 6-digit code from your authenticator app to enable 2FA.
                                </p>

                                <form onSubmit={handleVerify} className="space-y-6">
                                    <input
                                        type="text"
                                        maxLength={6}
                                        value={code}
                                        onChange={(e) => setCode(e.target.value.replace(/\D/g, ''))}
                                        placeholder="000 000"
                                        className="w-full text-center text-4xl font-mono tracking-[0.5em] py-4 bg-background border border-border rounded-xl focus:ring-2 focus:ring-primary outline-none"
                                        autoFocus
                                    />

                                    <button
                                        type="submit"
                                        disabled={code.length !== 6}
                                        className="w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-all disabled:opacity-50"
                                    >
                                        Enable 2FA
                                    </button>
                                </form>
                                <button onClick={() => setStep("scan")} className="text-sm text-primary hover:underline">
                                    Back to QR Code
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
