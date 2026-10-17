import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { QRCodeSVG } from "qrcode.react";
import { useAuth } from "@/auth/AuthContext";
import { AdminAuthError, api } from "@/api/client";
import type { MfaSetupResponse } from "@/api/client";
import { isCompleteTotp, looksLikeRecoveryCode, normaliseTotpInput } from "@/lib/adminMfa";
import { Loader2, Eye, EyeOff } from "lucide-react";
import logo from "../assets/logo.png";

/*
 * Admin sign-in (Section H 8.4):
 *   password -> (admin has MFA)        authenticator code or recovery code
 *            -> (MFA required, absent) enrolment: scan, confirm, save recovery codes
 * The step tokens live in this component's state only.
 */
type Step =
    | { kind: "password" }
    | { kind: "mfa"; mfaToken: string }
    | { kind: "enroll"; enrollmentToken: string; setup: MfaSetupResponse | null }
    | { kind: "recovery-codes"; codes: string[] };

const INPUT_CLASS = "w-full px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#1E1B4B] focus:ring-2 focus:ring-[#1E1B4B]/20 outline-none transition-all text-gray-900 placeholder:text-gray-400";
const BUTTON_CLASS = "w-full py-3.5 rounded-xl bg-[#1E1B4B] text-white font-semibold hover:bg-[#2D2A5B] flex items-center justify-center gap-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-[#1E1B4B]/30";

function messageFor(err: unknown, fallback: string): string {
    if (err instanceof AdminAuthError) {
        if (err.status === 429) return "Too many attempts. Wait a few minutes and try again.";
        if (err.code === "MFA_CHALLENGE_INVALID") return "This sign-in step expired. Enter your password again.";
        if (err.code === "MFA_INVALID") return "That code is not valid (or was already used).";
        return err.message || fallback;
    }
    return fallback;
}

export default function Login() {
    const { login, completeSignIn } = useAuth();
    const navigate = useNavigate();
    const [step, setStep] = useState<Step>({ kind: "password" });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [showPassword, setShowPassword] = useState(false);
    const [code, setCode] = useState("");
    const [useRecovery, setUseRecovery] = useState(false);

    const restart = (message: string | null = null) => {
        setStep({ kind: "password" });
        setCode("");
        setUseRecovery(false);
        setError(message);
    };

    const handlePassword = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        setError(null);
        try {
            // Only admin accounts can sign in here; there is no end-user area in this app.
            const next = await login({ username: email, password });
            setPassword("");
            if (next.kind === "session") {
                navigate("/admin");
            } else if (next.kind === "mfa") {
                setCode("");
                setStep({ kind: "mfa", mfaToken: next.mfaToken });
            } else {
                setCode("");
                const setup = await api.mfaSetup(next.enrollmentToken);
                setStep({ kind: "enroll", enrollmentToken: next.enrollmentToken, setup });
            }
        } catch (err) {
            console.error(err);
            setError(err instanceof AdminAuthError && err.status === 429
                ? "Too many attempts. Wait a few minutes and try again."
                : "Invalid credentials. Please try again.");
        } finally {
            setLoading(false);
        }
    };

    const handleMfa = async (e: React.FormEvent) => {
        e.preventDefault();
        if (step.kind !== "mfa") return;
        setLoading(true);
        setError(null);
        try {
            const res = await api.mfaVerify(step.mfaToken, useRecovery ? { recoveryCode: code } : { totp: code });
            completeSignIn(res, email);
            navigate("/admin");
        } catch (err) {
            if (err instanceof AdminAuthError && err.code === "MFA_CHALLENGE_INVALID") {
                restart(messageFor(err, "Verification failed"));
            } else {
                setError(messageFor(err, "Verification failed"));
            }
        } finally {
            setLoading(false);
        }
    };

    const handleEnroll = async (e: React.FormEvent) => {
        e.preventDefault();
        if (step.kind !== "enroll") return;
        setLoading(true);
        setError(null);
        try {
            const res = await api.mfaEnroll(code, step.enrollmentToken);
            completeSignIn(res, email);
            setCode("");
            setStep({ kind: "recovery-codes", codes: res.recovery_codes || [] });
        } catch (err) {
            if (err instanceof AdminAuthError && err.status === 401) {
                restart("This sign-in step expired. Enter your password again.");
            } else {
                setError(messageFor(err, "Enrolment failed"));
            }
        } finally {
            setLoading(false);
        }
    };

    const codeReady = useRecovery ? looksLikeRecoveryCode(code) : isCompleteTotp(code);

    return (
        <div className="min-h-screen flex items-center justify-center bg-white">
            <div className="w-full max-w-md px-8 py-12">
                {/* Logo */}
                <div className="flex justify-center mb-8">
                    <img src={logo} alt="CosmicForge" className="h-16 w-16" />
                </div>

                {error && (
                    <div className="mb-5 p-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm text-center">
                        {error}
                    </div>
                )}

                {step.kind === "password" && (
                    <>
                        <h1 className="text-2xl font-bold text-center text-[#1E1B4B] mb-2">Welcome Back!</h1>
                        <p className="text-center text-gray-500 text-sm mb-8">Sign in to the admin console</p>

                        <form onSubmit={handlePassword} className="space-y-5">
                            {/* Email */}
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-1.5">Email</label>
                                <input
                                    type="email"
                                    required
                                    autoComplete="username"
                                    value={email}
                                    onChange={(e) => setEmail(e.target.value)}
                                    className={INPUT_CLASS}
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
                                        autoComplete="current-password"
                                        value={password}
                                        onChange={(e) => setPassword(e.target.value)}
                                        className={`${INPUT_CLASS} pr-12`}
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

                            {/* Admin accounts have no self-service password reset. */}
                            <p className="text-right text-xs text-gray-500">
                                Forgot your password? Ask a platform administrator to reset it.
                            </p>

                            <button type="submit" disabled={loading} className={BUTTON_CLASS}>
                                {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : "Sign In"}
                            </button>
                        </form>
                    </>
                )}

                {step.kind === "mfa" && (
                    <>
                        <h1 className="text-2xl font-bold text-center text-[#1E1B4B] mb-2">Two-step verification</h1>
                        <p className="text-center text-gray-500 text-sm mb-8">
                            {useRecovery
                                ? "Enter one of your saved recovery codes. Each code works once."
                                : "Enter the 6-digit code from your authenticator app."}
                        </p>
                        <form onSubmit={handleMfa} className="space-y-5">
                            <input
                                autoFocus
                                required
                                inputMode={useRecovery ? "text" : "numeric"}
                                autoComplete="one-time-code"
                                value={code}
                                onChange={(e) => setCode(useRecovery ? e.target.value.trim() : normaliseTotpInput(e.target.value))}
                                className={`${INPUT_CLASS} text-center tracking-widest`}
                                placeholder={useRecovery ? "xxxxx-xxxxx" : "123456"}
                            />
                            <button type="submit" disabled={loading || !codeReady} className={BUTTON_CLASS}>
                                {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : "Verify"}
                            </button>
                            <div className="flex justify-between text-xs text-gray-500">
                                <button type="button" className="hover:text-gray-700" onClick={() => { setUseRecovery(!useRecovery); setCode(""); setError(null); }}>
                                    {useRecovery ? "Use authenticator code" : "Use a recovery code"}
                                </button>
                                <button type="button" className="hover:text-gray-700" onClick={() => restart()}>
                                    Start over
                                </button>
                            </div>
                        </form>
                    </>
                )}

                {step.kind === "enroll" && (
                    <>
                        <h1 className="text-2xl font-bold text-center text-[#1E1B4B] mb-2">Set up two-step verification</h1>
                        <p className="text-center text-gray-500 text-sm mb-6">
                            Admin accounts must use an authenticator app. Scan the code, then enter the 6-digit code it shows.
                        </p>
                        {step.setup && (
                            <div className="flex flex-col items-center gap-3 mb-6">
                                <QRCodeSVG value={step.setup.provisioning_uri} size={176} />
                                <p className="text-xs text-gray-500 text-center break-all">
                                    Can't scan? Enter this key manually: <span className="font-mono text-gray-700">{step.setup.secret}</span>
                                </p>
                            </div>
                        )}
                        <form onSubmit={handleEnroll} className="space-y-5">
                            <input
                                autoFocus
                                required
                                inputMode="numeric"
                                autoComplete="one-time-code"
                                value={code}
                                onChange={(e) => setCode(normaliseTotpInput(e.target.value))}
                                className={`${INPUT_CLASS} text-center tracking-widest`}
                                placeholder="123456"
                            />
                            <button type="submit" disabled={loading || !isCompleteTotp(code)} className={BUTTON_CLASS}>
                                {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : "Confirm and continue"}
                            </button>
                            <button type="button" className="w-full text-xs text-gray-500 hover:text-gray-700" onClick={() => restart()}>
                                Start over
                            </button>
                        </form>
                    </>
                )}

                {step.kind === "recovery-codes" && (
                    <>
                        <h1 className="text-2xl font-bold text-center text-[#1E1B4B] mb-2">Save your recovery codes</h1>
                        <p className="text-center text-gray-500 text-sm mb-6">
                            Each code signs you in once if you lose your authenticator. They are shown only now: store them somewhere safe.
                        </p>
                        <ul className="grid grid-cols-2 gap-2 mb-6 font-mono text-sm text-gray-800">
                            {step.codes.map((recovery) => (
                                <li key={recovery} className="px-3 py-2 rounded-lg bg-gray-50 border border-gray-200 text-center">{recovery}</li>
                            ))}
                        </ul>
                        <button type="button" className={BUTTON_CLASS} onClick={() => navigate("/admin")}>
                            I have saved them
                        </button>
                    </>
                )}
            </div>
        </div>
    );
}
