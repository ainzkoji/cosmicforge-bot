import { useState, useEffect } from "react";
import { QRCodeSVG } from "qrcode.react";
import { api, TwoFASetupResponse } from "@/api/client";
import { Loader2, Copy, Check, Shield } from "lucide-react";

export function TwoFASetup({ onComplete }: { onComplete: () => void }) {
    const [step, setStep] = useState<"init" | "verify">("init");
    const [setupData, setSetupData] = useState<TwoFASetupResponse | null>(null);
    const [verifyCode, setVerifyCode] = useState("");
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [copied, setCopied] = useState(false);

    useEffect(() => {
        initSetup();
    }, []);

    const initSetup = async () => {
        setLoading(true);
        try {
            const data = await api.setup2FA();
            setSetupData(data);
            setStep("verify");
        } catch (err: any) {
            setError("Failed to initialize 2FA setup");
        } finally {
            setLoading(false);
        }
    };

    const handleCopy = () => {
        if (setupData) {
            navigator.clipboard.writeText(setupData.items);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        }
    };

    const handleVerify = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        setError(null);
        try {
            await api.verify2FA(verifyCode);
            onComplete();
        } catch (err: any) {
            setError("Invalid code. Please try again.");
        } finally {
            setLoading(false);
        }
    };

    if (loading && !setupData) {
        return <div className="p-8 text-center"><Loader2 className="w-8 h-8 animate-spin mx-auto text-[#1E1B4B]" /></div>;
    }

    return (
        <div className="bg-white p-6 rounded-2xl border border-gray-200 shadow-sm max-w-sm mx-auto">
            <div className="text-center mb-6">
                <div className="w-12 h-12 bg-[#1E1B4B]/10 rounded-full flex items-center justify-center mx-auto mb-3">
                    <Shield className="w-6 h-6 text-[#1E1B4B]" />
                </div>
                <h3 className="text-xl font-bold text-[#1E1B4B]">Secure Your Account</h3>
                <p className="text-sm text-gray-500 mt-1">Scan the QR code with your authenticator app</p>
            </div>

            {setupData && (
                <div className="space-y-6">
                    <div className="flex justify-center p-4 bg-white border-2 border-dashed border-gray-200 rounded-xl">
                        <QRCodeSVG value={setupData.uri} size={180} />
                    </div>

                    <div>
                        <p className="text-xs text-center text-gray-400 mb-2 uppercase tracking-wide font-medium">Or enter code manually</p>
                        <div
                            onClick={handleCopy}
                            className="bg-gray-50 p-3 rounded-lg flex items-center justify-between cursor-pointer hover:bg-gray-100 transition-colors group"
                        >
                            <code className="text-[#1E1B4B] font-mono text-sm">{setupData.items}</code>
                            {copied ? <Check className="w-4 h-4 text-green-500" /> : <Copy className="w-4 h-4 text-gray-400 group-hover:text-gray-600" />}
                        </div>
                    </div>

                    <form onSubmit={handleVerify}>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5">Enter 6-digit code</label>
                        <input
                            type="text"
                            value={verifyCode}
                            onChange={(e) => setVerifyCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
                            className="w-full text-center text-2xl tracking-widest px-4 py-3 rounded-xl border border-gray-200 focus:border-[#1E1B4B] focus:ring-1 focus:ring-[#1E1B4B] outline-none mb-4 font-mono"
                            placeholder="000 000"
                        />

                        {error && <p className="text-red-500 text-sm text-center mb-4">{error}</p>}

                        <button
                            type="submit"
                            disabled={verifyCode.length !== 6 || loading}
                            className="w-full py-3 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] disabled:opacity-50 transition-colors flex items-center justify-center gap-2"
                        >
                            {loading && <Loader2 className="w-4 h-4 animate-spin" />}
                            Enable 2FA
                        </button>
                    </form>
                </div>
            )}
        </div>
    );
}

// Minimal placeholder for 2FA Verify Screen (Login flow)
export function TwoFAVerify({ onVerify }: { onVerify: (code: string) => Promise<void> }) {
    const [code, setCode] = useState("");
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        setError(null);
        try {
            await onVerify(code);
        } catch (err: any) {
            setError("Invalid code");
        } finally {
            setLoading(false);
        }
    };

    return (
        <form onSubmit={handleSubmit} className="space-y-4">
            <div>
                <label className="block text-sm font-medium text-gray-700 mb-1.5">Enter Authenticator Code</label>
                <input
                    type="text"
                    value={code}
                    onChange={(e) => setCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
                    className="w-full text-center text-2xl tracking-widest px-4 py-3 rounded-xl border border-gray-200 focus:border-[#1E1B4B] focus:ring-1 focus:ring-[#1E1B4B] outline-none font-mono"
                    placeholder="000 000"
                    autoFocus
                />
            </div>
            {error && <p className="text-red-500 text-sm text-center">{error}</p>}
            <button
                type="submit"
                disabled={code.length !== 6 || loading}
                className="w-full py-3 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] transition-colors"
            >
                {loading ? <Loader2 className="w-4 h-4 animate-spin mx-auto" /> : "Verify"}
            </button>
        </form>
    );
}
