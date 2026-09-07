import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Shield, Clock, CreditCard, Lock, ArrowRight, Loader2 } from "lucide-react";
import { api } from "@/api/client";

export default function KYCIntro() {
    const navigate = useNavigate();
    const [isStarting, setIsStarting] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleStart = async () => {
        setIsStarting(true);
        setError(null);
        try {
            await api.kycStart();
            navigate("/kyc/personal-info");
        } catch (e: any) {
            setError(e.message || "Failed to start KYC");
        } finally {
            setIsStarting(false);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 py-12 px-4">
            <div className="max-w-lg mx-auto">
                {/* Progress Stepper */}
                <div className="flex items-center justify-between mb-12">
                    {[
                        { num: 1, label: "Intro", active: true },
                        { num: 2, label: "Personal Info", active: false },
                        { num: 3, label: "ID Upload", active: false },
                        { num: 4, label: "Face Verification", active: false },
                        { num: 5, label: "Complete", active: false },
                    ].map((step, idx) => (
                        <div key={step.num} className="flex items-center">
                            <div className="flex flex-col items-center">
                                <div
                                    className={`w-10 h-10 rounded-full flex items-center justify-center text-sm font-bold ${step.active
                                        ? "bg-[#1E1B4B] text-white"
                                        : "bg-gray-200 text-gray-500"
                                        }`}
                                >
                                    {step.num}
                                </div>
                                <span className="text-xs mt-2 text-gray-600 hidden sm:block">
                                    {step.label}
                                </span>
                            </div>
                            {idx < 4 && (
                                <div className="w-8 sm:w-16 h-0.5 bg-gray-200 mx-2" />
                            )}
                        </div>
                    ))}
                </div>

                {/* Main Content Card */}
                <div className="bg-white rounded-2xl shadow-lg p-8 text-center">
                    {/* Shield Icon */}
                    <div className="w-20 h-20 mx-auto mb-6 bg-gradient-to-br from-[#1E1B4B] to-[#4752B3] rounded-2xl flex items-center justify-center">
                        <Shield className="w-10 h-10 text-white" />
                    </div>

                    <h1 className="text-3xl font-bold text-gray-900 mb-4">
                        Verify Your Identity
                    </h1>
                    <p className="text-gray-600 mb-8">
                        To ensure the security of your account and comply with global
                        financial regulations, we require a quick identity verification.
                        Your information is kept confidential.
                    </p>

                    {/* Benefits List */}
                    <div className="space-y-4 mb-8 text-left">
                        <div className="flex items-center gap-4 p-4 bg-gray-50 rounded-xl">
                            <div className="w-10 h-10 bg-[#1E1B4B]/10 rounded-lg flex items-center justify-center">
                                <Clock className="w-5 h-5 text-[#1E1B4B]" />
                            </div>
                            <div>
                                <p className="font-medium text-gray-900">Takes 5-10 minutes</p>
                                <p className="text-sm text-gray-500">Quick and straightforward process</p>
                            </div>
                        </div>

                        <div className="flex items-center gap-4 p-4 bg-gray-50 rounded-xl">
                            <div className="w-10 h-10 bg-[#1E1B4B]/10 rounded-lg flex items-center justify-center">
                                <CreditCard className="w-5 h-5 text-[#1E1B4B]" />
                            </div>
                            <div>
                                <p className="font-medium text-gray-900">Government ID required</p>
                                <p className="text-sm text-gray-500">Passport, driver's license, or national ID</p>
                            </div>
                        </div>

                        <div className="flex items-center gap-4 p-4 bg-gray-50 rounded-xl">
                            <div className="w-10 h-10 bg-[#1E1B4B]/10 rounded-lg flex items-center justify-center">
                                <Lock className="w-5 h-5 text-[#1E1B4B]" />
                            </div>
                            <div>
                                <p className="font-medium text-gray-900">Secure & encrypted</p>
                                <p className="text-sm text-gray-500">Your data is protected with bank-level encryption</p>
                            </div>
                        </div>
                    </div>

                    {error && (
                        <div className="mb-4 p-3 bg-red-50 text-red-600 rounded-lg text-sm">
                            {error}
                        </div>
                    )}

                    {/* CTA Button */}
                    <button
                        onClick={handleStart}
                        disabled={isStarting}
                        className="w-full py-4 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] transition-colors flex items-center justify-center gap-2 disabled:opacity-50"
                    >
                        {isStarting ? (
                            <>
                                <Loader2 className="w-5 h-5 animate-spin" />
                                Starting...
                            </>
                        ) : (
                            <>
                                Start Verification
                                <ArrowRight className="w-5 h-5" />
                            </>
                        )}
                    </button>

                    <button className="mt-4 text-sm text-gray-500 hover:text-[#1E1B4B] transition-colors">
                        Why is this required?
                    </button>
                </div>
            </div>
        </div>
    );
}
