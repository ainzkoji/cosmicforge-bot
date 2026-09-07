import { useState, useEffect } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Check, Clock, CheckCircle, Mail, HelpCircle, ArrowRight, Loader2, AlertCircle } from "lucide-react";
import { api } from "@/api/client";

export default function KYCStatus() {
    const navigate = useNavigate();
    const [loading, setLoading] = useState(true);
    const [statusData, setStatusData] = useState<any>(null);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Fetch status on mount
    useEffect(() => {
        const fetchStatus = async () => {
            try {
                const data = await api.kycGetChecklist();
                setStatusData(data);

                // If not started, redirect to intro
                if (data.case_status === "not_started") {
                    navigate("/kyc/intro");
                }
            } catch (e: any) {
                setError(e.message || "Failed to load status");
            } finally {
                setLoading(false);
            }
        };
        fetchStatus();
    }, [navigate]);

    const handleSubmitForReview = async () => {
        setIsSubmitting(true);
        setError(null);
        try {
            await api.kycSubmit();
            // Refresh status
            const data = await api.kycGetChecklist();
            setStatusData(data);
        } catch (e: any) {
            setError(e.message || "Failed to submit for review");
        } finally {
            setIsSubmitting(false);
        }
    };

    if (loading) {
        return (
            <div className="min-h-screen bg-gray-50 flex items-center justify-center">
                <Loader2 className="w-8 h-8 text-[#1E1B4B] animate-spin" />
            </div>
        );
    }

    if (!statusData) return null;

    const { case_status, checklist, can_submit } = statusData;

    // Check if we are fully approved
    const isApproved = case_status === "approved";
    const isSubmitted = case_status === "submitted" || case_status === "under_review";
    const isRejected = case_status === "rejected";

    return (
        <div className="min-h-screen bg-gray-50 py-12 px-4">
            <div className="max-w-lg mx-auto">
                {/* Progress Stepper */}
                <div className="flex items-center justify-between mb-12">
                    {[
                        { num: 1, label: "Intro", stepKey: "intro" },
                        { num: 2, label: "Personal Info", stepKey: "personal_info" },
                        { num: 3, label: "ID Upload", stepKey: "id_document" },
                        { num: 4, label: "Face Verification", stepKey: "face_verification" },
                        { num: 5, label: "Complete", stepKey: "complete" },
                    ].map((step, idx) => {
                        // Determine if step is completed based on backend checklist
                        let completed = false;
                        let active = false;

                        if (step.stepKey === "intro") {
                            completed = true;
                        } else if (step.stepKey === "complete") {
                            active = true;
                            completed = isApproved;
                        } else {
                            // Find matching step in checklist
                            const checkStep = checklist.find((s: any) => s.step === step.stepKey);
                            if (checkStep) {
                                completed = checkStep.is_complete;
                                // If this step is not complete but previous ones are, it's active
                                // Only simple approximation here for UI visualization
                            }
                        }

                        return (
                            <div key={step.num} className="flex items-center">
                                <div className="flex flex-col items-center">
                                    <div
                                        className={`w-10 h-10 rounded-full flex items-center justify-center text-sm font-bold ${completed
                                            ? "bg-green-500 text-white"
                                            : active
                                                ? "bg-[#1E1B4B] text-white"
                                                : "bg-gray-200 text-gray-500"
                                            }`}
                                    >
                                        {completed ? <Check className="w-5 h-5" /> : step.num}
                                    </div>
                                    <span className="text-xs mt-2 text-gray-600 hidden sm:block">
                                        {step.label}
                                    </span>
                                </div>
                                {idx < 4 && (
                                    <div className={`w-8 sm:w-16 h-0.5 mx-2 ${completed ? "bg-green-500" : "bg-gray-200"}`} />
                                )}
                            </div>
                        );
                    })}
                </div>

                {/* Status Card */}
                <div className="bg-white rounded-2xl shadow-lg p-8 text-center">

                    {/* Header Icon based on status */}
                    {isApproved ? (
                        <div className="w-24 h-24 mx-auto mb-6 bg-green-100 rounded-full flex items-center justify-center">
                            <CheckCircle className="w-12 h-12 text-green-500" />
                        </div>
                    ) : isRejected ? (
                        <div className="w-24 h-24 mx-auto mb-6 bg-red-100 rounded-full flex items-center justify-center">
                            <AlertCircle className="w-12 h-12 text-red-500" />
                        </div>
                    ) : isSubmitted ? (
                        <div className="w-24 h-24 mx-auto mb-6 bg-blue-100 rounded-full flex items-center justify-center">
                            <Clock className="w-12 h-12 text-blue-500" />
                        </div>
                    ) : (
                        <div className="w-24 h-24 mx-auto mb-6 bg-gray-100 rounded-full flex items-center justify-center">
                            <Loader2 className="w-12 h-12 text-gray-400" />
                        </div>
                    )}

                    <h1 className="text-3xl font-bold text-gray-900 mb-3">
                        {isApproved ? "Verification Complete" :
                            isRejected ? "Verification Failed" :
                                isSubmitted ? "Verification Submitted" : "Verification In Progress"}
                    </h1>

                    <p className="text-gray-600 mb-8">
                        {isApproved ? "Your identity has been successfully verified. You now have full access." :
                            isRejected ? "There were issues with your verification. Please check the details below." :
                                isSubmitted ? "Your documents are being reviewed. This usually takes 1-2 business days." :
                                    "Please complete all remaining steps to finish your verification."}
                    </p>

                    {/* Status Timeline / Checklist */}
                    <div className="bg-gray-50 rounded-xl p-6 mb-6 text-left">
                        <div className="space-y-4">
                            {checklist.map((step: any, idx: number) => (
                                <div key={step.step} className="flex items-center gap-4">
                                    <div className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 ${step.is_complete ? "bg-green-500" : "bg-gray-200"
                                        }`}>
                                        {step.is_complete ? (
                                            <Check className="w-4 h-4 text-white" />
                                        ) : (
                                            <span className="text-xs font-bold text-gray-500">{idx + 1}</span>
                                        )}
                                    </div>
                                    <div className="flex-1">
                                        <p className={`font-medium ${step.is_complete ? "text-gray-900" : "text-gray-500"}`}>
                                            {step.label}
                                        </p>
                                        <p className="text-sm text-gray-500">
                                            {step.is_complete ? "Completed" : "Pending"}
                                        </p>
                                    </div>
                                    {!step.is_complete && step.step !== 'face_verification' && (
                                        <Link
                                            to={step.step === 'personal_info' ? '/kyc/personal-info' : '/kyc/id-upload'}
                                            className="text-sm text-[#1E1B4B] hover:underline"
                                        >
                                            Start
                                        </Link>
                                    )}
                                    {!step.is_complete && step.step === 'face_verification' && (
                                        <Link
                                            to="/kyc/face-verification"
                                            className="text-sm text-[#1E1B4B] hover:underline"
                                        >
                                            Start
                                        </Link>
                                    )}
                                </div>
                            ))}
                        </div>
                    </div>

                    {error && (
                        <div className="mb-6 p-3 bg-red-50 text-red-600 rounded-lg text-sm">
                            {error}
                        </div>
                    )}

                    {/* Actions */}
                    {isApproved ? (
                        <Link
                            to="/dashboard"
                            className="w-full py-4 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] transition-colors flex items-center justify-center gap-2"
                        >
                            Return to Dashboard
                            <ArrowRight className="w-5 h-5" />
                        </Link>
                    ) : can_submit && !isSubmitted ? (
                        <button
                            onClick={handleSubmitForReview}
                            disabled={isSubmitting}
                            className={`w-full py-4 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] transition-colors flex items-center justify-center gap-2 ${isSubmitting ? 'opacity-70' : ''}`}
                        >
                            {isSubmitting ? (
                                <>
                                    <Loader2 className="w-5 h-5 animate-spin" />
                                    Submitting...
                                </>
                            ) : (
                                <>
                                    Submit for Review
                                    <ArrowRight className="w-5 h-5" />
                                </>
                            )}
                        </button>
                    ) : null}

                    {isSubmitted && !isApproved && (
                        <div className="flex items-center justify-center gap-2 text-sm text-gray-600 mt-4 bg-blue-50 p-4 rounded-xl">
                            <Mail className="w-5 h-5 text-blue-500" />
                            <span>You'll receive an email when your verification is complete</span>
                        </div>
                    )}

                    {/* Help Link */}
                    <button className="mt-4 flex items-center gap-2 text-sm text-gray-500 hover:text-[#1E1B4B] transition-colors mx-auto">
                        <HelpCircle className="w-4 h-4" />
                        Need help? Contact Support
                    </button>
                </div>
            </div>
        </div>
    );
}
