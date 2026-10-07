import { useState, useRef, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowLeft, ArrowRight, Check, Camera, Clock, Upload, User, CheckCircle, XCircle, Loader2 } from "lucide-react";
import { api } from "@/api/client";

export default function KYCFaceVerification() {
    const navigate = useNavigate();
    const fileInputRef = useRef<HTMLInputElement>(null);
    const cameraInputRef = useRef<HTMLInputElement>(null);

    const [selfieImage, setSelfieImage] = useState<File | null>(null);
    const [previewUrl, setPreviewUrl] = useState<string | null>(null);

    // API State

    const [uploadRef, setUploadRef] = useState<string | null>(null);
    const [uploadUrl, setUploadUrl] = useState<string | null>(null);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [isSubmitted, setIsSubmitted] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Start verification session on mount
    useEffect(() => {
        let cancelled = false;
        const startSession = async () => {
            try {
                // The server generates the selfie reference and a signed upload URL for it.
                const { selfie_upload_ref, upload_url } = await api.kycStartFaceVerification();
                if (cancelled) return;
                setUploadRef(selfie_upload_ref);
                setUploadUrl(upload_url);
            } catch (e: any) {
                console.error("Failed to start face verification session", e);
                if (!cancelled) setError(e?.message || "Failed to initialize verification session");
            }
        };
        startSession();
        return () => {
            cancelled = true;
        };
    }, []);

    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        // Allow re-selecting the same file after an error
        e.target.value = "";
        if (file) {
            if (file.type !== "image/jpeg" && file.type !== "image/png") {
                setError("Please use a JPG or PNG photo");
                return;
            }
            if (file.size > 10 * 1024 * 1024) {
                setError("File size must be less than 10MB");
                return;
            }
            setSelfieImage(file);
            setPreviewUrl(URL.createObjectURL(file));
            setError(null);
        }
    };

    const handleCapture = () => {
        // Opens the device's front camera where supported (falls back to a file picker)
        cameraInputRef.current?.click();
    };

    const handleSubmit = async () => {
        if (!selfieImage) return;
        if (!uploadRef || !uploadUrl) {
            setError("Verification session is not ready. Please reload the page and try again.");
            return;
        }

        setIsSubmitting(true);
        setError(null);

        try {
            // 1. Upload the selfie to the signed, user-bound URL issued by the server
            await api.kycUploadFile(uploadUrl, selfieImage);

            // 2. Tell the server the selfie is in place. The server verifies the upload
            //    itself and records the check as "pending manual review" - the client
            //    never reports a pass/fail result.
            await api.kycCompleteFaceVerification(uploadRef);

            setIsSubmitted(true);
        } catch (e: any) {
            setError(e.message || "Failed to submit your selfie. Please try again.");
        } finally {
            setIsSubmitting(false);
        }
    };

    if (isSubmitted) {
        return (
            <div className="min-h-screen bg-gray-50 py-12 px-4">
                <div className="max-w-lg mx-auto">
                    <div className="bg-white rounded-2xl shadow-lg p-8 text-center">
                        <div className="w-24 h-24 mx-auto mb-6 bg-blue-100 rounded-full flex items-center justify-center">
                            <Clock className="w-12 h-12 text-blue-500" />
                        </div>
                        <h1 className="text-2xl font-bold text-gray-900 mb-3">Selfie Submitted for Review</h1>
                        <p className="text-gray-600 mb-8">
                            Your selfie was uploaded and will be checked by our team together with your ID.
                            Continue to submit your verification for review.
                        </p>
                        <button
                            onClick={() => navigate("/dashboard/kyc/status")}
                            className="w-full py-4 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] transition-colors flex items-center justify-center gap-2"
                        >
                            Continue
                            <ArrowRight className="w-5 h-5" />
                        </button>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-gray-50 py-12 px-4">
            <div className="max-w-lg mx-auto">
                {/* Progress Stepper */}
                <div className="flex items-center justify-between mb-12">
                    {[
                        { num: 1, label: "Intro", completed: true },
                        { num: 2, label: "Personal Info", completed: true },
                        { num: 3, label: "ID Upload", completed: true },
                        { num: 4, label: "Face Verification", active: true },
                        { num: 5, label: "Complete", active: false },
                    ].map((step, idx) => (
                        <div key={step.num} className="flex items-center">
                            <div className="flex flex-col items-center">
                                <div
                                    className={`w-10 h-10 rounded-full flex items-center justify-center text-sm font-bold ${step.completed
                                        ? "bg-green-500 text-white"
                                        : step.active
                                            ? "bg-[#1E1B4B] text-white"
                                            : "bg-gray-200 text-gray-500"
                                        }`}
                                >
                                    {step.completed ? <Check className="w-5 h-5" /> : step.num}
                                </div>
                                <span className="text-xs mt-2 text-gray-600 hidden sm:block">
                                    {step.label}
                                </span>
                            </div>
                            {idx < 4 && (
                                <div className={`w-8 sm:w-16 h-0.5 mx-2 ${step.completed ? "bg-green-500" : "bg-gray-200"}`} />
                            )}
                        </div>
                    ))}
                </div>

                {/* Face Verification Card */}
                <div className="bg-white rounded-2xl shadow-lg p-8">
                    <h1 className="text-2xl font-bold text-gray-900 mb-2 text-center">Face Verification</h1>
                    <p className="text-gray-600 mb-8 text-center">
                        Take a selfie to verify your identity
                    </p>

                    {/* Camera Preview / Upload Area */}
                    <div className="relative mb-6">
                        <div className={`aspect-square max-w-xs mx-auto rounded-2xl overflow-hidden border-2 ${previewUrl ? "border-green-500" : "border-gray-200"
                            }`}>
                            {previewUrl ? (
                                <img src={previewUrl} alt="Selfie preview" className="w-full h-full object-cover" />
                            ) : (
                                <div className="w-full h-full bg-gray-100 flex flex-col items-center justify-center relative">
                                    {/* Face outline guide */}
                                    <div className="absolute inset-0 flex items-center justify-center">
                                        <div className="w-40 h-52 border-2 border-dashed border-gray-300 rounded-[50%]" />
                                    </div>
                                    <User className="w-24 h-24 text-gray-300" />
                                    <p className="text-sm text-gray-400 mt-4">Position your face here</p>
                                </div>
                            )}
                        </div>

                        {previewUrl && (
                            <button
                                onClick={() => {
                                    setSelfieImage(null);
                                    setPreviewUrl(null);
                                }}
                                disabled={isSubmitting}
                                className="absolute top-2 right-2 p-2 bg-white rounded-full shadow-md hover:bg-gray-100"
                            >
                                <XCircle className="w-5 h-5 text-gray-500" />
                            </button>
                        )}
                    </div>

                    {/* Tips */}
                    <div className="flex flex-col sm:flex-row justify-center gap-4 mb-6">
                        <div className="flex items-center gap-2 text-sm">
                            <div className="flex items-center gap-2 bg-green-50 px-3 py-2 rounded-lg">
                                <CheckCircle className="w-4 h-4 text-green-500" />
                                <span className="text-gray-600">Look directly at camera</span>
                            </div>
                        </div>
                        <div className="flex items-center gap-2 text-sm">
                            <div className="flex items-center gap-2 bg-green-50 px-3 py-2 rounded-lg">
                                <CheckCircle className="w-4 h-4 text-green-500" />
                                <span className="text-gray-600">Good lighting</span>
                            </div>
                        </div>
                    </div>

                    <div className="flex justify-center gap-4 mb-4">
                        <div className="flex items-center gap-2 text-sm bg-red-50 px-3 py-2 rounded-lg">
                            <XCircle className="w-4 h-4 text-red-500" />
                            <span className="text-gray-600">Remove glasses/hats</span>
                        </div>
                    </div>

                    {error && (
                        <div className="mb-6 p-3 bg-red-50 text-red-600 rounded-lg text-sm">
                            {error}
                        </div>
                    )}

                    {/* Capture Button */}
                    {!previewUrl && (
                        <>
                            <input
                                ref={cameraInputRef}
                                type="file"
                                accept="image/jpeg,image/png"
                                capture="user"
                                onChange={handleFileChange}
                                className="hidden"
                            />
                            <button
                                onClick={handleCapture}
                                disabled={isSubmitting}
                                className="w-full py-4 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] transition-colors flex items-center justify-center gap-2 mb-4"
                            >
                                <Camera className="w-5 h-5" />
                                Take Photo
                            </button>

                            <input
                                ref={fileInputRef}
                                type="file"
                                accept="image/jpeg,image/png"
                                onChange={handleFileChange}
                                className="hidden"
                            />
                            <button
                                onClick={() => fileInputRef.current?.click()}
                                disabled={isSubmitting}
                                className="w-full py-3 text-[#1E1B4B] font-medium hover:underline flex items-center justify-center gap-2"
                            >
                                <Upload className="w-4 h-4" />
                                Upload Photo Instead
                            </button>
                        </>
                    )}

                    {/* Submit Button */}
                    {previewUrl && (
                        <button
                            onClick={handleSubmit}
                            disabled={isSubmitting}
                            className={`w-full py-4 bg-[#1E1B4B] text-white font-semibold rounded-xl hover:bg-[#2D2A5B] transition-colors flex items-center justify-center gap-2 ${isSubmitting ? 'opacity-75 cursor-not-allowed' : ''}`}
                        >
                            {isSubmitting ? (
                                <>
                                    <Loader2 className="w-5 h-5 animate-spin" />
                                    Uploading...
                                </>
                            ) : (
                                <>
                                    <CheckCircle className="w-5 h-5" />
                                    Submit Selfie for Review
                                </>
                            )}
                        </button>
                    )}

                    {/* Back Link */}
                    <div className="mt-6 text-center">
                        <button
                            onClick={() => navigate("/dashboard/kyc/id-upload")}
                            disabled={isSubmitting}
                            className="flex items-center gap-2 text-gray-600 hover:text-[#1E1B4B] transition-colors mx-auto"
                        >
                            <ArrowLeft className="w-4 h-4" />
                            Back
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
