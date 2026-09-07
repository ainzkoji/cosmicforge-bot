import { useState, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowLeft, ArrowRight, Check, Upload, FileText, CreditCard, IdCard, Eye, Sun, Camera, Loader2 } from "lucide-react";
import { api } from "@/api/client";

type DocumentType = "passport" | "drivers_license" | "national_id";

export default function KYCIDUpload() {
    const navigate = useNavigate();
    const frontInputRef = useRef<HTMLInputElement>(null);
    const backInputRef = useRef<HTMLInputElement>(null);

    const [documentType, setDocumentType] = useState<DocumentType>("drivers_license");
    const [frontImage, setFrontImage] = useState<File | null>(null);
    const [backImage, setBackImage] = useState<File | null>(null);

    // Upload state
    const [isUploading, setIsUploading] = useState(false);
    const [uploadProgress, setUploadProgress] = useState(0); // Simple progress indicator (0-100)
    const [error, setError] = useState<string | null>(null);

    const handleFileChange = (side: "front" | "back", e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (file) {
            // Basic validation
            if (file.size > 10 * 1024 * 1024) {
                setError("File size must be less than 10MB");
                return;
            }
            if (side === "front") setFrontImage(file);
            else setBackImage(file);
            setError(null);
        }
    };

    const canContinue = frontImage && (documentType === "passport" || backImage);

    const uploadFile = async (file: File, side: "front" | "back") => {
        // 1. Get upload URL
        const { doc_id, upload_url, file_ref } = await api.kycRequestUploadUrl(documentType, side);

        // 2. Upload actual file
        await api.kycUploadFile(upload_url, file);

        // 3. Confirm upload
        await api.kycConfirmUpload(doc_id, file_ref, side, file.size, file.type);
    };

    const handleSubmit = async () => {
        if (!canContinue) return;

        setIsUploading(true);
        setError(null);
        setUploadProgress(10);

        try {
            // Upload front
            if (frontImage) {
                await uploadFile(frontImage, "front");
            }
            setUploadProgress(50);

            // Upload back if needed
            if (documentType !== "passport" && backImage) {
                await uploadFile(backImage, "back");
            }
            setUploadProgress(100);

            navigate("/kyc/face-verification");
        } catch (e: any) {
            setError(e.message || "Failed to upload documents. Please try again.");
            setIsUploading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 py-12 px-4">
            <div className="max-w-2xl mx-auto">
                {/* Progress Stepper */}
                <div className="flex items-center justify-between mb-12">
                    {[
                        { num: 1, label: "Intro", completed: true },
                        { num: 2, label: "Personal Info", completed: true },
                        { num: 3, label: "ID Upload", active: true },
                        { num: 4, label: "Face Verification", active: false },
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

                {/* Upload Card */}
                <div className="bg-white rounded-2xl shadow-lg p-8">
                    <h1 className="text-2xl font-bold text-gray-900 mb-2">Upload Your ID</h1>
                    <p className="text-gray-600 mb-8">
                        Choose a valid government-issued ID
                    </p>

                    {/* Document Type Selection */}
                    <div className="grid grid-cols-3 gap-4 mb-8">
                        {[
                            { type: "passport" as DocumentType, icon: FileText, label: "Passport" },
                            { type: "drivers_license" as DocumentType, icon: CreditCard, label: "Driver's License" },
                            { type: "national_id" as DocumentType, icon: IdCard, label: "National ID" },
                        ].map(({ type, icon: Icon, label }) => (
                            <button
                                key={type}
                                onClick={() => setDocumentType(type)}
                                disabled={isUploading}
                                className={`p-4 rounded-xl border-2 transition-all ${documentType === type
                                    ? "border-[#1E1B4B] bg-[#1E1B4B]/5"
                                    : "border-gray-200 hover:border-gray-300"
                                    } ${isUploading ? "opacity-50 cursor-not-allowed" : ""}`}
                            >
                                <Icon className={`w-8 h-8 mx-auto mb-2 ${documentType === type ? "text-[#1E1B4B]" : "text-gray-400"}`} />
                                <p className={`text-sm font-medium ${documentType === type ? "text-[#1E1B4B]" : "text-gray-600"}`}>
                                    {label}
                                </p>
                            </button>
                        ))}
                    </div>

                    {/* Upload Zones */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                        {/* Front Upload */}
                        <div>
                            <p className="text-sm font-medium text-gray-700 mb-3">Front of ID</p>
                            <input
                                ref={frontInputRef}
                                type="file"
                                accept="image/*"
                                onChange={(e) => handleFileChange("front", e)}
                                className="hidden"
                                disabled={isUploading}
                            />
                            <button
                                onClick={() => frontInputRef.current?.click()}
                                disabled={isUploading}
                                className={`w-full h-40 border-2 border-dashed rounded-xl flex flex-col items-center justify-center transition-colors ${frontImage
                                    ? "border-green-500 bg-green-50"
                                    : "border-gray-300 hover:border-[#1E1B4B]"
                                    } ${isUploading ? "opacity-50 cursor-not-allowed" : ""}`}
                            >
                                {frontImage ? (
                                    <>
                                        <Check className="w-8 h-8 text-green-500 mb-2" />
                                        <p className="text-sm text-green-600 font-medium">{frontImage.name}</p>
                                        <p className="text-xs text-gray-500 mt-1">Click to change</p>
                                    </>
                                ) : (
                                    <>
                                        <Upload className="w-8 h-8 text-[#1E1B4B] mb-2" />
                                        <p className="text-sm text-gray-600">Click to upload or drag and drop</p>
                                        <p className="text-xs text-gray-400 mt-1">JPG, PNG, PDF</p>
                                    </>
                                )}
                            </button>
                        </div>

                        {/* Back Upload (not for passport) */}
                        <div>
                            <p className="text-sm font-medium text-gray-700 mb-3">
                                Back of ID {documentType === "passport" && <span className="text-gray-400">(not required)</span>}
                            </p>
                            <input
                                ref={backInputRef}
                                type="file"
                                accept="image/*"
                                onChange={(e) => handleFileChange("back", e)}
                                className="hidden"
                                disabled={documentType === "passport" || isUploading}
                            />
                            <button
                                onClick={() => backInputRef.current?.click()}
                                disabled={documentType === "passport" || isUploading}
                                className={`w-full h-40 border-2 border-dashed rounded-xl flex flex-col items-center justify-center transition-colors ${documentType === "passport"
                                    ? "border-gray-200 bg-gray-50 cursor-not-allowed"
                                    : backImage
                                        ? "border-green-500 bg-green-50"
                                        : "border-gray-300 hover:border-[#1E1B4B]"
                                    } ${isUploading && documentType !== "passport" ? "opacity-50 cursor-not-allowed" : ""}`}
                            >
                                {documentType === "passport" ? (
                                    <p className="text-sm text-gray-400">Not required for passport</p>
                                ) : backImage ? (
                                    <>
                                        <Check className="w-8 h-8 text-green-500 mb-2" />
                                        <p className="text-sm text-green-600 font-medium">{backImage.name}</p>
                                        <p className="text-xs text-gray-500 mt-1">Click to change</p>
                                    </>
                                ) : (
                                    <>
                                        <Upload className="w-8 h-8 text-[#1E1B4B] mb-2" />
                                        <p className="text-sm text-gray-600">Click to upload or drag and drop</p>
                                        <p className="text-xs text-gray-400 mt-1">JPG, PNG, PDF</p>
                                    </>
                                )}
                            </button>
                        </div>
                    </div>

                    {/* Tips */}
                    <div className="bg-gray-50 rounded-xl p-4 mb-8">
                        <p className="text-sm font-medium text-gray-700 mb-3">Tips for successful upload</p>
                        <div className="grid grid-cols-3 gap-4">
                            <div className="flex items-center gap-2">
                                <Eye className="w-4 h-4 text-[#1E1B4B]" />
                                <span className="text-xs text-gray-600">All corners visible</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <Sun className="w-4 h-4 text-[#1E1B4B]" />
                                <span className="text-xs text-gray-600">Good lighting</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <Camera className="w-4 h-4 text-[#1E1B4B]" />
                                <span className="text-xs text-gray-600">No glare or blur</span>
                            </div>
                        </div>
                    </div>

                    {error && (
                        <div className="mb-6 p-3 bg-red-50 text-red-600 rounded-lg text-sm">
                            {error}
                        </div>
                    )}

                    {/* Navigation */}
                    <div className="flex items-center justify-between">
                        <button
                            onClick={() => navigate("/kyc/personal-info")}
                            disabled={isUploading}
                            className={`flex items-center gap-2 text-gray-600 hover:text-[#1E1B4B] transition-colors ${isUploading ? 'opacity-50' : ''}`}
                        >
                            <ArrowLeft className="w-4 h-4" />
                            Back
                        </button>
                        <button
                            onClick={handleSubmit}
                            disabled={!canContinue || isUploading}
                            className={`px-8 py-3 rounded-xl font-semibold flex items-center gap-2 transition-colors ${canContinue && !isUploading
                                ? "bg-[#1E1B4B] text-white hover:bg-[#2D2A5B]"
                                : "bg-gray-200 text-gray-400 cursor-not-allowed"
                                }`}
                        >
                            {isUploading ? (
                                <>
                                    <Loader2 className="w-5 h-5 animate-spin" />
                                    Uploading {uploadProgress}%
                                </>
                            ) : (
                                <>
                                    Continue
                                    <ArrowRight className="w-5 h-5" />
                                </>
                            )}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
