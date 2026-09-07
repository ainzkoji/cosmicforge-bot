import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowLeft, ArrowRight, Check, Loader2 } from "lucide-react";
import { api } from "@/api/client";

interface FormData {
    firstName: string;
    lastName: string;
    dateOfBirth: string;
    nationality: string;
    countryOfResidence: string;
    address: string;
    city: string;
    postalCode: string;
}

export default function KYCPersonalInfo() {
    const navigate = useNavigate();
    const [formData, setFormData] = useState<FormData>({
        firstName: "",
        lastName: "",
        dateOfBirth: "",
        nationality: "",
        countryOfResidence: "",
        address: "",
        city: "",
        postalCode: "",
    });
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
        setFormData({ ...formData, [e.target.name]: e.target.value });
    };

    const isFieldValid = (field: keyof FormData) => formData[field].length > 0;

    const isFormValid = Object.values(formData).every((v) => v.length > 0);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!isFormValid) return;

        setIsSubmitting(true);
        setError(null);

        try {
            await api.kycSubmitPersonalInfo({
                full_legal_name: `${formData.firstName} ${formData.lastName}`,
                date_of_birth: formData.dateOfBirth,
                nationality: formData.nationality,
                country_of_residence: formData.countryOfResidence,
                address_line1: formData.address,
                address_city: formData.city,
                address_postal_code: formData.postalCode,
            });
            navigate("/kyc/id-upload");
        } catch (e: any) {
            console.error("Submission error:", e);
            if (e.response && e.response.data) {
                // Handle Pydantic validation errors
                if (e.response.data.detail) {
                    if (Array.isArray(e.response.data.detail)) {
                        setError(e.response.data.detail.map((err: any) => `${err.loc.join('.')}: ${err.msg}`).join(', '));
                    } else {
                        setError(e.response.data.detail);
                    }
                } else {
                    setError(JSON.stringify(e.response.data));
                }
            } else {
                setError(e.message || "Failed to submit personal information");
            }
        } finally {
            setIsSubmitting(false);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 py-12 px-4">
            <div className="max-w-2xl mx-auto">
                {/* Progress Stepper */}
                <div className="flex items-center justify-between mb-12">
                    {[
                        { num: 1, label: "Intro", completed: true },
                        { num: 2, label: "Personal Info", active: true },
                        { num: 3, label: "ID Upload", active: false },
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

                {/* Form Card */}
                <div className="bg-white rounded-2xl shadow-lg p-8">
                    <h1 className="text-2xl font-bold text-gray-900 mb-2">Personal Information</h1>
                    <p className="text-gray-600 mb-8">
                        Please provide your legal name as it appears on your ID
                    </p>

                    <form onSubmit={handleSubmit} className="space-y-6">
                        {/* Name Row */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    First Name
                                </label>
                                <div className="relative">
                                    <input
                                        type="text"
                                        name="firstName"
                                        value={formData.firstName}
                                        onChange={handleChange}
                                        className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none"
                                        placeholder="Enter your first name"
                                    />
                                    {isFieldValid("firstName") && (
                                        <Check className="absolute right-3 top-1/2 -translate-y-1/2 w-5 h-5 text-green-500" />
                                    )}
                                </div>
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Last Name
                                </label>
                                <div className="relative">
                                    <input
                                        type="text"
                                        name="lastName"
                                        value={formData.lastName}
                                        onChange={handleChange}
                                        className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none"
                                        placeholder="Enter your last name"
                                    />
                                    {isFieldValid("lastName") && (
                                        <Check className="absolute right-3 top-1/2 -translate-y-1/2 w-5 h-5 text-green-500" />
                                    )}
                                </div>
                            </div>
                        </div>

                        {/* DOB and Nationality */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Date of Birth
                                </label>
                                <input
                                    type="date"
                                    name="dateOfBirth"
                                    value={formData.dateOfBirth}
                                    onChange={handleChange}
                                    className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none"
                                />
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Nationality
                                </label>
                                <select
                                    name="nationality"
                                    value={formData.nationality}
                                    onChange={handleChange}
                                    className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none bg-white"
                                >
                                    <option value="">Select nationality</option>
                                    <option value="US">United States</option>
                                    <option value="GB">United Kingdom</option>
                                    <option value="CA">Canada</option>
                                    <option value="AU">Australia</option>
                                    <option value="DE">Germany</option>
                                    <option value="FR">France</option>
                                    <option value="NG">Nigeria</option>
                                    <option value="OT">Other</option>
                                </select>
                            </div>
                        </div>

                        {/* Country and Address */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Country of Residence
                                </label>
                                <select
                                    name="countryOfResidence"
                                    value={formData.countryOfResidence}
                                    onChange={handleChange}
                                    className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none bg-white"
                                >
                                    <option value="">Select country</option>
                                    <option value="US">United States</option>
                                    <option value="GB">United Kingdom</option>
                                    <option value="CA">Canada</option>
                                    <option value="AU">Australia</option>
                                    <option value="DE">Germany</option>
                                    <option value="FR">France</option>
                                    <option value="NG">Nigeria</option>
                                    <option value="OT">Other</option>
                                </select>
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Residential Address
                                </label>
                                <div className="relative">
                                    <input
                                        type="text"
                                        name="address"
                                        value={formData.address}
                                        onChange={handleChange}
                                        className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none"
                                        placeholder="Street address"
                                    />
                                    {isFieldValid("address") && (
                                        <Check className="absolute right-3 top-1/2 -translate-y-1/2 w-5 h-5 text-green-500" />
                                    )}
                                </div>
                            </div>
                        </div>

                        {/* City and Postal Code */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    City
                                </label>
                                <div className="relative">
                                    <input
                                        type="text"
                                        name="city"
                                        value={formData.city}
                                        onChange={handleChange}
                                        className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none"
                                        placeholder="City"
                                    />
                                    {isFieldValid("city") && (
                                        <Check className="absolute right-3 top-1/2 -translate-y-1/2 w-5 h-5 text-green-500" />
                                    )}
                                </div>
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    Postal Code
                                </label>
                                <div className="relative">
                                    <input
                                        type="text"
                                        name="postalCode"
                                        value={formData.postalCode}
                                        onChange={handleChange}
                                        className="w-full px-4 py-3 border border-gray-200 rounded-xl focus:ring-2 focus:ring-[#1E1B4B] focus:border-transparent outline-none"
                                        placeholder="Postal code"
                                    />
                                    {isFieldValid("postalCode") && (
                                        <Check className="absolute right-3 top-1/2 -translate-y-1/2 w-5 h-5 text-green-500" />
                                    )}
                                </div>
                            </div>
                        </div>

                        {error && (
                            <div className="p-3 bg-red-50 text-red-600 rounded-lg text-sm">
                                {error}
                            </div>
                        )}

                        {/* Navigation */}
                        <div className="flex items-center justify-between pt-6">
                            <button
                                type="button"
                                onClick={() => navigate("/kyc")}
                                className="flex items-center gap-2 text-gray-600 hover:text-[#1E1B4B] transition-colors"
                            >
                                <ArrowLeft className="w-4 h-4" />
                                Back
                            </button>
                            <button
                                type="submit"
                                disabled={!isFormValid || isSubmitting}
                                className={`px-8 py-3 rounded-xl font-semibold flex items-center gap-2 transition-colors ${isFormValid && !isSubmitting
                                    ? "bg-[#1E1B4B] text-white hover:bg-[#2D2A5B]"
                                    : "bg-gray-200 text-gray-400 cursor-not-allowed"
                                    }`}
                            >
                                {isSubmitting ? (
                                    <>
                                        <Loader2 className="w-5 h-5 animate-spin" />
                                        Saving...
                                    </>
                                ) : (
                                    <>
                                        Continue
                                        <ArrowRight className="w-5 h-5" />
                                    </>
                                )}
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    );
}
