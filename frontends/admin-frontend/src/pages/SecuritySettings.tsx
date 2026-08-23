import { useState } from "react";
import { TwoFASetup } from "@/components/Auth/TwoFA";
import { DeviceManagement } from "@/components/Auth/DeviceManagement";
import { api } from "@/api/client";
import { Shield, Key } from "lucide-react";

export default function SecuritySettings() {
    // We haven't implemented a "get current user profile" endpoint in frontend client yet to know if 2FA is enabled
    // But we can assume for now we might fetch it or toggle state.
    // For demo purposes, I'll keep local state. In real app, this should come from user context.
    const [is2FAEnabled, setIs2FAEnabled] = useState(false); // Default to false for now
    const [show2FASetup, setShow2FASetup] = useState(false);

    // In a real implementation, we'd fetch user status on mount

    const handle2FAComplete = () => {
        setIs2FAEnabled(true);
        setShow2FASetup(false);
    };

    const handleDisable2FA = async () => {
        // Here we would prompt for a code to disable
        // For simplicity UI, just a confirm for now using window.prompt or similar
        const code = window.prompt("Enter your 2FA code to disable:");
        if (code) {
            try {
                await api.disable2FA(code);
                setIs2FAEnabled(false);
            } catch (e: any) {
                alert("Failed to disable 2FA: " + e.message);
            }
        }
    };

    return (
        <div className="max-w-4xl mx-auto space-y-8">
            <h1 className="text-3xl font-bold text-[#1E1B4B]">Security Settings</h1>

            {/* 2FA Section */}
            <div className="bg-white rounded-2xl border border-gray-200 p-6">
                <div className="flex items-start justify-between mb-6">
                    <div className="flex gap-4">
                        <div className="w-12 h-12 bg-[#1E1B4B]/5 rounded-xl flex items-center justify-center">
                            <Shield className="w-6 h-6 text-[#1E1B4B]" />
                        </div>
                        <div>
                            <h2 className="text-xl font-semibold text-[#1E1B4B]">Two-Factor Authentication</h2>
                            <p className="text-gray-500 text-sm mt-1">Add an extra layer of security to your account.</p>
                        </div>
                    </div>
                    {is2FAEnabled ? (
                        <button
                            onClick={handleDisable2FA}
                            className="text-red-600 font-medium hover:bg-red-50 px-4 py-2 rounded-lg transition-colors"
                        >
                            Disable 2FA
                        </button>
                    ) : (
                        !show2FASetup && (
                            <button
                                onClick={() => setShow2FASetup(true)}
                                className="bg-[#1E1B4B] text-white px-4 py-2 rounded-lg font-medium hover:bg-[#2D2A5B] transition-colors"
                            >
                                Setup 2FA
                            </button>
                        )
                    )}
                </div>

                {show2FASetup && (
                    <div className="mt-6 border-t border-gray-100 pt-6">
                        <TwoFASetup onComplete={handle2FAComplete} />
                        <div className="text-center mt-4">
                            <button onClick={() => setShow2FASetup(false)} className="text-gray-400 text-sm hover:text-gray-600">Cancel</button>
                        </div>
                    </div>
                )}

                {is2FAEnabled && (
                    <div className="flex items-center gap-3 p-4 bg-green-50 text-green-700 rounded-xl border border-green-100">
                        <div className="w-2 h-2 rounded-full bg-green-500" />
                        <span className="font-medium text-sm">2FA is currently active on your account.</span>
                    </div>
                )}
            </div>

            {/* Device Management */}
            <DeviceManagement />

            {/* Change Password (Placeholder) */}
            <div className="bg-white rounded-2xl border border-gray-200 p-6 flex items-center justify-between">
                <div className="flex gap-4">
                    <div className="w-12 h-12 bg-gray-100 rounded-xl flex items-center justify-center">
                        <Key className="w-6 h-6 text-gray-600" />
                    </div>
                    <div>
                        <h2 className="text-lg font-semibold text-[#1E1B4B]">Password</h2>
                        <p className="text-gray-500 text-sm">Last changed 3 months ago</p>
                    </div>
                </div>
                <button className="text-[#1E1B4B] font-medium border border-gray-200 px-4 py-2 rounded-lg hover:bg-gray-50 transition-colors">
                    Change Password
                </button>
            </div>
            {/* Login History */}
            <div className="bg-white rounded-2xl border border-gray-200 p-6">
                <h2 className="text-xl font-semibold text-[#1E1B4B] mb-4">Login History</h2>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm text-left">
                        <thead className="bg-gray-50 text-gray-500">
                            <tr>
                                <th className="px-4 py-2 rounded-l-lg">Device</th>
                                <th className="px-4 py-2">Location</th>
                                <th className="px-4 py-2">IP Address</th>
                                <th className="px-4 py-2 rounded-r-lg">Time</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                            <tr>
                                <td className="px-4 py-3 font-medium">Chrome on Windows</td>
                                <td className="px-4 py-3">Lagos, Nigeria</td>
                                <td className="px-4 py-3 font-mono text-xs">197.210.x.x</td>
                                <td className="px-4 py-3 text-gray-500">Just Now</td>
                            </tr>
                            <tr>
                                <td className="px-4 py-3 font-medium">Safari on iPhone</td>
                                <td className="px-4 py-3">Lagos, Nigeria</td>
                                <td className="px-4 py-3 font-mono text-xs">102.89.x.x</td>
                                <td className="px-4 py-3 text-gray-500">2 days ago</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Encryption Notice */}
            <div className="flex items-center gap-3 p-4 bg-gray-50 rounded-xl border border-gray-100 text-xs text-gray-500 justify-center">
                <Shield className="w-4 h-4" />
                <span>Your data is encrypted using AES-256 military-grade encryption. We never store your passwords in plain text.</span>
            </div>
        </div>
    );
}
