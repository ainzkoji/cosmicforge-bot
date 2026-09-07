import { Link } from "react-router-dom";
import { ArrowRight } from "lucide-react";

export default function Welcome() {
    return (
        <div className="min-h-screen flex flex-col bg-white">
            {/* Main Content */}
            <div className="flex-1 flex flex-col items-center justify-center px-8 py-12">
                {/* Illustration */}
                <div className="w-40 h-40 mb-8 relative flex items-center justify-center">
                    <img src="/src/assets/logo.png" alt="CosmicForge Stratos" className="w-full h-full object-contain drop-shadow-xl" />
                </div>

                {/* Title & Description */}
                <h1 className="text-4xl font-bold text-[#1E1B4B] text-center mb-4">
                    CosmicForge Stratos
                </h1>
                <p className="text-gray-500 text-center max-w-sm mb-12">
                    Your intelligent trading companion. Monitor markets, execute strategies, and grow your portfolio with confidence.
                </p>

                {/* Action Buttons */}
                <div className="w-full max-w-sm space-y-4">
                    <Link
                        to="/register"
                        className="w-full py-3.5 rounded-xl bg-[#2D3A8C] text-white font-semibold hover:bg-[#252f73] flex items-center justify-center gap-2 transition-all shadow-lg shadow-[#2D3A8C]/30"
                    >
                        Get Started <ArrowRight className="w-5 h-5" />
                    </Link>
                    <Link
                        to="/login"
                        className="w-full py-3.5 rounded-xl border-2 border-[#2D3A8C] text-[#2D3A8C] font-semibold hover:bg-[#2D3A8C]/5 flex items-center justify-center gap-2 transition-all"
                    >
                        Sign In
                    </Link>
                </div>
            </div>

            {/* Footer */}
            <div className="py-6 text-center text-gray-400 text-sm">
                <p>© 2026 CosmicForge. All rights reserved.</p>
            </div>
        </div>
    );
}
