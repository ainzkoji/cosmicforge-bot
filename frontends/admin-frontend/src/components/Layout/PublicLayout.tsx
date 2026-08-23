import { Link, Outlet, useLocation } from "react-router-dom";
import { useMarketing } from "@/context/MarketingContext";
import { useEffect } from "react";

export function PublicLayout() {
    const { trackEvent } = useMarketing();
    const location = useLocation();

    // Track page views on route change
    useEffect(() => {
        trackEvent("page_view", location.pathname);
    }, [location.pathname]);

    return (
        <div className="min-h-screen bg-white">
            {/* Navigation */}
            <nav className="fixed top-0 left-0 right-0 bg-white/95 backdrop-blur-sm border-b border-gray-100 z-50">
                <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
                    <Link to="/" className="flex items-center gap-2">
                        <img src="/src/assets/logo.png" alt="CosmicForge" className="h-10 w-10" />
                        <span className="font-bold text-xl text-[#1E1B4B]">CosmicForge Stratos</span>
                    </Link>
                    <div className="hidden md:flex items-center gap-8">
                        <Link to="/features" className="text-gray-600 hover:text-[#1E1B4B] transition-colors">Features</Link>
                        <Link to="/how-it-works" className="text-gray-600 hover:text-[#1E1B4B] transition-colors">How it Works</Link>
                        <Link to="/pricing" className="text-gray-600 hover:text-[#1E1B4B] transition-colors">Pricing</Link>
                    </div>
                    <div className="flex items-center gap-3">
                        <Link to="/login" className="px-4 py-2 text-[#1E1B4B] font-medium hover:bg-gray-50 rounded-lg transition-colors">
                            Login
                        </Link>
                        <Link to="/register" className="px-4 py-2 bg-[#1E1B4B] text-white font-medium rounded-lg hover:bg-[#2D2A5B] transition-colors">
                            Sign Up
                        </Link>
                    </div>
                </div>
            </nav>

            <Outlet />

            {/* Footer */}
            <footer className="bg-[#1E1B4B] text-white py-12 px-6">
                <div className="max-w-7xl mx-auto grid md:grid-cols-4 gap-8">
                    <div>
                        <div className="flex items-center gap-2 mb-4">
                            <img src="/src/assets/logo.png" alt="CosmicForge" className="h-8 w-8 brightness-0 invert" />
                            <span className="font-bold">CosmicForge Stratos</span>
                        </div>
                        <p className="text-gray-400 text-sm">AI-powered crypto trading platform for the modern investor.</p>
                    </div>
                    <div>
                        <h4 className="font-semibold mb-4">Product</h4>
                        <ul className="space-y-2 text-gray-400 text-sm">
                            <li><Link to="/features" className="hover:text-white">Features</Link></li>
                            <li><Link to="/pricing" className="hover:text-white">Pricing</Link></li>
                            <li><Link to="/how-it-works" className="hover:text-white">How it Works</Link></li>
                        </ul>
                    </div>
                    <div>
                        <h4 className="font-semibold mb-4">Company</h4>
                        <ul className="space-y-2 text-gray-400 text-sm">
                            <li><a href="#" className="hover:text-white">About</a></li>
                            <li><a href="#" className="hover:text-white">Blog</a></li>
                            <li><a href="#" className="hover:text-white">Careers</a></li>
                        </ul>
                    </div>
                    <div>
                        <h4 className="font-semibold mb-4">Legal</h4>
                        <ul className="space-y-2 text-gray-400 text-sm">
                            <li><a href="#" className="hover:text-white">Privacy Policy</a></li>
                            <li><a href="#" className="hover:text-white">Terms of Service</a></li>
                        </ul>
                    </div>
                </div>
                <div className="max-w-7xl mx-auto mt-12 pt-8 border-t border-white/10 text-center text-gray-400 text-sm">
                    © 2026 CosmicForge Stratos. All rights reserved.
                </div>
            </footer>
        </div>
    );
}
