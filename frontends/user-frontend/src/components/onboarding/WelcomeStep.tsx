import { motion } from 'framer-motion';
import { ArrowRight, Sparkles } from 'lucide-react';

interface WelcomeStepProps {
    onStart: () => void;
    isLoading: boolean;
}

export function WelcomeStep({ onStart, isLoading }: WelcomeStepProps) {
    return (
        <div className="flex flex-col items-center justify-center min-h-[60vh] text-center px-4">
            {/* Animated Graphic/Icon */}
            <motion.div
                initial={{ scale: 0.8, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                transition={{ duration: 0.8, ease: "easeOut" }}
                className="mb-8 relative"
            >
                <div className="absolute inset-0 bg-blue-500/30 blur-3xl rounded-full" />
                <div className="relative bg-gradient-to-br from-blue-500/20 to-purple-500/20 p-6 rounded-2xl border border-white/10 backdrop-blur-sm">
                    <Sparkles className="w-16 h-16 text-blue-400" />
                </div>
            </motion.div>

            {/* Title & Description */}
            <motion.h1
                initial={{ y: 20, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                transition={{ delay: 0.2, duration: 0.6 }}
                className="text-4xl md:text-5xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 via-purple-400 to-pink-400 mb-6"
            >
                Welcome to CosmicForge
            </motion.h1>

            <motion.p
                initial={{ y: 20, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                transition={{ delay: 0.4, duration: 0.6 }}
                className="text-lg text-gray-400 max-w-xl mb-12 leading-relaxed"
            >
                Let's set up your personal trading bot assistant. We'll guide you through customizing your experience level, risk tolerance, and trading strategy in just a few simple steps.
            </motion.p>

            {/* Action Button */}
            <motion.button
                initial={{ y: 20, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                transition={{ delay: 0.6, duration: 0.6 }}
                onClick={onStart}
                disabled={isLoading}
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                className="group relative px-8 py-4 bg-blue-600 hover:bg-blue-500 text-white rounded-full font-semibold transition-all shadow-[0_0_20px_rgba(37,99,235,0.3)] hover:shadow-[0_0_30px_rgba(37,99,235,0.5)] flex items-center gap-3"
            >
                {isLoading ? (
                    <span className="w-6 h-6 border-2 border-white/50 border-t-white rounded-full animate-spin" />
                ) : (
                    <>
                        Start Setup
                        <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                    </>
                )}
            </motion.button>
        </div>
    );
}
