import { Shield, BookOpen, Zap, Check } from 'lucide-react';
import { motion } from 'framer-motion';

interface ExperienceStepProps {
    onNext: (data: { experience_level: string }) => void;
    isLoading: boolean;
    defaultValue?: string;
}

export function ExperienceStep({ onNext, isLoading, defaultValue }: ExperienceStepProps) {
    return (
        <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-500">
            <div className="text-center space-y-4">
                <h2 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-400">
                    What's your trading experience?
                </h2>
                <p className="text-gray-400 max-w-lg mx-auto">
                    We'll customize your bot's behavior and available settings based on your expertise.
                </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <ExperienceCard
                    title="Beginner"
                    description="I'm new to trading bots. Keep it simple and safe."
                    icon={Shield}
                    value="beginner"
                    color="text-green-400"
                    borderColor="hover:border-green-500/50"
                    bgGradient="from-green-500/10 to-transparent"
                    onSelect={(val: string) => onNext({ experience_level: val })}
                    isLoading={isLoading}
                    isSelected={defaultValue === 'beginner'}
                />
                <ExperienceCard
                    title="Intermediate"
                    description="I understand the basics but want guided flexibility."
                    icon={BookOpen}
                    value="intermediate"
                    color="text-blue-400"
                    borderColor="hover:border-blue-500/50"
                    bgGradient="from-blue-500/10 to-transparent"
                    onSelect={(val: string) => onNext({ experience_level: val })}
                    isLoading={isLoading}
                    isSelected={defaultValue === 'intermediate'}
                />
                <ExperienceCard
                    title="Advanced"
                    description="I want full control over every parameter and risk setting."
                    icon={Zap}
                    value="advanced"
                    color="text-purple-400"
                    borderColor="hover:border-purple-500/50"
                    bgGradient="from-purple-500/10 to-transparent"
                    onSelect={(val: string) => onNext({ experience_level: val })}
                    isLoading={isLoading}
                    isSelected={defaultValue === 'advanced'}
                />
            </div>
        </div>
    );
}

function ExperienceCard({ title, description, icon: Icon, value, color, borderColor, bgGradient, onSelect, isLoading, isSelected }: any) {
    return (
        <button
            onClick={() => onSelect(value)}
            disabled={isLoading}
            className={`
        relative group p-6 rounded-2xl border text-left transition-all duration-300 h-full
        ${isSelected
                    ? `border-${color.split('-')[1]}-500 bg-${color.split('-')[1]}-500/10 shadow-[0_0_30px_rgba(0,0,0,0.3)]`
                    : `border-white/10 bg-white/5 hover:bg-white/10 ${borderColor}`
                }
      `}
        >
            <div className={`absolute inset-0 bg-gradient-to-br ${bgGradient} opacity-0 group-hover:opacity-100 transition-opacity rounded-2xl`} />

            <div className="relative z-10 space-y-4">
                <div className={`p-3 rounded-xl bg-white/5 w-fit ${color} group-hover:scale-110 transition-transform duration-300`}>
                    <Icon className="w-8 h-8" />
                </div>

                <div>
                    <h3 className="text-xl font-semibold text-white mb-2 flex items-center justify-between">
                        {title}
                        {isSelected && <Check className={`w-5 h-5 ${color}`} />}
                    </h3>
                    <p className="text-sm text-gray-400 leading-relaxed">
                        {description}
                    </p>
                </div>
            </div>
        </button>
    );
}
