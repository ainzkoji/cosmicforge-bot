
import { AlertTriangle, X } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

interface ConfirmationDialogProps {
    isOpen: boolean;
    onClose: () => void;
    onConfirm: () => void;
    title: string;
    message: string;
    confirmLabel?: string;
    cancelLabel?: string;
    variant?: 'danger' | 'warning' | 'info';
    isLoading?: boolean;
}

export const ConfirmationDialog = ({
    isOpen,
    onClose,
    onConfirm,
    title,
    message,
    confirmLabel = "Confirm",
    cancelLabel = "Cancel",
    variant = "danger",
    isLoading = false
}: ConfirmationDialogProps) => {
    if (!isOpen) return null;

    const variantStyles = {
        danger: {
            iconBg: "bg-red-500/10",
            iconColor: "text-red-500",
            buttonBg: "bg-red-500 hover:bg-red-600",
            buttonText: "text-white"
        },
        warning: {
            iconBg: "bg-amber-500/10",
            iconColor: "text-amber-500",
            buttonBg: "bg-amber-500 hover:bg-amber-600",
            buttonText: "text-black"
        },
        info: {
            iconBg: "bg-blue-500/10",
            iconColor: "text-blue-500",
            buttonBg: "bg-blue-500 hover:bg-blue-600",
            buttonText: "text-white"
        }
    };

    const styles = variantStyles[variant];

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm">
                <motion.div
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.9 }}
                    className="bg-card w-full max-w-md rounded-xl border border-border shadow-2xl overflow-hidden relative"
                >
                    <button
                        onClick={onClose}
                        className="absolute right-4 top-4 text-muted-foreground hover:text-foreground transition-colors"
                    >
                        <X className="w-5 h-5" />
                    </button>

                    <div className="p-6">
                        <div className="flex items-start gap-4">
                            <div className={`p-3 rounded-full shrink-0 ${styles.iconBg} ${styles.iconColor}`}>
                                <AlertTriangle className="w-6 h-6" />
                            </div>
                            <div>
                                <h3 className="text-xl font-bold mb-2">{title}</h3>
                                <p className="text-muted-foreground text-sm leading-relaxed">
                                    {message}
                                </p>
                            </div>
                        </div>

                        <div className="flex gap-3 justify-end mt-8">
                            <button
                                onClick={onClose}
                                disabled={isLoading}
                                className="px-4 py-2 text-sm font-medium rounded-lg text-muted-foreground hover:bg-muted transition-colors disabled:opacity-50"
                            >
                                {cancelLabel}
                            </button>
                            <button
                                onClick={onConfirm}
                                disabled={isLoading}
                                className={`px-4 py-2 text-sm font-bold rounded-lg transition-colors flex items-center gap-2 disabled:opacity-50 ${styles.buttonBg} ${styles.buttonText}`}
                            >
                                {isLoading && <div className="w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin" />}
                                {confirmLabel}
                            </button>
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};
