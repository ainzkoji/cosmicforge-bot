import { motion, AnimatePresence } from 'framer-motion';

const Modal = ({ isOpen, onClose, children, className = "" }: any) => {
    return (
        <AnimatePresence>
            {isOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
                    <motion.div
                        initial={{ opacity: 0, scale: 0.95 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, scale: 0.95 }}
                        className={`bg-card w-full max-w-4xl max-h-[90vh] overflow-hidden rounded-xl border border-border flex flex-col shadow-2xl ${className}`}
                    >
                        {children}
                    </motion.div>
                </div>
            )}
        </AnimatePresence>
    );
};

export default Modal;
