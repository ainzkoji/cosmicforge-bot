import { Link } from "react-router-dom";
import { CheckCircle, ArrowRight } from "lucide-react";
import { motion } from "framer-motion";

export default function PaymentSuccess() {
    return (
        <div className="min-h-screen flex items-center justify-center bg-background p-4">
            <motion.div
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                className="max-w-md w-full bg-card border border-border rounded-2xl p-8 text-center shadow-2xl"
            >
                <div className="w-20 h-20 bg-green-500/10 rounded-full flex items-center justify-center mx-auto mb-6">
                    <CheckCircle className="w-10 h-10 text-green-500" />
                </div>

                <h1 className="text-3xl font-bold mb-4">Payment Successful!</h1>
                <p className="text-muted-foreground mb-8">
                    Your subscription has been processed. You now have full access to all premium features.
                </p>

                <div className="space-y-3">
                    <Link
                        to="/dashboard"
                        className="block w-full py-3 bg-primary text-primary-foreground rounded-lg font-bold hover:bg-primary/90 transition-colors"
                    >
                        Go to Dashboard
                    </Link>
                    <Link
                        to="/dashboard/subscription"
                        className="block w-full py-3 border border-border text-foreground rounded-lg font-medium hover:bg-muted transition-colors"
                    >
                        View Receipt
                    </Link>
                </div>
            </motion.div>
        </div>
    );
}
