import { AdminLayout } from "@/components/admin/layout/AdminLayout";
import { Link } from "react-router-dom";
import { DollarSign } from "lucide-react";

/**
 * There is no payment/transaction API behind this page yet. It deliberately
 * shows no rows and no approve/reject actions until a real data source exists.
 */
export default function Transactions() {
    return (
        <AdminLayout>
            <div className="space-y-6">
                <div>
                    <h1 className="text-3xl font-bold" style={{ color: 'var(--admin-text-primary)' }}>
                        Transactions
                    </h1>
                    <p className="text-sm mt-1" style={{ color: 'var(--admin-text-secondary)' }}>
                        Payments, subscriptions and payouts
                    </p>
                </div>

                <div className="admin-card">
                    <div role="status" className="admin-empty-state" style={{ flexDirection: 'column', minHeight: 220 }}>
                        <DollarSign className="w-8 h-8" />
                        <div className="text-lg font-semibold" style={{ color: 'var(--admin-text-primary)' }}>
                            Not connected to payment data yet
                        </div>
                        <div className="text-sm max-w-xl">
                            This page has no transaction data source, so no transactions are listed and nothing can be
                            approved or rejected here. Review payments in the payment provider's dashboard.
                        </div>
                        <Link to="/admin/revenue" className="text-sm text-blue-400 hover:text-blue-300">
                            Open Revenue Analytics
                        </Link>
                    </div>
                </div>
            </div>
        </AdminLayout>
    );
}
