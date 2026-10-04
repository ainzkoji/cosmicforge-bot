
import { AdvancedDashboard } from "@/components/Dashboard/AdvancedDashboard";
import { CatiTrading } from "@/components/Dashboard/CatiTrading";

export default function Dashboard() {
    return (
        <div className="container mx-auto px-4 py-6 max-w-7xl">
            <CatiTrading />
            <AdvancedDashboard />
        </div>
    );
}
