// Redirects to the renamed page — kept so old bookmarks/imports don't cause 404s
import { Navigate } from "react-router-dom";
export default function CommissionSettings() {
    return <Navigate to="/admin/affiliate-revenue" replace />;
}
