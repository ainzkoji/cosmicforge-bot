import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './auth/AuthContext';
import { MarketingProvider } from './context/MarketingContext';
import { AdminProtectedRoute } from './components/Auth/AdminProtectedRoute';
import Login from '@/pages/Login';
import VerifyEmail from '@/pages/VerifyEmail';
import ForgotPassword from '@/pages/ForgotPassword';
import ResetPassword from '@/pages/ResetPassword';
import AdminDashboard from '@/pages/admin/Dashboard';
import UserManagement from '@/pages/admin/UserManagement';
import RevenueAnalytics from '@/pages/admin/RevenueAnalytics';
import ProfitabilityReport from '@/pages/admin/ProfitabilityReport';
import AffiliateRevenue from '@/pages/admin/AffiliateRevenue';
import AuditLogs from '@/pages/admin/AuditLogs';
import Compliance from '@/pages/admin/Compliance';
import SystemHealth from '@/pages/admin/SystemHealth';
import CatiStatus from '@/pages/admin/CatiStatus';
import BotMonitor from '@/pages/admin/BotMonitor';
import BotRunDetails from '@/pages/admin/BotRunDetails';
import Transactions from '@/pages/admin/Transactions';
import ActivityFeed from '@/pages/admin/ActivityFeed';
import PlatformSettings from '@/pages/admin/PlatformSettings';
import { MLLayout } from '@/pages/admin/ml/MLLayout';
import Overview from '@/pages/admin/ml/Overview';
import Readiness from '@/pages/admin/ml/Readiness';
import DataQuality from '@/pages/admin/ml/DataQuality';
import Activity from '@/pages/admin/ml/Activity';
import Controls from '@/pages/admin/ml/Controls';
import History from '@/pages/admin/ml/History';
import EventCalendar from '@/pages/admin/EventCalendar';
import EventReactionMonitor from '@/pages/admin/EventReactionMonitor';
import { NewsIntelligence } from '@/pages/admin/NewsIntelligence';
import TradingView from '@/pages/admin/TradingView';
import Signals from '@/pages/admin/Signals';
import SignalPairs from '@/pages/admin/SignalPairs';
import { ErrorBoundary } from '@/components/System/ErrorBoundary';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 30_000,
      refetchOnWindowFocus: false,
    },
  },
});

function App() {
  return (
    <ErrorBoundary>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <MarketingProvider>
          <AuthProvider>
            <Routes>
              {/* The admin console serves no public marketing pages (the customer
                  site lives in frontends/user-frontend). "/" goes to the admin
                  dashboard, which redirects to /login when signed out. */}
              <Route path="/" element={<Navigate to="/admin" replace />} />

              {/* Admin sign-in */}
              <Route path="/login" element={<Login />} />
              <Route path="/verify-email" element={<VerifyEmail />} />
              <Route path="/forgot-password" element={<ForgotPassword />} />
              <Route path="/reset-password" element={<ResetPassword />} />

              {/* Sole owner of /admin/* routes.
                  Keep admin-only UI here and do not add new admin routes to user-frontend. */}
              <Route element={<AdminProtectedRoute />}>
                <Route path="/admin" element={<AdminDashboard />} />
                <Route path="/admin/settings" element={<PlatformSettings />} />
                <Route path="/admin/users" element={<UserManagement />} />
                <Route path="/admin/revenue" element={<RevenueAnalytics />} />
                <Route path="/admin/profitability" element={<ProfitabilityReport />} />
                <Route path="/admin/affiliate-revenue" element={<AffiliateRevenue />} />
                <Route path="/admin/ml" element={<MLLayout />}>
                  <Route index element={<Navigate to="overview" replace />} />
                  <Route path="overview" element={<Overview />} />
                  <Route path="readiness" element={<Readiness />} />
                  <Route path="data-quality" element={<DataQuality />} />
                  <Route path="activity" element={<Activity />} />
                  <Route path="controls" element={<Controls />} />
                  <Route path="history" element={<History />} />
                </Route>
                <Route path="/admin/events" element={<EventCalendar />} />
                <Route path="/admin/events/reactions" element={<EventReactionMonitor />} />
                <Route path="/admin/news-intelligence" element={<NewsIntelligence />} />
                <Route path="/admin/news-intelligence/realtime" element={<NewsIntelligence />} />
                <Route path="/admin/news-intelligence/validation" element={<NewsIntelligence />} />
                <Route path="/admin/tradingview" element={<TradingView />} />
                <Route path="/admin/signals" element={<Signals />} />
                <Route path="/admin/signals/pairs" element={<SignalPairs />} />
                <Route path="/admin/audit" element={<AuditLogs />} />
                <Route path="/admin/compliance" element={<Compliance />} />
                <Route path="/admin/system-health" element={<SystemHealth />} />
                <Route path="/admin/cati" element={<CatiStatus />} />
                <Route path="/admin/bot-monitor" element={<BotMonitor />} />
                <Route path="/admin/bot/runs/:runId" element={<BotRunDetails />} />
                <Route path="/admin/transactions" element={<Transactions />} />
                <Route path="/admin/activity" element={<ActivityFeed />} />
              </Route>

              {/* The legacy end-user pages (/welcome, /onboarding, /payment-success and
                  everything under /dashboard/*) were removed: they belong to
                  frontends/user-frontend. Old links fall through to the fallback. */}

              {/* Fallback */}
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </AuthProvider>
        </MarketingProvider>
      </BrowserRouter>
    </QueryClientProvider>
    </ErrorBoundary>
  );
}

export default App;
