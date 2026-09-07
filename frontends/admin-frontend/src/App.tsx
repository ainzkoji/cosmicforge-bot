import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './auth/AuthContext';
import { MarketingProvider } from './context/MarketingContext';
import { ProtectedRoute } from './components/Auth/ProtectedRoute';
import { AdminProtectedRoute } from './components/Auth/AdminProtectedRoute';
import { DashboardLayout } from "./components/Layout/DashboardLayout";
import { PublicLayout } from "./components/Layout/PublicLayout";
import Home from '@/pages/Home';
import Login from '@/pages/Login';
import Welcome from '@/pages/Welcome';
import VerifyEmail from '@/pages/VerifyEmail';
import ForgotPassword from '@/pages/ForgotPassword';
import ResetPassword from '@/pages/ResetPassword';
import LandingPage from '@/pages/LandingPage';
import Features from '@/pages/Features';
import HowItWorks from '@/pages/HowItWorks';
import Pricing from '@/pages/Pricing';
import SecuritySettings from '@/pages/SecuritySettings';
import Profile from '@/pages/Profile';
import KYCIntro from '@/pages/KYCIntro';
import KYCPersonalInfo from '@/pages/KYCPersonalInfo';
import KYCIDUpload from '@/pages/KYCIDUpload';
import KYCFaceVerification from '@/pages/KYCFaceVerification';
import KYCStatus from '@/pages/KYCStatus';

import BrokerConnection from '@/pages/BrokerConnection';
import Analytics from '@/pages/Analytics';
import SocialTrading from '@/pages/SocialTrading';
import Academy from '@/pages/Academy';
import Subscription from '@/pages/Subscription';
import OnboardingWizard from '@/pages/OnboardingWizard';
import MyBots from '@/pages/MyBots';
import BotDetails from '@/pages/BotDetails';
import EditBot from '@/pages/EditBot';
import Support from '@/pages/Support';
import DeveloperSettings from '@/pages/DeveloperSettings';
import PaymentSuccess from '@/pages/PaymentSuccess';
import AdminDashboard from '@/pages/admin/Dashboard';
import UserManagement from '@/pages/admin/UserManagement';
import RevenueAnalytics from '@/pages/admin/RevenueAnalytics';
import ProfitabilityReport from '@/pages/admin/ProfitabilityReport';
import AffiliateRevenue from '@/pages/admin/AffiliateRevenue';
import AuditLogs from '@/pages/admin/AuditLogs';
import Compliance from '@/pages/admin/Compliance';
import SystemHealth from '@/pages/admin/SystemHealth';
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
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <MarketingProvider>
          <AuthProvider>
            <Routes>
              {/* Public Marketing Routes */}
              <Route element={<PublicLayout />}>
                <Route path="/" element={<LandingPage />} />
                <Route path="/features" element={<Features />} />
                <Route path="/how-it-works" element={<HowItWorks />} />
                <Route path="/pricing" element={<Pricing />} />
              </Route>

              {/* Auth Pages */}
              <Route path="/welcome" element={<Welcome />} />
              <Route path="/login" element={<Login />} />
              <Route path="/verify-email" element={<VerifyEmail />} />
              <Route path="/forgot-password" element={<ForgotPassword />} />
              <Route path="/reset-password" element={<ResetPassword />} />

              {/* Protected Routes */}
              <Route element={<ProtectedRoute />}>
                {/* Onboarding (Fullscreen, no sidebar) */}
                <Route path="/onboarding" element={<OnboardingWizard />} />
                <Route path="/payment-success" element={<PaymentSuccess />} />

                {/* Main Dashboard (With Sidebar) */}
                <Route path="/dashboard" element={<DashboardLayout />}>
                  <Route index element={<Home />} />

                  {/* Core Features */}
                  <Route path="bots" element={<MyBots />} />
                  <Route path="bots/:id" element={<BotDetails />} />
                  <Route path="bots/:id/edit" element={<EditBot />} />
                  {/* Strategy routes removed */}
                  <Route path="brokers" element={<BrokerConnection />} />
                  <Route path="analytics" element={<Analytics />} />
                  <Route path="social" element={<SocialTrading />} />
                  <Route path="academy" element={<Academy />} />
                  <Route path="subscription" element={<Subscription />} />

                  {/* Settings & Profile */}
                  <Route path="security" element={<SecuritySettings />} />
                  <Route path="developer" element={<DeveloperSettings />} />
                  <Route path="support" element={<Support />} />
                  <Route path="profile" element={<Profile />} />

                  {/* KYC Flow */}
                  <Route path="kyc">
                    <Route index element={<KYCIntro />} />
                    <Route path="personal-info" element={<KYCPersonalInfo />} />
                    <Route path="id-upload" element={<KYCIDUpload />} />
                    <Route path="face-verification" element={<KYCFaceVerification />} />
                    <Route path="status" element={<KYCStatus />} />
                  </Route>
                </Route>
              </Route>

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
                <Route path="/admin/bot-monitor" element={<BotMonitor />} />
                <Route path="/admin/bot/runs/:runId" element={<BotRunDetails />} />
                <Route path="/admin/transactions" element={<Transactions />} />
                <Route path="/admin/activity" element={<ActivityFeed />} />
              </Route>

              {/* Legacy mixed-app note:
                  this app still contains public/user pages today (for example LandingPage, Login,
                  Home, BrokerConnection, Analytics, Academy, Subscription, MyBots, BotDetails,
                  EditBot, Support, DeveloperSettings, Profile, and KYC flow pages).
                  Those are later cleanup items and should not change the rule that admin-frontend
                  is the sole future owner of /admin/*. */}

              {/* Fallback */}
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </AuthProvider>
        </MarketingProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
