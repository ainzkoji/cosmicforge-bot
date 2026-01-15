import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './auth/AuthContext';
import { MarketingProvider } from './context/MarketingContext';
import { ProtectedRoute } from './components/Auth/ProtectedRoute';
import { AdminProtectedRoute } from './components/Auth/AdminProtectedRoute';
import { DashboardLayout } from "./components/Layout/DashboardLayout"; // Updated Import
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
import Register from '@/pages/Register';
import Setup2FA from '@/pages/Setup2FA';
import BrokerConnection from '@/pages/BrokerConnection';
import Analytics from '@/pages/Analytics';
import SocialTrading from '@/pages/SocialTrading';
import Academy from '@/pages/Academy';
import Subscription from '@/pages/Subscription';
import OnboardingWizard from '@/pages/OnboardingWizard';
import StrategyGallery from '@/pages/StrategyGallery';
import StrategyDetails from '@/pages/StrategyDetails';
import StrategyBuilder from '@/pages/StrategyBuilder';
import StrategyConfig from '@/pages/StrategyConfig';
import MyBots from '@/pages/MyBots';
import BotDetails from '@/pages/BotDetails';
import EditBot from '@/pages/EditBot';
import Support from '@/pages/Support';
import DeveloperSettings from '@/pages/DeveloperSettings';
import PaymentSuccess from '@/pages/PaymentSuccess';
import AdminDashboard from '@/pages/admin/Dashboard';
import UserManagement from '@/pages/admin/UserManagement';
import RevenueAnalytics from '@/pages/admin/RevenueAnalytics';
import CommissionSettings from '@/pages/admin/CommissionSettings';
import AuditLogs from '@/pages/admin/AuditLogs';
import Compliance from '@/pages/admin/Compliance';
import SystemHealth from '@/pages/admin/SystemHealth';
import BotMonitor from '@/pages/admin/BotMonitor';
import Transactions from '@/pages/admin/Transactions';
import ActivityFeed from '@/pages/admin/ActivityFeed';

const queryClient = new QueryClient();

// Removed inline DashboardLayout

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
              <Route path="/register" element={<Register />} />
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
                  <Route path="strategies" element={<StrategyGallery />} />
                  <Route path="strategies/builder" element={<StrategyBuilder />} />
                  <Route path="strategies/config" element={<StrategyConfig />} />
                  <Route path="strategies/:id" element={<StrategyDetails />} />
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

              {/* Admin Routes (Protected - Admin Role Required) */}
              <Route element={<AdminProtectedRoute />}>
                <Route path="/admin" element={<AdminDashboard />} />
                <Route path="/admin/users" element={<UserManagement />} />
                <Route path="/admin/revenue" element={<RevenueAnalytics />} />
                <Route path="/admin/commissions" element={<CommissionSettings />} />
                <Route path="/admin/audit" element={<AuditLogs />} />
                <Route path="/admin/compliance" element={<Compliance />} />
                <Route path="/admin/system-health" element={<SystemHealth />} />
                <Route path="/admin/bot-monitor" element={<BotMonitor />} />
                <Route path="/admin/transactions" element={<Transactions />} />
                <Route path="/admin/activity" element={<ActivityFeed />} />
              </Route>

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
