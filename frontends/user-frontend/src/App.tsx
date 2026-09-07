import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from '@/auth/AuthContext';
import { MarketingProvider } from '@/context/MarketingContext';
import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { DashboardLayout } from "@/components/Layout/DashboardLayout";
import { PublicLayout } from "@/components/Layout/PublicLayout";
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

import Academy from '@/pages/Academy';
import Subscription from '@/pages/Subscription';
import OnboardingWizard from '@/pages/OnboardingWizard';
import AutoPilot from '@/pages/AutoPilot';
import Signals from '@/pages/Signals';
import MyBots from '@/pages/MyBots';
import BotDetails from '@/pages/BotDetails';
import EditBot from '@/pages/EditBot';
import Support from '@/pages/Support';
import DeveloperSettings from '@/pages/DeveloperSettings';
import PaymentSuccess from '@/pages/PaymentSuccess';
import Portfolio from '@/pages/Portfolio';
import Dashboard from '@/pages/Dashboard';
import BacktestList from '@/pages/Backtesting/BacktestList';
import CreateBacktest from '@/pages/Backtesting/CreateBacktest';
import BacktestDetails from '@/pages/Backtesting/BacktestDetails';


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
              {/* Public Routes */}
              <Route element={<PublicLayout />}>
                <Route path="/" element={<LandingPage />} />
                <Route path="/features" element={<Features />} />
                <Route path="/how-it-works" element={<HowItWorks />} />
                <Route path="/pricing" element={<Pricing />} />
                <Route path="/login" element={<Login />} />
                <Route path="/register" element={<Register />} />
                <Route path="/verify-email" element={<VerifyEmail />} />
                <Route path="/forgot-password" element={<ForgotPassword />} />
                <Route path="/reset-password" element={<ResetPassword />} />
                <Route path="/payment/success" element={<PaymentSuccess />} />
              </Route>

              {/* Protected Dashboard Routes */}
              <Route element={<ProtectedRoute />}>
                <Route path="/welcome" element={<Welcome />} />
                <Route path="/setup-2fa" element={<Setup2FA />} />
                <Route path="/onboarding" element={<OnboardingWizard />} />

                {/* /admin/* ownership retired from user-frontend.
                    The real admin portal now lives in frontends/admin-frontend. */}

                <Route path="/dashboard" element={<DashboardLayout />}>
                  <Route index element={<Dashboard />} />

                  {/* Core Features */}
                  <Route path="bots" element={<MyBots />} />
                  <Route path="bots/:id" element={<BotDetails />} />
                  <Route path="bots/:id/edit" element={<EditBot />} />
                  <Route path="auto-pilot" element={<AutoPilot />} />
                  <Route path="signals" element={<Signals />} />

                  {/* Backtesting (Phase 7) */}
                  <Route path="backtests" element={<BacktestList />} />
                  <Route path="backtests/new" element={<CreateBacktest />} />
                  <Route path="backtests/:id" element={<BacktestDetails />} />

                  {/* Marketplace redirect - removed but redirect old links */}
                  <Route path="strategies" element={<Navigate to="/dashboard/auto-pilot" replace />} />
                  <Route path="strategies/*" element={<Navigate to="/dashboard/auto-pilot" replace />} />

                  <Route path="brokers" element={<BrokerConnection />} />
                  <Route path="portfolio" element={<Portfolio />} />
                  <Route path="analytics" element={<Analytics />} />

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
