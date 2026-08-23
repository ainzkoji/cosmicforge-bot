# CosmicForge Trading Bot - Frontend

## Architecture Boundary

- `user-frontend` owns public, customer, and authenticated user flows.
- `user-frontend` is not the long-term owner of `/admin/*`.
- The admin tree currently present under `src/pages/admin` is legacy/misplaced and is frozen for migration into `frontends/admin-frontend`.
- Do not add new admin pages, admin hooks, admin components, or admin API wrappers here.
- See [`frontends/ARCHITECTURE_BOUNDARY.md`](C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/frontends/ARCHITECTURE_BOUNDARY.md).

## Folder Structure

```text
frontend/src/
├── main.tsx                    # Entry Point
├── App.tsx                     # Main Router & Layout Definition
├── index.css                   # Global Styles (Tailwind)
│
├── api/                        # API Client Layer
│   └── client.ts               # Centralized API Wrapper (Auth, Billing, etc.)
│
├── pages/                      # Page Components (Routes)
│   ├── LandingPage.tsx         # Public Home
│   ├── Pricing.tsx             # Public Pricing
│   ├── Login.tsx               # Auth: Login
│   ├── Register.tsx            # Auth: Register
│   ├── VerifyEmail.tsx         # Auth: Email Verification
│   ├── Setup2FA.tsx            # Auth: 2FA Setup
│   ├── ForgotPassword.tsx      # Auth: Recovery
│   ├── ResetPassword.tsx       # Auth: Reset
│   │
│   ├── Dashboard.tsx           # Main User Dashboard (Stats)
│   ├── BrokerConnection.tsx    # Exchange Account Management
│   ├── Subscription.tsx        # Billing & Plan Management
│   ├── Profile.tsx             # User Profile Settings
│   ├── SecuritySettings.tsx    # Password/2FA Settings
│   │
│   ├── OnboardingWizard.tsx    # New User Setup Flow
│   │
│   ├── KYCIntro.tsx            # Identity Verification: Start
│   ├── KYCPersonalInfo.tsx     # Identity Verification: Form
│   ├── KYCIDUpload.tsx         # Identity Verification: Docs
│   ├── KYCFaceVerification.tsx # Identity Verification: Liveness
│   ├── KYCStatus.tsx           # Identity Verification: Status
│   │
│   ├── StrategyGallery.tsx     # Strategy Marketplace
│   ├── StrategyDetails.tsx     # Strategy Info & Performance
│   ├── StrategyBuilder.tsx     # Custom Strategy Editor
│   └── MyBots.tsx              # Active Bot Management
│
├── components/                 # Reusable UI Components
│   ├── Layout/                 # Navbar, Sidebar, Footer
│   ├── UI/                     # Buttons, Cards, Inputs (Design System)
│   └── ...
│
└── context/                    # React Context (State)
    └── MarketingContext.tsx    # Analytics & Session Tracking
```

## Functional Modules

### 1. Authentication & Security
**Goal:** User entry, registration, and secure access.
*   **Pages:**
    *   `Login.tsx`, `Register.tsx`: Entry points.
    *   `VerifyEmail.tsx`: OTP verification step.
    *   `Setup2FA.tsx`: QR code display and verification.
    *   `SecuritySettings.tsx`: Password updates and session management.
*   **API Integration:** `client.ts` -> `login`, `register`, `verify2FA`.

### 2. Billing & Subscriptions
**Goal:** Managing plans, payments, and checkout flows.
*   **Pages:**
    *   `Pricing.tsx`: Public-facing plan comparison.
    *   `Subscription.tsx`: User dashboard for plan status, usage metrics, and invoices.
*   **API Integration:** `client.ts` -> `getPlans`, `createCheckoutSession`, `getSubscription`.

### 3. Onboarding & KYC
**Goal:** Guiding new users and verifying identity for compliance.
*   **Pages:**
    *   `OnboardingWizard.tsx`: Multi-step preference gathering.
    *   `KYC*.tsx`: Identity verification flow (Info -> Docs -> Liveness).
*   **API Integration:** `client.ts` -> `getOnboardingState`, `kycSubmit`.

### 4. Trading & Strategies
**Goal:** Browsing strategies, building custom ones, and managing active bots.
*   **Pages:**
    *   `StrategyGallery.tsx`: Marketplace implementation.
    *   `StrategyDetails.tsx`: In-depth strategy performance view.
    *   `StrategyBuilder.tsx`: Visual editor for custom strategies.
    *   `MyBots.tsx`: Dashboard for running instances and their PnL.
*   **API Integration:** `client.ts` -> Strategies API (Upcoming).

### 5. Broker Management
**Goal:** Connecting and managing exchange API keys.
*   **Pages:**
    *   `BrokerConnection.tsx`: List of connected accounts and connection wizard.
*   **API Integration:** `client.ts` -> `startBrokerConnection`, `validateBrokerConnection`.

## Running the Frontend
```bash
cd frontend
npm install
npm run dev
```

## Tech Stack
*   **Framework**: React + Vite
*   **Styling**: Tailwind CSS + Framer Motion (Animations)
*   **State Management**: React Query (Server State) + Context API
*   **Icons**: Lucide React
