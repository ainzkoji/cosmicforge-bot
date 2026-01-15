# CosmicForge Trading Bot - Backend
## Folder Structure
```text
backend/
├── .env                        # Environment Variables (Secrets)
├── requirements.txt            # Python Dependencies
├── pytest.ini                  # Test Configuration
├── README.md                   # Project Documentation
├── app/                        # Main Application Code
│   ├── main.py                 # App Entry Point & API Routes
│
│   ├── api/                    # API Route Handlers
│   │   ├── auth.py             # Authentication, 2FA, Sessions
│   │   ├── billing.py          # Plans, Checkout, Subscription Management
│   │   ├── brokers.py          # Broker Connections & Catalog
│   │   ├── kyc.py              # Identity Verification Endpoints
│   │   ├── monitoring.py       # Dashboard, Traces, Metrics
│   │   ├── onboarding.py       # Setup Wizard
│   │   ├── public.py           # Marketing/Public CMS Endpoints
│   │   └── strategies.py       # Strategy Management (Upcoming)
│   │
│   ├── core/                   # Core Business Logic & Services
│   │   ├── config.py           # Configuration & Settings (Env loading)
│   │   ├── security.py         # JWT, Password Hashing
│   │   ├── billing_service.py  # Payments & Entitlements Logic
│   │   ├── broker_service.py   # Broker Connection Logic
│   │   ├── broker_security.py  # API Key Encryption
│   │   ├── kyc_policy.py       # KYC Rules & Requirements
│   │   ├── kyc_storage.py      # Document Storage (S3/Local)
│   │   ├── kyc_encryption.py   # PII Encryption
│   │   ├── onboarding_service.py # Wizard State Management
│   │   └── strategy_service.py # Strategy Execution Logic
│   │
│   ├── persistence/            # Database & Data Access
│   │   ├── db.py               # SQLite Connection & Schema
│   │   ├── migrations.py       # DB Migrations
│   │   ├── run_manager.py      # Bot Run Persistence
│   │   ├── trade_tracker.py    # Trade History Persistence
│   │   ├── events.py           # Event Sourcing/Log
│   │   ├── alert_manager.py    # Alert Persistence
│   │   ├── global_analytics.py # Aggregated Stats
│   │   ├── exports.py          # CSV Exports
│   │   └── trace_recorder.py   # Debug Tracing
│   │
│   ├── schemas/                # Pydantic Models (Validation)
│   │   ├── auth.py             # User, Token, 2FA Models
│   │   ├── billing.py          # Plan, Subscription, Invoice Models
│   │   ├── broker.py           # Broker Account Models
│   │   ├── kyc.py              # KYC Case, Document Models
│   │   ├── onboarding.py       # Wizard Step Models
│   │   ├── public.py           # Marketing tracking & CMS Models
│   │   └── strategies.py       # Strategy Spec Models
│   │
│   ├── exchange/               # Exchange Adapters
│   │   ├── cache.py            # Data Caching Layer
│   │   └── binance/            # Binance Implementation
│   │
│   ├── execution/              # Trade Execution Engine
│   │   ├── executor.py         # Main Execution Logic
│   │   ├── position_manager.py # Position State Management
│   │   ├── add_manager.py      # Pyramiding/Adding Logic
│   │   ├── anti_flip.py        # Signal Filtering Logic
│   │   └── tp_sl.py            # TP/SL Calculation
│   │
│   ├── risk/                   # Risk Management
│   │   ├── risk_budget.py      # Portfolio Risk Controls
│   │   ├── circuit.py          # Circuit Breakers
│   │   ├── gate.py             # Pre-Trade Risk Gates
│   │   ├── sizing.py           # Position Sizing Logic
│   │   └── drawdown.py         # Drawdown Monitoring
│   │
│   ├── strategy/               # Trading Strategies
│   │   ├── base.py             # Strategy Abstract Base Class
│   │   ├── registry.py         # Strategy Registration System
│   │   ├── supertrend.py       # SuperTrend Impl
│   │   └── ...                 # Other strategy implementations
│   │
│   ├── runner/                 # Background Worker
│   │   └── ...
│   │
│   └── tests/                  # Unit & Integration Tests
│
├── data/                       # Local Data Storage (SQLite DBs)
├── logs/                       # Application Logs
└── scripts/                    # Utility & Migration Scripts
    ├── migrate_*.py            # Database Migrations
    ├── verify_*.py             # logical Verification Scripts
    └── check_*.py              # Sanity Check Scripts
```
## Functional Modules
### 1. Authentication & Security
**Goal:** User identity, access control, and account security.
*   **Endpoints:** `api/auth.py`
    *   Login, Register, Refresh Token.
    *   2FA (Setup, Verify, Disable).
    *   Password Reset Flow.
    *   Session & Device Management.
*   **Core Logic:**
    *   `core/security.py`: JWT generation, password hashing.
*   **Data Models:** `schemas/auth.py` (User, Token, LoginRequest).
### 2. Subscription & Billing
**Goal:** Payments, plan management, and feature gating.
*   **Endpoints:** `api/billing.py`
    *   `/plans`: Public pricing.
    *   `/checkout`: Session creation (Stripe/Mock).
    *   `/webhook`: Payment provider callbacks.
    *   `/subscription/manage`: Cancel/Upgrade.
*   **Core Logic:**
    *   `core/billing_service.py`: Payment provider abstraction, entitlement checks.
    *   `core/config.py`: Stripe keys configuration.
*   **Data Models:** `schemas/billing.py`, `persistence/db.py` (tables: `subscriptions`, `invoices`, `pricing_intents`).
### 3. Broker Integration
**Goal:** Connecting to user exchange accounts safely.
*   **Endpoints:** `api/brokers.py`
    *   `/catalog`: Supported exchanges.
    *   `/connect`: Start connection flow.
    *   `/accounts`: List connected accounts.
*   **Core Logic:**
    *   `core/broker_service.py`: Connection logic.
    *   `core/broker_security.py`: Credential handling/encryption.
*   **Data Models:** `schemas/broker.py`.
### 4. KYC (Identity Verification)
**Goal:** Compliance and user verification.
*   **Endpoints:** `api/kyc.py`
    *   Document upload, status checks, submission.
*   **Core Logic:**
    *   `core/kyc_policy.py`: Requirements logic.
    *   `core/kyc_storage.py` & `kyc_encryption.py`: Secure document handling.
*   **Data Models:** `persistence/db.py` (tables: `kyc_cases`, `kyc_documents`).
### 5. Onboarding Wizard
**Goal:** Guiding new users to their first bot.
*   **Endpoints:** `api/onboarding.py`
    *   Save/Load wizard steps.
    *   Complete onboarding (generates defaults).
*   **Core Logic:**
    *   `core/onboarding_service.py`: State management.
*   **Data Models:** `schemas/onboarding.py`.
### 6. Trading Core (Bot Engine)
**Goal:** Strategy execution and trade management.
*   **Entry Point:** `main.py` (API) & `runner` module (Background worker).
*   **Modules:**
    *   `strategy/`: Signals strategies (SuperTrend, Ensemble, etc.).
    *   `execution/`: TP/SL calculation, Flip Logic, Position Management.
    *   `risk/`: Risk budget engine, pre-trade checks.
    *   `persistence/`: `trade_tracker.py`, `run_manager.py` (Trade history, Run logs).
### 7. Public & Analytics
**Goal:** Marketing site support and usage tracking.
*   **Endpoints:** `api/public.py`
    *   CMS content (Home, Features).
    *   Event tracking (`track_event`).
*   **Persistence:** `persistence/global_analytics.py` (Leaderboards, Stats).
## Running the Backend
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```


# CosmicForge Trading Bot - Frontend
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
