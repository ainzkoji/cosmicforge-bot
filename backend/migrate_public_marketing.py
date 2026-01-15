"""
Migration: Public Marketing & Attribution System
- public_content_blocks: CMS content for public pages
- marketing_sessions: UTM tracking and attribution
- plans: Subscription plan catalog
- plan_entitlements: Feature limits per plan
- pricing_intents: Track plan selection before signup
- marketing_events: Page views and CTA clicks
- Updates users table with locale, timezone, terms fields
"""
import sqlite3
from pathlib import Path
import json
from datetime import datetime

DB_PATH = Path("data/bot.db")

def migrate():
    print(f"Migrating database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Public Content Blocks (CMS)
    print("Creating public_content_blocks table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public_content_blocks (
            id TEXT PRIMARY KEY,
            key TEXT NOT NULL UNIQUE,
            locale TEXT DEFAULT 'en',
            content_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_key ON public_content_blocks(key)")

    # 2. Marketing Sessions (Attribution)
    print("Creating marketing_sessions table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS marketing_sessions (
            id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            ip TEXT,
            user_agent TEXT,
            landing_page TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            utm_content TEXT,
            utm_term TEXT,
            ref_code TEXT,
            aff_broker TEXT,
            converted_user_id TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_marketing_sessions_ref ON marketing_sessions(ref_code)")

    # 3. Plans (Subscription Catalog)
    print("Creating plans table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plans (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            billing_period TEXT DEFAULT 'monthly',
            currency TEXT DEFAULT 'USD',
            status TEXT DEFAULT 'active',
            display_order INTEGER DEFAULT 0,
            description TEXT,
            badge TEXT,
            created_at TEXT NOT NULL
        )
    """)

    # 4. Plan Entitlements
    print("Creating plan_entitlements table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plan_entitlements (
            id TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            FOREIGN KEY (plan_id) REFERENCES plans(id),
            UNIQUE(plan_id, key)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_entitlements_plan ON plan_entitlements(plan_id)")

    # 5. Pricing Intents
    print("Creating pricing_intents table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pricing_intents (
            id TEXT PRIMARY KEY,
            marketing_session_id TEXT,
            plan_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            converted_at TEXT,
            user_id TEXT,
            FOREIGN KEY (plan_id) REFERENCES plans(id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pricing_intents_session ON pricing_intents(marketing_session_id)")

    # 6. Marketing Events
    print("Creating marketing_events table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS marketing_events (
            id TEXT PRIMARY KEY,
            session_id TEXT,
            event_type TEXT NOT NULL,
            page TEXT,
            metadata_json TEXT,
            created_at TEXT NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_marketing_events_session ON marketing_events(session_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_marketing_events_type ON marketing_events(event_type)")

    # 7. Update users table with new columns
    print("Adding columns to users table...")
    new_columns = [
        ("locale", "TEXT DEFAULT 'en'"),
        ("country", "TEXT"),
        ("timezone", "TEXT"),
        ("terms_accepted_at", "TEXT"),
        ("risk_disclaimer_accepted_at", "TEXT"),
        ("marketing_session_id", "TEXT"),
        ("selected_plan_id", "TEXT"),
    ]
    for col_name, col_def in new_columns:
        try:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
            print(f"  Added {col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"  {col_name} already exists")
            else:
                raise

    conn.commit()
    print("Schema migration complete! ✅")
    
    # Seed initial data
    seed_data(conn)
    
    conn.close()
    print("All done! ✅")


def seed_data(conn):
    """Seed initial plans and content"""
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat() + "Z"
    
    # --- Seed Plans ---
    print("Seeding plans...")
    plans = [
        ("plan_free", "Free", 0, "monthly", "USD", "active", 1, "Get started with automated trading", None),
        ("plan_pro", "Pro", 29, "monthly", "USD", "active", 2, "For serious traders", "Most Popular"),
        ("plan_enterprise", "Enterprise", 0, "monthly", "USD", "active", 3, "For institutions and teams", None),
    ]
    for p in plans:
        cursor.execute("""
            INSERT OR IGNORE INTO plans (id, name, price, billing_period, currency, status, display_order, description, badge, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (*p, now))
    
    # --- Seed Plan Entitlements ---
    print("Seeding plan entitlements...")
    entitlements = [
        # Free
        ("plan_free", "max_bots", "1"),
        ("plan_free", "max_accounts", "1"),
        ("plan_free", "live_trading", "false"),
        ("plan_free", "backtesting", "basic"),
        ("plan_free", "api_access", "false"),
        ("plan_free", "copy_trading", "false"),
        ("plan_free", "advanced_reports", "false"),
        # Pro
        ("plan_pro", "max_bots", "unlimited"),
        ("plan_pro", "max_accounts", "5"),
        ("plan_pro", "live_trading", "true"),
        ("plan_pro", "backtesting", "advanced"),
        ("plan_pro", "api_access", "true"),
        ("plan_pro", "copy_trading", "true"),
        ("plan_pro", "advanced_reports", "true"),
        # Enterprise
        ("plan_enterprise", "max_bots", "unlimited"),
        ("plan_enterprise", "max_accounts", "unlimited"),
        ("plan_enterprise", "live_trading", "true"),
        ("plan_enterprise", "backtesting", "advanced"),
        ("plan_enterprise", "api_access", "true"),
        ("plan_enterprise", "copy_trading", "true"),
        ("plan_enterprise", "advanced_reports", "true"),
        ("plan_enterprise", "dedicated_support", "true"),
        ("plan_enterprise", "custom_integrations", "true"),
    ]
    for plan_id, key, value in entitlements:
        cursor.execute("""
            INSERT OR IGNORE INTO plan_entitlements (id, plan_id, key, value)
            VALUES (?, ?, ?, ?)
        """, (f"{plan_id}_{key}", plan_id, key, value))
    
    # --- Seed Content Blocks ---
    print("Seeding content blocks...")
    content_blocks = [
        ("home.hero", json.dumps({
            "title": "Trade Smarter with AI-Powered Automation",
            "subtitle": "Unlock the power of algorithmic trading. CosmicForge Stratos provides intelligent, data-driven strategies to optimize your crypto portfolio effortlessly.",
            "cta_primary": "Get Started",
            "cta_secondary": "Learn More"
        })),
        ("home.stats", json.dumps({
            "users": "10,000+",
            "trades": "1M+",
            "uptime": "99.9%"
        })),
        ("features.categories", json.dumps([
            {"id": "core", "name": "Core Trading", "order": 1},
            {"id": "risk", "name": "Risk & Security", "order": 2},
            {"id": "monitoring", "name": "Monitoring", "order": 3},
            {"id": "backtesting", "name": "Backtesting & Optimization", "order": 4},
            {"id": "integrations", "name": "Integrations", "order": 5},
        ])),
        ("features.list", json.dumps([
            {"id": "ai_analysis", "category": "core", "title": "AI-Powered Analysis", "description": "Leverage machine learning to predict market trends.", "status": "live"},
            {"id": "multi_exchange", "category": "core", "title": "Multi-Exchange Support", "description": "Trade across Binance, Coinbase, Kraken and more.", "status": "live"},
            {"id": "risk_management", "category": "risk", "title": "Risk Management", "description": "Smart stop-loss and position sizing.", "status": "live"},
            {"id": "realtime_monitoring", "category": "monitoring", "title": "Real-time Monitoring", "description": "Live dashboard with instant alerts.", "status": "live"},
            {"id": "secure_api", "category": "risk", "title": "Secure API Connections", "description": "Bank-grade encryption for your keys.", "status": "live"},
            {"id": "backtesting", "category": "backtesting", "title": "Backtesting Tools", "description": "Test strategies against historical data.", "status": "live"},
            {"id": "copy_trading", "category": "core", "title": "Copy Trading", "description": "Follow successful traders automatically.", "status": "coming_soon"},
            {"id": "tradingview", "category": "integrations", "title": "TradingView Integration", "description": "Execute signals from TradingView.", "status": "coming_soon"},
        ])),
        ("how_it_works.steps", json.dumps([
            {"step": 1, "title": "Create Account", "description": "Sign up securely and verify your email in minutes.", "icon": "user-plus"},
            {"step": 2, "title": "Connect Exchange", "description": "Link your crypto exchange via secure API keys.", "icon": "link"},
            {"step": 3, "title": "Set Strategy", "description": "Choose or customize your AI trading parameters.", "icon": "settings"},
            {"step": 4, "title": "Start Trading", "description": "Activate your bot and monitor performance 24/7.", "icon": "trending-up"},
        ])),
        ("faq.items", json.dumps([
            {"q": "Is my exchange account safe?", "a": "Yes! We use read-only API keys and never request withdrawal permissions."},
            {"q": "How much does it cost?", "a": "We offer a free tier. Pro plans start at $29/month."},
            {"q": "Which exchanges are supported?", "a": "Binance, Coinbase Pro, Kraken, and more."},
            {"q": "Do I need trading experience?", "a": "No! Our AI handles the complex analysis."},
        ])),
    ]
    for key, content in content_blocks:
        cursor.execute("""
            INSERT OR IGNORE INTO public_content_blocks (id, key, locale, content_json, updated_at)
            VALUES (?, ?, 'en', ?, ?)
        """, (f"content_{key.replace('.', '_')}", key, content, now))
    
    conn.commit()
    print("Seed data complete! ✅")


if __name__ == "__main__":
    migrate()
