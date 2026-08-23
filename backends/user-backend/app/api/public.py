from fastapi import APIRouter, Depends, HTTPException, Query, Request
from shared_lib.persistence.db import DB, utc_now_iso
from app.schemas.public import (
    MarketingSessionCreate, MarketingSessionResponse,
    TrackEventRequest, PricingIntentCreate, PricingIntentResponse,
    Plan
)
import json
import uuid

router = APIRouter()

# --- CMS Content Endpoints ---

@router.get("/public/home")
def get_home_content():
    db = DB()
    with db.connect() as conn:
        cursor = conn.execute(
            "SELECT key, content_json FROM public_content_blocks WHERE key IN ('home.hero', 'home.stats') AND locale = 'en'"
        )
        data = {row["key"]: json.loads(row["content_json"]) for row in cursor}
    return data

@router.get("/public/features")
def get_features_content():
    db = DB()
    with db.connect() as conn:
        cursor = conn.execute(
            "SELECT key, content_json FROM public_content_blocks WHERE key IN ('features.list', 'features.categories') AND locale = 'en'"
        )
        data = {row["key"]: json.loads(row["content_json"]) for row in cursor}
    return data

@router.get("/public/how-it-works")
def get_how_it_works_content():
    db = DB()
    with db.connect() as conn:
        cursor = conn.execute(
            "SELECT key, content_json FROM public_content_blocks WHERE key IN ('how_it_works.steps', 'faq.items') AND locale = 'en'"
        )
        data = {row["key"]: json.loads(row["content_json"]) for row in cursor}
    return data

from app.core import billing_service

@router.get("/public/pricing")
def get_pricing_data():
    plans = billing_service.get_public_plans()
    return {"plans": [p.dict() for p in plans]}

# --- Tracking Endpoints ---

@router.post("/public/session", response_model=MarketingSessionResponse)
def create_marketing_session(req: MarketingSessionCreate, request: Request):
    session_id = str(uuid.uuid4())
    db = DB()
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO marketing_sessions 
               (id, created_at, ip, user_agent, landing_page, utm_source, utm_medium, utm_campaign, utm_content, utm_term, ref_code, aff_broker)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session_id, utc_now_iso(), 
                request.client.host, request.headers.get("user-agent"),
                req.landing_page, req.utm_source, req.utm_medium, 
                req.utm_campaign, req.utm_content, req.utm_term,
                req.ref_code, req.aff_broker
            )
        )
    return {"session_id": session_id}

@router.post("/public/track")
def track_event(req: TrackEventRequest):
    event_id = str(uuid.uuid4())
    try:
        db = DB()
        with db.connect() as conn:
            conn.execute(
                """INSERT INTO marketing_events (id, session_id, event_type, page, metadata_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    event_id, req.session_id, req.event_type, req.page,
                    json.dumps(req.metadata) if req.metadata else None,
                    utc_now_iso()
                )
            )
        return {"status": "ok"}
    except Exception as e:
        # Log error but don't crash - tracking failures shouldn't break the app
        print(f"[WARN] Tracking event failed: {str(e)}")
        return {"status": "ok"}  # Return OK anyway to not disrupt user experience

@router.post("/public/pricing/intent", response_model=PricingIntentResponse)
def create_pricing_intent(req: PricingIntentCreate):
    intent_id = str(uuid.uuid4())
    db = DB()
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO pricing_intents (id, session_id, plan_id, created_at)
               VALUES (?, ?, ?, ?)""",
            (intent_id, req.marketing_session_id, req.plan_id, utc_now_iso())
        )
        
        # Also log as an event
        conn.execute(
            """INSERT INTO marketing_events (id, session_id, event_type, page, metadata_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                str(uuid.uuid4()), req.marketing_session_id, "pricing_intent", "/pricing",
                json.dumps({"plan_id": req.plan_id}), utc_now_iso()
            )
        )
    return {"intent_id": intent_id}
