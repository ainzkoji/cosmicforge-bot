import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from shared_lib.persistence.db import DB, utc_now_iso
from shared_lib.events.hooks import EventHooks
from shared_lib.notifications.templates import TemplateRegistry

logger = logging.getLogger(__name__)

class NotificationDispatcher:
    """
    Listens to system events and generates Notification Jobs.
    """
    
    def __init__(self, db: DB):
        self.db = db
        EventHooks.register(self.on_event)
        logger.info("NotificationDispatcher: Registered.")

    def on_event(self, payload: Dict[str, Any]):
        """
        Main handler. Maps events to jobs.
        """
        try:
            action = payload.get("action")
            event_type = payload.get("event_type")
            
            # 1. Map Event
            mapping = self._get_mapping(action, event_type)
            if not mapping:
                return

            # 2. Resolve User Context
            # We need user_id to check prefs and send. 
            # In full system we resolve via run_id or bot_instance_id from details.
            # fallback: 'admin' or system owner? For now, we need a user_id.
            details = payload.get("details", {})
            user_id = self._resolve_user(payload)
            
            if not user_id:
                # Can't notify nobody.
                return

            # 3. Check Preferences & Create Jobs
            self._dispatch_for_user(user_id, mapping, payload)
            
        except Exception as e:
            logger.error(f"NotificationDispatcher: Failed to process event: {e}", exc_info=True)

    def _get_mapping(self, action: str, event_type: str) -> Optional[Dict]:
        """Maps audit actions to notification templates."""
        # Simple static mapping for common events
        if action == "EXECUTION_FILLED":
            return {"category": "trade", "severity": "INFO", "template": "TRADE_FILLED"}
        if action == "EXECUTION_FAILED":
            return {"category": "trade", "severity": "ERROR", "template": "ORDER_FAILED"}
        if action == "KILL_SWITCH_TRIGGERED":
             return {"category": "risk", "severity": "CRITICAL", "template": "RISK_ALERT"}
        if action == "CIRCUIT_TRIPPED":
             return {"category": "risk", "severity": "CRITICAL", "template": "RISK_ALERT"}
        # Fallback generic errors
        if event_type == "ERROR":
            return {"category": "system", "severity": "ERROR", "template": "SYSTEM_ERROR"}
            
        return None

    def _resolve_user(self, payload: Dict) -> Optional[str]:
        """Attempts to find the owner of the event."""
        # details.user_id might exist
        details = payload.get("details", {})
        if "user_id" in details:
            return details["user_id"]
        
        # run_id lookup? (too expensive to query DB every event?)
        # Ideally, audit details should include user_id or bot_id
        # For now, let's assume details has it or we query bot_instances if we have bot_id
        return None 

    def _dispatch_for_user(self, user_id: str, mapping: Dict, payload: Dict):
        category = mapping["category"]
        severity = mapping["severity"]
        template = mapping["template"]
        
        # Prefs check (cache this ideally, but SQL is fast enough for now)
        # We fetch enabled channels for this user+category
        prefs = self._get_user_channels(user_id, category)
        
        # For each enabled channel, create job
        for channel in prefs:
            self._create_job(user_id, channel, template, payload, mapping)

    def _get_user_channels(self, user_id: str, category: str):
        # Default: In-App is always on unless explicitly disabled?
        # Or use DB.
        enabled = []
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT channel, is_enabled FROM notification_preferences WHERE user_id=? AND category=?",
                (user_id, category)
            ).fetchall()
            
            # If no rows, defaults?
            # Start with SAFE defaults: In-App = ON. Email = OFF? 
            # If row exists, obey it.
            
            # Map valid logic
            found = {r["channel"]: bool(r["is_enabled"]) for r in rows}
            
            # Hardcoded defaults if missing
            if "in_app" not in found:
                enabled.append("in_app") # Always default on
            elif found["in_app"]:
                enabled.append("in_app")
                
            if found.get("email"): enabled.append("email")
            if found.get("telegram"): enabled.append("telegram")
            if found.get("push"): enabled.append("push")
            
        return enabled

    def _create_job(self, user_id: str, channel: str, template: str, payload: Dict, mappingDict: Dict):
        # Dedupe Logic
        dedupe_key = self._generate_dedupe_key(user_id, channel, template, payload)
        
        # Check specific dedupe (db query)
        if self._is_duplicate(dedupe_key):
             logger.info(f"NotificationDispatcher: Skipped duplicate {dedupe_key}")
             return

        # Render Content
        subject, body_text, body_html = TemplateRegistry.render(template, payload)

        # Insert Job
        job_id = str(uuid.uuid4())
        now = utc_now_iso()
        
        # Create In-App Alert immediately (write to alerts table)
        if channel == "in_app":
            self._write_in_app_alert(user_id, payload, mappingDict)
            # In-app doesn't need a "job" processed by worker usually, 
            # unless we want push behavior. 
            # But the requirement says "reuse alerts table". 
            # Writing to alerts table IS the delivery for in-app.
            # So we might skip creating a job row for 'in_app' channel 
            # unless we treat 'in_app' as 'push'. 
            # Let's write to alerts table and RETURN.
            return

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO notification_jobs (
                    id, user_id, channel, template_id, 
                    subject, body_text, body_html,
                    metadata_json, dedupe_key, status, 
                    created_at, updated_at, next_retry_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    job_id, user_id, channel, template,
                    subject, body_text, body_html,
                    json.dumps(payload), dedupe_key,
                    now, now, now
                )
            )

    def _generate_dedupe_key(self, user_id, channel, template, payload):
        # crude hash
        # e.g. trade_filled + order_id
        details = payload.get("details", {})
        ref_id = details.get("order_id") or details.get("trade_id") or payload.get("symbol") or "global"
        raw = f"{user_id}:{channel}:{template}:{ref_id}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _is_duplicate(self, key):
        # Check if job exists with this key created recently (e.g. 5 mins?)
        # Or just rely on unique index if we enforce it. 
        # Requirement said "If duplicate exists".
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM notification_jobs WHERE dedupe_key=? AND status != 'failed'",
                (key,)
            ).fetchone()
            return bool(row)

    def _write_in_app_alert(self, user_id: str, payload: Dict, mapping: Dict):
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO alerts (
                    ts, alert_type, severity, trace_id, symbol, message, details_json, user_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    utc_now_iso(),
                    mapping["category"].upper(), # e.g. TRADE
                    mapping["severity"],
                    payload.get("trace_id"),
                    payload.get("symbol"),
                    f"Notification: {mapping['template']}", # Placeholder message logic
                    json.dumps(payload),
                    user_id
                )
            )
