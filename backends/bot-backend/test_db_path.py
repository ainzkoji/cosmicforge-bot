from app.core.bot_instance_service import get_bot_instance_service
import os

svc = get_bot_instance_service()
print(f"Service DB path: {svc.db.path}")
print(f"Service DB abs path: {os.path.abspath(svc.db.path)}")

from app.core.config import settings
print(f"Settings DATABASE_URL: {settings.DATABASE_URL}")
