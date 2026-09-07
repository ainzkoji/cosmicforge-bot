from enum import Enum
from typing import List, Dict

class UserRole(str, Enum):
    USER = "user"
    ADMIN = "admin"

class Permission:
    # Bot Management
    BOT_READ = "bot:read"
    BOT_WRITE = "bot:write"          # Create, Update, Delete
    BOT_CONTROL = "bot:control"      # Start, Stop, Pause
    
    # Strategy
    STRATEGY_READ = "strategy:read"
    STRATEGY_EXECUTE = "strategy:execute" # Deploy auto-pilot
    
    # Broker
    BROKER_READ = "broker:read"
    BROKER_WRITE = "broker:write"
    
    # System / Admin
    SYSTEM_MONITOR = "system:monitor"
    USER_MANAGE = "user:manage"
    
    # Financial
    BILLING_READ = "billing:read"
    BILLING_WRITE = "billing:write"

# Canonical Role -> Permissions Mapping
ROLE_PERMISSIONS: Dict[str, List[str]] = {
    UserRole.USER: [
        Permission.BOT_READ,
        Permission.BOT_WRITE,
        Permission.BOT_CONTROL,
        Permission.STRATEGY_READ,
        Permission.STRATEGY_EXECUTE,
        Permission.BROKER_READ,
        Permission.BROKER_WRITE,
        Permission.BILLING_READ,
    ],
    UserRole.ADMIN: [
        # Admin has everything + system/user management
        Permission.BOT_READ,
        Permission.BOT_WRITE,
        Permission.BOT_CONTROL,
        Permission.STRATEGY_READ,
        Permission.STRATEGY_EXECUTE,
        Permission.BROKER_READ,
        Permission.BROKER_WRITE,
        Permission.BILLING_READ,
        Permission.BILLING_WRITE,
        Permission.SYSTEM_MONITOR,
        Permission.USER_MANAGE,
    ]
}

def resolve_permissions(role: str) -> List[str]:
    """Return list of permissions for a given role."""
    # Default to empty if unknown role
    return ROLE_PERMISSIONS.get(role, [])
