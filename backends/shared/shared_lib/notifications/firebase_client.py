"""
Firebase Admin SDK Initializer.
Singleton pattern to initialize Firebase only once.
"""
import os
import logging
import firebase_admin
from firebase_admin import credentials
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_initialized = False


def initialize_firebase():
    """
    Initialize Firebase Admin SDK with service account credentials.
    Uses FIREBASE_SERVICE_ACCOUNT_PATH or GOOGLE_APPLICATION_CREDENTIALS.
    
    Raises:
        FileNotFoundError: If service account JSON is not found.
        ValueError: If neither env var is set.
    """
    global _initialized
    
    if _initialized or firebase_admin._apps:
        logger.info("Firebase already initialized.")
        return
    
    # Explicitly load .env file to ensure variables are available
    load_dotenv()
    
    # Check for service account path in environment
    service_account_path = (
        os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH") or 
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    
    if not service_account_path:
        raise ValueError(
            "Firebase service account path not configured. "
            "Set FIREBASE_SERVICE_ACCOUNT_PATH or GOOGLE_APPLICATION_CREDENTIALS environment variable."
        )
    
    # If path is relative, make it relative to the backend directory
    if not os.path.isabs(service_account_path):
        # Try to find the backend directory (bot-backend or user-backend)
        current_dir = os.getcwd()
        
        # Check if we're already in a backend directory
        if os.path.exists(service_account_path):
            pass  # Use as-is
        elif os.path.exists(os.path.join(current_dir, service_account_path)):
            service_account_path = os.path.join(current_dir, service_account_path)
        else:
            # Try parent directories
            for parent_level in range(3):  # Check up to 3 levels up
                check_path = os.path.join(current_dir, *(['..'] * parent_level), service_account_path)
                if os.path.exists(check_path):
                    service_account_path = os.path.abspath(check_path)
                    break
    
    # Verify file exists
    if not os.path.exists(service_account_path):
        raise FileNotFoundError(
            f"Firebase service account JSON not found at: {service_account_path}"
        )
    
    try:
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)
        _initialized = True
        logger.info(f"Firebase Admin SDK initialized with credentials from: {service_account_path}")
    except Exception as e:
        logger.error(f"Failed to initialize Firebase Admin SDK: {e}")
        raise


def get_firebase_app():
    """
    Get the Firebase app instance.
    Initializes if not already done.
    
    Returns:
        firebase_admin.App: The Firebase app instance.
    """
    if not _initialized and not firebase_admin._apps:
        initialize_firebase()
    
    return firebase_admin.get_app()

