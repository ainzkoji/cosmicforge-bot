"""
KYC Document Storage
Handles secure file storage with presigned URLs pattern
"""
import os
import uuid
import hmac
import hashlib
import time
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime, timedelta

# Storage configuration
KYC_UPLOAD_DIR = Path(__file__).parent.parent.parent / "data" / "kyc_uploads"
ALLOWED_EXTENSIONS = {"jpg", "jpeg", "png", "pdf"}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

# Secret for signing URLs (in production, use env var)
URL_SIGNING_SECRET = os.getenv("KYC_URL_SECRET", "kyc-url-signing-secret-dev")


def ensure_upload_dir():
    """Ensure the upload directory exists"""
    KYC_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # Create subdirectories for organization
    (KYC_UPLOAD_DIR / "documents").mkdir(exist_ok=True)
    (KYC_UPLOAD_DIR / "selfies").mkdir(exist_ok=True)


def validate_file_type(filename: str) -> bool:
    """Check if file extension is allowed"""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return ext in ALLOWED_EXTENSIONS


def validate_file_size(size_bytes: int) -> bool:
    """Check if file size is within limit"""
    return 0 < size_bytes <= MAX_FILE_SIZE


def generate_file_ref(user_id: str, doc_type: str, side: str = "front") -> str:
    """
    Generate a unique file reference/path.
    Format: {doc_type}/{user_id}/{timestamp}_{uuid}_{side}
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    return f"documents/{user_id}/{doc_type}_{timestamp}_{unique_id}_{side}"


def generate_selfie_ref(user_id: str) -> str:
    """Generate a unique selfie file reference"""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    return f"selfies/{user_id}/selfie_{timestamp}_{unique_id}"


def _sign_url(file_ref: str, expires_at: int) -> str:
    """Create HMAC signature for URL"""
    message = f"{file_ref}:{expires_at}"
    signature = hmac.new(
        URL_SIGNING_SECRET.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()[:16]
    return signature


def generate_upload_url(user_id: str, doc_type: str, side: str = "front") -> dict:
    """
    Generate a presigned upload URL (simulated for local storage).
    In production, this would generate an S3 presigned URL.
    
    Returns:
        dict with upload_url, file_ref, expires_at
    """
    ensure_upload_dir()
    
    file_ref = generate_file_ref(user_id, doc_type, side)
    expires_at = int(time.time()) + 3600  # 1 hour
    signature = _sign_url(file_ref, expires_at)
    
    # For local dev, the "upload URL" is just our API endpoint
    # In production, this would be an S3 presigned PUT URL
    upload_url = f"/kyc/documents/upload/{file_ref}?expires={expires_at}&sig={signature}"
    
    return {
        "upload_url": upload_url,
        "file_ref": file_ref,
        "expires_at": expires_at,
        "method": "PUT",
        "max_size_bytes": MAX_FILE_SIZE,
        "allowed_types": list(ALLOWED_EXTENSIONS),
    }


def verify_upload_signature(file_ref: str, expires_at: int, signature: str) -> bool:
    """Verify the signature on an upload URL"""
    if time.time() > expires_at:
        return False
    
    expected_sig = _sign_url(file_ref, expires_at)
    return hmac.compare_digest(signature, expected_sig)


def save_uploaded_file(file_ref: str, content: bytes, extension: str) -> Tuple[bool, str]:
    """
    Save an uploaded file to storage.
    
    Returns:
        Tuple of (success, full_file_path or error_message)
    """
    ensure_upload_dir()
    
    if not validate_file_size(len(content)):
        return False, f"File too large. Max size: {MAX_FILE_SIZE // 1024 // 1024}MB"
    
    if extension.lower() not in ALLOWED_EXTENSIONS:
        return False, f"Invalid file type. Allowed: {ALLOWED_EXTENSIONS}"
    
    # Create full path
    file_path = KYC_UPLOAD_DIR / f"{file_ref}.{extension}"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(file_path, "wb") as f:
            f.write(content)
        return True, str(file_path)
    except Exception as e:
        return False, f"Failed to save file: {str(e)}"


def get_file_path(file_ref: str) -> Optional[Path]:
    """Get the full path to a stored file"""
    # Find file with any extension
    base_path = KYC_UPLOAD_DIR / file_ref
    for ext in ALLOWED_EXTENSIONS:
        full_path = Path(f"{base_path}.{ext}")
        if full_path.exists():
            return full_path
    return None


def generate_download_url(file_ref: str, expires_minutes: int = 15) -> Optional[dict]:
    """
    Generate a time-limited download URL for a stored file.
    In production, this would generate an S3 presigned GET URL.
    """
    file_path = get_file_path(file_ref)
    if not file_path:
        return None
    
    expires_at = int(time.time()) + (expires_minutes * 60)
    signature = _sign_url(file_ref, expires_at)
    
    return {
        "download_url": f"/kyc/documents/download/{file_ref}?expires={expires_at}&sig={signature}",
        "expires_at": expires_at,
    }


def delete_file(file_ref: str) -> bool:
    """Delete a file from storage"""
    file_path = get_file_path(file_ref)
    if file_path and file_path.exists():
        try:
            file_path.unlink()
            return True
        except:
            return False
    return False
