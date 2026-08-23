from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


# --- Enums ---
class UserStatus(str, Enum):
    pending_verification = "pending_verification"
    active = "active"
    suspended = "suspended"
    deleted = "deleted"


class UserRole(str, Enum):
    user = "user"
    admin = "admin"


# --- Token Schemas ---
class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class TokenPayload(BaseModel):
    sub: Optional[str] = None
    exp: Optional[int] = None
    type: Optional[str] = None
    role: Optional[str] = None


class RefreshTokenReq(BaseModel):
    refresh_token: str


# --- User Schemas ---
class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=20)
    locale: Optional[str] = "en"
    country: Optional[str] = None
    timezone: Optional[str] = None
    terms_accepted_at: Optional[str] = None
    risk_disclaimer_accepted_at: Optional[str] = None
    marketing_session_id: Optional[str] = None
    selected_plan_id: Optional[str] = None


class UserResponse(BaseModel):
    id: str
    email: EmailStr
    status: UserStatus
    role: UserRole
    is_verified: bool
    created_at: datetime
    locale: Optional[str] = None
    country: Optional[str] = None
    selected_plan_id: Optional[str] = None

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    email: EmailStr
    password: str


# --- Email Verification ---
class VerifyEmailRequest(BaseModel):
    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6)


class ResendVerificationRequest(BaseModel):
    email: EmailStr


# --- Password Reset ---
class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6)
    new_password: str = Field(..., min_length=8, max_length=20)


# --- Session Management ---
class SessionResponse(BaseModel):
    id: str
    device: Optional[str]
    ip: Optional[str]
    created_at: datetime
    is_current: bool = False


class SessionListResponse(BaseModel):
    sessions: List[SessionResponse]


# --- Broker Schemas (unchanged) ---
class BrokerLinkReq(BaseModel):
    exchange: str = "binance"
    name: str
    api_key: str
    api_secret: str
    passphrase: Optional[str] = None


class BrokerResponse(BaseModel):
    id: str
    exchange: str
    name: str
    is_active: bool
    created_at: datetime
