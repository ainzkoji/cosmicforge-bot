from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator
from typing import Optional, List
from datetime import datetime
from enum import Enum


# --- Password policy ---
PASSWORD_MIN_LENGTH = 8
# bcrypt hashes at most 72 bytes; longer input is rejected instead of being
# silently truncated (see app.core.security.MAX_PASSWORD_BYTES).
PASSWORD_MAX_BYTES = 72


def _check_password_bytes(value: str) -> str:
    if len(value.encode("utf-8")) > PASSWORD_MAX_BYTES:
        raise ValueError(
            f"Password is too long: at most {PASSWORD_MAX_BYTES} bytes "
            "(accented characters and emoji count as more than one)"
        )
    return value


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
    password: str = Field(..., min_length=PASSWORD_MIN_LENGTH, max_length=PASSWORD_MAX_BYTES)
    locale: Optional[str] = "en"
    country: Optional[str] = None
    timezone: Optional[str] = None
    terms_accepted_at: Optional[str] = None
    risk_disclaimer_accepted_at: Optional[str] = None
    marketing_session_id: Optional[str] = None
    selected_plan_id: Optional[str] = None
    confirmed_password: Optional[str] = None

    @field_validator("password")
    @classmethod
    def check_password_bytes(cls, v: str) -> str:
        return _check_password_bytes(v)

    @model_validator(mode='after')
    def check_passwords_match(self) -> 'UserCreate':
        if self.confirmed_password is not None and self.password != self.confirmed_password:
            raise ValueError('Passwords do not match')
        return self


class UserResponse(BaseModel):
    id: str
    email: EmailStr
    status: UserStatus
    role: UserRole
    permissions: List[str] = []
    entitlements: dict = {}
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
    new_password: str = Field(..., min_length=PASSWORD_MIN_LENGTH, max_length=PASSWORD_MAX_BYTES)

    @field_validator("new_password")
    @classmethod
    def check_password_bytes(cls, v: str) -> str:
        return _check_password_bytes(v)


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
