from pydantic import BaseModel, Field

class TwoFASetupResponse(BaseModel):
    items: str # The TOTP Secret
    uri: str   # The otpauth:// URI for QR codes

class TwoFAVerifyRequest(BaseModel):
    code: str = Field(..., min_length=6, max_length=6)

class SessionRevokeRequest(BaseModel):
    session_id: str
