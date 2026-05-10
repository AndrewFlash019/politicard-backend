import os
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.user import User
from app.schemas.user import UserCreate, UserLogin, UserResponse, Token
from app.services.auth import hash_password, verify_password, create_access_token

router = APIRouter(prefix="/auth", tags=["Authentication"])

RECOVERY_TTL_HOURS = 1
GENERIC_OK_MSG = "If that email exists, a reset link has been sent"


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register(user_data: UserCreate, db: Session = Depends(get_db)):
    # Check if email already exists
    existing_user = db.query(User).filter(User.email == user_data.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Create new user
    new_user = User(
        email=user_data.email,
        hashed_password=hash_password(user_data.password),
        full_name=user_data.full_name,
        zip_code=user_data.zip_code,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


@router.post("/login", response_model=Token)
def login(user_data: UserLogin, db: Session = Depends(get_db)):
    # Find user by email
    user = db.query(User).filter(User.email == user_data.email).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    # Verify password
    if not verify_password(user_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    # Create token
    access_token = create_access_token(data={"sub": str(user.id), "email": user.email})
    return {"access_token": access_token, "token_type": "bearer"}


# ─── Password recovery ──────────────────────────────────────────────────────
class ForgotPasswordIn(BaseModel):
    email: EmailStr


class ResetPasswordIn(BaseModel):
    token: str = Field(..., min_length=10, max_length=200)
    new_password: str = Field(..., min_length=8, max_length=200)


def _try_supabase_recovery_link(email: str) -> str | None:
    """Best-effort: ask Supabase to generate a recovery link so users get the
    real built-in password-reset email when SUPABASE_SERVICE_KEY is available.
    Returns the generated link (already emailed by Supabase if SMTP is set up
    on the project), or None if the SDK call isn't available."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
    if not (url and key):
        return None
    try:
        from supabase import create_client
        supabase = create_client(url, key)
        result = supabase.auth.admin.generate_link({"type": "recovery", "email": email})
        # Different SDK versions wrap differently; try both shapes
        if isinstance(result, dict):
            props = result.get("properties") or result.get("data", {}).get("properties") or {}
            return props.get("action_link")
        props = getattr(result, "properties", None)
        return getattr(props, "action_link", None) if props else None
    except Exception:
        return None


@router.post("/forgot-password")
def forgot_password(payload: ForgotPasswordIn, db: Session = Depends(get_db)):
    """Always returns 200 so attackers can't enumerate registered emails."""
    user = db.query(User).filter(User.email == payload.email).first()
    if user:
        token = secrets.token_urlsafe(32)
        user.recovery_token = token
        user.recovery_sent_at = datetime.now(timezone.utc)
        db.commit()
        # Fire-and-forget Supabase email; if it fails we still expose the token
        # via the local /reset-password flow (user follows link from email or
        # we manually send it if SMTP isn't wired up yet).
        _try_supabase_recovery_link(user.email)
    return {"success": True, "message": GENERIC_OK_MSG}


@router.post("/reset-password")
def reset_password(payload: ResetPasswordIn, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.recovery_token == payload.token).first()
    if not user or not user.recovery_sent_at:
        raise HTTPException(status_code=400, detail="Invalid or expired token")
    sent_at = user.recovery_sent_at
    if sent_at.tzinfo is None:
        sent_at = sent_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) - sent_at > timedelta(hours=RECOVERY_TTL_HOURS):
        raise HTTPException(status_code=400, detail="Reset link expired — request a new one")

    user.hashed_password = hash_password(payload.new_password)
    user.recovery_token = None
    user.recovery_sent_at = None
    db.commit()
    return {"success": True}
