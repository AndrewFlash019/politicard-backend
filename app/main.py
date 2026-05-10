import logging
import os
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db
from app.routes.auth import router as auth_router
from app.routes.users import router as users_router
from app.routes.officials import router as officials_router, metrics_router, feedback_router
from app.routes.ai import router as ai_router
from app.routes.civic import router as civic_router
from app.routes.feed import router as feed_router, constituent_router, engagement_router, official_alignment_router
from app.routes.typology import router as typology_router

log = logging.getLogger("politiscore")
logging.basicConfig(level=logging.INFO)

# ─── Env validation (2H) ────────────────────────────────────────────────────
_REQUIRED = ("SUPABASE_URL", "SUPABASE_KEY")
_RECOMMENDED = ("SUPABASE_SERVICE_KEY", "CONGRESS_API_KEY", "DATABASE_URL", "ANTHROPIC_API_KEY")

_missing_required = [k for k in _REQUIRED if not os.getenv(k)]
if _missing_required:
    raise RuntimeError(
        "Refusing to start: missing required env vars " + ", ".join(_missing_required)
    )
for k in _RECOMMENDED:
    if not os.getenv(k):
        log.warning("env var %s not set — some features will be degraded", k)

ENV = os.getenv("ENV", "production").lower()
INTERNAL_KEY = os.getenv("INTERNAL_HEALTH_KEY", "")

# ─── App + rate limiter ──────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])

app = FastAPI(
    title="PolitiScore API",
    description="Civic engagement platform backend",
    version="1.0.0",
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.exception_handler(RateLimitExceeded)
def _rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"error": "Rate limit exceeded", "retry_after": getattr(exc, "retry_after", 60)},
    )


# ─── CORS hardening (2I) ─────────────────────────────────────────────────────
# Explicit allow-list. allow_credentials=True requires a non-wildcard
# allow_origins (the CORS spec rejects "*" with credentials), so localhost
# stays in the list across all envs and we never fall back to "*".
ALLOWED_ORIGINS = [
    "https://politiscore.com",
    "https://www.politiscore.com",
    "https://app.politiscore.com",
    "https://magnificent-meerkat-40c5aa.netlify.app",
    "http://localhost:3000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Deprecated", "X-Use-Instead"],
)


# ─── Per-route rate limit decorators applied via middleware. We use a small
# lookup table because slowapi's per-route decorators require touching every
# route; this approach scopes by URL prefix so it covers v1 aliases too.
_ROUTE_LIMITS = (
    ("/auth/",                 "10/minute"),
    ("/constituent-votes",     "30/minute"),
    ("/officials/",            "120/minute"),  # incl. .../legislative-activity, etc
    ("/feed/",                 "60/minute"),
    ("/typology/",             "60/minute"),
)


@app.middleware("http")
async def _per_route_limit(request: Request, call_next):
    path = request.url.path
    for prefix, rule in _ROUTE_LIMITS:
        if path.startswith(prefix) or path.startswith("/api/v1" + prefix):
            try:
                limiter.limit(rule)(lambda r: r)(request)
            except RateLimitExceeded as exc:
                return JSONResponse(
                    status_code=429,
                    content={"error": "Rate limit exceeded", "retry_after": getattr(exc, "retry_after", 60)},
                )
            break
    return await call_next(request)


# ─── Mount routers (current paths + /api/v1 aliases) ────────────────────────
_VERSIONED_ROUTERS = [
    auth_router, users_router, officials_router, metrics_router, ai_router,
    civic_router, feed_router, constituent_router, engagement_router,
    official_alignment_router, typology_router, feedback_router,
]
for r in _VERSIONED_ROUTERS:
    app.include_router(r)               # legacy
    app.include_router(r, prefix="/api/v1")  # versioned alias


@app.middleware("http")
async def _deprecation_header(request: Request, call_next):
    response = await call_next(request)
    if not request.url.path.startswith("/api/") and request.url.path not in ("/", "/health", "/docs", "/openapi.json", "/redoc"):
        response.headers["X-Deprecated"] = "true"
        response.headers["X-Use-Instead"] = f"/api/v1{request.url.path}"
    return response


# ─── Health (2D) ────────────────────────────────────────────────────────────
@app.get("/health")
@app.get("/api/v1/health")
def health(db: Session = Depends(get_db)):
    db_status = "connected"
    officials_count = None
    feed_cards_count = None
    try:
        officials_count = db.execute(text("SELECT COUNT(*) FROM elected_officials")).scalar()
        feed_cards_count = db.execute(text("SELECT COUNT(*) FROM feed_cards")).scalar()
    except Exception as e:
        db_status = f"error: {str(e)[:120]}"
    return {
        "status": "healthy" if db_status == "connected" else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": db_status,
        "version": "1.0.0",
        "env": ENV,
        "officials_count": officials_count,
        "feed_cards_count": feed_cards_count,
    }


@app.get("/health/detailed")
@app.get("/api/v1/health/detailed")
def health_detailed(
    x_internal_key: str | None = Header(default=None, alias="X-Internal-Key"),
    db: Session = Depends(get_db),
):
    if not INTERNAL_KEY or x_internal_key != INTERNAL_KEY:
        raise HTTPException(status_code=403, detail="X-Internal-Key required")

    timings: dict[str, float] = {}
    counts: dict[str, int] = {}
    import time as _t
    for name, sql in [
        ("officials",          "SELECT COUNT(*) FROM elected_officials"),
        ("feed_cards",         "SELECT COUNT(*) FROM feed_cards"),
        ("legislative_activity", "SELECT COUNT(*) FROM legislative_activity"),
        ("constituent_votes",  "SELECT COUNT(*) FROM constituent_votes"),
        ("typology_results",   "SELECT COUNT(*) FROM typology_results"),
    ]:
        t0 = _t.perf_counter()
        try:
            counts[name] = int(db.execute(text(sql)).scalar() or 0)
        except Exception:
            counts[name] = -1
        timings[name + "_ms"] = round((_t.perf_counter() - t0) * 1000, 1)

    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "env": ENV,
        "counts": counts,
        "query_ms": timings,
    }


# ─── Error log ingestion (2D) ───────────────────────────────────────────────
class _ErrorLogIn(BaseModel):
    error_type: str | None = Field(None, max_length=120)
    message: str = Field(..., max_length=4000)
    stack: str | None = Field(None, max_length=20000)
    user_id: str | None = Field(None, max_length=120)
    url: str | None = Field(None, max_length=500)


@app.post("/errors/log")
@app.post("/api/v1/errors/log")
def log_error(payload: _ErrorLogIn, db: Session = Depends(get_db)):
    db.execute(
        text(
            """
            INSERT INTO error_logs (error_type, message, stack, user_id, url, created_at)
            VALUES (:t, :m, :s, :u, :url, NOW())
            """
        ),
        {
            "t": (payload.error_type or "")[:120] or None,
            "m": payload.message[:4000],
            "s": (payload.stack or "")[:20000] or None,
            "u": (payload.user_id or "")[:120] or None,
            "url": (payload.url or "")[:500] or None,
        },
    )
    db.commit()
    return {"logged": True}


# ─── Analytics events (2F) ──────────────────────────────────────────────────
class _AnalyticsEventIn(BaseModel):
    event_type: str = Field(..., min_length=1, max_length=80)
    user_id: str | None = Field(None, max_length=120)
    properties: dict | None = None


@app.post("/analytics/event")
@app.post("/api/v1/analytics/event")
def track_event(payload: _AnalyticsEventIn, db: Session = Depends(get_db)):
    import json
    db.execute(
        text(
            """
            INSERT INTO analytics_events (event_type, user_id, properties, created_at)
            VALUES (:t, :u, CAST(:p AS JSONB), NOW())
            """
        ),
        {
            "t": payload.event_type[:80],
            "u": (payload.user_id or "")[:120] or None,
            "p": json.dumps(payload.properties or {}),
        },
    )
    db.commit()
    return {"tracked": True}


# ─── Waitlist (2G) ──────────────────────────────────────────────────────────
class _WaitlistIn(BaseModel):
    email: EmailStr
    zip_code: str | None = Field(None, max_length=10)
    source: str | None = Field("organic", max_length=40)


@app.post("/waitlist")
@app.post("/api/v1/waitlist")
def waitlist_signup(payload: _WaitlistIn, db: Session = Depends(get_db)):
    db.execute(
        text(
            """
            INSERT INTO waitlist (email, zip_code, source, created_at)
            VALUES (:e, :z, :s, NOW())
            ON CONFLICT (email) DO UPDATE
              SET zip_code = COALESCE(EXCLUDED.zip_code, waitlist.zip_code),
                  source   = COALESCE(EXCLUDED.source,   waitlist.source)
            """
        ),
        {
            "e": payload.email,
            "z": (payload.zip_code or "")[:10] or None,
            "s": (payload.source or "organic")[:40],
        },
    )
    db.commit()
    pos = db.execute(text("SELECT COUNT(*) FROM waitlist")).scalar() or 0
    return {"success": True, "position": int(pos)}


# ─── Root ───────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"name": "PolitiScore API", "version": "1.0.0", "docs": "/docs", "health": "/health"}
