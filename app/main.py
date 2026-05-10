from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.auth import router as auth_router
from app.routes.users import router as users_router
from app.routes.officials import router as officials_router, metrics_router, feedback_router
from app.routes.ai import router as ai_router
from app.routes.civic import router as civic_router
from app.routes.feed import router as feed_router, constituent_router, engagement_router, official_alignment_router
from app.routes.typology import router as typology_router

app = FastAPI(
    title="PolitiScore API",
    description="Civic engagement platform backend",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(officials_router)
app.include_router(metrics_router)
app.include_router(ai_router)
app.include_router(civic_router)
app.include_router(feed_router)
app.include_router(constituent_router)
app.include_router(engagement_router)
app.include_router(official_alignment_router)
app.include_router(typology_router)
app.include_router(feedback_router)
