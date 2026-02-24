from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.auth import router as auth_router
from app.routes.users import router as users_router
from app.routes.officials import router as officials_router
from app.routes.ai import router as ai_router
from app.routes.civic import router as civic_router
app = FastAPI(
    title="PolitiCard API",
    description="Civic engagement platform backend",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(officials_router)
app.include_router(ai_router)
app.include_router(civic_router)