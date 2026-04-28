"""
UBID Platform — FastAPI application entry point.
Serves the API and static reviewer UI.
"""
import warnings
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from fastapi import Depends

from src.api.auth import Principal, ROLE_VIEWER, require_role
from src.api.routers import analytics, review, ubid
from src.config import settings
from src.database.session import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if not settings.api_keys:
        warnings.warn(
            "API_KEYS is empty — all API requests will be rejected with 503. "
            "Set API_KEYS in .env to enable access.",
            RuntimeWarning,
            stacklevel=2,
        )
    if "*" in settings.cors_origins:
        raise RuntimeError(
            "CORS_ORIGINS must not contain '*' — set an explicit allowlist."
        )
    yield


app = FastAPI(
    title="UBID Platform",
    description="Unified Business Identifier — Karnataka Commerce & Industry",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key"],
    allow_credentials=False,
)

app.include_router(ubid.router, prefix="/api/v1")
app.include_router(review.router, prefix="/api/v1")
app.include_router(analytics.router, prefix="/api/v1")

# Serve the reviewer UI
_ui_dir = Path(__file__).parent.parent.parent / "ui"
if _ui_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_ui_dir)), name="static")

    @app.get("/", include_in_schema=False)
    def serve_ui():
        return FileResponse(str(_ui_dir / "index.html"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/me")
def whoami(principal: Principal = Depends(require_role(ROLE_VIEWER))):
    return {"name": principal.name, "role": principal.role}
