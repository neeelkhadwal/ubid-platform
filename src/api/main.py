"""
UBID Platform — FastAPI application entry point.
Serves the API and static reviewer UI.
"""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.api.routers import analytics, review, ubid
from src.database.session import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="UBID Platform",
    description="Unified Business Identifier — Karnataka Commerce & Industry",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
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
