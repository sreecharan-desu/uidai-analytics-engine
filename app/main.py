from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.config import config
from app.utils.logger import get_logger
from app.routers import insights, datasets, analytics
from app.services.aggregation_service import prewarm_cache
import os

logger = get_logger()

app = FastAPI(
    title="UIDAI Insights API",
    docs_url=None, 
    redoc_url=None
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    # Basic Content security policy to allow scripts/styles
    response.headers["Content-Security-Policy"] = (
         "default-src 'self'; "
         "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
         "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdnjs.cloudflare.com; "
         "font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; "
         "img-src 'self' data: https:; "
         "connect-src 'self';"
    )
    return response

# Routes
app.include_router(insights.router, prefix="/api/insights", tags=["insights"])
app.include_router(datasets.router, prefix="/api/datasets", tags=["datasets"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["analytics"])

@app.get("/")
def read_root():
    return {
        "status": "healthy",
        "message": "UIDAI Insights API",
        "docs": "https://uidai.sreecharandesu.in/docs"
    }

@app.get("/dashboard")
def dashboard():
    return FileResponse(os.path.join("public", "dashboard.html"))

@app.get("/docs")
def custom_docs():
    return FileResponse(os.path.join("public", "docs.html"))

@app.on_event("startup")
async def startup_event():
    import asyncio
    asyncio.create_task(prewarm_cache())

# Mount Public folder for /datasets/ downloads or other assets
if os.path.exists("public"):
    app.mount("/", StaticFiles(directory="public", html=True), name="public")
else:
    logger.warning("Public directory not found. Static files will not be served via FastAPI mount.")
