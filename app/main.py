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
async def kill_switch_middleware(request: Request, call_next):
    # Check if Kill Switch is active via Env Var
    if os.getenv("KILL_SWITCH") == "true":
        return JSONResponse(
            status_code=503,
            content={
                "error": "Service Temporarily Suspended",
                "message": "The project has been paused to prevent excessive usage. Please contact the administrator."
            }
        )
    return await call_next(request)

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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PUBLIC_DIR = os.path.join(BASE_DIR, "public")

from fastapi.responses import RedirectResponse

@app.get("/dashboard")
def dashboard():
    # On Vercel, static files in 'public' are often not available to the function 
    # but are served by Vercel's CDN. 
    # Try finding it locally first (for local dev).
    path = os.path.join(PUBLIC_DIR, "dashboard.html")
    if not os.path.exists(path):
         path = os.path.join("public", "dashboard.html")
    
    if os.path.exists(path):
         return FileResponse(path)
    
    # Fallback: Redirect to the static file hosted by Vercel
    return RedirectResponse(url="/dashboard.html")

@app.get("/docs")
def custom_docs():
    path = os.path.join(PUBLIC_DIR, "docs.html")
    if not os.path.exists(path):
         path = os.path.join("public", "docs.html")
    if os.path.exists(path):
        return FileResponse(path)
    return JSONResponse(status_code=404, content={"error": "Docs Not Found"})

@app.on_event("startup")
async def startup_event():
    import asyncio
    asyncio.create_task(prewarm_cache())

# Mount Public folder for /datasets/ downloads or other assets
if os.path.exists("public"):
    app.mount("/", StaticFiles(directory="public", html=True), name="public")
else:
    logger.warning("Public directory not found. Static files will not be served via FastAPI mount.")
