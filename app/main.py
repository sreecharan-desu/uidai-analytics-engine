from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

from app.utils.logger import get_logger
from app.api.v1.api import api_router
from app.dependencies import validate_api_key

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
         "frame-src 'self' https://app.powerbi.com;"
    )
    return response

# Routes
app.include_router(api_router, prefix="/api")

@app.get("/", response_class=HTMLResponse)
def read_root():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>UIDAI Ecosystem - Intelligence Hub</title>
        <meta property="og:type" content="website">
        <meta property="og:title" content="UIDAI Ecosystem - Intelligence Hub">
        <meta property="og:description" content="Policy-ready analysis of Aadhaar enrolment and update trends across India.">
        <meta property="og:image" content="https://uidai.sreecharandesu.in/og-image.png">
        <meta name="twitter:card" content="summary_large_image">
        <meta http-equiv="refresh" content="0; url=/dashboard">
        <script>window.location.href = "/dashboard";</script>
    </head>
    <body style="background: #000;">
    </body>
    </html>
    """

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PUBLIC_DIR = os.path.join(BASE_DIR, "public")

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
    return RedirectResponse(url="/docs.html")

# Mount Public folder for /datasets/ downloads or other assets
if os.path.exists("public"):
    app.mount("/", StaticFiles(directory="public", html=True), name="public")
else:
    logger.warning("Public directory not found. Static files will not be served via FastAPI mount.")
