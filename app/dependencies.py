from fastapi import Request, HTTPException, status
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger()

async def validate_api_key(request: Request):
    api_key = request.headers.get("x-api-key") or request.query_params.get("api_key")
    
    if not api_key or api_key != settings.CLIENT_API_KEY:
        received = api_key if api_key else ""
        logger.warning(f"Unauthorized API access attempt. Match: {api_key == settings.CLIENT_API_KEY}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Unauthorized: Invalid or missing API Key"
        )
    return api_key
