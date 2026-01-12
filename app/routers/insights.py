from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from app.dependencies import validate_api_key
from app.services.insights_service import fetch_insights
from app.utils.logger import get_logger

logger = get_logger()
router = APIRouter()

class InsightsQuery(BaseModel):
    dataset: str
    filters: Optional[Dict[str, Any]] = {}
    limit: Optional[int] = 100
    page: Optional[int] = 1

@router.post("/query", dependencies=[Depends(validate_api_key)])
async def query_insights(body: InsightsQuery):
    try:
        # Sanitize inputs
        limit_num = max(1, min(body.limit or 100, 1000))
        page_num = max(1, body.page or 1)
        
        result = await fetch_insights(
            dataset=body.dataset,
            filters=body.filters or {},
            limit=limit_num,
            page=page_num
        )
        return result
        
    except ValueError as e:
        # Business Logic Error (Invalid dataset, upstream error)
        # Node returns 400 for invalid dataset, 500 for others?
        # Controller port says: "Invalid dataset type." -> 400.
        msg = str(e)
        if "Invalid dataset" in msg:
             raise HTTPException(status_code=400, detail=msg)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {msg}")
    except Exception as e:
        logger.error(f"Error processing insights query: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
