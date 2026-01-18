
from fastapi import APIRouter, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse, FileResponse
import pandas as pd
import io
import os
from app.dependencies import validate_api_key

router = APIRouter()

from app.services.integration_service import get_master_partitions

@router.get("/powerbi", dependencies=[Depends(validate_api_key)])
async def get_powerbi_master_data():
    """
    Returns the list of master integrated CSV partition URLs for PowerBI.
    The client should download and combine these parts.
    """
    try:
        urls = get_master_partitions()
        if not urls:
             # Fallback: if partitions aren't ready, redirect to the old full path (might 404)
             return RedirectResponse(url="https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/aadhaar_powerbi_master.csv")
        return {"partitions": urls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
