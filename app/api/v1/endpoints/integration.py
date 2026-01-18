
from fastapi import APIRouter, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse, FileResponse
from app.services.integration_service import get_integrated_data
import pandas as pd
import io
import os
from app.dependencies import validate_api_key

router = APIRouter()

@router.get("/powerbi", dependencies=[Depends(validate_api_key)])
async def get_powerbi_master_data():
    """
    Returns the master integrated dataset for PowerBI.
    Redirects to the statically hosted GitHub Release file for maximum speed (CDN).
    """
    try:
        return RedirectResponse(url="https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/aadhaar_powerbi_master.csv")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
