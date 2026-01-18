
from fastapi import APIRouter, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse, FileResponse
from app.services.integration_service import get_integrated_data
import pandas as pd
import io
import os
from app.dependencies import validate_api_key

router = APIRouter()

@router.get("/powerbi", dependencies=[Depends(validate_api_key)])
async def get_powerbi_master_data(format: str = 'csv'):
    """
    Returns the master integrated dataset for PowerBI.
    
    - **CSV**: Redirects to the statically hosted GitHub Release file for maximum speed (CDN).
    - **JSON**: Fetches/Caches the dataset via backend (Warning: Large Payload).
    """
    try:
        # PBI Optimization: Direct CDN Redirect for CSV
        if format.lower() == 'csv':
            return RedirectResponse(url="https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/aadhaar_powerbi_master.csv")

        # JSON Optimization
        if format.lower() == 'json':
            # Check for local pre-computed JSON (Fastest for local)
            json_path = os.path.join(os.getcwd(), 'public', 'datasets', 'aadhaar_powerbi_master.json')
            if os.path.exists(json_path):
                 # Serve local file directly without parsing (Stream it)
                 return FileResponse(json_path, media_type="application/json", filename="aadhaar_powerbi_master.json")
            
            # Fallback: Redirect to Gzipped JSON on GitHub (Fastest for Remote)
            # PowerBI/Clients must handle GZIP. If they don't, we might need a stream-decompress proxy,
            # but usually, standard clients handle .gz transparently or we can serve the raw URL if needed.
            # Here we redirect to the raw .json.gz to save bandwidth.
            return RedirectResponse(url="https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/aadhaar_powerbi_master.json.gz")

        # Fallback (Should typically be covered by redirect)
        df = get_integrated_data()
        return Response(
             content=df.to_json(orient='records', date_format='iso'),
             media_type="application/json"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
