from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse, StreamingResponse
import requests
import io
import pandas as pd
from app.dependencies import validate_api_key

router = APIRouter()

@router.get("/powerbi", dependencies=[Depends(validate_api_key)])
async def get_powerbi_master_data():
    """
    Serves the pre-processed Master Dataset for PowerBI.
    
    Instead of processing 500MB+ of raw data on-the-fly (which timeouts),
    this endpoint streams the 'master_dataset_final.csv' which is 
    generated and verified by the Jupyter Notebook.
    """
    try:
        repo = "sreecharan-desu/uidai-analytics-engine"
        # URL for the pre-processed master CSV in GitHub Releases
        url = f"https://github.com/{repo}/releases/download/dataset-latest/master_dataset_final.csv"
        
        # We assume the file exists because we just uploaded it.
        # We proxy the download to let PowerBI see it as a stream from our API.
        
        # Using requests to get the stream
        r = requests.get(url, stream=True)
        r.raise_for_status()
            
        return StreamingResponse(
            io.BytesIO(r.content), 
            media_type="text/csv", 
            headers={"Content-Disposition": "attachment; filename=aadhaar_powerbi_master.csv"}
        )

    except Exception as e:
        print(f"Error serving Master CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))

