from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import RedirectResponse
from app.services.integration_service import get_master_partitions
from app.dependencies import validate_api_key

router = APIRouter()

@router.get("/powerbi", dependencies=[Depends(validate_api_key)])
async def get_powerbi_master_data():
    """
    Returns the list of master integrated CSV partition URLs for PowerBI.
    The client (PowerBI) should download and combine these parts.
    """
    try:
        urls = get_master_partitions()
        if not urls:
             # Fallback: if partitions aren't ready, redirect to the old full path (which may 404)
             return RedirectResponse(url="https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/aadhaar_powerbi_master.csv")
        
        return {"partitions": urls}
    except Exception as e:
        # Log error to Vercel console
        print(f"Integration API Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
