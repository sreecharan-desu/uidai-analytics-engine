from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
import os
import httpx

from app.dependencies import validate_api_key

router = APIRouter()

# Config
STORAGE_REPO = "sreecharan-desu/uidai-data-storage"
GH_PAT = os.getenv("GH_PAT") or os.getenv("GH_TOKEN")

# Dataset Maps
PROCESSED_DATASET_MAP = {
    "biometric": "biometric_full.csv",
    "enrollment": "enrollment_full.csv",
    "enrolment": "enrollment_full.csv", 
    "demographic": "demographic_full.csv",
    "master": "master_dataset_final.csv"
}

RAW_DATASET_MAP = {
    "biometric": "biometric.csv",
    "enrollment": "enrolment.csv",
    "enrolment": "enrolment.csv",
    "demographic": "demographic.csv"
}

async def stream_from_github(filename: str, tag: str):
    """Streams a file from a private GitHub release using async httpx."""
    if not GH_PAT:
        raise HTTPException(status_code=500, detail="Server configuration error: Missing GitHub Token.")

    headers = {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    asset_url = None
    
    # 1. Get Asset ID (Fast one-off request)
    async with httpx.AsyncClient() as client:
        # Get Release Assets
        api_url = f"https://api.github.com/repos/{STORAGE_REPO}/releases/tags/{tag}"
        resp = await client.get(api_url, headers=headers)
        
        if resp.status_code != 200:
            print(f"GitHub API Error: {resp.text}")
            raise HTTPException(status_code=404, detail="Release not found.")
            
        assets = resp.json().get("assets", [])
        for asset in assets:
            if asset["name"] == filename:
                asset_url = asset["url"] # This is the API url for the asset
                break
                
        if not asset_url:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found in release '{tag}'.")

    # 2. Define Stream Generator (Manages its own client)
    async def iterfile():
        # Use a fresh client for the stream to ensure context is kept open during iteration
        async with httpx.AsyncClient() as client:
             # We need to set Accept header for binary stream
            stream_headers = headers.copy()
            stream_headers["Accept"] = "application/octet-stream"
            
            # Using client.stream context manager ensures proper cleanup
            req = client.build_request("GET", asset_url, headers=stream_headers)
            r = await client.send(req, stream=True)
            r.raise_for_status()
            
            async for chunk in r.aiter_bytes():
                yield chunk
            
            await r.aclose()

    return StreamingResponse(iterfile(), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

@router.get("/raw/{dataset_name}", dependencies=[Depends(validate_api_key)])
async def get_raw_dataset(dataset_name: str):
    """
    Streams the RAW (unprocessed) version of the requested dataset from Private GitHub Release.
    """
    clean_name = dataset_name.lower().replace(".csv", "")
    
    if clean_name not in RAW_DATASET_MAP:
        raise HTTPException(status_code=404, detail=f"Raw dataset '{dataset_name}' not found.")
    
    return await stream_from_github(RAW_DATASET_MAP[clean_name], tag="dataset-raw")

@router.get("/{dataset_name}", dependencies=[Depends(validate_api_key)])
async def get_processed_dataset(dataset_name: str):
    """
    Streams the LATEST PROCESSED version of the requested dataset from Private GitHub Release.
    """
    clean_name = dataset_name.lower().replace(".csv", "")
    
    if clean_name not in PROCESSED_DATASET_MAP:
        raise HTTPException(status_code=404, detail=f"Processed dataset '{dataset_name}' not found.")
    
    return await stream_from_github(PROCESSED_DATASET_MAP[clean_name], tag="dataset-latest")


