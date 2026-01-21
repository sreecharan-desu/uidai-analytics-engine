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
    
    async with httpx.AsyncClient() as client:
        # 1. Get Release Assets to find ID
        api_url = f"https://api.github.com/repos/{STORAGE_REPO}/releases/tags/{tag}"
        resp = await client.get(api_url, headers=headers)
        
        if resp.status_code != 200:
            print(f"GitHub API Error: {resp.text}")
            raise HTTPException(status_code=404, detail="Release not found.")
            
        assets = resp.json().get("assets", [])
        asset_id = None
        for asset in assets:
            if asset["name"] == filename:
                asset_id = asset["id"]
                break
                
        if not asset_id:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found in release '{tag}'.")

        # 2. Stream Asset
        asset_url = f"https://api.github.com/repos/{STORAGE_REPO}/releases/assets/{asset_id}"
        headers["Accept"] = "application/octet-stream"
        
        # We need a new client/request for the stream that stays open
        # But StreamingResponse expects an async generator.
        # We need to be careful: httpx stream context manager closes when we exit the block.
        # We need to yield from the stream.
        
        req = client.build_request("GET", asset_url, headers=headers)
        r = await client.send(req, stream=True)
        r.raise_for_status()
        
        async def async_generator():
            async for chunk in r.aiter_bytes():
                yield chunk
            await r.aclose()

    return StreamingResponse(async_generator(), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

@router.get("/raw/{dataset_name}", dependencies=[Depends(validate_api_key)])
async def get_raw_dataset(dataset_name: str):
    """
    Streams the RAW (unprocessed) version of the requested dataset from Private GitHub Release.
    """
    clean_name = dataset_name.lower().replace(".csv", "")
    
    if clean_name not in RAW_DATASET_MAP:
        raise HTTPException(status_code=404, detail=f"Raw dataset '{dataset_name}' not found.")
    
    return stream_from_github(RAW_DATASET_MAP[clean_name], tag="dataset-raw")

@router.get("/{dataset_name}", dependencies=[Depends(validate_api_key)])
async def get_processed_dataset(dataset_name: str):
    """
    Streams the LATEST PROCESSED version of the requested dataset from Private GitHub Release.
    """
    clean_name = dataset_name.lower().replace(".csv", "")
    
    if clean_name not in PROCESSED_DATASET_MAP:
        raise HTTPException(status_code=404, detail=f"Processed dataset '{dataset_name}' not found.")
    
    return stream_from_github(PROCESSED_DATASET_MAP[clean_name], tag="dataset-latest")


