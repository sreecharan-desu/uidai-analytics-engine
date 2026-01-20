from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import RedirectResponse
import os
from typing import Optional

router = APIRouter()

# Dataset Mapping
DATASET_MAP = {
    "biometric": "biometric_full.csv",
    "enrollment": "enrollment_full.csv",
    "enrolment": "enrollment_full.csv", # User might use either spelling
    "demographic": "demographic_full.csv",
    "master": "master_dataset_final.csv"
}

@router.get("/{dataset_name}")
async def get_dataset(dataset_name: str):
    """
    Redirects to the latest version of the requested dataset stored in GitHub Releases.
    
    Supports:
    - biometric
    - enrollment
    - demographic
    - master
    
    (Extensions .csv are optional in the request path)
    """
    # Normalize input (remove .csv extension if present, lowercase)
    clean_name = dataset_name.lower().replace(".csv", "")
    
    if clean_name not in DATASET_MAP:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found. Available: {list(DATASET_MAP.keys())}")
    
    file_name = DATASET_MAP[clean_name]
    
    repo = "sreecharan-desu/uidai-analytics-engine"
    # Construct GitHub Release Asset URL
    # Format: https://github.com/{owner}/{repo}/releases/download/{tag}/{filename}
    url = f"https://github.com/{repo}/releases/download/dataset-latest/{file_name}"
    
    # 307 Temporary Redirect to allow for future changes in storage
    return RedirectResponse(url=url)
