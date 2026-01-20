from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import RedirectResponse
import os
from typing import Optional
from app.dependencies import validate_api_key

router = APIRouter()

# Dataset Mapping
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
    "enrollment": "enrolment.csv", # Map enrollment -> enrolment.csv
    "enrolment": "enrolment.csv",
    "demographic": "demographic.csv"
}

def generate_r2_url(file_key: str):
    """Generates a presigned URL for a file in R2."""
    r2_account_id = os.getenv("R2_ACCOUNT_ID")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_bucket_name = os.getenv("R2_BUCKET_NAME")
    
    if not all([r2_account_id, r2_access_key, r2_secret_key, r2_bucket_name]):
         raise HTTPException(status_code=500, detail="Server misconfiguration: R2 credentials missing.")

    try:
        import boto3
        from botocore.config import Config
        
        s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{r2_account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=r2_access_key,
            aws_secret_access_key=r2_secret_key,
            config=Config(signature_version='s3v4')
        )
        
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': r2_bucket_name, 'Key': file_key},
            ExpiresIn=3600
        )
        return url
    except Exception as e:
        print(f"Error generating R2 link: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate secure download link.")

@router.get("/raw/{dataset_name}", dependencies=[Depends(validate_api_key)])
async def get_raw_dataset(dataset_name: str):
    """
    Redirects to the RAW (unprocessed) version of the requested dataset.
    """
    clean_name = dataset_name.lower().replace(".csv", "")
    
    if clean_name not in RAW_DATASET_MAP:
        raise HTTPException(status_code=404, detail=f"Raw dataset '{dataset_name}' not found. Available: {list(RAW_DATASET_MAP.keys())}")
    
    url = generate_r2_url(RAW_DATASET_MAP[clean_name])
    return RedirectResponse(url=url)

@router.get("/{dataset_name}", dependencies=[Depends(validate_api_key)])
async def get_processed_dataset(dataset_name: str):
    """
    Redirects to the LATEST PROCESSED version of the requested dataset.
    """
    clean_name = dataset_name.lower().replace(".csv", "")
    
    if clean_name not in PROCESSED_DATASET_MAP:
        raise HTTPException(status_code=404, detail=f"Processed dataset '{dataset_name}' not found. Available: {list(PROCESSED_DATASET_MAP.keys())}")
    
    url = generate_r2_url(PROCESSED_DATASET_MAP[clean_name])
    return RedirectResponse(url=url)


