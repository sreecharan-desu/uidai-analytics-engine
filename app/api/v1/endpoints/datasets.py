from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import RedirectResponse
import os
from typing import Optional
from app.dependencies import validate_api_key

router = APIRouter()

# Dataset Mapping
DATASET_MAP = {
    "biometric": "biometric_full.csv",
    "enrollment": "enrollment_full.csv",
    "enrolment": "enrollment_full.csv", # User might use either spelling
    "demographic": "demographic_full.csv",
    "master": "master_dataset_final.csv"
}

@router.get("/{dataset_name}", dependencies=[Depends(validate_api_key)])
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
    
    # Cloudflare R2 / S3 Configuration
    r2_account_id = os.getenv("R2_ACCOUNT_ID")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_bucket_name = os.getenv("R2_BUCKET_NAME")
    
    if not all([r2_account_id, r2_access_key, r2_secret_key, r2_bucket_name]):
         raise HTTPException(status_code=500, detail="Server misconfiguration: R2 credentials missing.")

    try:
        import boto3
        from botocore.config import Config
        
        # Initialize S3 Client for R2
        s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{r2_account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=r2_access_key,
            aws_secret_access_key=r2_secret_key,
            config=Config(signature_version='s3v4')
        )
        
        # Generate Presigned URL (Valid for 1 hour)
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': r2_bucket_name, 'Key': file_name},
            ExpiresIn=3600
        )
        
        # Redirect to the secure, temporary R2 link
        return RedirectResponse(url=url)
        
    except Exception as e:
        print(f"Error generating R2 link: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate secure download link.")
