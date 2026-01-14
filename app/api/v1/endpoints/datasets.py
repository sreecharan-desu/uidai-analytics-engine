from fastapi import APIRouter, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse
import re
from app.dependencies import validate_api_key

router = APIRouter()

GITHUB_REPO = 'sreecharan-desu/uidai-analytics-engine'
RELEASE_TAG = 'dataset-latest'

@router.get("/{dataset}")
async def get_dataset_redirect(dataset: str, year: str = None, api_key: str = Depends(validate_api_key)):
    valid_datasets = ['biometric', 'enrolment', 'demographic']
    
    if dataset not in valid_datasets:
        return Response(
            content='{"error": "Dataset not found. Available: biometric, enrolment, demographic"}', 
            status_code=404, 
            media_type="application/json"
        )
    
    file_name = ''
    if year:
        if not re.match(r'^\d{4}$', str(year)):
            return Response(
                content='{"error": "Invalid year format. Use YYYY."}',
                status_code=400,
                media_type="application/json"
            )
        file_name = f"{dataset}_{year}.csv"
    else:
        file_name = f"{dataset}_full.csv"
        
    download_url = f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/{file_name}"
    
    return RedirectResponse(download_url)
