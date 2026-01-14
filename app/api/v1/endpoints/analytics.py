from fastapi import APIRouter, Response, HTTPException, Depends
from fastapi.responses import JSONResponse
from app.services.aggregation_service import get_aggregate_insights
from app.utils.logger import get_logger
from app.dependencies import validate_api_key
from datetime import datetime
import csv
import io

logger = get_logger()
router = APIRouter()

@router.get("/{dataset}")
async def get_analytics(dataset: str, year: str = None, format: str = None, view: str = 'state', api_key: str = Depends(validate_api_key)):
    try:
        valid_datasets = ['biometric', 'enrolment', 'demographic']
        if dataset not in valid_datasets:
            return Response(content='{"error": "Invalid dataset"}', status_code=400, media_type="application/json")
            
        data = await get_aggregate_insights(dataset, year)
        
        if format == 'csv':
            # Flatten Logic from Node
            rows = []
            if view == 'state':
                 for state, count in data['by_state'].items():
                     rows.append({"State": state, "Updates": count})
            elif view == 'age':
                 for age, count in data['by_age_group'].items():
                     rows.append({"AgeGroup": age, "Updates": count})
                     
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=rows[0].keys() if rows else [])
            writer.writeheader()
            writer.writerows(rows)
            
            headers = {
                'Content-Disposition': f'attachment; filename="{dataset}_{view}_{year or "all"}.csv"'
            }
            return Response(content=output.getvalue(), media_type="text/csv", headers=headers)
            
        headers = {
            "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=600",
            "X-Generated-At": datetime.now().isoformat()
        }
        
        return JSONResponse(
            content={
                "dataset": dataset,
                "year": year or 'all',
                "generated_at": datetime.now().isoformat(),
                "data": data
            },
            headers=headers
        )
        
    except ValueError as e:
        # e.g. Dataset file not found
        return Response(content=f'{{"error": "{str(e)}"}}', status_code=500, media_type="application/json")
    except Exception as e:
        logger.error(f"Analytics Error: {e}")
        return Response(content=f'{{"error": "{str(e)}"}}', status_code=500, media_type="application/json")
