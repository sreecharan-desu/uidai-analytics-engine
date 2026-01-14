from fastapi import APIRouter
from app.api.v1.endpoints import analytics, datasets, insights

api_router = APIRouter()

api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
api_router.include_router(insights.router, prefix="/insights", tags=["insights"])
api_router.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
