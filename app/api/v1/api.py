from fastapi import APIRouter
from app.api.v1.endpoints import integration, datasets

api_router = APIRouter()

api_router.include_router(integration.router, prefix="/integration", tags=["powerbi-integration"])
api_router.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
