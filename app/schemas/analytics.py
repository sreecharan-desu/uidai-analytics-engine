from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List

class AnalyticsData(BaseModel):
    total_updates: int
    by_state: Dict[str, int]
    by_age_group: Dict[str, int]
    by_month: Dict[str, int]
    by_district: Dict[str, Dict[str, int]]
    state_breakdown: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

class AnalyticsResponse(BaseModel):
    dataset: str
    year: str
    generated_at: str
    data: AnalyticsData

class InsightQuery(BaseModel):
    dataset: str = Field(..., description="Dataset name: 'enrolment', 'biometric', 'demographic'")
    filters: Dict[str, Any] = Field(default_factory=dict, description="Key-value pairs to filter data")
    limit: int = Field(50, ge=1, le=1000)
    page: int = Field(1, ge=1)
