import httpx
import json
from app.core.config import settings
from app.utils.logger import get_logger
from app.utils.redis_client import redis_client

logger = get_logger()

async def fetch_insights(dataset: str, filters: dict, limit: int, page: int):
    # Hardcoded fields
    default_select = []
    ds_lower = dataset.lower()
    
    if ds_lower == 'enrolment':
        default_select = ['date', 'state', 'district', 'pincode', 'age_0_5', 'age_5_17', 'age_18_greater']
    elif ds_lower == 'demographic':
        default_select = ['date', 'state', 'district', 'pincode', 'demo_age_5_17', 'demo_age_17_']
    elif ds_lower == 'biometric':
        default_select = ['date', 'state', 'district', 'pincode', 'bio_age_5_17', 'bio_age_17_']
    
    # Cache Key Construction (Stable)
    # Sort keys for stability
    stable_filters = {k: filters[k] for k in sorted(filters.keys())}
    stable_dynamic_key = json.dumps(stable_filters, separators=(',', ':'))
    
    select_fields_id = ",".join(sorted(default_select)) if default_select else 'all'
    cache_key = f"insight:{dataset}:{page}:{limit}:{select_fields_id}:{stable_dynamic_key}"
    
    # 1. Check Redis (L2)
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            if isinstance(cached, str):
                cached = json.loads(cached)
            logger.info(f"Serving from Redis Cache: {cache_key}")
            cached['meta']['source'] = 'cache'
            cached['meta']['from_cache'] = True
            return cached
    except Exception as e:
        logger.warning(f"Redis Cache Error: {str(e)}")
        
    # Map Resource ID
    resource_id = settings.RESOURCES.get(ds_lower)
    if not resource_id:
        raise ValueError("Invalid dataset type.")
        
    offset = (page - 1) * limit
    
    # 2. External API
    api_url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": settings.DATA_GOV_API_KEY,
        "format": "json",
        "limit": limit,
        "offset": offset
    }
    
    for k, v in filters.items():
        val = v
        # Normalization for State/District to Title Case
        if k.lower() in ['state', 'district'] and isinstance(val, str):
            val = val.title() # Python title() is slightly different but close enough for this
        params[f"filters[{k}]"] = val
        
    logger.info(f"Fetching from Data.gov.in: {dataset} params={params}")
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(api_url, params=params)
        if resp.status_code != 200:
             raise ValueError(f"External API Error: {resp.status_code}")
        
        data = resp.json()
        
    if data.get("status") != "ok":
        raise ValueError(data.get("message") or "Error fetching from Source API")
        
    fields = [f['id'] for f in data.get('field', [])]
    records = data.get('records', [])
    
    # Internal Field Selection
    if default_select:
        new_records = []
        for r in records:
            filtered = {f: r[f] for f in default_select if f in r}
            new_records.append(filtered)
        records = new_records
        
    response_payload = {
        "meta": {
            "dataset": dataset,
            "total": data.get("total"),
            "page": page,
            "limit": limit,
            "from_cache": False,
            "fields": fields,
            "source": "api"
        },
        "data": records
    }
    
    # 3. Store in Redis
    try:
        # Upstash-redis-py: set(name, value, ex)
        # It handles dict serialization? Usually better to serialize.
        # But let's see library docs. It usually accepts string.
        await redis_client.set(cache_key, json.dumps(response_payload), ex=86400)
    except Exception as e:
        logger.warning(f"Failed to set Redis cache: {str(e)}")
        
    return response_payload
