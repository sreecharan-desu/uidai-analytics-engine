from upstash_redis.asyncio import Redis
from app.core.config import settings

redis_client = Redis(url=settings.UPSTASH_REDIS_REST_URL, token=settings.UPSTASH_REDIS_REST_TOKEN)
