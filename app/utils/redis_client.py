from upstash_redis.asyncio import Redis
from app.config import config

redis_client = Redis(url=config.UPSTASH_REDIS_REST_URL, token=config.UPSTASH_REDIS_REST_TOKEN)
