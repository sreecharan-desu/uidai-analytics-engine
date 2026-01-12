
import asyncio
import os
import sys

# Ensure app imports work
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.redis_client import redis_client

async def flush_db():
    print("Flushing Redis DB...")
    try:
        # Upstash Redis client might not support flushdb directly depending on lib version,
        # but typically it's .flushdb(). If not, we iterate keys.
        # Checking docs/lib: upstash-redis usually wraps HTTP API.
        # Let's try executing valid command.
        res = await redis_client.flushdb()
        print(f"Result: {res}")
    except Exception as e:
        print(f"Failed to flush: {e}")

if __name__ == "__main__":
    asyncio.run(flush_db())
