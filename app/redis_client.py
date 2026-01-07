"""
Redis layer for Aether.
aether:jobs:pending  → Sorted Set  score=run_at_unix, member=job_id
aether:jobs:claimed  → Sorted Set  score=claimed_at_unix, member=job_id
"""

from __future__ import annotations
import redis.asyncio as aioredis
from typing import List, Optional
from app.config import get_settings
from app.logger import get_logger

logger = get_logger(__name__)

_redis: Optional[aioredis.Redis] = None

PENDING_KEY  = "aether:jobs:pending"
CLAIMED_KEY  = "aether:jobs:claimed"
HEARTBEAT_NS = "aether:heartbeat:"


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        settings = get_settings()
        _redis = aioredis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    return _redis


async def close_redis():
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None


async def ping_redis() -> bool:
    try:
        r = await get_redis()
        return await r.ping()
    except Exception:
        return False


async def enqueue_job(job_id: str, run_at_unix: float, priority: int):
    r = await get_redis()
    await r.zadd(PENDING_KEY, {job_id: run_at_unix})
    logger.info("job.enqueued", extra={"job_id": job_id})


async def remove_job(job_id: str):
    r = await get_redis()
    await r.zrem(PENDING_KEY, job_id)
    await r.zrem(CLAIMED_KEY, job_id)


async def release_claimed_job(job_id: str):
    r = await get_redis()
    await r.zrem(CLAIMED_KEY, job_id)


async def requeue_job(job_id: str, run_at_unix: float, priority: int):
    await release_claimed_job(job_id)
    await enqueue_job(job_id, run_at_unix, priority)


async def pending_count() -> int:
    r = await get_redis()
    return await r.zcard(PENDING_KEY)


async def claimed_count() -> int:
    r = await get_redis()
    return await r.zcard(CLAIMED_KEY)


async def set_heartbeat(worker_id: str, ttl_seconds: int):
    r = await get_redis()
    await r.set(HEARTBEAT_NS + worker_id, "1", ex=ttl_seconds)


async def get_alive_workers() -> List[str]:
    r = await get_redis()
    keys = await r.keys(HEARTBEAT_NS + "*")
    return [k.replace(HEARTBEAT_NS, "") for k in keys]
