"""
Redis layer for Aether.

Key design:
  aether:jobs:pending   → Sorted Set  score=run_at_unix, member=job_id
  aether:jobs:claimed   → Sorted Set  score=claimed_at_unix, member=job_id
  aether:heartbeat:{worker_id} → String  (TTL = heartbeat_interval * 3)

The CLAIM_SCRIPT is the core of exactly-once execution:
  It atomically moves a job from pending → claimed in a single Redis round-trip.
  No two workers can claim the same job because ZRANGEBYSCORE + ZREM + ZADD
  execute as one atomic unit inside a Lua script.
"""

from __future__ import annotations
import time
import redis.asyncio as aioredis
from typing import List, Optional
from app.config import get_settings
from app.logger import get_logger

logger = get_logger(__name__)

_redis: Optional[aioredis.Redis] = None

PENDING_KEY  = "aether:jobs:pending"
CLAIMED_KEY  = "aether:jobs:claimed"
HEARTBEAT_NS = "aether:heartbeat:"


# ── Lua script: atomic claim ──────────────────────────────────────────────────
# KEYS[1] = pending sorted set
# KEYS[2] = claimed sorted set
# ARGV[1] = current unix timestamp (float, as string)
# ARGV[2] = batch size
#
# Returns: list of job_ids that were successfully claimed by THIS call.
# Any concurrent caller racing on the same jobs will get an empty list for
# those jobs because ZREM is atomic — only one caller can remove a member.

CLAIM_SCRIPT = """
local pending_key = KEYS[1]
local claimed_key = KEYS[2]
local now         = tonumber(ARGV[1])
local batch       = tonumber(ARGV[2])

-- Get up to `batch` jobs whose run_at <= now
local candidates = redis.call('ZRANGEBYSCORE', pending_key, '-inf', now, 'LIMIT', 0, batch)

local claimed = {}
for _, job_id in ipairs(candidates) do
    -- Atomic remove: only succeeds for one caller
    local removed = redis.call('ZREM', pending_key, job_id)
    if removed == 1 then
        -- Add to claimed set with current timestamp as score (for timeout detection)
        redis.call('ZADD', claimed_key, now, job_id)
        table.insert(claimed, job_id)
    end
end

return claimed
"""


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        settings = get_settings()
        _redis = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
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


# ── Job queue operations ──────────────────────────────────────────────────────

async def enqueue_job(job_id: str, run_at_unix: float, priority: int):
    """
    Add job to pending sorted set.
    Score = run_at_unix * 10 + priority  (lower = runs first)
    Priority 0 (CRITICAL) runs before priority 3 (LOW) at the same timestamp.
    """
    r = await get_redis()
    score = run_at_unix * 10 + priority
    await r.zadd(PENDING_KEY, {job_id: score})
    logger.info("job.enqueued", extra={"job_id": job_id, "score": score})


async def claim_jobs(batch_size: int) -> List[str]:
    """
    Atomically claim up to batch_size jobs whose run_at <= now.
    Returns list of job_ids claimed exclusively by this worker.
    This is the exactly-once guarantee.
    """
    r = await get_redis()
    now = time.time()
    # score threshold accounts for priority offset (max priority offset = 3)
    score_threshold = now * 10 + 9

    script = r.register_script(CLAIM_SCRIPT)
    result = await script(
        keys=[PENDING_KEY, CLAIMED_KEY],
        args=[str(score_threshold), str(batch_size)],
    )
    return result or []


async def release_claimed_job(job_id: str):
    """Remove from claimed set after job completes (success or failure)."""
    r = await get_redis()
    await r.zrem(CLAIMED_KEY, job_id)


async def requeue_job(job_id: str, run_at_unix: float, priority: int):
    """Re-add to pending (for retries or recurring jobs)."""
    r = await get_redis()
    await release_claimed_job(job_id)
    await enqueue_job(job_id, run_at_unix, priority)


async def remove_job(job_id: str):
    """Remove from all sets (on cancel)."""
    r = await get_redis()
    await r.zrem(PENDING_KEY, job_id)
    await r.zrem(CLAIMED_KEY, job_id)


async def pending_count() -> int:
    r = await get_redis()
    return await r.zcard(PENDING_KEY)


async def claimed_count() -> int:
    r = await get_redis()
    return await r.zcard(CLAIMED_KEY)


# ── Worker heartbeat ──────────────────────────────────────────────────────────

async def set_heartbeat(worker_id: str, ttl_seconds: int):
    r = await get_redis()
    key = HEARTBEAT_NS + worker_id
    await r.set(key, "1", ex=ttl_seconds)


async def get_alive_workers() -> List[str]:
    r = await get_redis()
    keys = await r.keys(HEARTBEAT_NS + "*")
    return [k.replace(HEARTBEAT_NS, "") for k in keys]
