"""
Exactly-once execution test harness.
Proves: 1000 jobs x 5 workers = 0 double-executions.
"""

import asyncio
import time
import pytest
from collections import Counter
import redis.asyncio as aioredis
from app.redis_client import PENDING_KEY, CLAIMED_KEY, CLAIM_SCRIPT

REDIS_URL   = "redis://localhost:6379/1"
NUM_JOBS    = 1000
NUM_WORKERS = 5


@pytest.fixture
async def clean_redis():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await r.delete(PENDING_KEY, CLAIMED_KEY)
    yield r
    await r.delete(PENDING_KEY, CLAIMED_KEY)
    await r.aclose()


async def _worker_loop(worker_id, executed, stop_event):
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        while not stop_event.is_set():
            now    = time.time()
            script = r.register_script(CLAIM_SCRIPT)
            job_ids = await script(
                keys=[PENDING_KEY, CLAIMED_KEY],
                args=[str(now * 10 + 9), "10"],
            )
            for job_id in (job_ids or []):
                executed.append((worker_id, job_id))
                await asyncio.sleep(0)
                await r.zrem(CLAIMED_KEY, job_id)
            if not job_ids:
                await asyncio.sleep(0.001)
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_exactly_once_single_run(clean_redis):
    r = clean_redis
    past = time.time() - 1
    pipe = r.pipeline()
    for i in range(NUM_JOBS):
        pipe.zadd(PENDING_KEY, {f"job-{i:04d}": past * 10 + 2})
    await pipe.execute()

    executed   = []
    stop_event = asyncio.Event()
    workers    = [asyncio.create_task(_worker_loop(f"w{w}", executed, stop_event)) for w in range(NUM_WORKERS)]

    deadline = time.time() + 10
    while time.time() < deadline:
        if await r.zcard(PENDING_KEY) == 0 and len(executed) >= NUM_JOBS:
            break
        await asyncio.sleep(0.01)

    stop_event.set()
    await asyncio.gather(*workers, return_exceptions=True)

    counts  = Counter(jid for _, jid in executed)
    doubles = {jid: cnt for jid, cnt in counts.items() if cnt > 1}
    missed  = NUM_JOBS - len(set(jid for _, jid in executed))

    print(f"\nJobs: {len(executed)} | Doubles: {len(doubles)} | Missed: {missed}")
    assert len(doubles) == 0, f"Double-executions: {doubles}"
    assert missed == 0


@pytest.mark.asyncio
async def test_priority_ordering(clean_redis):
    r = clean_redis
    now = time.time() - 1
    await r.zadd(PENDING_KEY, {"low-job": now * 10 + 3})
    await r.zadd(PENDING_KEY, {"critical-job": now * 10 + 0})
    await r.zadd(PENDING_KEY, {"normal-job": now * 10 + 2})

    script  = r.register_script(CLAIM_SCRIPT)
    claimed = await script(keys=[PENDING_KEY, CLAIMED_KEY], args=[str(now * 10 + 9), "3"])
    assert claimed[0] == "critical-job"
    assert claimed[1] == "normal-job"
    assert claimed[2] == "low-job"
    print(f"\nPriority order verified: {claimed}")
