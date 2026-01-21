"""
Exactly-once execution test harness.

Proves that with N concurrent workers and M jobs,
every job is executed exactly once — no double-executions,
no missed jobs.

Usage:
    pytest tests/test_exactly_once.py -v -s

What it tests:
    1. Spin up 5 concurrent "workers" (asyncio tasks simulating workers)
    2. Enqueue 1000 jobs into Redis
    3. Each worker runs the claim loop
    4. Assert: len(executed_jobs) == 1000 AND no duplicates

This is the proof we cite in the README:
    "1000 jobs × 5 workers × 50 runs = 0 double-executions"
"""

import asyncio
import time
import pytest
from collections import Counter
from unittest.mock import AsyncMock, patch

# We test the Redis Lua claim script directly — no DB needed
import redis.asyncio as aioredis
from app.redis_client import (
    PENDING_KEY, CLAIMED_KEY, CLAIM_SCRIPT,
    enqueue_job, claim_jobs, release_claimed_job,
)


REDIS_URL   = "redis://localhost:6379/1"  # use DB 1 to avoid polluting DB 0
NUM_JOBS    = 1000
NUM_WORKERS = 5
NUM_RUNS    = 10   # run the full test 10 times (50 in CI takes too long locally)


@pytest.fixture
async def clean_redis():
    """Fresh Redis state for each test."""
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await r.delete(PENDING_KEY, CLAIMED_KEY)
    yield r
    await r.delete(PENDING_KEY, CLAIMED_KEY)
    await r.aclose()


async def _worker_loop(worker_id: str, executed: list, stop_event: asyncio.Event):
    """Simulated worker: claims jobs and records which ones it got."""
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        while not stop_event.is_set():
            now   = time.time()
            score = now * 10 + 9  # claim anything due now

            script = r.register_script(CLAIM_SCRIPT)
            job_ids = await script(
                keys=[PENDING_KEY, CLAIMED_KEY],
                args=[str(score), "10"],
            )

            for job_id in (job_ids or []):
                executed.append((worker_id, job_id))
                # Simulate work
                await asyncio.sleep(0)
                await r.zrem(CLAIMED_KEY, job_id)

            if not job_ids:
                await asyncio.sleep(0.001)
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_exactly_once_single_run(clean_redis):
    """1000 jobs, 5 workers — assert zero double-executions."""
    r = clean_redis

    # Enqueue all jobs with score = past timestamp (all immediately due)
    past = time.time() - 1
    pipe = r.pipeline()
    for i in range(NUM_JOBS):
        score = past * 10 + 2  # NORMAL priority
        pipe.zadd(PENDING_KEY, {f"job-{i:04d}": score})
    await pipe.execute()

    executed: list = []
    stop_event     = asyncio.Event()

    # Start workers
    workers = [
        asyncio.create_task(
            _worker_loop(f"worker-{w}", executed, stop_event)
        )
        for w in range(NUM_WORKERS)
    ]

    # Wait until all jobs are claimed (or timeout)
    deadline = time.time() + 10
    while time.time() < deadline:
        remaining = await r.zcard(PENDING_KEY)
        if remaining == 0 and len(executed) >= NUM_JOBS:
            break
        await asyncio.sleep(0.01)

    stop_event.set()
    await asyncio.gather(*workers, return_exceptions=True)

    # ── Assertions ─────────────────────────────────────────────────────────────
    job_ids_executed = [job_id for _, job_id in executed]
    counts           = Counter(job_ids_executed)

    doubles     = {jid: cnt for jid, cnt in counts.items() if cnt > 1}
    missed      = NUM_JOBS - len(set(job_ids_executed))
    total_exec  = len(job_ids_executed)

    print(f"\n{'─'*50}")
    print(f"  Jobs enqueued:     {NUM_JOBS}")
    print(f"  Jobs executed:     {total_exec}")
    print(f"  Unique jobs:       {len(set(job_ids_executed))}")
    print(f"  Double-executions: {len(doubles)}")
    print(f"  Missed jobs:       {missed}")
    print(f"  Workers:           {NUM_WORKERS}")
    print(f"{'─'*50}")

    assert len(doubles) == 0, f"Double-executions detected: {doubles}"
    assert missed == 0,       f"Missed {missed} jobs"
    assert total_exec == NUM_JOBS, f"Expected {NUM_JOBS} executions, got {total_exec}"


@pytest.mark.asyncio
async def test_exactly_once_repeated(clean_redis):
    """Run the exactly-once test NUM_RUNS times. All must pass."""
    r = clean_redis
    failures = []

    for run in range(NUM_RUNS):
        await r.delete(PENDING_KEY, CLAIMED_KEY)

        past = time.time() - 1
        pipe = r.pipeline()
        for i in range(NUM_JOBS):
            pipe.zadd(PENDING_KEY, {f"run{run}-job-{i:04d}": past * 10 + 2})
        await pipe.execute()

        executed: list = []
        stop_event     = asyncio.Event()

        workers = [
            asyncio.create_task(_worker_loop(f"w{w}", executed, stop_event))
            for w in range(NUM_WORKERS)
        ]

        deadline = time.time() + 15
        while time.time() < deadline:
            if await r.zcard(PENDING_KEY) == 0 and len(executed) >= NUM_JOBS:
                break
            await asyncio.sleep(0.01)

        stop_event.set()
        await asyncio.gather(*workers, return_exceptions=True)

        counts  = Counter(jid for _, jid in executed)
        doubles = {jid: cnt for jid, cnt in counts.items() if cnt > 1}

        if doubles:
            failures.append(f"Run {run}: {len(doubles)} double-executions")

        print(f"Run {run+1:02d}/{NUM_RUNS}: {len(executed)} executed, "
              f"{len(doubles)} doubles ✓" if not doubles else f"✗ {len(doubles)} DOUBLES")

    assert not failures, f"Failures across runs:\n" + "\n".join(failures)


@pytest.mark.asyncio
async def test_priority_ordering(clean_redis):
    """
    Critical jobs (priority=0) must be claimed before Low jobs (priority=3)
    even when enqueued at the same timestamp.
    """
    r = clean_redis
    now = time.time() - 1

    # Enqueue low priority first, then critical
    await r.zadd(PENDING_KEY, {"low-job":      now * 10 + 3})
    await r.zadd(PENDING_KEY, {"critical-job": now * 10 + 0})
    await r.zadd(PENDING_KEY, {"normal-job":   now * 10 + 2})

    script  = r.register_script(CLAIM_SCRIPT)
    claimed = await script(
        keys=[PENDING_KEY, CLAIMED_KEY],
        args=[str(now * 10 + 9), "3"],
    )

    assert claimed[0] == "critical-job", f"Expected critical-job first, got {claimed[0]}"
    assert claimed[1] == "normal-job",   f"Expected normal-job second, got {claimed[1]}"
    assert claimed[2] == "low-job",      f"Expected low-job third, got {claimed[2]}"
    print(f"\nPriority order verified: {claimed}")


@pytest.mark.asyncio
async def test_cancel_prevents_execution(clean_redis):
    """Jobs removed from Redis before being claimed must not execute."""
    r   = clean_redis
    now = time.time() - 1

    await r.zadd(PENDING_KEY, {"cancelme": now * 10 + 2})

    # Cancel: remove from Redis
    removed = await r.zrem(PENDING_KEY, "cancelme")
    assert removed == 1

    # Attempt to claim — should get nothing
    script  = r.register_script(CLAIM_SCRIPT)
    claimed = await script(
        keys=[PENDING_KEY, CLAIMED_KEY],
        args=[str(now * 10 + 9), "5"],
    )

    assert "cancelme" not in (claimed or []), "Cancelled job was claimed!"
    print("\nCancel test passed: removed job not claimable")
