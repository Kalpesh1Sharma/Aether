"""
Scheduler — the heart of Aether.

Each worker process runs this loop:
1. Claim a batch of due jobs from Redis (atomic, exactly-once)
2. For each claimed job: run executor in asyncio task
3. On success: mark DONE, schedule next run if recurring
4. On failure: exponential backoff retry OR move to DLQ
5. Every N seconds: send heartbeat + scan for stale jobs

The claim step is the key insight:
  Redis Lua script atomically moves jobs from pending → claimed.
  Only one worker gets each job. No database locks needed.
"""

from __future__ import annotations
import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from croniter import croniter

from app.config import get_settings
from app.logger import get_logger, log_job_event
from app.models import JobStatus, Priority, generate_job_id
import app.db as db
import app.redis_client as redis_client
from app.executors import dispatch

logger    = get_logger(__name__)
settings  = get_settings()

# Track active tasks for graceful shutdown
_active_tasks: set[asyncio.Task] = set()
_shutdown      = False
_running_count = 0


async def run_job(job: Dict[str, Any]):
    """Execute a single job with full lifecycle management."""
    global _running_count
    job_id    = job["id"]
    worker_id = settings.worker_id

    _running_count += 1
    start_time = time.monotonic()

    try:
        # ── Mark running ──────────────────────────────────────────────────────
        await db.update_job_status(job_id, JobStatus.RUNNING, worker_id=worker_id)
        await db.append_job_log(job_id, "job.started", worker_id=worker_id)
        log_job_event(logger, "job.started", job_id, worker_id)

        # ── Execute with timeout ──────────────────────────────────────────────
        payload  = job["payload"] if isinstance(job["payload"], dict) else {}
        result   = await asyncio.wait_for(
            dispatch(job_id, job["job_type"], payload, job["timeout_seconds"]),
            timeout=job["timeout_seconds"],
        )

        latency_ms = (time.monotonic() - start_time) * 1000

        # ── Success ───────────────────────────────────────────────────────────
        await db.update_job_status(
            job_id, JobStatus.DONE,
            worker_id=worker_id, result=result,
        )
        await redis_client.release_claimed_job(job_id)
        await db.append_job_log(
            job_id, "job.done", worker_id=worker_id,
            extra={"latency_ms": latency_ms, "result_keys": list(result.keys()) if result else []},
        )
        log_job_event(logger, "job.done", job_id, worker_id, {"latency_ms": round(latency_ms, 2)})

        # ── Schedule next run if recurring (cron) ────────────────────────────
        if job.get("cron"):
            await _schedule_next_cron_run(job)

    except asyncio.TimeoutError:
        await _handle_failure(job, worker_id, "Execution timed out", start_time)

    except Exception as exc:
        await _handle_failure(job, worker_id, str(exc), start_time)

    finally:
        _running_count -= 1


async def _handle_failure(
    job: Dict[str, Any],
    worker_id: str,
    error: str,
    start_time: float,
):
    job_id      = job["id"]
    retry_count = job.get("retry_count", 0)
    max_retries = job.get("max_retries", settings.dlq_max_retries)

    latency_ms = (time.monotonic() - start_time) * 1000
    log_job_event(logger, "job.failed", job_id, worker_id, {"error": error, "retry_count": retry_count})

    if retry_count < max_retries:
        # Exponential backoff: 2^retry seconds (1s, 2s, 4s, 8s…)
        backoff_seconds = 2 ** retry_count
        next_run_at     = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)

        await db.update_job_status(
            job_id, JobStatus.FAILED,
            worker_id=worker_id, error=error, increment_retry=True,
        )
        # Re-enqueue with backoff
        await db.update_job_status(job_id, JobStatus.PENDING)
        await redis_client.requeue_job(job_id, next_run_at.timestamp(), job.get("priority", Priority.NORMAL.value))
        await db.append_job_log(
            job_id, "job.retry_scheduled", worker_id=worker_id,
            extra={"backoff_seconds": backoff_seconds, "retry_count": retry_count + 1},
        )
        log_job_event(logger, "job.retry_scheduled", job_id, worker_id,
                      {"backoff_seconds": backoff_seconds, "next_attempt": retry_count + 1})
    else:
        # Exhausted retries → Dead Letter Queue
        await db.update_job_status(
            job_id, JobStatus.DEAD,
            worker_id=worker_id, error=error,
        )
        await redis_client.release_claimed_job(job_id)
        await db.append_job_log(
            job_id, "job.dead", worker_id=worker_id,
            extra={"error": error, "total_attempts": retry_count + 1},
        )
        log_job_event(logger, "job.dead", job_id, worker_id,
                      {"error": error, "total_attempts": retry_count + 1})


async def _schedule_next_cron_run(job: Dict[str, Any]):
    """
    For recurring jobs, create the next run.
    Uses croniter for drift-free scheduling (always computes from last run_at,
    not from now — prevents slow drift over time).
    """
    cron_expr = job["cron"]
    last_run  = job["run_at"]
    if isinstance(last_run, str):
        last_run = datetime.fromisoformat(last_run)

    itr      = croniter(cron_expr, last_run)
    next_run = itr.get_next(datetime)

    new_job_id = generate_job_id()
    job_data   = {
        "id":               new_job_id,
        "name":             job["name"],
        "job_type":         job["job_type"],
        "priority":         job.get("priority", Priority.NORMAL.value),
        "payload":          job["payload"] if isinstance(job["payload"], dict) else {},
        "run_at":           next_run,
        "cron":             cron_expr,
        "max_retries":      job["max_retries"],
        "timeout_seconds":  job["timeout_seconds"],
        "parent_job_id":    None,
        "chain_step_index": None,
    }
    await db.create_job(job_data)
    await redis_client.enqueue_job(new_job_id, next_run.timestamp(), job_data["priority"])
    log_job_event(logger, "job.cron_scheduled", new_job_id, extra={"next_run": next_run.isoformat()})


async def claim_and_dispatch():
    """One iteration of the claim loop."""
    job_ids = await redis_client.claim_jobs(settings.claim_batch_size)
    if not job_ids:
        return

    for job_id in job_ids:
        job = await db.get_job(job_id)
        if not job:
            # Job was deleted after being enqueued — clean up
            await redis_client.release_claimed_job(job_id)
            continue

        # Final guard: cancelled jobs must not execute even if claimed
        if job["status"] == JobStatus.CANCELLED.value:
            await redis_client.release_claimed_job(job_id)
            log_job_event(logger, "job.skip_cancelled", job_id)
            continue

        await db.update_job_status(job_id, JobStatus.CLAIMED, worker_id=settings.worker_id)

        task = asyncio.create_task(run_job(job))
        _active_tasks.add(task)
        task.add_done_callback(_active_tasks.discard)


async def reclaim_stale_jobs():
    """
    Heartbeat monitor: find jobs claimed by dead workers and reset them.
    Runs every heartbeat_interval seconds.
    """
    stale_jobs = await db.get_jobs_for_retry(settings.worker_job_claim_timeout)
    for job in stale_jobs:
        logger.warning("job.stale_reclaim", extra={"job_id": job["id"], "worker_id": job.get("worker_id")})
        await db.reset_job_to_pending(job["id"])
        await redis_client.enqueue_job(job["id"], time.time(), job.get("priority", Priority.NORMAL.value))


async def heartbeat_loop():
    """Send heartbeat to Redis and PostgreSQL periodically."""
    while not _shutdown:
        await redis_client.set_heartbeat(
            settings.worker_id,
            ttl_seconds=settings.worker_heartbeat_interval * 3,
        )
        await db.upsert_heartbeat(settings.worker_id, _running_count)
        await asyncio.sleep(settings.worker_heartbeat_interval)


async def stale_job_monitor():
    """Periodically reclaim jobs from crashed workers."""
    while not _shutdown:
        try:
            await reclaim_stale_jobs()
        except Exception as e:
            logger.error("stale_monitor.error", extra={"error": str(e)})
        await asyncio.sleep(settings.worker_heartbeat_interval * 2)


async def scheduler_loop():
    """
    Main scheduler loop.
    Polls Redis for due jobs and dispatches them concurrently.
    Sleep time adapts: short when there's work, backs off when idle.
    """
    global _shutdown
    logger.info("scheduler.started", extra={"worker_id": settings.worker_id})
    idle_count = 0

    while not _shutdown:
        try:
            if _running_count < settings.worker_concurrency:
                await claim_and_dispatch()
                idle_count = 0
            else:
                idle_count += 1  # at capacity, back off
        except Exception as e:
            logger.error("scheduler.loop_error", extra={"error": str(e)})

        # Adaptive sleep: fast when busy, slower when idle
        sleep_ms = settings.poll_interval_ms * min(1 + idle_count // 5, 5)
        await asyncio.sleep(sleep_ms / 1000)

    logger.info("scheduler.stopping", extra={"worker_id": settings.worker_id})

    # Graceful shutdown: wait for in-flight jobs
    if _active_tasks:
        logger.info("scheduler.draining", extra={"in_flight": len(_active_tasks)})
        await asyncio.gather(*_active_tasks, return_exceptions=True)

    logger.info("scheduler.stopped", extra={"worker_id": settings.worker_id})


async def start_worker():
    """Entry point: start all background loops for a worker process."""
    global _shutdown
    _shutdown = False

    await db.init_db()

    await asyncio.gather(
        scheduler_loop(),
        heartbeat_loop(),
        stale_job_monitor(),
    )


async def stop_worker():
    global _shutdown
    _shutdown = True
