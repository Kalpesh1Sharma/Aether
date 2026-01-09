"""
Scheduler — core claim and dispatch loop.
"""

from __future__ import annotations
import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

from app.config import get_settings
from app.logger import get_logger, log_job_event
from app.models import JobStatus, Priority
import app.db as db
import app.redis_client as redis_client
from app.executors import dispatch

logger   = get_logger(__name__)
settings = get_settings()

_shutdown      = False
_running_count = 0


async def run_job(job: Dict[str, Any]):
    global _running_count
    job_id    = job["id"]
    worker_id = settings.worker_id
    _running_count += 1
    start_time = time.monotonic()

    try:
        await db.update_job_status(job_id, JobStatus.RUNNING, worker_id=worker_id)
        await db.append_job_log(job_id, "job.started", worker_id=worker_id)
        log_job_event(logger, "job.started", job_id, worker_id)

        payload = job["payload"] if isinstance(job["payload"], dict) else {}
        result  = await asyncio.wait_for(
            dispatch(job_id, job["job_type"], payload, job["timeout_seconds"]),
            timeout=job["timeout_seconds"],
        )

        latency_ms = (time.monotonic() - start_time) * 1000
        await db.update_job_status(job_id, JobStatus.DONE, worker_id=worker_id, result=result)
        await redis_client.release_claimed_job(job_id)
        log_job_event(logger, "job.done", job_id, worker_id, {"latency_ms": round(latency_ms, 2)})

    except Exception as exc:
        await _handle_failure(job, worker_id, str(exc), start_time)
    finally:
        _running_count -= 1


async def _handle_failure(job, worker_id, error, start_time):
    job_id      = job["id"]
    retry_count = job.get("retry_count", 0)
    max_retries = job.get("max_retries", settings.dlq_max_retries)

    if retry_count < max_retries:
        backoff_seconds = 2 ** retry_count
        next_run_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
        await db.update_job_status(job_id, JobStatus.FAILED, worker_id=worker_id, error=error, increment_retry=True)
        await db.update_job_status(job_id, JobStatus.PENDING)
        await redis_client.requeue_job(job_id, next_run_at.timestamp(), job.get("priority", Priority.NORMAL.value))
    else:
        await db.update_job_status(job_id, JobStatus.DEAD, worker_id=worker_id, error=error)
        await redis_client.release_claimed_job(job_id)


async def claim_and_dispatch():
    job_ids = await redis_client.claim_jobs(settings.claim_batch_size)
    if not job_ids:
        return
    for job_id in job_ids:
        job = await db.get_job(job_id)
        if not job:
            await redis_client.release_claimed_job(job_id)
            continue
        if job["status"] == JobStatus.CANCELLED.value:
            await redis_client.release_claimed_job(job_id)
            continue
        await db.update_job_status(job_id, JobStatus.CLAIMED, worker_id=settings.worker_id)
        asyncio.create_task(run_job(job))


async def scheduler_loop():
    global _shutdown
    logger.info("scheduler.started", extra={"worker_id": settings.worker_id})
    while not _shutdown:
        try:
            if _running_count < settings.worker_concurrency:
                await claim_and_dispatch()
        except Exception as e:
            logger.error("scheduler.loop_error", extra={"error": str(e)})
        await asyncio.sleep(settings.poll_interval_ms / 1000)


async def start_worker():
    global _shutdown
    _shutdown = False
    await db.init_db()
    await scheduler_loop()


async def stop_worker():
    global _shutdown
    _shutdown = True
