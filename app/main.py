from __future__ import annotations
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.config import get_settings
from app.logger import get_logger
from app.models import (
    CreateJobRequest, JobResponse, JobListResponse,
    MetricsResponse, HealthResponse, RetryResponse,
    JobStatus, Priority, generate_job_id,
)
import app.db as db
import app.redis_client as redis_client
from croniter import croniter

logger   = get_logger(__name__)
settings = get_settings()
_start_time = time.time()


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    logger.info("aether.api.started")
    yield
    await db.close_pool()
    await redis_client.close_redis()
    logger.info("aether.api.stopped")


app = FastAPI(
    title="Aether",
    description="Distributed task orchestration engine for AI workloads",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _compute_run_at(req: CreateJobRequest) -> datetime:
    """Resolve the first run_at from the request."""
    if req.run_at:
        return req.run_at
    if req.delay_seconds is not None:
        return datetime.now(timezone.utc) + timedelta(seconds=req.delay_seconds)
    if req.cron:
        itr = croniter(req.cron, datetime.now(timezone.utc))
        return itr.get_next(datetime)
    # Default: run immediately
    return datetime.now(timezone.utc)


def _row_to_response(row: dict) -> JobResponse:
    payload = row["payload"]
    result  = row.get("result")

    if isinstance(payload, str):
        import json
        payload = json.loads(payload)
    if isinstance(result, str):
        import json
        result = json.loads(result)

    return JobResponse(
        id=row["id"],
        name=row["name"],
        job_type=row["job_type"],
        status=row["status"],
        priority=row["priority"],
        payload=payload,
        run_at=row["run_at"],
        cron=row.get("cron"),
        max_retries=row["max_retries"],
        retry_count=row["retry_count"],
        timeout_seconds=row["timeout_seconds"],
        parent_job_id=row.get("parent_job_id"),
        chain_step_index=row.get("chain_step_index"),
        created_at=row["created_at"],
        started_at=row.get("started_at"),
        finished_at=row.get("finished_at"),
        error=row.get("error"),
        result=result,
        worker_id=row.get("worker_id"),
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/jobs", response_model=JobResponse, status_code=201)
async def create_job(req: CreateJobRequest):
    """
    Create and schedule a job.

    - **run_at**: exact UTC datetime
    - **delay_seconds**: run N seconds from now
    - **cron**: recurring cron expression (e.g. `0 9 * * 1`)
    - **job_type**: `webhook` | `llm_task` | `chain`
    """
    job_id  = generate_job_id()
    run_at  = _compute_run_at(req)

    job_data = {
        "id":               job_id,
        "name":             req.name,
        "job_type":         req.job_type.value,
        "priority":         req.priority.value,
        "payload":          req.payload,
        "run_at":           run_at,
        "cron":             req.cron,
        "max_retries":      req.max_retries,
        "timeout_seconds":  req.timeout_seconds,
        "parent_job_id":    req.parent_job_id,
        "chain_step_index": req.chain_step_index,
    }

    row = await db.create_job(job_data)
    await redis_client.enqueue_job(job_id, run_at.timestamp(), req.priority.value)

    logger.info("api.job_created", extra={"job_id": job_id, "type": req.job_type, "run_at": run_at.isoformat()})
    return _row_to_response(row)


@app.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    status:    Optional[str] = Query(None, description="Filter by status"),
    job_type:  Optional[str] = Query(None, description="Filter by job_type"),
    priority:  Optional[int] = Query(None, description="Filter by priority (0-3)"),
    page:      int           = Query(1, ge=1),
    page_size: int           = Query(20, ge=1, le=100),
):
    rows, total = await db.list_jobs(status, job_type, priority, page, page_size)
    return JobListResponse(
        jobs=[_row_to_response(r) for r in rows],
        total=total,
        page=page,
        page_size=page_size,
    )


@app.get("/jobs/dead", response_model=JobListResponse)
async def list_dead_jobs(page: int = 1, page_size: int = 20):
    """Dead Letter Queue — jobs that exhausted all retries."""
    rows, total = await db.list_jobs(status="dead", page=page, page_size=page_size)
    return JobListResponse(
        jobs=[_row_to_response(r) for r in rows],
        total=total, page=page, page_size=page_size,
    )


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    row = await db.get_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return _row_to_response(row)


@app.delete("/jobs/{job_id}", status_code=204)
async def cancel_job(job_id: str):
    """
    Cancel a pending job.
    Running jobs are not interrupted but are marked cancelled and
    won't be retried or rescheduled.
    """
    row = await db.get_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if row["status"] in (JobStatus.DONE.value, JobStatus.DEAD.value, JobStatus.CANCELLED.value):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel job in status: {row['status']}"
        )

    await db.update_job_status(job_id, JobStatus.CANCELLED)
    await redis_client.remove_job(job_id)
    logger.info("api.job_cancelled", extra={"job_id": job_id})


@app.post("/jobs/{job_id}/retry", response_model=RetryResponse)
async def retry_dead_job(job_id: str):
    """Force-retry a dead job immediately."""
    row = await db.get_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if row["status"] != JobStatus.DEAD.value:
        raise HTTPException(status_code=409, detail=f"Can only retry DEAD jobs, got: {row['status']}")

    run_at = datetime.now(timezone.utc)
    await db.update_job_status(job_id, JobStatus.PENDING)
    # Reset retry count so it gets full retries again
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE jobs SET retry_count = 0, error = NULL WHERE id = $1", job_id)

    await redis_client.enqueue_job(job_id, run_at.timestamp(), row["priority"])
    logger.info("api.job_retried", extra={"job_id": job_id})
    return RetryResponse(job_id=job_id, message="Job re-queued", new_run_at=run_at)


@app.get("/jobs/{job_id}/logs")
async def get_job_logs(job_id: str):
    """Full execution trace for a job."""
    row = await db.get_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    logs = await db.get_job_logs(job_id)
    return {"job_id": job_id, "logs": logs}


@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics():
    counts     = await db.get_status_counts()
    latencies  = await db.get_latency_percentiles()
    throughput = await db.get_throughput_last_minute()
    workers    = await redis_client.get_alive_workers()

    total = sum(counts.values())
    return MetricsResponse(
        pending=counts.get("pending", 0),
        running=counts.get("running", 0) + counts.get("claimed", 0),
        done=counts.get("done", 0),
        failed=counts.get("failed", 0),
        dead=counts.get("dead", 0),
        cancelled=counts.get("cancelled", 0),
        total=total,
        workers_alive=len(workers),
        p50_latency_ms=latencies["p50"],
        p95_latency_ms=latencies["p95"],
        p99_latency_ms=latencies["p99"],
        throughput_last_minute=throughput,
    )


@app.get("/health", response_model=HealthResponse)
async def health():
    redis_ok    = await redis_client.ping_redis()
    workers     = await redis_client.get_alive_workers()
    uptime      = time.time() - _start_time

    try:
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        postgres_ok = True
    except Exception:
        postgres_ok = False

    status = "ok" if (redis_ok and postgres_ok) else "degraded"
    return HealthResponse(
        status=status,
        redis=redis_ok,
        postgres=postgres_ok,
        workers_alive=len(workers),
        uptime_seconds=round(uptime, 2),
    )


# ── Internal endpoint for chain result accumulation ───────────────────────────

@app.post("/internal/chain-callback", include_in_schema=False)
async def chain_callback(body: dict):
    """Internal endpoint used by mid-chain LLM steps to pass results forward."""
    return {"received": True}
