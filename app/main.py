from __future__ import annotations
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from app.config import get_settings
from app.logger import get_logger
from app.models import (
    CreateJobRequest, JobResponse, JobListResponse,
    JobStatus, Priority, generate_job_id,
)
import app.db as db
import app.redis_client as redis_client
from croniter import croniter

logger   = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    logger.info("aether.api.started")
    yield
    await db.close_pool()
    await redis_client.close_redis()


app = FastAPI(
    title="Aether",
    description="Distributed task orchestration engine for AI workloads",
    version="1.0.0",
    lifespan=lifespan,
)


def _compute_run_at(req: CreateJobRequest) -> datetime:
    if req.run_at:
        return req.run_at
    if req.delay_seconds is not None:
        return datetime.now(timezone.utc) + timedelta(seconds=req.delay_seconds)
    if req.cron:
        itr = croniter(req.cron, datetime.now(timezone.utc))
        return itr.get_next(datetime)
    return datetime.now(timezone.utc)


def _row_to_response(row: dict) -> JobResponse:
    import json
    payload = row["payload"]
    result  = row.get("result")
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(result, str):
        result = json.loads(result)
    return JobResponse(
        id=row["id"], name=row["name"], job_type=row["job_type"],
        status=row["status"], priority=row["priority"], payload=payload,
        run_at=row["run_at"], cron=row.get("cron"),
        max_retries=row["max_retries"], retry_count=row["retry_count"],
        timeout_seconds=row["timeout_seconds"],
        parent_job_id=row.get("parent_job_id"),
        chain_step_index=row.get("chain_step_index"),
        created_at=row["created_at"], started_at=row.get("started_at"),
        finished_at=row.get("finished_at"), error=row.get("error"),
        result=result, worker_id=row.get("worker_id"),
    )


@app.post("/jobs", response_model=JobResponse, status_code=201)
async def create_job(req: CreateJobRequest):
    job_id   = generate_job_id()
    run_at   = _compute_run_at(req)
    job_data = {
        "id": job_id, "name": req.name, "job_type": req.job_type.value,
        "priority": req.priority.value, "payload": req.payload,
        "run_at": run_at, "cron": req.cron,
        "max_retries": req.max_retries, "timeout_seconds": req.timeout_seconds,
        "parent_job_id": req.parent_job_id, "chain_step_index": req.chain_step_index,
    }
    row = await db.create_job(job_data)
    await redis_client.enqueue_job(job_id, run_at.timestamp(), req.priority.value)
    return _row_to_response(row)


@app.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    status: Optional[str] = Query(None),
    job_type: Optional[str] = Query(None),
    priority: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    rows, total = await db.list_jobs(status, job_type, priority, page, page_size)
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
    row = await db.get_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if row["status"] in (JobStatus.DONE.value, JobStatus.DEAD.value, JobStatus.CANCELLED.value):
        raise HTTPException(status_code=409, detail=f"Cannot cancel job in status: {row['status']}")
    await db.update_job_status(job_id, JobStatus.CANCELLED)
    await redis_client.remove_job(job_id)
