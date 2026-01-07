from __future__ import annotations
import asyncpg
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from app.config import get_settings
from app.models import JobStatus, JobType, Priority
from app.logger import get_logger

logger = get_logger(__name__)

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=20,
            command_timeout=30,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def init_db():
    """Create tables if they don't exist."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                job_type        TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending',
                priority        INTEGER NOT NULL DEFAULT 2,
                payload         JSONB NOT NULL DEFAULT '{}',
                run_at          TIMESTAMPTZ NOT NULL,
                cron            TEXT,
                max_retries     INTEGER NOT NULL DEFAULT 3,
                retry_count     INTEGER NOT NULL DEFAULT 0,
                timeout_seconds INTEGER NOT NULL DEFAULT 60,
                parent_job_id   TEXT,
                chain_step_index INTEGER,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at      TIMESTAMPTZ,
                finished_at     TIMESTAMPTZ,
                error           TEXT,
                result          JSONB,
                worker_id       TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_status   ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_run_at   ON jobs(run_at);
            CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);

            CREATE TABLE IF NOT EXISTS job_logs (
                id         BIGSERIAL PRIMARY KEY,
                job_id     TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                event      TEXT NOT NULL,
                worker_id  TEXT,
                message    TEXT,
                extra      JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);

            CREATE TABLE IF NOT EXISTS worker_heartbeats (
                worker_id    TEXT PRIMARY KEY,
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                job_count    INTEGER NOT NULL DEFAULT 0
            );
        """)
    logger.info("database.initialized")


# ── Job CRUD ──────────────────────────────────────────────────────────────────

async def create_job(job_data: Dict[str, Any]) -> Dict[str, Any]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO jobs (
                id, name, job_type, status, priority, payload,
                run_at, cron, max_retries, retry_count, timeout_seconds,
                parent_job_id, chain_step_index, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            RETURNING *
        """,
            job_data["id"], job_data["name"], job_data["job_type"],
            JobStatus.PENDING.value, job_data["priority"], json.dumps(job_data["payload"]),
            job_data["run_at"], job_data.get("cron"), job_data["max_retries"],
            0, job_data["timeout_seconds"],
            job_data.get("parent_job_id"), job_data.get("chain_step_index"),
            datetime.utcnow(),
        )
        return dict(row)


async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
        return dict(row) if row else None


async def list_jobs(
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    priority: Optional[int] = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[List[Dict], int]:
    pool = await get_pool()
    filters, args = [], []
    idx = 1

    if status:
        filters.append(f"status = ${idx}"); args.append(status); idx += 1
    if job_type:
        filters.append(f"job_type = ${idx}"); args.append(job_type); idx += 1
    if priority is not None:
        filters.append(f"priority = ${idx}"); args.append(priority); idx += 1

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    offset = (page - 1) * page_size

    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM jobs {where}", *args)
        rows = await conn.fetch(
            f"SELECT * FROM jobs {where} ORDER BY run_at DESC LIMIT ${idx} OFFSET ${idx+1}",
            *args, page_size, offset,
        )
    return [dict(r) for r in rows], total


async def update_job_status(
    job_id: str,
    status: JobStatus,
    worker_id: str = None,
    error: str = None,
    result: Dict = None,
    increment_retry: bool = False,
) -> Optional[Dict[str, Any]]:
    pool = await get_pool()
    sets = ["status = $2"]
    args: List[Any] = [job_id, status.value]
    idx = 3

    if status == JobStatus.RUNNING:
        sets.append(f"started_at = ${idx}"); args.append(datetime.utcnow()); idx += 1
    if status in (JobStatus.DONE, JobStatus.FAILED, JobStatus.DEAD, JobStatus.CANCELLED):
        sets.append(f"finished_at = ${idx}"); args.append(datetime.utcnow()); idx += 1
    if worker_id:
        sets.append(f"worker_id = ${idx}"); args.append(worker_id); idx += 1
    if error:
        sets.append(f"error = ${idx}"); args.append(error); idx += 1
    if result is not None:
        sets.append(f"result = ${idx}"); args.append(json.dumps(result)); idx += 1
    if increment_retry:
        sets.append("retry_count = retry_count + 1")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE jobs SET {', '.join(sets)} WHERE id = $1 RETURNING *",
            *args,
        )
        return dict(row) if row else None


async def get_jobs_for_retry(worker_claim_timeout: int) -> List[Dict[str, Any]]:
    """Find claimed/running jobs whose workers have gone silent — reclaim them."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT j.* FROM jobs j
            LEFT JOIN worker_heartbeats w ON j.worker_id = w.worker_id
            WHERE j.status IN ('claimed', 'running')
              AND (
                w.last_seen_at IS NULL
                OR w.last_seen_at < NOW() - ($1 * INTERVAL '1 second')
              )
        """, worker_claim_timeout)
        return [dict(r) for r in rows]


async def reset_job_to_pending(job_id: str) -> None:
    """Put a stale job back to pending so it can be reclaimed."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE jobs
            SET status = 'pending', worker_id = NULL,
                started_at = NULL, error = NULL
            WHERE id = $1
        """, job_id)


# ── Job logs ──────────────────────────────────────────────────────────────────

async def append_job_log(
    job_id: str, event: str, worker_id: str = None,
    message: str = None, extra: Dict = None
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO job_logs (job_id, event, worker_id, message, extra)
            VALUES ($1, $2, $3, $4, $5)
        """, job_id, event, worker_id, message, json.dumps(extra or {}))


async def get_job_logs(job_id: str) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM job_logs WHERE job_id = $1 ORDER BY created_at ASC",
            job_id,
        )
        return [dict(r) for r in rows]


# ── Worker heartbeats ─────────────────────────────────────────────────────────

async def upsert_heartbeat(worker_id: str, job_count: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO worker_heartbeats (worker_id, last_seen_at, job_count)
            VALUES ($1, NOW(), $2)
            ON CONFLICT (worker_id) DO UPDATE
            SET last_seen_at = NOW(), job_count = $2
        """, worker_id, job_count)


async def get_alive_worker_count(heartbeat_interval: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("""
            SELECT COUNT(*) FROM worker_heartbeats
            WHERE last_seen_at > NOW() - ($1 * INTERVAL '1 second')
        """, heartbeat_interval * 3)


# ── Metrics ───────────────────────────────────────────────────────────────────

async def get_status_counts() -> Dict[str, int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT status, COUNT(*) as cnt FROM jobs GROUP BY status")
        return {r["status"]: r["cnt"] for r in rows}


async def get_latency_percentiles() -> Dict[str, Optional[float]]:
    """p50/p95/p99 of execution time for completed jobs in last hour."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99
            FROM (
                SELECT EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000 AS latency_ms
                FROM jobs
                WHERE status = 'done'
                  AND finished_at > NOW() - INTERVAL '1 hour'
                  AND started_at IS NOT NULL
            ) t
        """)
        if row:
            return {"p50": row["p50"], "p95": row["p95"], "p99": row["p99"]}
        return {"p50": None, "p95": None, "p99": None}


async def get_throughput_last_minute() -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("""
            SELECT COUNT(*) FROM jobs
            WHERE status = 'done'
              AND finished_at > NOW() - INTERVAL '1 minute'
        """) or 0
