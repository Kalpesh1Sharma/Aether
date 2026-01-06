from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
import ulid


class JobStatus(str, Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD = "dead"        # exhausted all retries → DLQ
    CANCELLED = "cancelled"


class JobType(str, Enum):
    WEBHOOK = "webhook"
    LLM_TASK = "llm_task"
    CHAIN = "chain"


class Priority(int, Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


# ── Payload schemas per job type ──────────────────────────────────────────────

class WebhookPayload(BaseModel):
    url: str
    method: str = "POST"
    headers: Dict[str, str] = {}
    body: Dict[str, Any] = {}


class LLMTaskPayload(BaseModel):
    prompt: str
    system_prompt: Optional[str] = None
    callback_url: str                        # where to POST the LLM result
    model: Optional[str] = None              # override default model


class ChainStep(BaseModel):
    job_type: JobType
    payload: Dict[str, Any]                  # will be merged with previous step output


class ChainPayload(BaseModel):
    steps: List[ChainStep]                   # executed in order, output → next input
    callback_url: str                        # final result destination


# ── Request / Response schemas ────────────────────────────────────────────────

class CreateJobRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    job_type: JobType
    payload: Dict[str, Any]
    priority: Priority = Priority.NORMAL

    # Scheduling — exactly one of these should be set
    run_at: Optional[datetime] = None        # one-shot at specific time
    cron: Optional[str] = None               # recurring cron expression
    delay_seconds: Optional[float] = None    # run N seconds from now

    max_retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: int = Field(default=60, ge=1, le=3600)

    # For chained jobs: which job_id triggered this
    parent_job_id: Optional[str] = None
    chain_step_index: Optional[int] = None

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v):
        if v is not None:
            from croniter import croniter
            if not croniter.is_valid(v):
                raise ValueError(f"Invalid cron expression: {v}")
        return v


class JobResponse(BaseModel):
    id: str
    name: str
    job_type: JobType
    status: JobStatus
    priority: Priority
    payload: Dict[str, Any]
    run_at: datetime
    cron: Optional[str]
    max_retries: int
    retry_count: int
    timeout_seconds: int
    parent_job_id: Optional[str]
    chain_step_index: Optional[int]
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error: Optional[str]
    result: Optional[Dict[str, Any]]
    worker_id: Optional[str]


class JobListResponse(BaseModel):
    jobs: List[JobResponse]
    total: int
    page: int
    page_size: int


class MetricsResponse(BaseModel):
    pending: int
    running: int
    done: int
    failed: int
    dead: int
    cancelled: int
    total: int
    workers_alive: int
    p50_latency_ms: Optional[float]
    p95_latency_ms: Optional[float]
    p99_latency_ms: Optional[float]
    throughput_last_minute: int


class HealthResponse(BaseModel):
    status: str              # "ok" | "degraded"
    redis: bool
    postgres: bool
    workers_alive: int
    uptime_seconds: float


class RetryResponse(BaseModel):
    job_id: str
    message: str
    new_run_at: datetime


def generate_job_id() -> str:
    return str(ulid.new())
