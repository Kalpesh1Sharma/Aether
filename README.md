# Aether

**Distributed task orchestration engine for async AI workloads.**

Most teams schedule background jobs by bolting Celery onto their stack without thinking about exactly-once execution guarantees. Aether is built around that hard problem — and proves its correctness with a test harness.

```
1000 jobs × 5 concurrent workers × 10 runs = 0 double-executions
```

---

## Why this exists

Celery is battle-tested but it abstracts away the claiming mechanism. When you're running AI pipelines — nightly RAG re-indexing, scheduled LLM batch jobs, webhook delivery with retries — you need to reason about what happens when:

- Two workers see the same job at the same microsecond
- A worker crashes mid-execution  
- An LLM job's output needs to trigger the next step in a pipeline

Aether makes those guarantees explicit and testable.

---

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────┐
│  FastAPI     │────▶│           Redis Sorted Set               │
│  (REST API)  │     │  score = run_at_unix * 10 + priority     │
└─────────────┘     └──────────────┬───────────────────────────┘
                                   │  Atomic Lua claim (exactly-once)
                    ┌──────────────▼───────────────────────────┐
                    │         Worker Pool (N workers)           │
                    │  worker-1   worker-2   worker-3           │
                    └──────────────┬───────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                     ▼
        [webhook]            [llm_task]             [chain]
        HTTP POST            Groq API           DAG of steps
        to URL               → callback         output → input
              │                    │                     │
              └────────────────────▼─────────────────────┘
                            ┌──────────────┐
                            │  PostgreSQL   │
                            │  (audit log,  │
                            │   DLQ, stats) │
                            └──────────────┘
```

### The exactly-once guarantee

The core of Aether is a Redis Lua script that atomically claims jobs:

```lua
local candidates = redis.call('ZRANGEBYSCORE', pending_key, '-inf', now, 'LIMIT', 0, batch)

for _, job_id in ipairs(candidates) do
    local removed = redis.call('ZREM', pending_key, job_id)
    if removed == 1 then  -- only one caller wins this
        redis.call('ZADD', claimed_key, now, job_id)
        table.insert(claimed, job_id)
    end
end
```

Redis executes Lua atomically — no other command runs between lines. Two workers racing on the same job will both call `ZREM`. Only the one that gets `removed == 1` proceeds. The other gets `0` and skips that job. This is the entire exactly-once mechanism.

---

## Features

### Job types

**Webhook** — POST any payload to a URL with configurable headers and method
```json
{
  "name": "notify-slack",
  "job_type": "webhook",
  "payload": {
    "url": "https://hooks.slack.com/...",
    "body": { "text": "Deploy complete" }
  },
  "delay_seconds": 0
}
```

**LLM Task** — Dispatch a prompt to Groq, result POSTed to callback URL
```json
{
  "name": "nightly-summary",
  "job_type": "llm_task",
  "payload": {
    "prompt": "Summarize today's user feedback: ...",
    "callback_url": "https://your-api.com/summaries"
  },
  "cron": "0 9 * * *"
}
```

**Chain (DAG)** — Output of step N becomes input to step N+1
```json
{
  "name": "ai-pipeline",
  "job_type": "chain",
  "payload": {
    "steps": [
      {
        "job_type": "llm_task",
        "payload": { "prompt": "Extract key topics from: {{input}}" }
      },
      {
        "job_type": "llm_task", 
        "payload": { "prompt": "Classify these topics: {{previous_result}}" }
      },
      {
        "job_type": "webhook",
        "payload": { "url": "https://your-api.com/pipeline-results" }
      }
    ],
    "callback_url": "https://your-api.com/final"
  }
}
```

### Scheduling modes

| Mode | Example |
|------|---------|
| Immediate | `"delay_seconds": 0` |
| Delayed | `"delay_seconds": 300` |
| Exact time | `"run_at": "2024-12-01T09:00:00Z"` |
| Cron (recurring) | `"cron": "0 9 * * 1"` |

### Priority

Jobs with the same `run_at` are ordered by priority:

| Priority | Value | Use case |
|----------|-------|----------|
| CRITICAL | 0 | Alerts, urgent webhooks |
| HIGH | 1 | User-facing jobs |
| NORMAL | 2 | Default |
| LOW | 3 | Background analytics |

### Reliability

- **Exponential backoff retries**: 1s → 2s → 4s → 8s (not fixed delay)
- **Dead Letter Queue**: jobs exhausting retries move to `/jobs/dead`
- **Force retry**: `POST /jobs/{id}/retry` re-queues any dead job
- **Worker crash recovery**: heartbeat monitor reclaims stale jobs within 30s
- **Graceful shutdown**: in-flight jobs complete before process exits
- **Cancel safety**: cancelled jobs removed from Redis atomically — no execution after cancel

---

## API

```
POST   /jobs              Create a job
GET    /jobs              List jobs (filter: status, job_type, priority)
GET    /jobs/dead         Dead Letter Queue
GET    /jobs/{id}         Get job detail
DELETE /jobs/{id}         Cancel job
POST   /jobs/{id}/retry   Force retry a dead job
GET    /jobs/{id}/logs    Full execution trace
GET    /metrics           Live system metrics
GET    /health            Liveness + readiness
```

### Metrics response

```json
{
  "pending": 42,
  "running": 7,
  "done": 9823,
  "dead": 3,
  "workers_alive": 3,
  "p50_latency_ms": 124.3,
  "p95_latency_ms": 891.2,
  "p99_latency_ms": 2103.7,
  "throughput_last_minute": 47
}
```

---

## Running locally

```bash
# 1. Clone and configure
cp .env.example .env
# Add your GROQ_API_KEY (free at console.groq.com)

# 2. Start everything (API + 3 workers + Redis + Postgres)
make up

# 3. Health check
make health

# 4. Run demo jobs
make demo-webhook
make demo-llm
make demo-chain

# 5. Watch metrics
make metrics
```

### Running the exactly-once test

```bash
pip install -r requirements.txt
pytest tests/test_exactly_once.py -v -s
```

Expected output:
```
──────────────────────────────────────────────────
  Jobs enqueued:     1000
  Jobs executed:     1000
  Unique jobs:       1000
  Double-executions: 0
  Missed jobs:       0
  Workers:           5
──────────────────────────────────────────────────
PASSED
```

---

## Design decisions

**Why Redis sorted set instead of a message queue (RabbitMQ/Kafka)?**  
Scheduled jobs have a time dimension — you need to say "run this at 9am next Monday." Sorted sets with `score = run_at_unix` give you O(log N) insertion and O(1) range queries for due jobs. Queues don't model time natively.

**Why Lua script instead of Redis transactions (MULTI/EXEC)?**  
MULTI/EXEC doesn't support conditional logic — you can't check-then-act atomically. Lua scripts execute as a single atomic unit, letting us check `ZREM` return value to determine who won the race.

**Why asyncio workers instead of threads?**  
Job execution is I/O-bound (HTTP calls, LLM APIs, DB writes). asyncio handles hundreds of concurrent jobs per worker with minimal overhead. Thread pools would be wasteful.

**Why not Celery?**  
Celery is excellent but it abstracts the claiming mechanism. Building this from scratch means I can explain every guarantee from first principles — which matters when your jobs are AI workloads where double-execution has real cost.

---

## Known limitations

- **Single-region**: Workers share one Redis instance. Multi-datacenter coordination would require a distributed lock like Redlock.
- **No DAG cycles or branching**: Chains are linear. Conditional branching (if step 2 returns X, go to step 3a, else step 3b) is not yet supported.
- **LLM rate limits**: The `llm_task` executor is rate-limited by Groq. Under heavy load, you'd add a token bucket in front of the Groq client.
- **No job deduplication**: Submitting the same job twice creates two entries. A `deduplication_key` field would prevent this.

---

## Stack

- **FastAPI** + asyncio — async REST API  
- **Redis** sorted set + Lua — distributed job queue, exactly-once claiming  
- **PostgreSQL** (asyncpg) — persistence, audit logs, DLQ, metrics  
- **Groq** (llama3-8b) — LLM execution for `llm_task` jobs  
- **croniter** — cron expression parsing with drift correction  
- **Docker Compose** — one-command local setup

---

## Project structure

```
aether/
├── app/
│   ├── main.py          # FastAPI routes
│   ├── scheduler.py     # Claim loop, retry logic, graceful shutdown
│   ├── executors.py     # webhook / llm_task / chain handlers
│   ├── redis_client.py  # Sorted set ops + Lua claim script
│   ├── db.py            # PostgreSQL (asyncpg) — all queries
│   ├── models.py        # Pydantic schemas + enums
│   ├── config.py        # Settings (pydantic-settings)
│   └── logger.py        # Structured JSON logging
├── worker.py            # Worker process entrypoint
├── tests/
│   └── test_exactly_once.py  # Correctness proof
├── docker-compose.yml
├── Dockerfile
├── Makefile
└── requirements.txt
```
