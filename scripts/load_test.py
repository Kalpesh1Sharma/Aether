"""
Aether Load Test
================
Fires N concurrent jobs at the API and measures throughput + latency.

Usage:
    python scripts/load_test.py                        # 100 jobs, default settings
    python scripts/load_test.py --jobs 500             # 500 jobs
    python scripts/load_test.py --jobs 1000 --workers 20  # 1000 jobs, 20 concurrent
    python scripts/load_test.py --type llm_task        # test LLM jobs

Results are printed to stdout and saved to scripts/load_test_results.json
"""

import asyncio
import argparse
import json
import time
import statistics
from datetime import datetime, timezone
from typing import List, Dict, Any
import httpx

API_BASE    = "http://localhost:8000"
CALLBACK    = "https://httpbin.org/post"   # free echo endpoint for testing


# ── Job templates per type ────────────────────────────────────────────────────

def webhook_payload(i: int) -> Dict[str, Any]:
    return {
        "name":     f"load-test-webhook-{i:04d}",
        "job_type": "webhook",
        "payload":  {
            "url":  CALLBACK,
            "body": {"job_index": i, "source": "aether-load-test"},
        },
        "priority":       2,
        "delay_seconds":  0,
        "max_retries":    1,
        "timeout_seconds": 10,
    }


def llm_payload(i: int) -> Dict[str, Any]:
    prompts = [
        "Explain distributed systems in one sentence.",
        "What is eventual consistency?",
        "Define idempotency in APIs.",
        "What is a dead letter queue?",
        "Explain the CAP theorem briefly.",
    ]
    return {
        "name":     f"load-test-llm-{i:04d}",
        "job_type": "llm_task",
        "payload":  {
            "prompt":       prompts[i % len(prompts)],
            "callback_url": CALLBACK,
        },
        "priority":        1,
        "delay_seconds":   0,
        "max_retries":     1,
        "timeout_seconds": 30,
    }


def chain_payload(i: int) -> Dict[str, Any]:
    return {
        "name":     f"load-test-chain-{i:04d}",
        "job_type": "chain",
        "payload":  {
            "steps": [
                {
                    "job_type": "llm_task",
                    "payload":  {
                        "prompt":       "List 3 keywords about async programming.",
                        "callback_url": CALLBACK,
                    },
                },
                {
                    "job_type": "webhook",
                    "payload":  {"url": CALLBACK, "body": {}},
                },
            ],
            "callback_url": CALLBACK,
        },
        "priority":        2,
        "delay_seconds":   0,
        "max_retries":     1,
        "timeout_seconds": 60,
    }


PAYLOAD_FN = {
    "webhook":  webhook_payload,
    "llm_task": llm_payload,
    "chain":    chain_payload,
}


# ── Core load test ────────────────────────────────────────────────────────────

async def submit_job(
    client: httpx.AsyncClient,
    payload: Dict[str, Any],
    results: list,
    semaphore: asyncio.Semaphore,
):
    async with semaphore:
        start = time.monotonic()
        try:
            resp = await client.post(f"{API_BASE}/jobs", json=payload, timeout=10)
            latency_ms = (time.monotonic() - start) * 1000
            if resp.status_code == 201:
                results.append({"status": "ok", "latency_ms": latency_ms, "job_id": resp.json()["id"]})
            else:
                results.append({"status": "error", "latency_ms": latency_ms, "code": resp.status_code})
        except Exception as e:
            latency_ms = (time.monotonic() - start) * 1000
            results.append({"status": "exception", "latency_ms": latency_ms, "error": str(e)})


async def poll_completion(job_ids: List[str], timeout: int = 120) -> Dict[str, int]:
    """Poll /metrics until done+dead count matches submitted jobs."""
    async with httpx.AsyncClient() as client:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = await client.get(f"{API_BASE}/metrics", timeout=5)
                m    = resp.json()
                done = m.get("done", 0)
                dead = m.get("dead", 0)
                pending = m.get("pending", 0)
                running = m.get("running", 0)
                print(f"\r  Pending: {pending:4d} | Running: {running:4d} | Done: {done:4d} | Dead: {dead:4d}", end="", flush=True)
                if pending == 0 and running == 0:
                    print()  # newline after progress
                    return {"done": done, "dead": dead}
            except Exception:
                pass
            await asyncio.sleep(1)
    print()
    return {"done": -1, "dead": -1, "timeout": True}


async def run_load_test(num_jobs: int, concurrency: int, job_type: str):
    print(f"\n{'═'*55}")
    print(f"  Aether Load Test")
    print(f"{'═'*55}")
    print(f"  API:         {API_BASE}")
    print(f"  Jobs:        {num_jobs}")
    print(f"  Concurrency: {concurrency}")
    print(f"  Job type:    {job_type}")
    print(f"{'─'*55}")

    # ── Health check ──────────────────────────────────────────
    async with httpx.AsyncClient() as client:
        try:
            health = await client.get(f"{API_BASE}/health", timeout=5)
            h = health.json()
            print(f"  Health:  {h['status'].upper()} | Redis: {'✓' if h['redis'] else '✗'} | Postgres: {'✓' if h['postgres'] else '✗'} | Workers: {h['workers_alive']}")
            if h["status"] != "ok":
                print("  ERROR: API is not healthy. Start with: make up")
                return
        except Exception as e:
            print(f"  ERROR: Cannot reach API at {API_BASE} — {e}")
            print("  Start Aether first: make up")
            return

    print(f"{'─'*55}")

    # ── Submit jobs ───────────────────────────────────────────
    payload_fn  = PAYLOAD_FN[job_type]
    results     = []
    semaphore   = asyncio.Semaphore(concurrency)
    submit_start = time.monotonic()

    print(f"  Submitting {num_jobs} jobs...")
    async with httpx.AsyncClient() as client:
        tasks = [
            submit_job(client, payload_fn(i), results, semaphore)
            for i in range(num_jobs)
        ]
        await asyncio.gather(*tasks)

    submit_duration = time.monotonic() - submit_start
    ok_count   = sum(1 for r in results if r["status"] == "ok")
    err_count  = num_jobs - ok_count
    latencies  = [r["latency_ms"] for r in results if r["status"] == "ok"]
    job_ids    = [r["job_id"] for r in results if r.get("job_id")]

    print(f"  Submitted:   {ok_count}/{num_jobs} OK  ({err_count} errors)")
    print(f"  Submit time: {submit_duration:.2f}s")
    print(f"  Throughput:  {ok_count / submit_duration:.1f} submissions/sec")

    if latencies:
        print(f"\n  Submission latency:")
        print(f"    p50:  {statistics.median(latencies):.1f} ms")
        print(f"    p95:  {sorted(latencies)[int(len(latencies)*0.95)]:.1f} ms")
        print(f"    p99:  {sorted(latencies)[int(len(latencies)*0.99)]:.1f} ms")
        print(f"    max:  {max(latencies):.1f} ms")

    # ── Wait for execution ────────────────────────────────────
    if ok_count > 0:
        print(f"\n  Waiting for workers to execute jobs...")
        exec_start  = time.monotonic()
        final       = await poll_completion(job_ids, timeout=180)
        exec_duration = time.monotonic() - exec_start

        if not final.get("timeout"):
            print(f"  Execution time: {exec_duration:.2f}s")
            print(f"  Exec throughput: {ok_count / exec_duration:.1f} jobs/sec")

    # ── Final metrics from API ────────────────────────────────
    async with httpx.AsyncClient() as client:
        try:
            m = (await client.get(f"{API_BASE}/metrics", timeout=5)).json()
            print(f"\n  Final metrics from /metrics:")
            print(f"    p50 latency: {m.get('p50_latency_ms', 'N/A')} ms")
            print(f"    p95 latency: {m.get('p95_latency_ms', 'N/A')} ms")
            print(f"    p99 latency: {m.get('p99_latency_ms', 'N/A')} ms")
            print(f"    Done:        {m.get('done', 'N/A')}")
            print(f"    Dead:        {m.get('dead', 'N/A')}")
            print(f"    Workers:     {m.get('workers_alive', 'N/A')}")
        except Exception:
            pass

    # ── Save results ──────────────────────────────────────────
    output = {
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "config": {
            "num_jobs":    num_jobs,
            "concurrency": concurrency,
            "job_type":    job_type,
            "api_base":    API_BASE,
        },
        "submission": {
            "ok":               ok_count,
            "errors":           err_count,
            "duration_seconds": round(submit_duration, 3),
            "throughput_rps":   round(ok_count / submit_duration, 2),
            "p50_ms":           round(statistics.median(latencies), 2) if latencies else None,
            "p95_ms":           round(sorted(latencies)[int(len(latencies)*0.95)], 2) if latencies else None,
            "p99_ms":           round(sorted(latencies)[int(len(latencies)*0.99)], 2) if latencies else None,
        },
    }

    with open("scripts/load_test_results.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n  Results saved → scripts/load_test_results.json")
    print(f"{'═'*55}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Aether load test")
    parser.add_argument("--jobs",       type=int, default=100,       help="Number of jobs to submit")
    parser.add_argument("--concurrency",type=int, default=10,        help="Concurrent submissions")
    parser.add_argument("--type",       type=str, default="webhook",
                        choices=["webhook", "llm_task", "chain"],    help="Job type to test")
    args = parser.parse_args()

    asyncio.run(run_load_test(args.jobs, args.concurrency, args.type))


if __name__ == "__main__":
    main()
