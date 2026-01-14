"""
Executors handle the actual work for each job type.

webhook  → HTTP POST to a URL
llm_task → Groq LLM call, result POSTed to callback_url
chain    → Spawns next step in DAG, passing previous output forward
"""

from __future__ import annotations
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx
from groq import AsyncGroq

from app.config import get_settings
from app.logger import get_logger, log_job_event
from app.models import JobType, Priority, generate_job_id

logger = get_logger(__name__)
settings = get_settings()


async def execute_webhook(payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    """POST (or configured method) to callback URL, return response summary."""
    url     = payload["url"]
    method  = payload.get("method", "POST").upper()
    headers = payload.get("headers", {})
    body    = payload.get("body", {})

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(method, url, json=body, headers=headers)
        response.raise_for_status()
        try:
            result_body = response.json()
        except Exception:
            result_body = {"raw": response.text}

        return {
            "status_code": response.status_code,
            "response": result_body,
            "url": url,
        }


async def execute_llm_task(payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    """
    Call Groq LLM and POST the result to callback_url.
    Returns the LLM response text and callback status.
    """
    prompt        = payload["prompt"]
    system_prompt = payload.get("system_prompt", "You are a helpful assistant.")
    callback_url  = payload["callback_url"]
    model         = payload.get("model") or settings.groq_model

    client = AsyncGroq(api_key=settings.groq_api_key)

    completion = await asyncio.wait_for(
        client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=1024,
        ),
        timeout=timeout,
    )

    llm_result = completion.choices[0].message.content
    usage      = dict(completion.usage) if completion.usage else {}

    # POST result to callback
    async with httpx.AsyncClient(timeout=30) as http:
        cb_response = await http.post(callback_url, json={
            "result":    llm_result,
            "model":     model,
            "usage":     usage,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        cb_status = cb_response.status_code

    return {
        "llm_result":       llm_result,
        "model":            model,
        "usage":            usage,
        "callback_status":  cb_status,
    }


async def execute_chain_step(
    job_id: str,
    payload: Dict[str, Any],
    previous_result: Optional[Dict[str, Any]],
    timeout: int,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Execute one step in a chain.
    Returns (step_result, next_job_id_if_spawned).

    Chain payload structure:
        {
          "steps": [
            {"job_type": "llm_task", "payload": {...}},
            {"job_type": "webhook",  "payload": {...}}
          ],
          "callback_url": "https://...",
          "current_step": 0,
          "accumulated_results": []
        }

    Each step's payload is merged with {"previous_result": <prior output>}.
    """
    from app.db import create_job as db_create_job
    from app.redis_client import enqueue_job

    steps               = payload["steps"]
    callback_url        = payload["callback_url"]
    current_step        = payload.get("current_step", 0)
    accumulated_results = payload.get("accumulated_results", [])

    step        = steps[current_step]
    step_type   = step["job_type"]
    step_payload = {**step["payload"]}

    # Inject previous step's output into this step
    if previous_result:
        step_payload["previous_result"] = previous_result

    # Execute this step
    if step_type == JobType.WEBHOOK:
        result = await execute_webhook(step_payload, timeout)
    elif step_type == JobType.LLM_TASK:
        # For mid-chain LLM tasks, override callback with internal accumulator
        # Final step uses real callback_url
        is_last = current_step == len(steps) - 1
        if not is_last:
            step_payload["callback_url"] = "http://localhost:8000/internal/chain-callback"
        result = await execute_llm_task(step_payload, timeout)
    else:
        raise ValueError(f"Unsupported step type in chain: {step_type}")

    accumulated_results.append({"step": current_step, "result": result})

    next_job_id = None
    next_step   = current_step + 1

    if next_step < len(steps):
        # Spawn next step as a new job immediately
        next_job_id = generate_job_id()
        next_payload = {
            "steps":               steps,
            "callback_url":        callback_url,
            "current_step":        next_step,
            "accumulated_results": accumulated_results,
        }
        job_data = {
            "id":               next_job_id,
            "name":             f"chain-step-{next_step}",
            "job_type":         JobType.CHAIN.value,
            "priority":         Priority.HIGH.value,
            "payload":          next_payload,
            "run_at":           datetime.now(timezone.utc),
            "cron":             None,
            "max_retries":      2,
            "timeout_seconds":  timeout,
            "parent_job_id":    job_id,
            "chain_step_index": next_step,
        }
        await db_create_job(job_data)
        await enqueue_job(next_job_id, datetime.now(timezone.utc).timestamp(), Priority.HIGH.value)
        logger.info("chain.step_spawned", extra={"parent": job_id, "next": next_job_id, "step": next_step})
    else:
        # Final step — POST all accumulated results to callback_url
        async with httpx.AsyncClient(timeout=30) as http:
            await http.post(callback_url, json={
                "chain_complete":     True,
                "steps_executed":     len(steps),
                "accumulated_results": accumulated_results,
                "timestamp":          datetime.now(timezone.utc).isoformat(),
            })
        logger.info("chain.complete", extra={"job_id": job_id, "steps": len(steps)})

    return result, next_job_id


async def dispatch(
    job_id: str,
    job_type: str,
    payload: Dict[str, Any],
    timeout: int,
    previous_result: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Route to correct executor based on job type."""
    if job_type == JobType.WEBHOOK:
        return await execute_webhook(payload, timeout)

    elif job_type == JobType.LLM_TASK:
        return await execute_llm_task(payload, timeout)

    elif job_type == JobType.CHAIN:
        result, _ = await execute_chain_step(job_id, payload, previous_result, timeout)
        return result

    else:
        raise ValueError(f"Unknown job type: {job_type}")
