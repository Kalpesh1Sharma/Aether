"""
Executors handle the actual work for each job type.
webhook → HTTP POST to a URL
"""

from __future__ import annotations
from typing import Any, Dict
import httpx
from app.logger import get_logger

logger = get_logger(__name__)


async def execute_webhook(payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
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
        return {"status_code": response.status_code, "response": result_body, "url": url}


async def dispatch(job_id: str, job_type: str, payload: Dict[str, Any], timeout: int, previous_result=None) -> Dict[str, Any]:
    from app.models import JobType
    if job_type == JobType.WEBHOOK:
        return await execute_webhook(payload, timeout)
    raise ValueError(f"Unsupported job type: {job_type}")
