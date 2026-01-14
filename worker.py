"""
Worker entrypoint.

Usage:
    WORKER_ID=worker-1 python worker.py
    WORKER_ID=worker-2 python worker.py
    WORKER_ID=worker-3 python worker.py

Each worker is an independent process. They share Redis + Postgres
but claim jobs independently via the atomic Lua script.
"""

import asyncio
import signal
import sys

from app.scheduler import start_worker, stop_worker
from app.logger import get_logger
from app.config import get_settings

logger   = get_logger("worker")
settings = get_settings()


async def main():
    logger.info("worker.starting", extra={"worker_id": settings.worker_id})

    loop = asyncio.get_running_loop()

    def _handle_signal(sig):
        logger.info("worker.signal_received", extra={"signal": sig.name})
        asyncio.create_task(stop_worker())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    await start_worker()


if __name__ == "__main__":
    # Graceful shutdown: SIGTERM waits for in-flight jobs to finish
asyncio.run(main())
