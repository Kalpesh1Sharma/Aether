"""
Worker entrypoint.

Runs the Aether scheduler loop alongside a minimal HTTP health server
so Render's health check passes on free Web Service tier.

Usage:
    WORKER_ID=worker-1 python worker.py
"""

import asyncio
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from app.scheduler import start_worker, stop_worker
from app.logger import get_logger
from app.config import get_settings

logger   = get_logger("worker")
settings = get_settings()


# ── Minimal HTTP server for Render health check ───────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/"):
            body = b'{"status":"ok","role":"worker"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress access logs


def run_health_server(port: int = 8000):
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    logger.info("worker.starting", extra={"worker_id": settings.worker_id})

    # Start health server in background thread (for Render health check)
    port = int(__import__("os").environ.get("PORT", 8000))
    health_thread = threading.Thread(
        target=run_health_server, args=(port,), daemon=True
    )
    health_thread.start()
    logger.info("worker.health_server_started", extra={"port": port})

    loop = asyncio.get_running_loop()

    def _handle_signal(sig):
        logger.info("worker.signal_received", extra={"signal": sig.name})
        asyncio.create_task(stop_worker())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    await start_worker()


if __name__ == "__main__":
    asyncio.run(main())