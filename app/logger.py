import logging
import sys
from pythonjsonlogger import jsonlogger
from app.config import get_settings


def get_logger(name: str) -> logging.Logger:
    settings = get_settings()
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger


def log_job_event(
    logger: logging.Logger,
    event: str,
    job_id: str,
    worker_id: str = None,
    extra: dict = None,
):
    """Structured log for every job lifecycle event."""
    payload = {
        "event": event,
        "job_id": job_id,
        "worker_id": worker_id or get_settings().worker_id,
    }
    if extra:
        payload.update(extra)
    logger.info(event, extra=payload)
