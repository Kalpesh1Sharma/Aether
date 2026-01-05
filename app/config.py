from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql://aether:aether@localhost:5432/aether"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # LLM
    groq_api_key: str = ""
    groq_model: str = "llama3-8b-8192"

    # Worker
    worker_id: str = "worker-1"
    worker_concurrency: int = 10
    worker_heartbeat_interval: int = 10
    worker_job_claim_timeout: int = 300  # seconds before job is reclaimed

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Scheduler
    claim_batch_size: int = 5
    poll_interval_ms: int = 200
    dlq_max_retries: int = 3

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
