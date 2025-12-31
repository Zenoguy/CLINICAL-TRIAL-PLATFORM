"""Configuration management using Pydantic settings."""
from typing import Literal
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings."""

    # Environment
    environment: Literal['development', 'staging', 'production'] = 'development'
    debug: bool = True

    # Database
    database_url: str = Field(
        default="postgresql://ctadmin:dev_password@localhost:5432/clinical_trial_db",
        alias="DATABASE_URL"
    )
    database_pool_size: int = 20
    database_max_overflow: int = 10

    # Messaging
    rabbitmq_url: str = Field(
        default="amqp://admin:dev_password@localhost:5672/",
        alias="RABBITMQ_URL"
    )
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        alias="REDIS_URL"
    )

    # Latency Tiers
    p0_target_latency_seconds: int = 300    # 5 min
    p1_target_latency_seconds: int = 900    # 15 min
    p2_target_latency_seconds: int = 3600   # 1 hour
    p3_target_latency_seconds: int = 21600  # 6 hours

    # Queue Configuration
    max_queue_depth: int = 1000
    retry_max_attempts: int = 3
    retry_backoff_factor: int = 2

    # Monitoring
    prometheus_port: int = 8001
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
