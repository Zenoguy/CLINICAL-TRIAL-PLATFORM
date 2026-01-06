from pathlib import Path
from functools import lru_cache
import os
import yaml

from pydantic import Field
from pydantic_settings import BaseSettings


# ---------------------------------------------------------------------
# Data paths / dataset configuration (YAML-driven)
# ---------------------------------------------------------------------
class DataConfig(BaseSettings):
    cpid_root_dir: str


# ---------------------------------------------------------------------
# Snapshot / ingestion metadata
# ---------------------------------------------------------------------
class SnapshotConfig(BaseSettings):
    source_name: str = "CPID_EDC_Metrics"
    timezone: str = "UTC"


# ---------------------------------------------------------------------
# Supabase configuration (ENV-ONLY, never YAML)
# ---------------------------------------------------------------------
class SupabaseConfig(BaseSettings):
    supabase_url: str = Field(..., alias="SUPABASE_URL")
    service_role_key: str = Field(..., alias="SUPABASE_SERVICE_ROLE_KEY")

    class Config:
        env_file = ".env"
        extra = "ignore"


# ---------------------------------------------------------------------
# Root application config
# ---------------------------------------------------------------------
class AppConfig(BaseSettings):
    env: str = "development"
    data: DataConfig
    snapshot: SnapshotConfig
    supabase: SupabaseConfig


# ---------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------
def load_config() -> AppConfig:
    """
    Load application config.

    - Structural config comes from config/{env}.yaml
    - Secrets (Supabase) come strictly from environment variables
    """
    env = os.getenv("APP_ENV", "development")
    config_path = Path(f"config/{env}.yaml")

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f) or {}

    # Inject empty supabase block — values resolved from ENV
    raw["supabase"] = {}

    return AppConfig(**raw)


# ---------------------------------------------------------------------
# Singleton accessor (IMPORTANT)
# ---------------------------------------------------------------------
@lru_cache
def get_settings() -> AppConfig:
    return load_config()
