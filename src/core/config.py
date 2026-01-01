from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path
import yaml
import os


class DataConfig(BaseSettings):
    cpid_root_dir: str


class SnapshotConfig(BaseSettings):
    source_name: str = "CPID_EDC_Metrics"
    timezone: str = "UTC"


class AppConfig(BaseSettings):
    env: str = "development"
    data: DataConfig
    snapshot: SnapshotConfig


def load_config() -> AppConfig:
    """
    Loads config based on APP_ENV environment variable.
    Defaults to development.
    """
    env = os.getenv("APP_ENV", "development")
    config_path = Path(f"config/{env}.yaml")

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    return AppConfig(**raw)
