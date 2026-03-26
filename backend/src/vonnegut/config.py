# backend/src/vonnegut/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "VONNEGUT_"}

    database_url: str = "sqlite+aiosqlite:///./vonnegut.db"
    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:5174"]
    secret_key: str | None = None
    anthropic_api_key: str | None = None
    pipeline_row_limit: int = 100_000
    pipeline_batch_size: int = 1000
