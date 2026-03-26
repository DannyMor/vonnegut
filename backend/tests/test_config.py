# backend/tests/test_config.py
from vonnegut.config import Settings


def test_default_settings():
    settings = Settings()
    assert settings.database_url == "sqlite+aiosqlite:///./vonnegut.db"
    assert settings.cors_origins == ["http://localhost:5173", "http://localhost:5174"]
    assert settings.pipeline_row_limit == 100_000
    assert settings.pipeline_batch_size == 1000


def test_custom_settings_from_env(monkeypatch):
    monkeypatch.setenv("VONNEGUT_DATABASE_URL", "sqlite+aiosqlite:///custom.db")
    monkeypatch.setenv("VONNEGUT_CORS_ORIGINS", '["http://localhost:3000"]')
    settings = Settings()
    assert settings.database_url == "sqlite+aiosqlite:///custom.db"
    assert settings.cors_origins == ["http://localhost:3000"]
