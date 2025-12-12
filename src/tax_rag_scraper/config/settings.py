"""Application settings and configuration."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    # Crawler settings
    MAX_REQUESTS_PER_CRAWL: int = 100
    MAX_CONCURRENCY: int = 5
    REQUEST_TIMEOUT: int = 30

    # Storage
    STORAGE_DIR: str = "storage"

    class Config:
        env_file = ".env"
        case_sensitive = True
