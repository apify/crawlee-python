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

    # Rate limiting
    MAX_REQUESTS_PER_MINUTE: int = 60
    MIN_DELAY_BETWEEN_REQUESTS_MS: int = 100
    MAX_DELAY_BETWEEN_REQUESTS_MS: int = 500

    # Respect robots.txt
    RESPECT_ROBOTS_TXT: bool = True

    # Deep crawling (NEW)
    MAX_CRAWL_DEPTH: int = 2
    FOLLOW_LINKS: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True
