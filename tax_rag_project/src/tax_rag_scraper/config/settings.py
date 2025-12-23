"""Application settings and configuration."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    # Crawler settings
    MAX_REQUESTS_PER_CRAWL: int = 100
    MAX_CONCURRENCY: int = 5
    REQUEST_TIMEOUT: int = 30
    USER_AGENT: str = 'CRA-Tax-Documentation-Bot/1.0'

    # Storage
    STORAGE_DIR: str = 'storage'
    DATASET_NAME: str = 'cra_tax_documents'
    KV_STORE_NAME: str = 'crawler_metadata'

    # Rate limiting
    MAX_REQUESTS_PER_MINUTE: int = 60
    MIN_DELAY_BETWEEN_REQUESTS_MS: int = 100
    MAX_DELAY_BETWEEN_REQUESTS_MS: int = 500
    MIN_REQUEST_DELAY: float = 1.0
    MAX_REQUEST_DELAY: float = 3.0
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_MULTIPLIER: float = 2.0

    # Respect robots.txt
    RESPECT_ROBOTS_TXT: bool = True

    # Deep crawling
    MAX_CRAWL_DEPTH: int = 2
    FOLLOW_LINKS: bool = True

    # Qdrant Cloud configuration
    # Get credentials at https://cloud.qdrant.io
    QDRANT_URL: str = ''  # Required when USE_QDRANT is True: https://your-cluster.cloud.qdrant.io
    QDRANT_API_KEY: str = ''  # Required when USE_QDRANT is True: Your Qdrant Cloud API key
    QDRANT_COLLECTION: str = 'tax_documents'
    USE_QDRANT: bool = True

    # Embeddings configuration
    # OpenAI API key is REQUIRED when USE_QDRANT is True
    EMBEDDING_MODEL: str = 'text-embedding-3-small'
    EMBEDDING_BATCH_SIZE: int = 5  # Reduced from 10 to avoid rate limits
    OPENAI_API_KEY: str = ''  # Required when USE_QDRANT is True: Get from https://platform.openai.com/api-keys

    # Document processing
    CHUNK_SIZE: int = 800
    CHUNK_OVERLAP: int = 200
    MIN_CHUNK_SIZE: int = 100
    MAX_CHUNK_SIZE: int = 1500

    # Logging
    LOG_LEVEL: str = 'INFO'
    LOG_FORMAT: str = 'text'
    LOG_FILE: str = ''

    # Crawler targets
    START_URLS: str = 'https://www.canada.ca/en/revenue-agency.html'
    INCLUDE_URL_PATTERNS: str = '/en/revenue-agency/.*,/en/services/tax/.*'
    EXCLUDE_URL_PATTERNS: str = '/login,/search,/auth'

    # Deployment
    ENVIRONMENT: str = 'development'
    DEBUG: bool = False
    ALERT_WEBHOOK_URL: str = ''
    HEALTH_CHECK_PORT: int = 8080

    class Config:
        """Pydantic configuration for settings."""

        env_file = '.env'
        case_sensitive = True
