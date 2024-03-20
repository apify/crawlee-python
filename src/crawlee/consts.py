from enum import Enum

DEFAULT_API_PARAM_LIMIT = 1000


class CrawleeEnvVars(str, Enum):
    """Enum for the environment variables used by Crawlee."""

    LOCAL_STORAGE_DIR = 'CRAWLEE_LOCAL_STORAGE_DIR'
    PERSIST_STORAGE = 'CRAWLEE_PERSIST_STORAGE'
