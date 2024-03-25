from enum import Enum

DEFAULT_API_PARAM_LIMIT = 1000

MAX_PAYLOAD_SIZE_BYTES = 9437184  # 9MB

REQUEST_QUEUE_HEAD_MAX_LIMIT = 1000


class CrawleeEnvVars(str, Enum):
    """Enum for the environment variables used by Crawlee."""

    LOCAL_STORAGE_DIR = 'CRAWLEE_LOCAL_STORAGE_DIR'
    PERSIST_STORAGE = 'CRAWLEE_PERSIST_STORAGE'
