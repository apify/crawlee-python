from dataclasses import dataclass, field

from crawlee._utils.env_vars import CrawleeEnvVars, fetch_and_parse_env_var


@dataclass
class Config:
    """Configuration of the Crawler.

    Args:
        default_dataset_id: The default dataset ID.
        default_key_value_store_id: The default key-value store ID.
        default_request_queue_id: The default request queue ID.
        purge_on_start: Whether to purge the storage on start.
    """

    default_dataset_id: str = 'default'
    default_key_value_store_id: str = 'default'
    default_request_queue_id: str = 'default'
    purge_on_start: bool = field(
        default_factory=lambda: fetch_and_parse_env_var(CrawleeEnvVars.PURGE_ON_START, default=False)
    )
