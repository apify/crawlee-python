from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Configuration of the Crawler.

    Args:
        default_dataset_id: The default dataset ID.
        default_key_value_store_id: The default key-value store ID.
        default_request_queue_id: The default request queue ID.
        purge_on_start: Whether to purge the storage on start.
    """

    default_dataset_id: Annotated[str, Field(alias='crawlee_default_dataset_id')] = 'default'
    default_key_value_store_id: Annotated[str, Field(alias='crawlee_default_key_value_store_id')] = 'default'
    default_request_queue_id: Annotated[str, Field(alias='crawlee_default_request_queue_id')] = 'default'
    purge_on_start: Annotated[bool, Field(alias='crawlee_purge_on_start')] = False
