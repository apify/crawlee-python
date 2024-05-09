# ruff: noqa: TCH003 TCH002

from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Configuration(BaseSettings):
    """Configuration of the Crawler.

    Args:
        internal_timeout: timeout for internal operations such as marking a request as processed
        verbose_log: allows verbose logging
        default_storage_id: The default storage ID.
        purge_on_start: Whether to purge the storage on start.
    """

    model_config = SettingsConfigDict(populate_by_name=True)

    internal_timeout: Annotated[timedelta | None, Field(alias='crawlee_internal_timeout', default=None)]
    verbose_log: Annotated[bool, Field(alias='crawlee_verbose_log', default=False)]
    default_storage_id: Annotated[str, Field(alias='crawlee_default_storage_id', default='default')]
    purge_on_start: Annotated[bool, Field(alias='crawlee_purge_on_start', default=True)]
    write_metadata: Annotated[bool, Field(alias='crawlee_write_metadata', default=True)]
    persist_storage: Annotated[bool, Field(alias='crawlee_persist_storage', default=True)]
    local_storage_dir: Annotated[str, Field(alias='crawlee_local_storage_dir', default='./storage')]
