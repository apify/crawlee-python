from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import AliasChoices, BeforeValidator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from crawlee._utils.docs import docs_group
from crawlee._utils.models import timedelta_ms

if TYPE_CHECKING:
    from typing_extensions import Self

__all__ = ['Configuration']


@docs_group('Data structures')
class Configuration(BaseSettings):
    """Configuration settings for the Crawlee project.

    This class stores common configurable parameters for Crawlee. Default values are provided for all settings,
    so typically, no adjustments are necessary. However, you may modify settings for specific use cases,
    such as changing the default storage directory, the default storage IDs, the timeout for internal
    operations, and more.

    Settings can also be configured via environment variables, prefixed with `CRAWLEE_`.
    """

    model_config = SettingsConfigDict(populate_by_name=True)

    internal_timeout: Annotated[timedelta | None, Field(alias='crawlee_internal_timeout')] = None
    """Timeout for the internal asynchronous operations."""

    verbose_log: Annotated[bool, Field(alias='crawlee_verbose_log')] = False
    """Whether to enable verbose logging."""

    default_browser_path: Annotated[
        str | None,
        Field(
            validation_alias=AliasChoices(
                'apify_default_browser_path',
                'crawlee_default_browser_path',
            )
        ),
    ] = None
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    disable_browser_sandbox: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_disable_browser_sandbox',
                'crawlee_disable_browser_sandbox',
            )
        ),
    ] = False
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    log_level: Annotated[
        Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        Field(
            validation_alias=AliasChoices(
                'apify_log_level',
                'crawlee_log_level',
            )
        ),
        BeforeValidator(lambda value: str(value).upper()),
    ] = 'INFO'
    """The logging level."""

    default_dataset_id: Annotated[
        str,
        Field(
            validation_alias=AliasChoices(
                'actor_default_dataset_id',
                'apify_default_dataset_id',
                'crawlee_default_dataset_id',
            )
        ),
    ] = 'default'
    """The default dataset ID."""

    default_key_value_store_id: Annotated[
        str,
        Field(
            validation_alias=AliasChoices(
                'actor_default_key_value_store_id',
                'apify_default_key_value_store_id',
                'crawlee_default_key_value_store_id',
            )
        ),
    ] = 'default'
    """The default key-value store ID."""

    default_request_queue_id: Annotated[
        str,
        Field(
            validation_alias=AliasChoices(
                'actor_default_request_queue_id',
                'apify_default_request_queue_id',
                'crawlee_default_request_queue_id',
            )
        ),
    ] = 'default'
    """The default request queue ID."""

    purge_on_start: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_purge_on_start',
                'crawlee_purge_on_start',
            )
        ),
    ] = True
    """Whether to purge the storage on the start."""

    write_metadata: Annotated[bool, Field(alias='crawlee_write_metadata')] = True
    """Whether to write the storage metadata."""

    persist_storage: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_persist_storage',
                'crawlee_persist_storage',
            )
        ),
    ] = True
    """Whether to persist the storage."""

    persist_state_interval: Annotated[
        timedelta_ms,
        Field(
            validation_alias=AliasChoices(
                'apify_persist_state_interval_millis',
                'crawlee_persist_state_interval_millis',
            )
        ),
    ] = timedelta(minutes=1)
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    system_info_interval: Annotated[
        timedelta_ms,
        Field(
            validation_alias=AliasChoices(
                'apify_system_info_interval_millis',
                'crawlee_system_info_interval_millis',
            )
        ),
    ] = timedelta(seconds=1)
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    max_used_cpu_ratio: Annotated[
        float,
        Field(
            validation_alias=AliasChoices(
                'apify_max_used_cpu_ratio',
                'crawlee_max_used_cpu_ratio',
            )
        ),
    ] = 0.95
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    memory_mbytes: Annotated[
        int | None,
        Field(
            validation_alias=AliasChoices(
                'actor_memory_mbytes',
                'apify_memory_mbytes',
                'crawlee_memory_mbytes',
            )
        ),
    ] = None
    """The maximum memory in megabytes. The `Snapshotter.max_memory_size` is set to this value."""

    available_memory_ratio: Annotated[
        float,
        Field(
            validation_alias=AliasChoices(
                'apify_available_memory_ratio',
                'crawlee_available_memory_ratio',
            )
        ),
    ] = 0.25
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    storage_dir: Annotated[
        str,
        Field(
            validation_alias=AliasChoices(
                'apify_local_storage_dir',
                'crawlee_storage_dir',
            ),
        ),
    ] = './storage'
    """The path to the storage directory."""

    chrome_executable_path: Annotated[
        str | None,
        Field(
            validation_alias=AliasChoices(
                'apify_chrome_executable_path',
                'crawlee_chrome_executable_path',
            )
        ),
    ] = None
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    headless: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_headless',
                'crawlee_headless',
            )
        ),
    ] = True
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    xvfb: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_xvfb',
                'crawlee_xvfb',
            )
        ),
    ] = False
    """This setting is currently unused. For more details, see https://github.com/apify/crawlee-python/issues/670."""

    @classmethod
    def get_global_configuration(cls) -> Self:
        """Retrieve the global instance of the configuration."""
        from crawlee import service_container

        if service_container.get_configuration_if_set() is None:
            service_container.set_configuration(cls())

        global_instance = service_container.get_configuration()

        if not isinstance(global_instance, cls):
            raise TypeError(
                f'Requested global configuration object of type {cls}, but {global_instance.__class__} was found'
            )

        return global_instance
