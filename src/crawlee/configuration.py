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
    """Specifies the path to the browser executable. Currently primarily for Playwright-based features. This option
    is passed directly to Playwright's `browser_type.launch` method as `executable_path` argument. For more details,
    refer to the Playwright documentation:
    https://playwright.dev/docs/api/class-browsertype#browser-type-launch.
    """

    disable_browser_sandbox: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_disable_browser_sandbox',
                'crawlee_disable_browser_sandbox',
            )
        ),
    ] = False
    """Disables the sandbox for the browser. Currently primarily for Playwright-based features. This option
    is passed directly to Playwright's `browser_type.launch` method as `chromium_sandbox`. For more details,
    refer to the Playwright documentation:
    https://playwright.dev/docs/api/class-browsertype#browser-type-launch."""

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
    """The default dataset ID. This option is utilized by the storage client."""

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
    """The default key-value store ID. This option is utilized by the storage client."""

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
    """The default request queue ID. This option is utilized by the storage client."""

    purge_on_start: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_purge_on_start',
                'crawlee_purge_on_start',
            )
        ),
    ] = True
    """Whether to purge the storage on the start. This option is utilized by the `MemoryStorageClient`."""

    write_metadata: Annotated[bool, Field(alias='crawlee_write_metadata')] = True
    """Whether to write the storage metadata. This option is utilized by the `MemoryStorageClient`."""

    persist_storage: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_persist_storage',
                'crawlee_persist_storage',
            )
        ),
    ] = True
    """Whether to persist the storage. This option is utilized by the `MemoryStorageClient`."""

    persist_state_interval: Annotated[
        timedelta_ms,
        Field(
            validation_alias=AliasChoices(
                'apify_persist_state_interval_millis',
                'crawlee_persist_state_interval_millis',
            )
        ),
    ] = timedelta(minutes=1)
    """Interval at which `PersistState` events are emitted. The event ensures the state persistence during
    the crawler run. This option is utilized by the `EventManager`."""

    system_info_interval: Annotated[
        timedelta_ms,
        Field(
            validation_alias=AliasChoices(
                'apify_system_info_interval_millis',
                'crawlee_system_info_interval_millis',
            )
        ),
    ] = timedelta(seconds=1)
    """Interval at which `SystemInfo` events are emitted. The event represents the current status of the system.
    This option is utilized by the `LocalEventManager`."""

    max_used_cpu_ratio: Annotated[
        float,
        Field(
            validation_alias=AliasChoices(
                'apify_max_used_cpu_ratio',
                'crawlee_max_used_cpu_ratio',
            )
        ),
    ] = 0.95
    """The maximum CPU usage ratio. If the CPU usage exceeds this value, the system is considered overloaded.
    This option is used by the `Snapshotter`."""

    max_used_memory_ratio: Annotated[
        float,
        Field(
            validation_alias=AliasChoices(
                'apify_max_used_memory_ratio',
                'crawlee_max_used_memory_ratio',
            )
        ),
    ] = 0.9
    """The maximum memory usage ratio. If the memory usage exceeds this ratio, it is considered overloaded.
    This option is used by the `Snapshotter`."""

    max_event_loop_delay: Annotated[
        timedelta_ms,
        Field(
            validation_alias=AliasChoices(
                'apify_max_event_loop_delay_millis',
                'crawlee_max_event_loop_delay_millis',
            )
        ),
    ] = timedelta(milliseconds=50)
    """The maximum event loop delay. If the event loop delay exceeds this value, it is considered overloaded.
    This option is used by the `Snapshotter`."""

    max_client_errors: Annotated[
        int,
        Field(
            validation_alias=AliasChoices(
                'apify_max_client_errors',
                'crawlee_max_client_errors',
            )
        ),
    ] = 1
    """The maximum number of client errors (HTTP 429) allowed before the system is considered overloaded.
    This option is used by the `Snapshotter`."""

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
    """The maximum used memory in megabytes. This option is utilized by the `Snapshotter`."""

    available_memory_ratio: Annotated[
        float,
        Field(
            validation_alias=AliasChoices(
                'apify_available_memory_ratio',
                'crawlee_available_memory_ratio',
            )
        ),
    ] = 0.25
    """The maximum proportion of system memory to use. If `memory_mbytes` is not provided, this ratio is used to
    calculate the maximum memory. This option is utilized by the `Snapshotter`."""

    storage_dir: Annotated[
        str,
        Field(
            validation_alias=AliasChoices(
                'apify_local_storage_dir',
                'crawlee_storage_dir',
            ),
        ),
    ] = './storage'
    """The path to the storage directory. This option is utilized by the `MemoryStorageClient`."""

    chrome_executable_path: Annotated[
        str | None,
        Field(
            validation_alias=AliasChoices(
                'apify_chrome_executable_path',
                'crawlee_chrome_executable_path',
            )
        ),
    ] = None
    """This setting is currently unused."""

    headless: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_headless',
                'crawlee_headless',
            )
        ),
    ] = True
    """Whether to run the browser in headless mode. Currently primarily for Playwright-based features. This option
    is passed directly to Playwright's `browser_type.launch` method as `headless`. For more details,
    refer to the Playwright documentation:
    https://playwright.dev/docs/api/class-browsertype#browser-type-launch.
    """

    xvfb: Annotated[
        bool,
        Field(
            validation_alias=AliasChoices(
                'apify_xvfb',
                'crawlee_xvfb',
            )
        ),
    ] = False
    """This setting is currently unused."""

    @classmethod
    def get_global_configuration(cls) -> Self:
        """Retrieve the global instance of the configuration.

        Mostly for the backwards compatibility. It is recommended to use the `service_locator.get_configuration()`
        instead.
        """
        from crawlee import service_locator

        config = service_locator.get_configuration()

        if not isinstance(config, cls):
            raise TypeError(f'Requested global configuration object of type {cls}, but {config.__class__} was found')

        return config
