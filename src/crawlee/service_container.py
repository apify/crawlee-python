from __future__ import annotations

from lazy_object_proxy import Proxy as LazyObjectProxy

from crawlee._utils.docs import docs_group
from crawlee.base_storage_client._base_storage_client import BaseStorageClient
from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events._event_manager import EventManager

__all__ = [
    'get_configuration',
    'get_event_manager',
    'get_storage_client',
    'set_configuration',
    'set_event_manager',
    'set_storage_client',
]


class _ServiceLocator:
    """Service locator for managing the services used by Crawlee.

    All services are initialized to its default value lazily.
    """

    def __init__(self) -> None:
        self._configuration: Configuration | None = None
        self._event_manager: EventManager | None = None
        self._storage_client: BaseStorageClient | None = None

        # Flags to check if the services were already set.
        self._configuration_was_set = False
        self._event_manager_was_set = False
        self._storage_client_was_set = False

    @property
    def configuration(self) -> Configuration:
        if self._configuration is None:
            self._configuration = Configuration()

        return self._configuration

    @configuration.setter
    def configuration(self, value: Configuration) -> None:
        self._configuration = value
        self._configuration_was_set = True

    @property
    def storage_client(self) -> BaseStorageClient:
        if self._storage_client is None:
            from crawlee.memory_storage_client import MemoryStorageClient

            self._storage_client = MemoryStorageClient()

        return self._storage_client

    @storage_client.setter
    def storage_client(self, value: BaseStorageClient) -> None:
        self._storage_client = value
        self._storage_client_was_set = True

    @property
    def event_manager(self) -> EventManager:
        if self._event_manager is None:
            from crawlee.events import LocalEventManager

            self._event_manager = LocalEventManager()

        return self._event_manager

    @event_manager.setter
    def event_manager(self, value: EventManager) -> None:
        self._event_manager = value
        self._event_manager_was_set = True

    @property
    def configuration_was_set(self) -> bool:
        return self._configuration_was_set

    @property
    def event_manager_was_set(self) -> bool:
        return self._event_manager_was_set

    @property
    def storage_client_was_set(self) -> bool:
        return self._storage_client_was_set


_service_locator = _ServiceLocator()


@docs_group('Functions')
def get_configuration() -> Configuration:
    """Get the configuration."""
    proxy_config: Configuration = LazyObjectProxy(lambda: _service_locator.configuration)
    return proxy_config


@docs_group('Functions')
def set_configuration(configuration: Configuration) -> None:
    """Set the configuration.

    Args:
        configuration: The configuration to set.

    Raises:
        ServiceConflictError: If the configuration was already set.
    """
    if _service_locator.configuration_was_set:
        raise ServiceConflictError(Configuration, configuration, _service_locator.configuration)

    _service_locator.configuration = configuration


@docs_group('Functions')
def get_event_manager() -> EventManager:
    """Get the event manager."""
    proxy_event_manager: EventManager = LazyObjectProxy(lambda: _service_locator.event_manager)
    return proxy_event_manager


@docs_group('Functions')
def set_event_manager(event_manager: EventManager) -> None:
    """Set the event manager.

    Args:
        event_manager: The event manager to set.

    Raises:
        ServiceConflictError: If the event manager was already set.
    """
    if _service_locator.event_manager_was_set:
        raise ServiceConflictError(EventManager, event_manager, _service_locator.event_manager)

    _service_locator.event_manager = event_manager


@docs_group('Functions')
def get_storage_client() -> BaseStorageClient:
    """Get the storage client."""
    proxy_storage_client: BaseStorageClient = LazyObjectProxy(lambda: _service_locator.storage_client)
    return proxy_storage_client


@docs_group('Functions')
def set_storage_client(storage_client: BaseStorageClient) -> None:
    """Set the storage client.

    Args:
        storage_client: The storage client to set.

    Raises:
        ServiceConflictError: If the storage client was already set.
    """
    if _service_locator.storage_client_was_set:
        raise ServiceConflictError(BaseStorageClient, storage_client, _service_locator.storage_client)

    _service_locator.storage_client = storage_client
