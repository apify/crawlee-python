from __future__ import annotations

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
    return _service_locator.configuration


@docs_group('Functions')
def set_configuration(
    configuration: Configuration,
    *,
    force: bool = False,
) -> None:
    """Set the configuration.

    Args:
        configuration: The configuration to set.
        force: If True, the configuration will be set even if it was already set.

    Raises:
        ServiceConflictError: If the configuration was already set.
    """
    if _service_locator.configuration_was_set and not force:
        raise ServiceConflictError(Configuration, configuration, _service_locator.configuration)

    _service_locator.configuration = configuration


@docs_group('Functions')
def get_event_manager() -> EventManager:
    """Get the event manager."""
    return _service_locator.event_manager


@docs_group('Functions')
def set_event_manager(
    event_manager: EventManager,
    *,
    force: bool = False,
) -> None:
    """Set the event manager.

    Args:
        event_manager: The event manager to set.
        force: If True, the event manager will be set even if it was already set.

    Raises:
        ServiceConflictError: If the event manager was already set.
    """
    if _service_locator.event_manager_was_set and not force:
        raise ServiceConflictError(EventManager, event_manager, _service_locator.event_manager)

    _service_locator.event_manager = event_manager


@docs_group('Functions')
def get_storage_client() -> BaseStorageClient:
    """Get the storage client."""
    return _service_locator.storage_client


@docs_group('Functions')
def set_storage_client(
    storage_client: BaseStorageClient,
    *,
    force: bool = False,
) -> None:
    """Set the storage client.

    Args:
        storage_client: The storage client to set.
        force: If True, the storage client will be set even if it was already set.

    Raises:
        ServiceConflictError: If the storage client was already set.
    """
    if _service_locator.storage_client_was_set and not force:
        raise ServiceConflictError(BaseStorageClient, storage_client, _service_locator.storage_client)

    _service_locator.storage_client = storage_client
