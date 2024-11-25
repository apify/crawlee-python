from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from crawlee.base_storage_client._base_storage_client import BaseStorageClient
    from crawlee.configuration import Configuration
    from crawlee.events._event_manager import EventManager

__all__ = [
    'get_configuration',
    'get_event_manager',
    'get_storage_client',
    'set_configuration',
    'set_event_manager',
    'set_storage_client',
]


@dataclass
class _ServiceLocator:
    """Service locator for managing the services used by Crawlee.

    All services are initialized to its default value lazily.
    """

    _configuration: Configuration | None = field(default=None, init=False)
    _event_manager: EventManager | None = field(default=None, init=False)
    _storage_client: BaseStorageClient | None = field(default=None, init=False)

    @property
    def configuration(self) -> Configuration:
        if self._configuration is None:
            from crawlee.configuration import Configuration

            self._configuration = Configuration()

        return self._configuration

    @configuration.setter
    def configuration(self, value: Configuration) -> None:
        self._configuration = value

    @property
    def storage_client(self) -> BaseStorageClient:
        if self._storage_client is None:
            from crawlee.memory_storage_client import MemoryStorageClient

            self._storage_client = MemoryStorageClient()

        return self._storage_client

    @storage_client.setter
    def storage_client(self, value: BaseStorageClient) -> None:
        self._storage_client = value

    @property
    def event_manager(self) -> EventManager:
        if self._event_manager is None:
            from crawlee.events import LocalEventManager

            self._event_manager = LocalEventManager()

        return self._event_manager

    @event_manager.setter
    def event_manager(self, value: EventManager) -> None:
        self._event_manager = value


_service_locator = _ServiceLocator()


@docs_group('Functions')
def get_configuration() -> Configuration:
    """Get the configuration."""
    return _service_locator.configuration


@docs_group('Functions')
def set_configuration(configuration: Configuration) -> None:
    """Set the configuration."""
    _service_locator.configuration = configuration


@docs_group('Functions')
def get_event_manager() -> EventManager:
    """Get the event manager."""
    return _service_locator.event_manager


@docs_group('Functions')
def set_event_manager(event_manager: EventManager) -> None:
    """Set the event manager."""
    _service_locator.event_manager = event_manager


@docs_group('Functions')
def get_storage_client() -> BaseStorageClient:
    """Get the storage client."""
    return _service_locator.storage_client


@docs_group('Functions')
def set_storage_client(storage_client: BaseStorageClient) -> None:
    """Set the storage client."""
    _service_locator.storage_client = storage_client
