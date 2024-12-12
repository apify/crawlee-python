from __future__ import annotations

from crawlee._utils.docs import docs_group
from crawlee.base_storage_client._base_storage_client import BaseStorageClient
from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events._event_manager import EventManager


@docs_group('Classes')
class ServiceLocator:
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

    def get_configuration(self) -> Configuration:
        """Get the configuration."""
        if self._configuration is None:
            self._configuration = Configuration()

        return self._configuration

    def set_configuration(self, configuration: Configuration) -> None:
        """Set the configuration.

        Args:
            configuration: The configuration to set.

        Raises:
            ServiceConflictError: If the configuration was already set.
        """
        if self._configuration_was_set:
            raise ServiceConflictError(Configuration, configuration, self._configuration)

        self._configuration = configuration
        self._configuration_was_set = True

    def get_event_manager(self) -> EventManager:
        """Get the event manager."""
        if self._event_manager is None:
            from crawlee.events import LocalEventManager

            self._event_manager = LocalEventManager()

        return self._event_manager

    def set_event_manager(self, event_manager: EventManager) -> None:
        """Set the event manager.

        Args:
            event_manager: The event manager to set.

        Raises:
            ServiceConflictError: If the event manager was already set.
        """
        if self._event_manager_was_set:
            raise ServiceConflictError(EventManager, event_manager, self._event_manager)

        self._event_manager = event_manager
        self._event_manager_was_set = True

    def get_storage_client(self) -> BaseStorageClient:
        """Get the storage client."""
        if self._storage_client is None:
            from crawlee.memory_storage_client import MemoryStorageClient

            self._storage_client = MemoryStorageClient.from_config()

        return self._storage_client

    def set_storage_client(self, storage_client: BaseStorageClient) -> None:
        """Set the storage client.

        Args:
            storage_client: The storage client to set.

        Raises:
            ServiceConflictError: If the storage client was already set.
        """
        if self._storage_client_was_set:
            raise ServiceConflictError(BaseStorageClient, storage_client, self._storage_client)

        self._storage_client = storage_client
        self._storage_client_was_set = True


service_locator = ServiceLocator()
