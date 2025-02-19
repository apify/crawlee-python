from __future__ import annotations

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events import EventManager
from crawlee.storage_clients import StorageClient


@docs_group('Classes')
class ServiceLocator:
    """Service locator for managing the services used by Crawlee.

    All services are initialized to its default value lazily.
    """

    def __init__(self) -> None:
        self._configuration: Configuration | None = None
        self._event_manager: EventManager | None = None
        self._storage_client: StorageClient | None = None

        # Flags to check if the services were already set.
        self._configuration_was_retrieved = False
        self._event_manager_was_retrieved = False
        self._storage_client_was_retrieved = False

    def get_configuration(self) -> Configuration:
        """Get the configuration."""
        if self._configuration is None:
            self._configuration = Configuration()

        self._configuration_was_retrieved = True
        return self._configuration

    def set_configuration(self, configuration: Configuration) -> None:
        """Set the configuration.

        Args:
            configuration: The configuration to set.

        Raises:
            ServiceConflictError: If the configuration has already been retrieved before.
        """
        if self._configuration_was_retrieved:
            raise ServiceConflictError(Configuration, configuration, self._configuration)

        self._configuration = configuration

    def get_event_manager(self) -> EventManager:
        """Get the event manager."""
        if self._event_manager is None:
            from crawlee.events import LocalEventManager

            self._event_manager = (
                LocalEventManager().from_config(config=self._configuration)
                if self._configuration
                else LocalEventManager.from_config()
            )

        self._event_manager_was_retrieved = True
        return self._event_manager

    def set_event_manager(self, event_manager: EventManager) -> None:
        """Set the event manager.

        Args:
            event_manager: The event manager to set.

        Raises:
            ServiceConflictError: If the event manager has already been retrieved before.
        """
        if self._event_manager_was_retrieved:
            raise ServiceConflictError(EventManager, event_manager, self._event_manager)

        self._event_manager = event_manager

    def get_storage_client(self) -> StorageClient:
        """Get the storage client."""
        if self._storage_client is None:
            from crawlee.storage_clients import MemoryStorageClient

            self._storage_client = (
                MemoryStorageClient.from_config(config=self._configuration)
                if self._configuration
                else MemoryStorageClient.from_config()
            )

        self._storage_client_was_retrieved = True
        return self._storage_client

    def set_storage_client(self, storage_client: StorageClient) -> None:
        """Set the storage client.

        Args:
            storage_client: The storage client to set.

        Raises:
            ServiceConflictError: If the storage client has already been retrieved before.
        """
        if self._storage_client_was_retrieved:
            raise ServiceConflictError(StorageClient, storage_client, self._storage_client)

        self._storage_client = storage_client


service_locator = ServiceLocator()
