from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events import EventManager, LocalEventManager
from crawlee.storage_clients import FileSystemStorageClient, StorageClient

if TYPE_CHECKING:
    from crawlee.storages._storage_instance_manager import StorageInstanceManager

from logging import getLogger

logger = getLogger(__name__)


@docs_group('Configuration')
class ServiceLocator:
    """Service locator for managing the services used by Crawlee.

    All services are initialized to its default value lazily.
    """

    def __init__(
        self,
        configuration: Configuration | None = None,
        event_manager: EventManager | None = None,
        storage_client: StorageClient | None = None,
    ) -> None:
        self._configuration = configuration
        self._event_manager = event_manager
        self._storage_client = storage_client
        self._storage_instance_manager: StorageInstanceManager | None = None

    def get_configuration(self) -> Configuration:
        """Get the configuration."""
        if self._configuration is None:
            logger.warning('No configuration set, implicitly creating and using default Configuration.')
            self._configuration = Configuration()

        return self._configuration

    def set_configuration(self, configuration: Configuration) -> None:
        """Set the configuration.

        Args:
            configuration: The configuration to set.

        Raises:
            ServiceConflictError: If the configuration has already been retrieved before.
        """
        if self._configuration is configuration:
            # Same instance, no need to anything
            return
        if self._configuration:
            raise ServiceConflictError(Configuration, configuration, self._configuration)

        self._configuration = configuration

    def get_event_manager(self) -> EventManager:
        """Get the event manager."""
        if self._event_manager is None:
            logger.warning('No event manager set, implicitly creating and using default LocalEventManager.')
            self._event_manager = (
                LocalEventManager().from_config(config=self._configuration)
                if self._configuration
                else LocalEventManager.from_config()
            )

        return self._event_manager

    def set_event_manager(self, event_manager: EventManager) -> None:
        """Set the event manager.

        Args:
            event_manager: The event manager to set.

        Raises:
            ServiceConflictError: If the event manager has already been retrieved before.
        """
        if self._event_manager is event_manager:
            # Same instance, no need to anything
            return
        if self._event_manager:
            raise ServiceConflictError(EventManager, event_manager, self._event_manager)

        self._event_manager = event_manager

    def get_storage_client(self) -> StorageClient:
        """Get the storage client."""
        if self._storage_client is None:
            logger.warning('No storage client set, implicitly creating and using default FileSystemStorageClient.')
            self._storage_client = FileSystemStorageClient()

        return self._storage_client

    def set_storage_client(self, storage_client: StorageClient) -> None:
        """Set the storage client.

        Args:
            storage_client: The storage client to set.

        Raises:
            ServiceConflictError: If the storage client has already been retrieved before.
        """
        if self._storage_client is storage_client:
            # Same instance, no need to anything
            return
        if self._storage_client:
            raise ServiceConflictError(StorageClient, storage_client, self._storage_client)

        self._storage_client = storage_client

    @property
    def storage_instance_manager(self) -> StorageInstanceManager:
        """Get the storage instance manager."""
        if self._storage_instance_manager is None:
            # Import here to avoid circular imports.
            from crawlee.storages._storage_instance_manager import StorageInstanceManager  # noqa: PLC0415

            self._storage_instance_manager = StorageInstanceManager()

        return self._storage_instance_manager


service_locator = ServiceLocator()
