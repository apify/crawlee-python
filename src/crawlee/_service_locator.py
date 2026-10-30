from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events import EventManager, LocalEventManager
from crawlee.storage_backends import FileSystemStorageBackend, StorageBackend

if TYPE_CHECKING:
    from crawlee.storages._storage_instance_manager import StorageInstanceManager

from logging import getLogger

logger = getLogger(__name__)


@docs_group('Configuration')
class ServiceLocator:
    """Service locator for managing the services used by Crawlee.

    All services are initialized to its default value lazily.
    """

    global_storage_instance_manager: StorageInstanceManager | None = None

    def __init__(
        self,
        configuration: Configuration | None = None,
        event_manager: EventManager | None = None,
        storage_backend: StorageBackend | None = None,
    ) -> None:
        self._configuration = configuration
        self._event_manager = event_manager
        self._storage_backend = storage_backend

    def get_configuration(self) -> Configuration:
        """Get the configuration."""
        if self._configuration is None:
            logger.debug('No configuration set, implicitly creating and using default Configuration.')
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
            logger.debug('No event manager set, implicitly creating and using default LocalEventManager.')
            if self._configuration is None:
                logger.warning(
                    'Implicit creation of event manager will implicitly set configuration as side effect. '
                    'It is advised to explicitly first set the configuration instead.'
                )
            self._event_manager = LocalEventManager().from_config(config=self._configuration)

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

    def get_storage_backend(self) -> StorageBackend:
        """Get the storage backend."""
        if self._storage_backend is None:
            logger.debug('No storage backend set, implicitly creating and using default FileSystemStorageBackend.')
            if self._configuration is None:
                logger.warning(
                    'Implicit creation of storage backend will implicitly set configuration as side effect. '
                    'It is advised to explicitly first set the configuration instead.'
                )
            self._storage_backend = FileSystemStorageBackend()

        return self._storage_backend

    def set_storage_backend(self, storage_backend: StorageBackend) -> None:
        """Set the storage backend.

        Args:
            storage_backend: The storage backend to set.

        Raises:
            ServiceConflictError: If the storage backend has already been retrieved before.
        """
        if self._storage_backend is storage_backend:
            # Same instance, no need to anything
            return
        if self._storage_backend:
            raise ServiceConflictError(StorageBackend, storage_backend, self._storage_backend)

        self._storage_backend = storage_backend

    @property
    def storage_instance_manager(self) -> StorageInstanceManager:
        """Get the storage instance manager. It is global manager shared by all instances of ServiceLocator."""
        if ServiceLocator.global_storage_instance_manager is None:
            # Import here to avoid circular imports.
            from crawlee.storages._storage_instance_manager import StorageInstanceManager  # noqa: PLC0415

            ServiceLocator.global_storage_instance_manager = StorageInstanceManager()

        return ServiceLocator.global_storage_instance_manager


service_locator = ServiceLocator()
