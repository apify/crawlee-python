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

    All services are initialized to their default values lazily. The event manager is one per process, shared by
    every instance.
    """

    global_storage_instance_manager: StorageInstanceManager | None = None

    global_event_manager: EventManager | None = None

    def __init__(
        self,
        *,
        configuration: Configuration | None = None,
        storage_backend: StorageBackend | None = None,
    ) -> None:
        self._configuration = configuration
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
        """Get the event manager shared by the whole process.

        The default one is created from the global configuration.
        """
        if ServiceLocator.global_event_manager is None:
            logger.debug('No event manager set, implicitly creating and using default LocalEventManager.')
            if service_locator._configuration is None:
                logger.warning(
                    'Implicit creation of the event manager will implicitly set the global configuration as side '
                    'effect. The event manager is shared by the whole process, so it does not use a configuration '
                    'passed to a crawler. It is advised to explicitly first set the global configuration or the event '
                    'manager instead.'
                )
            ServiceLocator.global_event_manager = LocalEventManager.from_config(service_locator.get_configuration())

        return ServiceLocator.global_event_manager

    def set_event_manager(self, event_manager: EventManager) -> None:
        """Set the event manager shared by the whole process.

        Args:
            event_manager: The event manager to set.

        Raises:
            ServiceConflictError: If the event manager has already been retrieved before.
        """
        if ServiceLocator.global_event_manager is event_manager:
            # Same instance, no need to anything
            return
        if ServiceLocator.global_event_manager:
            raise ServiceConflictError(EventManager, event_manager, ServiceLocator.global_event_manager)

        ServiceLocator.global_event_manager = event_manager

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
