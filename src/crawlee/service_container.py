from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import NotRequired, TypedDict

from crawlee.configuration import Configuration
from crawlee.events.local_event_manager import LocalEventManager
from crawlee.memory_storage_client.memory_storage_client import MemoryStorageClient

if TYPE_CHECKING:
    from crawlee.base_storage_client.base_storage_client import BaseStorageClient
    from crawlee.events.event_manager import EventManager
    from crawlee.types import StorageClientType


class _Services(TypedDict):
    local_storage_client: NotRequired[BaseStorageClient]
    cloud_storage_client: NotRequired[BaseStorageClient]
    configuration: NotRequired[Configuration]
    event_manager: NotRequired[EventManager]


_services = _Services()
_default_storage_client_type: StorageClientType = 'local'


def get_storage_client(*, client_type: StorageClientType | None = None) -> BaseStorageClient:
    """Get the storage client instance for the current environment.

    Args:
        client_type: Allows retrieving a specific storage client type, regardless of where we are running

    Returns:
        The current storage client instance.
    """
    if client_type is None:
        client_type = _default_storage_client_type

    if client_type == 'cloud':
        if 'cloud_storage_client' not in _services:
            raise RuntimeError('Cloud client was not provided.')
        return _services['cloud_storage_client']

    if 'local_storage_client' not in _services:
        _services['local_storage_client'] = MemoryStorageClient()

    return _services['local_storage_client']


def set_local_storage_client(local_client: BaseStorageClient) -> None:
    """Set the local storage client instance.

    Args:
        local_client: The local storage client instance.
    """
    _services['local_storage_client'] = local_client


def set_cloud_storage_client(cloud_client: BaseStorageClient) -> None:
    """Set the cloud storage client instance.

    Args:
        cloud_client: The cloud storage client instance.
    """
    _services['cloud_storage_client'] = cloud_client


def set_default_storage_client_type(client_type: StorageClientType) -> None:
    """Set the default storage client type."""
    _default_storage_client_type = client_type


def get_configuration() -> Configuration:
    """Get the configuration object."""
    if 'configuration' not in _services:
        _services['configuration'] = Configuration()

    return _services['configuration']


def set_configuration(configuration: Configuration) -> None:
    """Set the configuration object."""
    _services['configuration'] = configuration


def get_event_manager() -> EventManager:
    """Get the event manager."""
    if 'event_manager' not in _services:
        _services['event_manager'] = LocalEventManager()

    return _services['event_manager']


def set_event_manager(event_manager: EventManager) -> None:
    """Set the event manager."""
    _services['event_manager'] = event_manager
