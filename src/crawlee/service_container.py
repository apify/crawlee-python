from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.events import LocalEventManager
from crawlee.memory_storage_client import MemoryStorageClient

if TYPE_CHECKING:
    from crawlee.base_storage_client._base_storage_client import BaseStorageClient
    from crawlee.events._event_manager import EventManager

__all__ = [
    'get_configuration',
    'get_configuration_if_set',
    'get_event_manager',
    'get_storage_client',
    'set_cloud_storage_client',
    'set_configuration',
    'set_default_storage_client_type',
    'set_event_manager',
    'set_local_storage_client',
]

StorageClientType = Literal['cloud', 'local']


@dataclass
class _Services:
    """An internal container for singleton service instances."""

    local_storage_client: BaseStorageClient = field(default_factory=MemoryStorageClient)
    cloud_storage_client: BaseStorageClient = field(default_factory=MemoryStorageClient)
    configuration: Configuration = field(default_factory=Configuration)
    event_manager: EventManager = field(default_factory=LocalEventManager)


_services = _Services()

_DEFAULT_STORAGE_CLIENT_TYPE: StorageClientType = 'local'


@docs_group('Functions')
def get_storage_client(client_type: StorageClientType = _DEFAULT_STORAGE_CLIENT_TYPE) -> BaseStorageClient:
    """Get the storage client instance for the current environment.

    Args:
        client_type: Allows retrieving a specific storage client type, regardless of where we are running.

    Raises:
        ValueError: If the client type is unknown.

    Returns:
        The current storage client instance.
    """
    if client_type == 'local':
        return _services.local_storage_client

    if client_type == 'cloud':
        return _services.cloud_storage_client

    raise ValueError(f'Unknown storage client type: {client_type}')


@docs_group('Functions')
def set_local_storage_client(local_client: BaseStorageClient) -> None:
    """Set the local storage client instance.

    Args:
        local_client: The local storage client instance.
    """
    _services.local_storage_client = local_client


@docs_group('Functions')
def set_cloud_storage_client(cloud_client: BaseStorageClient) -> None:
    """Set the cloud storage client instance.

    Args:
        cloud_client: The cloud storage client instance.
    """
    _services.cloud_storage_client = cloud_client


@docs_group('Functions')
def get_configuration() -> Configuration:
    """Get the configuration object."""
    return _services.configuration


@docs_group('Functions')
def set_configuration(configuration: Configuration) -> None:
    """Set the configuration object."""
    _services.configuration = configuration


@docs_group('Functions')
def get_event_manager() -> EventManager:
    """Get the event manager."""
    return _services.event_manager


@docs_group('Functions')
def set_event_manager(event_manager: EventManager) -> None:
    """Set the event manager."""
    _services.event_manager = event_manager
