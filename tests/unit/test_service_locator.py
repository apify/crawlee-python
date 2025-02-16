from __future__ import annotations

import pytest

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events import LocalEventManager
from crawlee.storage_clients import MemoryStorageClient


def test_default_configuration() -> None:
    default_config = Configuration()
    config = service_locator.get_configuration()
    assert config == default_config  # == because these are in fact different instances, which should be fine


def test_custom_configuration() -> None:
    custom_config = Configuration(default_browser_path='custom_path')
    service_locator.set_configuration(custom_config)
    config = service_locator.get_configuration()
    assert config is custom_config


def test_configuration_overwrite() -> None:
    default_config = Configuration()
    service_locator.set_configuration(default_config)

    custom_config = Configuration(default_browser_path='custom_path')
    service_locator.set_configuration(custom_config)
    assert service_locator.get_configuration() is custom_config


def test_configuration_conflict() -> None:
    service_locator.get_configuration()
    custom_config = Configuration(default_browser_path='custom_path')

    with pytest.raises(ServiceConflictError, match='Configuration is already in use.'):
        service_locator.set_configuration(custom_config)


def test_default_event_manager() -> None:
    default_event_manager = service_locator.get_event_manager()
    assert isinstance(default_event_manager, LocalEventManager)


def test_custom_event_manager() -> None:
    custom_event_manager = LocalEventManager()
    service_locator.set_event_manager(custom_event_manager)
    event_manager = service_locator.get_event_manager()
    assert event_manager is custom_event_manager


def test_event_manager_overwrite() -> None:
    custom_event_manager = LocalEventManager()
    service_locator.set_event_manager(custom_event_manager)

    another_custom_event_manager = LocalEventManager()
    service_locator.set_event_manager(another_custom_event_manager)

    assert custom_event_manager != another_custom_event_manager
    assert service_locator.get_event_manager() is another_custom_event_manager


def test_event_manager_conflict() -> None:
    service_locator.get_event_manager()
    custom_event_manager = LocalEventManager()

    with pytest.raises(ServiceConflictError, match='EventManager is already in use.'):
        service_locator.set_event_manager(custom_event_manager)


def test_default_storage_client() -> None:
    default_storage_client = service_locator.get_storage_client()
    assert isinstance(default_storage_client, MemoryStorageClient)


def test_custom_storage_client() -> None:
    custom_storage_client = MemoryStorageClient.from_config()
    service_locator.set_storage_client(custom_storage_client)
    storage_client = service_locator.get_storage_client()
    assert storage_client is custom_storage_client


def test_storage_client_overwrite() -> None:
    custom_storage_client = MemoryStorageClient.from_config()
    service_locator.set_storage_client(custom_storage_client)

    another_custom_storage_client = MemoryStorageClient.from_config()
    service_locator.set_storage_client(another_custom_storage_client)

    assert custom_storage_client != another_custom_storage_client
    assert service_locator.get_storage_client() is another_custom_storage_client


def test_storage_client_conflict() -> None:
    service_locator.get_storage_client()
    custom_storage_client = MemoryStorageClient.from_config()

    with pytest.raises(ServiceConflictError, match='StorageClient is already in use.'):
        service_locator.set_storage_client(custom_storage_client)
