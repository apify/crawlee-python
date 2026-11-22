from __future__ import annotations

from datetime import timedelta

import pytest

from crawlee import service_locator
from crawlee._service_locator import ServiceLocator
from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events import LocalEventManager
from crawlee.storage_backends import FileSystemStorageBackend, MemoryStorageBackend


def test_default_configuration() -> None:
    default_config = Configuration()
    config = service_locator.get_configuration()
    assert config == default_config  # == because these are in fact different instances, which should be fine


def test_custom_configuration() -> None:
    custom_config = Configuration(default_browser_path='custom_path')
    service_locator.set_configuration(custom_config)
    config = service_locator.get_configuration()
    assert config is custom_config


def test_configuration_overwrite_not_possible() -> None:
    default_config = Configuration()
    service_locator.set_configuration(default_config)

    custom_config = Configuration(default_browser_path='custom_path')
    with pytest.raises(ServiceConflictError):
        service_locator.set_configuration(custom_config)


def test_configuration_conflict() -> None:
    service_locator.get_configuration()
    custom_config = Configuration(default_browser_path='custom_path')

    with pytest.raises(ServiceConflictError, match=r'Configuration is already in use.'):
        service_locator.set_configuration(custom_config)


def test_default_event_manager() -> None:
    default_event_manager = service_locator.get_event_manager()
    assert isinstance(default_event_manager, LocalEventManager)


def test_custom_event_manager() -> None:
    custom_event_manager = LocalEventManager()
    service_locator.set_event_manager(custom_event_manager)
    event_manager = service_locator.get_event_manager()
    assert event_manager is custom_event_manager


def test_event_manager_overwrite_not_possible() -> None:
    custom_event_manager = LocalEventManager()
    service_locator.set_event_manager(custom_event_manager)

    another_custom_event_manager = LocalEventManager()
    with pytest.raises(ServiceConflictError):
        service_locator.set_event_manager(another_custom_event_manager)


def test_event_manager_conflict() -> None:
    service_locator.get_event_manager()
    custom_event_manager = LocalEventManager()

    with pytest.raises(ServiceConflictError, match=r'EventManager is already in use.'):
        service_locator.set_event_manager(custom_event_manager)


def test_locators_share_event_manager() -> None:
    """Every service locator returns the same event manager, also one with services of its own."""
    scoped_locator = ServiceLocator(configuration=Configuration(), storage_backend=MemoryStorageBackend())

    assert scoped_locator.get_event_manager() is service_locator.get_event_manager()


def test_event_manager_uses_global_config() -> None:
    """The default event manager takes its intervals from the global configuration, whoever creates it."""
    service_locator.set_configuration(Configuration(persist_state_interval=timedelta(seconds=7)))
    scoped_locator = ServiceLocator(configuration=Configuration(persist_state_interval=timedelta(seconds=3)))

    event_manager = scoped_locator.get_event_manager()

    assert event_manager._persist_state_interval == timedelta(seconds=7)


def test_default_storage_backend() -> None:
    default_storage_backend = service_locator.get_storage_backend()
    assert isinstance(default_storage_backend, FileSystemStorageBackend)


def test_custom_storage_backend() -> None:
    custom_storage_backend = MemoryStorageBackend()
    service_locator.set_storage_backend(custom_storage_backend)
    storage_backend = service_locator.get_storage_backend()
    assert storage_backend is custom_storage_backend


def test_storage_backend_overwrite_not_possible() -> None:
    custom_storage_backend = MemoryStorageBackend()
    service_locator.set_storage_backend(custom_storage_backend)

    another_custom_storage_backend = MemoryStorageBackend()
    with pytest.raises(ServiceConflictError):
        service_locator.set_storage_backend(another_custom_storage_backend)


def test_storage_backend_conflict() -> None:
    service_locator.get_storage_backend()
    custom_storage_backend = MemoryStorageBackend()

    with pytest.raises(ServiceConflictError, match=r'StorageBackend is already in use.'):
        service_locator.set_storage_backend(custom_storage_backend)
