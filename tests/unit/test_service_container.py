from __future__ import annotations

import pytest

from crawlee.configuration import Configuration
from crawlee.errors import ServiceConflictError
from crawlee.events import LocalEventManager
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.service_container import (
    get_configuration,
    get_event_manager,
    get_storage_client,
    set_configuration,
    set_event_manager,
    set_storage_client,
)


def test_configuration() -> None:
    default_config = Configuration()
    config = get_configuration()
    assert config == default_config

    custom_config = Configuration(default_browser_path='custom_path')
    set_configuration(custom_config)
    config = get_configuration()
    assert config == custom_config

    with pytest.raises(ServiceConflictError, match='Configuration has already been set.'):
        set_configuration(custom_config)


def test_event_manager() -> None:
    default_event_manager = get_event_manager()
    assert isinstance(default_event_manager, LocalEventManager)

    custom_event_manager = LocalEventManager()
    set_event_manager(custom_event_manager)
    event_manager = get_event_manager()
    assert event_manager == custom_event_manager

    with pytest.raises(ServiceConflictError, match='EventManager has already been set.'):
        set_event_manager(custom_event_manager)


def test_storage_client() -> None:
    default_storage_client = get_storage_client()
    assert isinstance(default_storage_client, MemoryStorageClient)

    custom_storage_client = MemoryStorageClient()
    set_storage_client(custom_storage_client)
    storage_client = get_storage_client()
    assert storage_client == custom_storage_client

    with pytest.raises(ServiceConflictError, match='StorageClient has already been set.'):
        set_storage_client(custom_storage_client)
