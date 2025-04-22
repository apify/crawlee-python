# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

from pathlib import Path

import pytest

from crawlee import Request, service_locator
from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storage_clients.models import BatchRequestsOperationResponse


async def test_write_metadata(tmp_path: Path) -> None:
    dataset_name = 'test'
    dataset_no_metadata_name = 'test-no-metadata'
    ms = MemoryStorageClient.from_config(
        Configuration(
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
            write_metadata=True,
        ),
    )
    ms_no_metadata = MemoryStorageClient.from_config(
        Configuration(
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
            write_metadata=False,
        )
    )
    datasets_client = ms.datasets()
    datasets_no_metadata_client = ms_no_metadata.datasets()
    await datasets_client.get_or_create(name=dataset_name)
    await datasets_no_metadata_client.get_or_create(name=dataset_no_metadata_name)
    assert Path(ms.datasets_directory, dataset_name, METADATA_FILENAME).exists() is True
    assert Path(ms_no_metadata.datasets_directory, dataset_no_metadata_name, METADATA_FILENAME).exists() is False


@pytest.mark.parametrize(
    'persist_storage',
    [
        True,
        False,
    ],
)
async def test_persist_storage(persist_storage: bool, tmp_path: Path) -> None:  # noqa: FBT001
    ms = MemoryStorageClient.from_config(
        Configuration(
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
            persist_storage=persist_storage,
        )
    )

    # Key value stores
    kvs_client = ms.key_value_stores()
    kvs_info = await kvs_client.get_or_create(name='kvs')
    await ms.key_value_store(kvs_info.id).set_record('test', {'x': 1}, 'application/json')

    path = Path(ms.key_value_stores_directory) / (kvs_info.name or '') / 'test.json'
    assert path.exists() is persist_storage

    # Request queues
    rq_client = ms.request_queues()
    rq_info = await rq_client.get_or_create(name='rq')

    request = Request.from_url('http://lorem.com')
    await ms.request_queue(rq_info.id).add_request(request)

    path = Path(ms.request_queues_directory) / (rq_info.name or '') / f'{request.id}.json'
    assert path.exists() is persist_storage

    # Datasets
    ds_client = ms.datasets()
    ds_info = await ds_client.get_or_create(name='ds')

    await ms.dataset(ds_info.id).push_items([{'foo': 'bar'}])


def test_persist_storage_set_to_false_via_string_env_var(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv('CRAWLEE_PERSIST_STORAGE', 'false')
    ms = MemoryStorageClient.from_config(
        Configuration(crawlee_storage_dir=str(tmp_path)),  # type: ignore[call-arg]
    )
    assert ms.persist_storage is False


def test_persist_storage_set_to_false_via_numeric_env_var(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv('CRAWLEE_PERSIST_STORAGE', '0')
    ms = MemoryStorageClient.from_config(Configuration(crawlee_storage_dir=str(tmp_path)))  # type: ignore[call-arg]
    assert ms.persist_storage is False


def test_persist_storage_true_via_constructor_arg(tmp_path: Path) -> None:
    ms = MemoryStorageClient.from_config(
        Configuration(
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
            persist_storage=True,
        )
    )
    assert ms.persist_storage is True


def test_default_write_metadata_behavior(tmp_path: Path) -> None:
    # Default behavior
    ms = MemoryStorageClient.from_config(
        Configuration(crawlee_storage_dir=str(tmp_path)),  # type: ignore[call-arg]
    )
    assert ms.write_metadata is True


def test_write_metadata_set_to_false_via_env_var(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Test if env var changes write_metadata to False
    monkeypatch.setenv('CRAWLEE_WRITE_METADATA', 'false')
    ms = MemoryStorageClient.from_config(
        Configuration(crawlee_storage_dir=str(tmp_path)),  # type: ignore[call-arg]
    )
    assert ms.write_metadata is False


def test_write_metadata_false_via_constructor_arg_overrides_env_var(tmp_path: Path) -> None:
    # Test if constructor arg takes precedence over env var value
    ms = MemoryStorageClient.from_config(
        Configuration(
            write_metadata=False,
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        )
    )
    assert ms.write_metadata is False


async def test_purge_datasets(tmp_path: Path) -> None:
    ms = MemoryStorageClient.from_config(
        Configuration(
            write_metadata=True,
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        )
    )
    # Create default and non-default datasets
    datasets_client = ms.datasets()
    default_dataset_info = await datasets_client.get_or_create(name='default')
    non_default_dataset_info = await datasets_client.get_or_create(name='non-default')

    # Check all folders inside datasets directory before and after purge
    assert default_dataset_info.name is not None
    assert non_default_dataset_info.name is not None

    default_path = Path(ms.datasets_directory, default_dataset_info.name)
    non_default_path = Path(ms.datasets_directory, non_default_dataset_info.name)

    assert default_path.exists() is True
    assert non_default_path.exists() is True

    await ms._purge_default_storages()

    assert default_path.exists() is False
    assert non_default_path.exists() is True


async def test_purge_key_value_stores(tmp_path: Path) -> None:
    ms = MemoryStorageClient.from_config(
        Configuration(
            write_metadata=True,
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        )
    )

    # Create default and non-default key-value stores
    kvs_client = ms.key_value_stores()
    default_kvs_info = await kvs_client.get_or_create(name='default')
    non_default_kvs_info = await kvs_client.get_or_create(name='non-default')
    default_kvs_client = ms.key_value_store(default_kvs_info.id)
    # INPUT.json should be kept
    await default_kvs_client.set_record('INPUT', {'abc': 123}, 'application/json')
    # test.json should not be kept
    await default_kvs_client.set_record('test', {'abc': 123}, 'application/json')

    # Check all folders and files inside kvs directory before and after purge
    assert default_kvs_info.name is not None
    assert non_default_kvs_info.name is not None

    default_kvs_path = Path(ms.key_value_stores_directory, default_kvs_info.name)
    non_default_kvs_path = Path(ms.key_value_stores_directory, non_default_kvs_info.name)
    kvs_directory = Path(ms.key_value_stores_directory, 'default')

    assert default_kvs_path.exists() is True
    assert non_default_kvs_path.exists() is True

    assert (kvs_directory / 'INPUT.json').exists() is True
    assert (kvs_directory / 'test.json').exists() is True

    await ms._purge_default_storages()

    assert default_kvs_path.exists() is True
    assert non_default_kvs_path.exists() is True

    assert (kvs_directory / 'INPUT.json').exists() is True
    assert (kvs_directory / 'test.json').exists() is False


async def test_purge_request_queues(tmp_path: Path) -> None:
    ms = MemoryStorageClient.from_config(
        Configuration(
            write_metadata=True,
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        )
    )
    # Create default and non-default request queues
    rq_client = ms.request_queues()
    default_rq_info = await rq_client.get_or_create(name='default')
    non_default_rq_info = await rq_client.get_or_create(name='non-default')

    # Check all folders inside rq directory before and after purge
    assert default_rq_info.name
    assert non_default_rq_info.name

    default_rq_path = Path(ms.request_queues_directory, default_rq_info.name)
    non_default_rq_path = Path(ms.request_queues_directory, non_default_rq_info.name)

    assert default_rq_path.exists() is True
    assert non_default_rq_path.exists() is True

    await ms._purge_default_storages()

    assert default_rq_path.exists() is False
    assert non_default_rq_path.exists() is True


async def test_not_implemented_method(tmp_path: Path) -> None:
    ms = MemoryStorageClient.from_config(
        Configuration(
            write_metadata=True,
            crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        )
    )
    ddt = ms.dataset('test')
    with pytest.raises(NotImplementedError, match='This method is not supported in memory storage.'):
        await ddt.stream_items(item_format='json')

    with pytest.raises(NotImplementedError, match='This method is not supported in memory storage.'):
        await ddt.stream_items(item_format='json')


async def test_default_storage_path_used(monkeypatch: pytest.MonkeyPatch) -> None:
    # Reset the configuration in service locator
    service_locator._configuration = None
    service_locator._configuration_was_retrieved = False

    # Remove the env var for setting the storage directory
    monkeypatch.delenv('CRAWLEE_STORAGE_DIR', raising=False)

    # Initialize the service locator with default configuration
    msc = MemoryStorageClient.from_config()
    assert msc.storage_dir == './storage'


async def test_storage_path_from_env_var_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # We expect the env var to override the default value
    monkeypatch.setenv('CRAWLEE_STORAGE_DIR', './env_var_storage_dir')
    service_locator.set_configuration(Configuration())
    ms = MemoryStorageClient.from_config()
    assert ms.storage_dir == './env_var_storage_dir'


async def test_parametrized_storage_path_overrides_env_var() -> None:
    # We expect the parametrized value to be used
    ms = MemoryStorageClient.from_config(
        Configuration(crawlee_storage_dir='./parametrized_storage_dir'),  # type: ignore[call-arg]
    )
    assert ms.storage_dir == './parametrized_storage_dir'


async def test_batch_requests_operation_response() -> None:
    """Test that `BatchRequestsOperationResponse` creation from example responses."""
    process_request = {
        'requestId': 'EAaArVRs5qV39C9',
        'uniqueKey': 'https://example.com',
        'wasAlreadyHandled': False,
        'wasAlreadyPresent': True,
    }
    unprocess_request_full = {'uniqueKey': 'https://example2.com', 'method': 'GET', 'url': 'https://example2.com'}
    unprocess_request_minimal = {'uniqueKey': 'https://example3.com', 'url': 'https://example3.com'}
    BatchRequestsOperationResponse.model_validate(
        {
            'processedRequests': [process_request],
            'unprocessedRequests': [unprocess_request_full, unprocess_request_minimal],
        }
    )
