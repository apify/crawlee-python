from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from crawlee._utils.env_vars import CrawleeEnvVars
from crawlee.memory_storage import MemoryStorageClient

if TYPE_CHECKING:
    from pathlib import Path


async def test_write_metadata(tmp_path: Path) -> None:
    dataset_name = 'test'
    dataset_no_metadata_name = 'test-no-metadata'
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), write_metadata=True)
    ms_no_metadata = MemoryStorageClient(local_data_directory=str(tmp_path), write_metadata=False)
    datasets_client = ms.datasets()
    datasets_no_metadata_client = ms_no_metadata.datasets()
    await datasets_client.get_or_create(name=dataset_name)
    await datasets_no_metadata_client.get_or_create(name=dataset_no_metadata_name)
    assert os.path.exists(os.path.join(ms.datasets_directory, dataset_name, '__metadata__.json')) is True
    assert (
        os.path.exists(os.path.join(ms_no_metadata.datasets_directory, dataset_no_metadata_name, '__metadata__.json'))
        is False
    )


async def test_persist_storage(tmp_path: Path) -> None:
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), persist_storage=True)
    ms_no_persist = MemoryStorageClient(local_data_directory=str(tmp_path), persist_storage=False)
    kvs_client = ms.key_value_stores()
    kvs_no_metadata_client = ms_no_persist.key_value_stores()
    kvs_info = await kvs_client.get_or_create(name='kvs')
    kvs_no_metadata_info = await kvs_no_metadata_client.get_or_create(name='kvs-no-persist')
    await ms.key_value_store(kvs_info.id).set_record('test', {'x': 1}, 'application/json')
    await ms_no_persist.key_value_store(kvs_no_metadata_info.id).set_record('test', {'x': 1}, 'application/json')
    assert os.path.exists(os.path.join(ms.key_value_stores_directory, kvs_info.name, 'test.json')) is True
    assert (
        os.path.exists(os.path.join(ms_no_persist.key_value_stores_directory, kvs_no_metadata_info.name, 'test.json'))
        is False
    )


def test_config_via_env_vars_persist_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Env var changes persist_storage to False
    monkeypatch.setenv('CRAWLEE_PERSIST_STORAGE', 'false')
    ms = MemoryStorageClient(local_data_directory=str(tmp_path))
    assert ms.persist_storage is False
    monkeypatch.setenv('CRAWLEE_PERSIST_STORAGE', '0')
    ms = MemoryStorageClient(local_data_directory=str(tmp_path))
    assert ms.persist_storage is False
    monkeypatch.setenv('CRAWLEE_PERSIST_STORAGE', '')
    ms = MemoryStorageClient(local_data_directory=str(tmp_path))
    assert ms.persist_storage is False
    # Test if constructor arg takes precedence over env var value
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), persist_storage=True)
    assert ms.persist_storage is True


def test_config_via_env_vars_write_metadata(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Env var changes write_metadata to True
    monkeypatch.setenv('DEBUG', '*')
    ms = MemoryStorageClient(local_data_directory=str(tmp_path))
    assert ms.write_metadata is True
    # Test if constructor arg takes precedence over env var value
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), write_metadata=False)
    assert ms.write_metadata is False


async def test_purge_datasets(tmp_path: Path) -> None:
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), write_metadata=True)
    # Create default and non-default datasets
    datasets_client = ms.datasets()
    default_dataset_info = await datasets_client.get_or_create(name='default')
    non_default_dataset_info = await datasets_client.get_or_create(name='non-default')

    # Check all folders inside datasets directory before and after purge
    folders_before_purge = os.listdir(ms.datasets_directory)
    assert default_dataset_info.name in folders_before_purge
    assert non_default_dataset_info.name in folders_before_purge

    await ms._purge_inner()
    folders_after_purge = os.listdir(ms.datasets_directory)
    assert default_dataset_info.name not in folders_after_purge
    assert non_default_dataset_info.name in folders_after_purge


async def test_purge_key_value_stores(tmp_path: Path) -> None:
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), write_metadata=True)

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
    folders_before_purge = os.listdir(ms.key_value_stores_directory)
    assert default_kvs_info.name in folders_before_purge
    assert non_default_kvs_info.name in folders_before_purge
    default_folder_files_before_purge = os.listdir(os.path.join(ms.key_value_stores_directory, 'default'))
    assert 'INPUT.json' in default_folder_files_before_purge
    assert 'test.json' in default_folder_files_before_purge

    await ms._purge_inner()
    folders_after_purge = os.listdir(ms.key_value_stores_directory)
    assert default_kvs_info.name in folders_after_purge
    assert non_default_kvs_info.name in folders_after_purge
    default_folder_files_after_purge = os.listdir(os.path.join(ms.key_value_stores_directory, 'default'))
    assert 'INPUT.json' in default_folder_files_after_purge
    assert 'test.json' not in default_folder_files_after_purge


async def test_purge_request_queues(tmp_path: Path) -> None:
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), write_metadata=True)
    # Create default and non-default request queues
    rq_client = ms.request_queues()
    default_rq_info = await rq_client.get_or_create(name='default')
    non_default_rq_info = await rq_client.get_or_create(name='non-default')

    # Check all folders inside rq directory before and after purge
    folders_before_purge = os.listdir(ms.request_queues_directory)
    assert default_rq_info.name in folders_before_purge
    assert non_default_rq_info.name in folders_before_purge
    await ms._purge_inner()
    folders_after_purge = os.listdir(ms.request_queues_directory)
    assert default_rq_info.name not in folders_after_purge
    assert non_default_rq_info.name in folders_after_purge


async def test_not_implemented_method(tmp_path: Path) -> None:
    ms = MemoryStorageClient(local_data_directory=str(tmp_path), write_metadata=True)
    ddt = ms.dataset('test')
    with pytest.raises(NotImplementedError, match='This method is not supported in local memory storage.'):
        await ddt.stream_items(item_format='json')

    with pytest.raises(NotImplementedError, match='This method is not supported in local memory storage.'):
        await ddt.stream_items(item_format='json')


async def test_storage_path_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CrawleeEnvVars.LOCAL_STORAGE_DIR)
    default_ms = MemoryStorageClient()
    assert default_ms._local_data_directory == './storage'

    # We expect the env var to override the default value
    monkeypatch.setenv(CrawleeEnvVars.LOCAL_STORAGE_DIR, './env_var_storage_dir')
    env_var_ms = MemoryStorageClient()
    assert env_var_ms._local_data_directory == './env_var_storage_dir'

    # We expect the parametrized value to override the env var
    parametrized_ms = MemoryStorageClient(local_data_directory='./parametrized_storage_dir')
    assert parametrized_ms._local_data_directory == './parametrized_storage_dir'
