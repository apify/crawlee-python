from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path

import aioshutil
from aiofiles import ospath
from aiofiles.os import rename, scandir
from apify_shared.consts import ApifyEnvVars
from apify_shared.utils import ignore_docs

from apify._memory_storage.resource_clients.dataset import DatasetClient
from apify._memory_storage.resource_clients.dataset_collection import DatasetCollectionClient
from apify._memory_storage.resource_clients.key_value_store import KeyValueStoreClient
from apify._memory_storage.resource_clients.key_value_store_collection import KeyValueStoreCollectionClient
from apify._memory_storage.resource_clients.request_queue import RequestQueueClient
from apify._memory_storage.resource_clients.request_queue_collection import RequestQueueCollectionClient
from apify._utils import maybe_parse_bool

"""
Memory storage emulates data storages that are available on the Apify platform.
Specifically, it emulates clients for datasets, key-value stores and request queues.
The data are held in-memory and persisted locally if `persist_storage` is True.
The metadata of the storages is also persisted if `write_metadata` is True.
"""


@ignore_docs
class MemoryStorageClient:
    """Class representing an in-memory storage."""

    _local_data_directory: str
    _datasets_directory: str
    _key_value_stores_directory: str
    _request_queues_directory: str
    _write_metadata: bool
    _persist_storage: bool
    _datasets_handled: list[DatasetClient]
    _key_value_stores_handled: list[KeyValueStoreClient]
    _request_queues_handled: list[RequestQueueClient]

    _purged_on_start: bool = False
    _purge_lock: asyncio.Lock

    """Indicates whether a purge was already performed on this instance"""

    def __init__(
        self: MemoryStorageClient,
        *,
        local_data_directory: str | None = None,
        write_metadata: bool | None = None,
        persist_storage: bool | None = None,
    ) -> None:
        """Initialize the MemoryStorageClient.

        Args:
            local_data_directory (str, optional): A local directory where all data will be persisted
            persist_storage (bool, optional): Whether to persist the data to the `local_data_directory` or just keep them in memory
            write_metadata (bool, optional): Whether to persist metadata of the storages as well
        """
        self._local_data_directory = local_data_directory or os.getenv(ApifyEnvVars.LOCAL_STORAGE_DIR) or './storage'
        self._datasets_directory = os.path.join(self._local_data_directory, 'datasets')
        self._key_value_stores_directory = os.path.join(self._local_data_directory, 'key_value_stores')
        self._request_queues_directory = os.path.join(self._local_data_directory, 'request_queues')
        self._write_metadata = write_metadata if write_metadata is not None else '*' in os.getenv('DEBUG', '')
        self._persist_storage = persist_storage if persist_storage is not None else maybe_parse_bool(os.getenv(ApifyEnvVars.PERSIST_STORAGE, 'true'))
        self._datasets_handled = []
        self._key_value_stores_handled = []
        self._request_queues_handled = []
        self._purge_lock = asyncio.Lock()

    def datasets(self: MemoryStorageClient) -> DatasetCollectionClient:
        """Retrieve the sub-client for manipulating datasets."""
        return DatasetCollectionClient(base_storage_directory=self._datasets_directory, memory_storage_client=self)

    def dataset(self: MemoryStorageClient, dataset_id: str) -> DatasetClient:
        """Retrieve the sub-client for manipulating a single dataset.

        Args:
            dataset_id (str): ID of the dataset to be manipulated
        """
        return DatasetClient(base_storage_directory=self._datasets_directory, memory_storage_client=self, id=dataset_id)

    def key_value_stores(self: MemoryStorageClient) -> KeyValueStoreCollectionClient:
        """Retrieve the sub-client for manipulating key-value stores."""
        return KeyValueStoreCollectionClient(base_storage_directory=self._key_value_stores_directory, memory_storage_client=self)

    def key_value_store(self: MemoryStorageClient, key_value_store_id: str) -> KeyValueStoreClient:
        """Retrieve the sub-client for manipulating a single key-value store.

        Args:
            key_value_store_id (str): ID of the key-value store to be manipulated
        """
        return KeyValueStoreClient(base_storage_directory=self._key_value_stores_directory, memory_storage_client=self, id=key_value_store_id)

    def request_queues(self: MemoryStorageClient) -> RequestQueueCollectionClient:
        """Retrieve the sub-client for manipulating request queues."""
        return RequestQueueCollectionClient(base_storage_directory=self._request_queues_directory, memory_storage_client=self)

    def request_queue(
        self: MemoryStorageClient,
        request_queue_id: str,
        *,
        client_key: str | None = None,  # noqa: ARG002
    ) -> RequestQueueClient:
        """Retrieve the sub-client for manipulating a single request queue.

        Args:
            request_queue_id (str): ID of the request queue to be manipulated
            client_key (str): A unique identifier of the client accessing the request queue
        """
        return RequestQueueClient(base_storage_directory=self._request_queues_directory, memory_storage_client=self, id=request_queue_id)

    async def _purge_on_start(self: MemoryStorageClient) -> None:
        # Optimistic, non-blocking check
        if self._purged_on_start is True:
            return

        async with self._purge_lock:
            # Another check under the lock just to be sure
            if self._purged_on_start is True:
                return  # type: ignore[unreachable] # Mypy doesn't understand that the _purged_on_start can change while we're getting the async lock

            await self._purge()
            self._purged_on_start = True

    async def _purge(self: MemoryStorageClient) -> None:
        """Clean up the default storage directories before the run starts.

        Specifically, `purge` cleans up:
         - local directory containing the default dataset;
         - all records from the default key-value store in the local directory, except for the "INPUT" key;
         - local directory containing the default request queue.
        """
        # Key-value stores
        if await ospath.exists(self._key_value_stores_directory):
            key_value_store_folders = await scandir(self._key_value_stores_directory)
            for key_value_store_folder in key_value_store_folders:
                if key_value_store_folder.name.startswith('__APIFY_TEMPORARY') or key_value_store_folder.name.startswith('__OLD'):
                    await self._batch_remove_files(key_value_store_folder.path)
                elif key_value_store_folder.name == 'default':
                    await self._handle_default_key_value_store(key_value_store_folder.path)

        # Datasets
        if await ospath.exists(self._datasets_directory):
            dataset_folders = await scandir(self._datasets_directory)
            for dataset_folder in dataset_folders:
                if dataset_folder.name == 'default' or dataset_folder.name.startswith('__APIFY_TEMPORARY'):
                    await self._batch_remove_files(dataset_folder.path)
        # Request queues
        if await ospath.exists(self._request_queues_directory):
            request_queue_folders = await scandir(self._request_queues_directory)
            for request_queue_folder in request_queue_folders:
                if request_queue_folder.name == 'default' or request_queue_folder.name.startswith('__APIFY_TEMPORARY'):
                    await self._batch_remove_files(request_queue_folder.path)

    async def _handle_default_key_value_store(self: MemoryStorageClient, folder: str) -> None:
        """Remove everything from the default key-value store folder except `possible_input_keys`."""
        folder_exists = await ospath.exists(folder)
        temporary_path = os.path.normpath(os.path.join(folder, '../__APIFY_MIGRATING_KEY_VALUE_STORE__'))

        # For optimization, we want to only attempt to copy a few files from the default key-value store
        possible_input_keys = [
            'INPUT',
            'INPUT.json',
            'INPUT.bin',
            'INPUT.txt',
        ]

        if folder_exists:
            # Create a temporary folder to save important files in
            Path(temporary_path).mkdir(parents=True, exist_ok=True)

            # Go through each file and save the ones that are important
            for entity in possible_input_keys:
                original_file_path = os.path.join(folder, entity)
                temp_file_path = os.path.join(temporary_path, entity)
                with contextlib.suppress(Exception):
                    await rename(original_file_path, temp_file_path)

            # Remove the original folder and all its content
            counter = 0
            temp_path_for_old_folder = os.path.normpath(os.path.join(folder, f'../__OLD_DEFAULT_{counter}__'))
            done = False
            try:
                while not done:
                    await rename(folder, temp_path_for_old_folder)
                    done = True
            except Exception:
                counter += 1
                temp_path_for_old_folder = os.path.normpath(os.path.join(folder, f'../__OLD_DEFAULT_{counter}__'))

            # Replace the temporary folder with the original folder
            await rename(temporary_path, folder)

            # Remove the old folder
            await self._batch_remove_files(temp_path_for_old_folder)

    async def _batch_remove_files(self: MemoryStorageClient, folder: str, counter: int = 0) -> None:
        folder_exists = await ospath.exists(folder)

        if folder_exists:
            temporary_folder = (
                folder
                if os.path.basename(folder).startswith('__APIFY_TEMPORARY_')
                else os.path.normpath(os.path.join(folder, f'../__APIFY_TEMPORARY_{counter}__'))
            )

            try:
                # Rename the old folder to the new one to allow background deletions
                await rename(folder, temporary_folder)
            except Exception:
                # Folder exists already, try again with an incremented counter
                return await self._batch_remove_files(folder, counter + 1)

            await aioshutil.rmtree(temporary_folder, ignore_errors=True)
        return None
