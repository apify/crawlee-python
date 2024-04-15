from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path

import aioshutil
from aiofiles import ospath
from aiofiles.os import rename, scandir
from typing_extensions import override

from crawlee._utils.data_processing import maybe_parse_bool
from crawlee._utils.env_vars import CrawleeEnvVars
from crawlee.resource_clients.dataset_client import DatasetClient
from crawlee.resource_clients.dataset_collection_client import DatasetCollectionClient
from crawlee.resource_clients.key_value_store_client import KeyValueStoreClient
from crawlee.resource_clients.key_value_store_collection_client import KeyValueStoreCollectionClient
from crawlee.resource_clients.request_queue_client import RequestQueueClient
from crawlee.resource_clients.request_queue_collection_client import RequestQueueCollectionClient
from crawlee.storage_clients.base_storage_client import BaseStorageClient


class MemoryStorageClient(BaseStorageClient):
    """Represents an in-memory storage client for managing datasets, key-value stores, and request queues.

    It emulates in-memory storage similar to the Apify platform, supporting both in-memory and local file system-based
    persistence.

    The behavior of the storage, such as data persistence and metadata writing, can be customized via initialization
    parameters or environment variables.
    """

    _MIGRATING_KEY_VALUE_STORE_DIR_NAME = '__CRAWLEE_MIGRATING_KEY_VALUE_STORE'
    """Name of the directory used to temporarily store files during the migration of the default key-value store."""

    _TEMPORARY_DIR_NAME = '__CRAWLEE_TEMPORARY'
    """Name of the directory used to temporarily store files during purges."""

    def __init__(
        self,
        *,
        local_data_directory: str | None = None,
        write_metadata: bool | None = None,
        persist_storage: bool | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            local_data_directory: Path to the local directory where data will be persisted. If None, defaults to
                CrawleeEnvVars.LOCAL_STORAGE_DIR or './storage'.

            write_metadata: Flag indicating whether to write metadata for the storages. Defaults based on DEBUG
                environment variable.

            persist_storage: Flag indicating whether to persist the storage data locally. Defaults based on
                CrawleeEnvVars.PERSIST_STORAGE.
        """
        self._local_data_directory = local_data_directory or os.getenv(CrawleeEnvVars.LOCAL_STORAGE_DIR) or './storage'
        self.write_metadata = write_metadata if write_metadata is not None else '*' in os.getenv('DEBUG', '')
        self.persist_storage = (
            persist_storage
            if persist_storage is not None
            else maybe_parse_bool(os.getenv(CrawleeEnvVars.PERSIST_STORAGE, 'true'))
        )

        self._purged_on_start = False  # Indicates whether a purge was already performed on this instance.
        self.datasets_handled: list[DatasetClient] = []
        self.key_value_stores_handled: list[KeyValueStoreClient] = []
        self.request_queues_handled: list[RequestQueueClient] = []
        self._purge_lock = asyncio.Lock()

    @property
    def datasets_directory(self) -> str:
        """Path to the directory containing datasets."""
        return os.path.join(self._local_data_directory, 'datasets')

    @property
    def key_value_stores_directory(self) -> str:
        """Path to the directory containing key-value stores."""
        return os.path.join(self._local_data_directory, 'key_value_stores')

    @property
    def request_queues_directory(self) -> str:
        """Path to the directory containing request queues."""
        return os.path.join(self._local_data_directory, 'request_queues')

    @override
    def dataset(self, id: str) -> DatasetClient:
        return DatasetClient(
            base_storage_directory=self.datasets_directory,
            memory_storage_client=self,
            id=id,
        )

    @override
    def datasets(self) -> DatasetCollectionClient:
        return DatasetCollectionClient(
            base_storage_directory=self.datasets_directory,
            memory_storage_client=self,
        )

    @override
    def key_value_store(self, id: str) -> KeyValueStoreClient:
        return KeyValueStoreClient(
            base_storage_directory=self.key_value_stores_directory,
            memory_storage_client=self,
            id=id,
        )

    @override
    def key_value_stores(self) -> KeyValueStoreCollectionClient:
        return KeyValueStoreCollectionClient(
            base_storage_directory=self.key_value_stores_directory,
            memory_storage_client=self,
        )

    @override
    def request_queue(self, id: str) -> RequestQueueClient:
        return RequestQueueClient(
            base_storage_directory=self.request_queues_directory,
            memory_storage_client=self,
            id=id,
        )

    @override
    def request_queues(self) -> RequestQueueCollectionClient:
        return RequestQueueCollectionClient(
            base_storage_directory=self.request_queues_directory,
            memory_storage_client=self,
        )

    async def purge_on_start(self) -> None:
        """Performs a purge of the default storage directories.

        This method ensures that the purge is executed only once during the lifetime of the instance.
        It is primarily used to clean up residual data from previous runs to maintain a clean state.
        """
        # Optimistic, non-blocking check
        if self._purged_on_start is True:
            return

        async with self._purge_lock:
            # Another check under the lock just to be sure
            if self._purged_on_start is True:
                # Mypy doesn't understand that the _purged_on_start can change while we're getting the async lock
                return  # type: ignore[unreachable]

            await self._purge_inner()
            self._purged_on_start = True

    async def _purge_inner(self) -> None:
        """Cleans up the storage directories, preparing the environment for a new run.

        It aims to remove residues from previous executions to avoid data contamination between runs.

        It specifically targets:
         - The local directory containing the default dataset.
         - All records from the default key-value store in the local directory, except for the 'INPUT' key.
         - The local directory containing the default request queue.
        """
        # Key-value stores
        if await ospath.exists(self.key_value_stores_directory):
            key_value_store_folders = await scandir(self.key_value_stores_directory)
            for key_value_store_folder in key_value_store_folders:
                if key_value_store_folder.name.startswith(
                    self._TEMPORARY_DIR_NAME
                ) or key_value_store_folder.name.startswith('__OLD'):
                    await self._batch_remove_files(key_value_store_folder.path)
                elif key_value_store_folder.name == 'default':
                    await self._handle_default_key_value_store(key_value_store_folder.path)

        # Datasets
        if await ospath.exists(self.datasets_directory):
            dataset_folders = await scandir(self.datasets_directory)
            for dataset_folder in dataset_folders:
                if dataset_folder.name == 'default' or dataset_folder.name.startswith(self._TEMPORARY_DIR_NAME):
                    await self._batch_remove_files(dataset_folder.path)

        # Request queues
        if await ospath.exists(self.request_queues_directory):
            request_queue_folders = await scandir(self.request_queues_directory)
            for request_queue_folder in request_queue_folders:
                if request_queue_folder.name == 'default' or request_queue_folder.name.startswith(
                    self._TEMPORARY_DIR_NAME
                ):
                    await self._batch_remove_files(request_queue_folder.path)

    async def _handle_default_key_value_store(self, folder: str) -> None:
        """Manages the cleanup of the default key-value store.

        It removes all files to ensure a clean state except for a set of predefined input keys (`possible_input_keys`).

        Args:
            folder: Path to the default key-value store directory to clean.
        """
        folder_exists = await ospath.exists(folder)
        temporary_path = os.path.normpath(os.path.join(folder, '..', self._MIGRATING_KEY_VALUE_STORE_DIR_NAME))

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

    async def _batch_remove_files(self, folder: str, counter: int = 0) -> None:
        """Removes a folder and its contents in batches to minimize blocking time.

        This method first renames the target folder to a temporary name, then deletes the temporary folder,
        allowing the file system operations to proceed without hindering other asynchronous tasks.

        Args:
            folder: The directory path to remove.
            counter: A counter used for generating temporary directory names in case of conflicts.
        """
        folder_exists = await ospath.exists(folder)

        if folder_exists:
            temporary_folder = (
                folder
                if os.path.basename(folder).startswith(f'{self._TEMPORARY_DIR_NAME}_')
                else os.path.normpath(os.path.join(folder, '..', f'{self._TEMPORARY_DIR_NAME}_{counter}'))
            )

            try:
                # Rename the old folder to the new one to allow background deletions
                await rename(folder, temporary_folder)
            except Exception:
                # Folder exists already, try again with an incremented counter
                return await self._batch_remove_files(folder, counter + 1)

            await aioshutil.rmtree(temporary_folder, ignore_errors=True)
        return None
