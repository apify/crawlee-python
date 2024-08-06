from __future__ import annotations

import asyncio
import contextlib
import os
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import aioshutil
from aiofiles import ospath
from aiofiles.os import rename, scandir
from typing_extensions import override

from crawlee.base_storage_client import BaseStorageClient
from crawlee.configuration import Configuration
from crawlee.memory_storage_client.dataset_client import DatasetClient
from crawlee.memory_storage_client.dataset_collection_client import DatasetCollectionClient
from crawlee.memory_storage_client.key_value_store_client import KeyValueStoreClient
from crawlee.memory_storage_client.key_value_store_collection_client import KeyValueStoreCollectionClient
from crawlee.memory_storage_client.request_queue_client import RequestQueueClient
from crawlee.memory_storage_client.request_queue_collection_client import RequestQueueCollectionClient

if TYPE_CHECKING:
    from crawlee.base_storage_client.types import ResourceClient

TResourceClient = TypeVar('TResourceClient', DatasetClient, KeyValueStoreClient, RequestQueueClient)

logger = getLogger(__name__)


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

    def __init__(self, configuration: Configuration | None = None) -> None:
        """Create a new instance.

        Args:
            configuration: Configuration object to use. If None, a default Configuration object will be created.
        """
        self._explicit_configuration = configuration

        self.datasets_handled: list[DatasetClient] = []
        self.key_value_stores_handled: list[KeyValueStoreClient] = []
        self.request_queues_handled: list[RequestQueueClient] = []

        self._purged_on_start = False  # Indicates whether a purge was already performed on this instance.
        self._purge_lock = asyncio.Lock()

    @property
    def _configuration(self) -> Configuration:
        return self._explicit_configuration or Configuration.get_global_configuration()

    @property
    def write_metadata(self) -> bool:
        """Whether to write metadata to the storage."""
        return self._configuration.write_metadata

    @property
    def persist_storage(self) -> bool:
        """Whether to persist the storage."""
        return self._configuration.persist_storage

    @property
    def storage_dir(self) -> str:
        """Path to the storage directory."""
        return self._configuration.storage_dir

    @property
    def datasets_directory(self) -> str:
        """Path to the directory containing datasets."""
        return os.path.join(self.storage_dir, 'datasets')

    @property
    def key_value_stores_directory(self) -> str:
        """Path to the directory containing key-value stores."""
        return os.path.join(self.storage_dir, 'key_value_stores')

    @property
    def request_queues_directory(self) -> str:
        """Path to the directory containing request queues."""
        return os.path.join(self.storage_dir, 'request_queues')

    @override
    def dataset(self, id: str) -> DatasetClient:
        return DatasetClient(
            memory_storage_client=self,
            id=id,
        )

    @override
    def datasets(self) -> DatasetCollectionClient:
        return DatasetCollectionClient(
            memory_storage_client=self,
        )

    @override
    def key_value_store(self, id: str) -> KeyValueStoreClient:
        return KeyValueStoreClient(
            memory_storage_client=self,
            id=id,
        )

    @override
    def key_value_stores(self) -> KeyValueStoreCollectionClient:
        return KeyValueStoreCollectionClient(
            memory_storage_client=self,
        )

    @override
    def request_queue(self, id: str) -> RequestQueueClient:
        return RequestQueueClient(
            memory_storage_client=self,
            id=id,
        )

    @override
    def request_queues(self) -> RequestQueueCollectionClient:
        return RequestQueueCollectionClient(
            memory_storage_client=self,
        )

    @override
    async def purge_on_start(self) -> None:
        # Optimistic, non-blocking check
        if self._purged_on_start is True:
            logger.debug('Storage was already purged on start.')
            return

        async with self._purge_lock:
            # Another check under the lock just to be sure
            if self._purged_on_start is True:
                # Mypy doesn't understand that the _purged_on_start can change while we're getting the async lock
                return  # type: ignore[unreachable]

            await self._purge_default_storages()
            self._purged_on_start = True

    def get_cached_resource_client(
        self, resource_client_class: type[TResourceClient], id: str | None, name: str | None
    ) -> TResourceClient | None:
        """Try to return a resource client from the internal cache."""
        if issubclass(resource_client_class, DatasetClient):
            cache = self.datasets_handled
        elif issubclass(resource_client_class, KeyValueStoreClient):
            cache = self.key_value_stores_handled
        elif issubclass(resource_client_class, RequestQueueClient):
            cache = self.request_queues_handled
        else:
            return None

        for storage_client in cache:
            if storage_client.id == id or (
                storage_client.name and name and storage_client.name.lower() == name.lower()
            ):
                return storage_client  # pyright: ignore

        return None

    def add_resource_client_to_cache(self, resource_client: ResourceClient) -> None:
        """Add a new resource client to the internal cache."""
        if isinstance(resource_client, DatasetClient):
            self.datasets_handled.append(resource_client)
        if isinstance(resource_client, KeyValueStoreClient):
            self.key_value_stores_handled.append(resource_client)
        if isinstance(resource_client, RequestQueueClient):
            self.request_queues_handled.append(resource_client)

    async def _purge_default_storages(self) -> None:
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
                elif key_value_store_folder.name == self._configuration.default_key_value_store_id:
                    await self._handle_default_key_value_store(key_value_store_folder.path)

        # Datasets
        if await ospath.exists(self.datasets_directory):
            dataset_folders = await scandir(self.datasets_directory)
            for dataset_folder in dataset_folders:
                if dataset_folder.name == self._configuration.default_dataset_id or dataset_folder.name.startswith(
                    self._TEMPORARY_DIR_NAME
                ):
                    await self._batch_remove_files(dataset_folder.path)

        # Request queues
        if await ospath.exists(self.request_queues_directory):
            request_queue_folders = await scandir(self.request_queues_directory)
            for request_queue_folder in request_queue_folders:
                if (
                    request_queue_folder.name == self._configuration.default_request_queue_id
                    or request_queue_folder.name.startswith(self._TEMPORARY_DIR_NAME)
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
