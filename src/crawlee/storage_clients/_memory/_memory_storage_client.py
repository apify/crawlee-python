from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients import StorageClient

from ._dataset_client import DatasetClient
from ._dataset_collection_client import DatasetCollectionClient
from ._key_value_store_client import KeyValueStoreClient
from ._key_value_store_collection_client import KeyValueStoreCollectionClient
from ._request_queue_client import RequestQueueClient
from ._request_queue_collection_client import RequestQueueCollectionClient

if TYPE_CHECKING:
    from crawlee.storage_clients._base import ResourceClient


TResourceClient = TypeVar('TResourceClient', DatasetClient, KeyValueStoreClient, RequestQueueClient)

logger = getLogger(__name__)


@docs_group('Classes')
class MemoryStorageClient(StorageClient):
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

    _DATASETS_DIR_NAME = 'datasets'
    """Name of the directory containing datasets."""

    _KEY_VALUE_STORES_DIR_NAME = 'key_value_stores'
    """Name of the directory containing key-value stores."""

    _REQUEST_QUEUES_DIR_NAME = 'request_queues'
    """Name of the directory containing request queues."""

    def __init__(
        self,
        *,
        write_metadata: bool,
        persist_storage: bool,
        storage_dir: str,
        default_request_queue_id: str,
        default_key_value_store_id: str,
        default_dataset_id: str,
    ) -> None:
        """Initialize a new instance.

        In most cases, you should use the `from_config` constructor to create a new instance based on
        the provided configuration.

        Args:
            write_metadata: Whether to write metadata to the storage.
            persist_storage: Whether to persist the storage.
            storage_dir: Path to the storage directory.
            default_request_queue_id: The default request queue ID.
            default_key_value_store_id: The default key-value store ID.
            default_dataset_id: The default dataset ID.
        """
        # Set the internal attributes.
        self._write_metadata = write_metadata
        self._persist_storage = persist_storage
        self._storage_dir = storage_dir
        self._default_request_queue_id = default_request_queue_id
        self._default_key_value_store_id = default_key_value_store_id
        self._default_dataset_id = default_dataset_id

        self.datasets_handled: list[DatasetClient] = []
        self.key_value_stores_handled: list[KeyValueStoreClient] = []
        self.request_queues_handled: list[RequestQueueClient] = []

        self._purged_on_start = False  # Indicates whether a purge was already performed on this instance.
        self._purge_lock = asyncio.Lock()

    @classmethod
    def from_config(cls, config: Configuration | None = None) -> MemoryStorageClient:
        """Initialize a new instance based on the provided `Configuration`.

        Args:
            config: The `Configuration` instance. Uses the global (default) one if not provided.
        """
        config = config or Configuration.get_global_configuration()

        return cls(
            write_metadata=config.write_metadata,
            persist_storage=config.persist_storage,
            storage_dir=config.storage_dir,
            default_request_queue_id=config.default_request_queue_id,
            default_key_value_store_id=config.default_key_value_store_id,
            default_dataset_id=config.default_dataset_id,
        )

    @property
    def write_metadata(self) -> bool:
        """Whether to write metadata to the storage."""
        return self._write_metadata

    @property
    def persist_storage(self) -> bool:
        """Whether to persist the storage."""
        return self._persist_storage

    @property
    def storage_dir(self) -> str:
        """Path to the storage directory."""
        return self._storage_dir

    @property
    def datasets_directory(self) -> str:
        """Path to the directory containing datasets."""
        return os.path.join(self.storage_dir, self._DATASETS_DIR_NAME)

    @property
    def key_value_stores_directory(self) -> str:
        """Path to the directory containing key-value stores."""
        return os.path.join(self.storage_dir, self._KEY_VALUE_STORES_DIR_NAME)

    @property
    def request_queues_directory(self) -> str:
        """Path to the directory containing request queues."""
        return os.path.join(self.storage_dir, self._REQUEST_QUEUES_DIR_NAME)

    @override
    def dataset(self, id: str) -> DatasetClient:
        return DatasetClient(memory_storage_client=self, id=id)

    @override
    def datasets(self) -> DatasetCollectionClient:
        return DatasetCollectionClient(memory_storage_client=self)

    @override
    def key_value_store(self, id: str) -> KeyValueStoreClient:
        return KeyValueStoreClient(memory_storage_client=self, id=id)

    @override
    def key_value_stores(self) -> KeyValueStoreCollectionClient:
        return KeyValueStoreCollectionClient(memory_storage_client=self)

    @override
    def request_queue(self, id: str) -> RequestQueueClient:
        return RequestQueueClient(memory_storage_client=self, id=id)

    @override
    def request_queues(self) -> RequestQueueCollectionClient:
        return RequestQueueCollectionClient(memory_storage_client=self)

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
        self,
        resource_client_class: type[TResourceClient],
        id: str | None,
        name: str | None,
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
                return storage_client

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
        """Clean up the storage directories, preparing the environment for a new run.

        It aims to remove residues from previous executions to avoid data contamination between runs.

        It specifically targets:
         - The local directory containing the default dataset.
         - All records from the default key-value store in the local directory, except for the 'INPUT' key.
         - The local directory containing the default request queue.
        """
        # Key-value stores
        if await asyncio.to_thread(os.path.exists, self.key_value_stores_directory):
            key_value_store_folders = await asyncio.to_thread(os.scandir, self.key_value_stores_directory)
            for key_value_store_folder in key_value_store_folders:
                if key_value_store_folder.name.startswith(
                    self._TEMPORARY_DIR_NAME
                ) or key_value_store_folder.name.startswith('__OLD'):
                    await self._batch_remove_files(key_value_store_folder.path)
                elif key_value_store_folder.name == self._default_key_value_store_id:
                    await self._handle_default_key_value_store(key_value_store_folder.path)

        # Datasets
        if await asyncio.to_thread(os.path.exists, self.datasets_directory):
            dataset_folders = await asyncio.to_thread(os.scandir, self.datasets_directory)
            for dataset_folder in dataset_folders:
                if dataset_folder.name == self._default_dataset_id or dataset_folder.name.startswith(
                    self._TEMPORARY_DIR_NAME
                ):
                    await self._batch_remove_files(dataset_folder.path)

        # Request queues
        if await asyncio.to_thread(os.path.exists, self.request_queues_directory):
            request_queue_folders = await asyncio.to_thread(os.scandir, self.request_queues_directory)
            for request_queue_folder in request_queue_folders:
                if request_queue_folder.name == self._default_request_queue_id or request_queue_folder.name.startswith(
                    self._TEMPORARY_DIR_NAME
                ):
                    await self._batch_remove_files(request_queue_folder.path)

    async def _handle_default_key_value_store(self, folder: str) -> None:
        """Manage the cleanup of the default key-value store.

        It removes all files to ensure a clean state except for a set of predefined input keys (`possible_input_keys`).

        Args:
            folder: Path to the default key-value store directory to clean.
        """
        folder_exists = await asyncio.to_thread(os.path.exists, folder)
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
                    await asyncio.to_thread(os.rename, original_file_path, temp_file_path)

            # Remove the original folder and all its content
            counter = 0
            temp_path_for_old_folder = os.path.normpath(os.path.join(folder, f'../__OLD_DEFAULT_{counter}__'))
            done = False
            try:
                while not done:
                    await asyncio.to_thread(os.rename, folder, temp_path_for_old_folder)
                    done = True
            except Exception:
                counter += 1
                temp_path_for_old_folder = os.path.normpath(os.path.join(folder, f'../__OLD_DEFAULT_{counter}__'))

            # Replace the temporary folder with the original folder
            await asyncio.to_thread(os.rename, temporary_path, folder)

            # Remove the old folder
            await self._batch_remove_files(temp_path_for_old_folder)

    async def _batch_remove_files(self, folder: str, counter: int = 0) -> None:
        """Remove a folder and its contents in batches to minimize blocking time.

        This method first renames the target folder to a temporary name, then deletes the temporary folder,
        allowing the file system operations to proceed without hindering other asynchronous tasks.

        Args:
            folder: The directory path to remove.
            counter: A counter used for generating temporary directory names in case of conflicts.
        """
        folder_exists = await asyncio.to_thread(os.path.exists, folder)

        if folder_exists:
            temporary_folder = (
                folder
                if os.path.basename(folder).startswith(f'{self._TEMPORARY_DIR_NAME}_')
                else os.path.normpath(os.path.join(folder, '..', f'{self._TEMPORARY_DIR_NAME}_{counter}'))
            )

            try:
                # Rename the old folder to the new one to allow background deletions
                await asyncio.to_thread(os.rename, folder, temporary_folder)
            except Exception:
                # Folder exists already, try again with an incremented counter
                return await self._batch_remove_files(folder, counter + 1)

            await asyncio.to_thread(shutil.rmtree, temporary_folder, ignore_errors=True)
        return None

    def _get_default_storage_id(self, storage_client_class: type[TResourceClient]) -> str:
        """Get the default storage ID based on the storage class."""
        if issubclass(storage_client_class, DatasetClient):
            return self._default_dataset_id

        if issubclass(storage_client_class, KeyValueStoreClient):
            return self._default_key_value_store_id

        if issubclass(storage_client_class, RequestQueueClient):
            return self._default_request_queue_id

        raise ValueError(f'Invalid storage class: {storage_client_class.__name__}')

    def _get_storage_dir(self, storage_client_class: type[TResourceClient]) -> str:
        """Get the storage directory based on the storage class."""
        if issubclass(storage_client_class, DatasetClient):
            return self.datasets_directory

        if issubclass(storage_client_class, KeyValueStoreClient):
            return self.key_value_stores_directory

        if issubclass(storage_client_class, RequestQueueClient):
            return self.request_queues_directory

        raise ValueError(f'Invalid storage class: {storage_client_class.__name__}')
