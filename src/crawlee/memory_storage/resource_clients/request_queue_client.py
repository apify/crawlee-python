from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

import aiofiles
import aioshutil
from aiofiles.os import makedirs
from sortedcollections import ValueSortedDict  # type: ignore

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.data_processing import (
    filter_out_none_values_recursively,
    raise_on_duplicate_storage,
    raise_on_non_existing_storage,
)
from crawlee._utils.file import (
    force_remove,
    force_rename,
    json_dumps,
    persist_metadata_if_enabled,
)
from crawlee._utils.requests import unique_key_to_request_id
from crawlee.memory_storage.resource_clients.base_resource_client import BaseResourceClient
from crawlee.memory_storage.resource_clients.types import ResourceInfo
from crawlee.models import RequestData
from crawlee.storages.types import RequestQueueOperationInfo, StorageTypes

if TYPE_CHECKING:
    from crawlee.memory_storage.memory_storage_client import MemoryStorageClient


class RequestQueueClient(BaseResourceClient):
    """Sub-client for manipulating a single request queue."""

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id_: str | None = None,
        name: str | None = None,
        created_at: datetime | None = None,
        accessed_at: datetime | None = None,
        modified_at: datetime | None = None,
        handled_request_count: int = 0,
        pending_request_count: int = 0,
    ) -> None:
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client
        self.id = id_ or crypto_random_object_id()
        self.name = name
        self._created_at = created_at or datetime.now(timezone.utc)
        self._accessed_at = accessed_at or datetime.now(timezone.utc)
        self._modified_at = modified_at or datetime.now(timezone.utc)
        self.handled_request_count = handled_request_count
        self.pending_request_count = pending_request_count

        self.resource_directory = os.path.join(base_storage_directory, name or self.id)
        self.requests = ValueSortedDict(lambda request: request.order_no or -float('inf'))
        self.file_operation_lock = asyncio.Lock()
        self._last_used_timestamp = Decimal(0.0)

    async def get(self) -> ResourceInfo | None:
        """Retrieve the request queue.

        Returns:
            The retrieved request queue, or None, if it does not exist
        """
        found = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id_=self.id, name=self.name
        )

        if found:
            async with found.file_operation_lock:
                await found.update_timestamps(has_been_modified=False)
                return found.to_resource_info()

        return None

    async def update(self, *, name: str | None = None) -> ResourceInfo:
        """Update the request queue with specified fields.

        Args:
            name: The new name for the request queue

        Returns:
            The updated request queue
        """
        # Check by id
        existing_queue_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id_=self.id, name=self.name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        # Skip if no changes
        if name is None:
            return existing_queue_by_id.to_resource_info()

        async with existing_queue_by_id.file_operation_lock:
            # Check that name is not in use already
            existing_queue_by_name = next(
                (
                    queue
                    for queue in self._memory_storage_client.request_queues_handled
                    if queue.name and queue.name.lower() == name.lower()
                ),
                None,
            )

            if existing_queue_by_name is not None:
                raise_on_duplicate_storage(StorageTypes.REQUEST_QUEUE, 'name', name)

            existing_queue_by_id.name = name
            previous_dir = existing_queue_by_id.resource_directory
            existing_queue_by_id.resource_directory = os.path.join(
                self._memory_storage_client.request_queues_directory,
                name,
            )

            await force_rename(previous_dir, existing_queue_by_id.resource_directory)

            # Update timestamps
            await existing_queue_by_id.update_timestamps(has_been_modified=True)

            return existing_queue_by_id.to_resource_info()

    async def delete(self) -> None:
        """Delete the request queue."""
        queue = next(
            (queue for queue in self._memory_storage_client.request_queues_handled if queue.id == self.id),
            None,
        )

        if queue is not None:
            async with queue.file_operation_lock:
                self._memory_storage_client.request_queues_handled.remove(queue)
                queue.pending_request_count = 0
                queue.handled_request_count = 0
                queue.requests.clear()

                if os.path.exists(queue.resource_directory):
                    await aioshutil.rmtree(queue.resource_directory)

    async def list_head(self, *, limit: int | None = None) -> dict:  # TODO: predelat
        """Retrieve a given number of requests from the beginning of the queue.

        Args:
            limit: How many requests to retrieve

        Returns:
            The desired number of requests from the beginning of the queue.
        """
        existing_queue_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id_=self.id, name=self.name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        async with existing_queue_by_id.file_operation_lock:
            await existing_queue_by_id.update_timestamps(has_been_modified=False)

            items: list[RequestData] = []

            # Iterate all requests in the queue which have sorted key larger than infinity, which means `orderNo`
            # is not `None`. This will iterate them in order of `orderNo`.
            for request_key in existing_queue_by_id.requests.irange_key(min_key=-float('inf'), inclusive=(False, True)):
                if len(items) == limit:
                    break

                request = existing_queue_by_id.requests.get(request_key)

                # Check that the request still exists and was not handled,
                # in case something deleted it or marked it as handled concurrenctly
                if request and request.order_no:
                    items.append(request)

            return {
                'limit': limit,
                'hadMultipleClients': False,
                'queueModifiedAt': existing_queue_by_id._modified_at,  # noqa: SLF001
                'items': [self._json_to_request(item.json_) for item in items],
            }

    async def add_request(
        self,
        request: RequestData,
        *,
        forefront: bool | None = None,
    ) -> RequestQueueOperationInfo:
        """Add a request to the queue.

        Args:
            request: The request to add to the queue
            forefront: Whether to add the request to the head or the end of the queue

        Returns:
            The added request.
        """
        existing_queue_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id_=self.id, name=self.name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        request_model = self._create_internal_request(request, forefront)

        async with existing_queue_by_id.file_operation_lock:
            existing_request_with_id = existing_queue_by_id.requests.get(request_model.id)

            # We already have the request present, so we return information about it
            if existing_request_with_id is not None:
                await existing_queue_by_id.update_timestamps(has_been_modified=False)

                return RequestQueueOperationInfo(
                    request_id=request_model.id,
                    request_unique_key=request_model.unique_key,
                    was_already_present=True,
                    was_already_handled=existing_request_with_id.order_no is None,
                )

            existing_queue_by_id.requests[request_model.id] = request_model
            if request_model.order_no is None:
                existing_queue_by_id.handled_request_count += 1
            else:
                existing_queue_by_id.pending_request_count += 1
            await existing_queue_by_id.update_timestamps(has_been_modified=True)
            await self._persist_single_request_to_storage(
                request=request_model,
                entity_directory=existing_queue_by_id.resource_directory,
                persist_storage=self._memory_storage_client.persist_storage,
            )

            # We return wasAlreadyHandled is false even though the request may have been added as handled,
            # because that's how API behaves.
            return RequestQueueOperationInfo(
                request_id=request_model.id,
                request_unique_key=request_model.unique_key,
                was_already_present=False,
                was_already_handled=False,
            )

    async def get_request(self, request_id: str) -> RequestData | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve

        Returns:
            The retrieved request, or None, if it did not exist.
        """
        existing_queue_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id_=self.id, name=self.name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        async with existing_queue_by_id.file_operation_lock:
            await existing_queue_by_id.update_timestamps(has_been_modified=False)

            request: RequestData = existing_queue_by_id.requests.get(request_id)
            return self._json_to_request(request.json_ if request is not None else None)

    async def update_request(
        self,
        request: RequestData,
        *,
        forefront: bool | None = None,
    ) -> RequestQueueOperationInfo:
        """Update a request in the queue.

        Args:
            request: The updated request
            forefront: Whether to put the updated request in the beginning or the end of the queue

        Returns:
            The updated request
        """
        existing_queue_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id_=self.id, name=self.name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        request_model = self._create_internal_request(request, forefront)

        # First we need to check the existing request to be able to return information about its handled state.
        existing_request = existing_queue_by_id.requests.get(request_model.id)

        # Undefined means that the request is not present in the queue.
        # We need to insert it, to behave the same as API.
        if existing_request is None:
            return await self.add_request(request, forefront=forefront)

        async with existing_queue_by_id.file_operation_lock:
            # When updating the request, we need to make sure that
            # the handled counts are updated correctly in all cases.
            existing_queue_by_id.requests[request_model.id] = request_model

            pending_count_adjustment = 0
            is_request_handled_state_changing = not isinstance(existing_request.order_no, type(request_model.order_no))
            request_was_handled_before_update = existing_request.order_no is None

            # We add 1 pending request if previous state was handled
            if is_request_handled_state_changing:
                pending_count_adjustment = 1 if request_was_handled_before_update else -1

            existing_queue_by_id.pending_request_count += pending_count_adjustment
            existing_queue_by_id.handled_request_count -= pending_count_adjustment
            await existing_queue_by_id.update_timestamps(has_been_modified=True)
            await self._persist_single_request_to_storage(
                request=request_model,
                entity_directory=existing_queue_by_id.resource_directory,
                persist_storage=self._memory_storage_client.persist_storage,
            )

            return RequestQueueOperationInfo(
                request_id=request_model.id,
                request_unique_key=request_model.unique_key,
                was_already_present=True,
                was_already_handled=request_was_handled_before_update,
            )

    async def delete_request(self, request_id: str) -> None:
        """Delete a request from the queue.

        Args:
            request_id: ID of the request to delete.
        """
        existing_queue_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id_=self.id, name=self.name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        async with existing_queue_by_id.file_operation_lock:
            request = existing_queue_by_id.requests.get(request_id)

            if request:
                del existing_queue_by_id.requests[request_id]
                if request.order_no is None:
                    existing_queue_by_id.handled_request_count -= 1
                else:
                    existing_queue_by_id.pending_request_count -= 1
                await existing_queue_by_id.update_timestamps(has_been_modified=True)
                await self._delete_request_file_from_storage(
                    entity_directory=existing_queue_by_id.resource_directory,
                    request_id=request_id,
                )

    async def _persist_single_request_to_storage(
        self,
        *,
        request: RequestData,
        entity_directory: str,
        persist_storage: bool,
    ) -> None:
        """Updates or writes a single request item to the disk.

        This function writes a given request dictionary to a JSON file, named after the request's ID,
        within a specified directory. The writing process is skipped if `persist_storage` is False.
        Before writing, it ensures that the target directory exists, creating it if necessary.

        Args:
            request: The dictionary containing the request data.
            entity_directory: The directory path where the request file should be stored.
            persist_storage: A boolean flag indicating whether the request should be persisted to the disk.
        """
        # Skip writing files to the disk if the client has the option set to false
        if not persist_storage:
            return

        # Ensure the directory for the entity exists
        await makedirs(entity_directory, exist_ok=True)

        # Write the request to the file
        file_path = os.path.join(entity_directory, f'{request.id}.json')
        async with aiofiles.open(file_path, mode='wb') as f:
            await f.write(json_dumps(request).encode('utf-8'))

    async def _delete_request_file_from_storage(self, *, request_id: str, entity_directory: str) -> None:
        """Deletes a specific request item from the disk.

        This function removes a file representing a request, identified by the request's ID, from a
        specified directory. Before attempting to remove the file, it ensures that the target directory
        exists, creating it if necessary.

        Args:
            request_id: The identifier of the request to be deleted.
            entity_directory: The directory path where the request file is stored.
        """
        # Ensure the directory for the entity exists
        await makedirs(entity_directory, exist_ok=True)

        file_path = os.path.join(entity_directory, f'{request_id}.json')
        await force_remove(file_path)

    def to_resource_info(self) -> ResourceInfo:
        """Retrieve the request queue store info."""
        return ResourceInfo(
            accessed_at=self._accessed_at,
            created_at=self._created_at,
            had_multiple_clients=False,
            handled_request_count=self.handled_request_count,
            id=self.id,
            modified_at=self._modified_at,
            name=self.name,
            pending_request_count=self.pending_request_count,
            stats={},
            total_request_count=len(self.requests),
            user_id='1',
            resource_directory=self.resource_directory,
        )

    async def update_timestamps(self, *, has_been_modified: bool) -> None:
        """Update the timestamps of the request queue."""
        self._accessed_at = datetime.now(timezone.utc)

        if has_been_modified:
            self._modified_at = datetime.now(timezone.utc)

        request_queue_info = self.to_resource_info()
        await persist_metadata_if_enabled(
            data=request_queue_info.__dict__,
            entity_directory=self.resource_directory,
            write_metadata=self._memory_storage_client.write_metadata,
        )

    def _json_to_request(self, request_json: str | None) -> RequestData | None:
        if request_json is None:
            return None
        request_dict = filter_out_none_values_recursively(json.loads(request_json))
        if request_dict is None:
            return None
        return RequestData(**request_dict)

    def _create_internal_request(self, request: RequestData, forefront: bool | None) -> RequestData:
        order_no = self._calculate_order_no(request, forefront)
        id_ = unique_key_to_request_id(request.unique_key)

        if request.id is not None and request.id != id_:
            raise ValueError('Request ID does not match its unique_key.')

        json_request = json_dumps({**(request.__dict__), 'id': id_})
        return RequestData(
            url=request.url,
            unique_key=request.unique_key,
            id=id_,
            method=request.method,
            retry_count=request.retry_count,
            order_no=order_no,
            json_=json_request,
        )

    def _calculate_order_no(self, request: RequestData, forefront: bool | None) -> Decimal | None:
        if request.handled_at is not None:
            return None

        # Get the current timestamp in milliseconds
        timestamp = Decimal(datetime.now(timezone.utc).timestamp()) * 1000
        timestamp = round(timestamp, 6)

        # Make sure that this timestamp was not used yet, so that we have unique orderNos
        if timestamp <= self._last_used_timestamp:
            timestamp = self._last_used_timestamp + Decimal(0.000001)

        self._last_used_timestamp = timestamp

        return -timestamp if forefront else timestamp

    @classmethod
    def _get_storages_dir(cls, memory_storage_client: MemoryStorageClient) -> str:
        return memory_storage_client.request_queues_directory

    @classmethod
    def _get_storage_client_cache(cls, memory_storage_client: MemoryStorageClient) -> list[RequestQueueClient]:
        return memory_storage_client.request_queues_handled

    @classmethod
    def _create_from_directory(
        cls,
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id_: str | None = None,
        name: str | None = None,
    ) -> RequestQueueClient:
        created_at = datetime.now(timezone.utc)
        accessed_at = datetime.now(timezone.utc)
        modified_at = datetime.now(timezone.utc)
        handled_request_count = 0
        pending_request_count = 0
        entries: list[dict] = []

        # Access the request queue folder
        for entry in os.scandir(storage_directory):
            if entry.is_file():
                if entry.name == '__metadata__.json':
                    # We have found the queue's metadata file, build out information based on it
                    with open(os.path.join(storage_directory, entry.name), encoding='utf-8') as f:
                        metadata = json.load(f)

                    id_ = metadata['id']
                    name = metadata['name']
                    created_at = datetime.fromisoformat(metadata['createdAt'])
                    accessed_at = datetime.fromisoformat(metadata['accessedAt'])
                    modified_at = datetime.fromisoformat(metadata['modifiedAt'])
                    handled_request_count = metadata['handledRequestCount']
                    pending_request_count = metadata['pendingRequestCount']
                    continue

                with open(os.path.join(storage_directory, entry.name), encoding='utf-8') as f:
                    request = json.load(f)
                    if request.order_no:
                        request.order_no = Decimal(request.order_no)
                entries.append(request)

        new_client = cls(
            base_storage_directory=memory_storage_client.request_queues_directory,
            memory_storage_client=memory_storage_client,
            id_=id_,
            name=name,
            accessed_at=accessed_at,
            created_at=created_at,
            modified_at=modified_at,
            handled_request_count=handled_request_count,
            pending_request_count=pending_request_count,
        )

        for request in entries:
            new_client.requests[request['id']] = request

        return new_client
