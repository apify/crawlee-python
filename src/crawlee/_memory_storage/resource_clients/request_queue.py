from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

import aioshutil
from apify_shared.utils import filter_out_none_values_recursively, ignore_docs, json_dumps
from sortedcollections import ValueSortedDict

from apify._crypto import crypto_random_object_id
from apify._memory_storage.file_storage_utils import delete_request, update_metadata, update_request_queue_item
from apify._memory_storage.resource_clients.base_resource_client import BaseResourceClient
from apify._utils import force_rename, raise_on_duplicate_storage, raise_on_non_existing_storage, unique_key_to_request_id
from apify.consts import StorageTypes

if TYPE_CHECKING:
    from apify._memory_storage.memory_storage_client import MemoryStorageClient


@ignore_docs
class RequestQueueClient(BaseResourceClient):
    """Sub-client for manipulating a single request queue."""

    _id: str
    _resource_directory: str
    _memory_storage_client: MemoryStorageClient
    _name: str | None
    _requests: ValueSortedDict
    _created_at: datetime
    _accessed_at: datetime
    _modified_at: datetime
    _handled_request_count = 0
    _pending_request_count = 0
    _last_used_timestamp = Decimal(0.0)
    _file_operation_lock: asyncio.Lock

    def __init__(
        self: RequestQueueClient,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
    ) -> None:
        """Initialize the RequestQueueClient."""
        self._id = id or crypto_random_object_id()
        self._resource_directory = os.path.join(base_storage_directory, name or self._id)
        self._memory_storage_client = memory_storage_client
        self._name = name
        self._requests = ValueSortedDict(lambda req: req.get('orderNo') or -float('inf'))
        self._created_at = datetime.now(timezone.utc)
        self._accessed_at = datetime.now(timezone.utc)
        self._modified_at = datetime.now(timezone.utc)
        self._file_operation_lock = asyncio.Lock()

    async def get(self: RequestQueueClient) -> dict | None:
        """Retrieve the request queue.

        Returns:
            dict, optional: The retrieved request queue, or None, if it does not exist
        """
        found = self._find_or_create_client_by_id_or_name(memory_storage_client=self._memory_storage_client, id=self._id, name=self._name)

        if found:
            async with found._file_operation_lock:
                await found._update_timestamps(has_been_modified=False)
                return found._to_resource_info()

        return None

    async def update(self: RequestQueueClient, *, name: str | None = None) -> dict:
        """Update the request queue with specified fields.

        Args:
            name (str, optional): The new name for the request queue

        Returns:
            dict: The updated request queue
        """
        # Check by id
        existing_queue_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self._id)

        # Skip if no changes
        if name is None:
            return existing_queue_by_id._to_resource_info()

        async with existing_queue_by_id._file_operation_lock:
            # Check that name is not in use already
            existing_queue_by_name = next(
                (queue for queue in self._memory_storage_client._request_queues_handled if queue._name and queue._name.lower() == name.lower()), None
            )

            if existing_queue_by_name is not None:
                raise_on_duplicate_storage(StorageTypes.REQUEST_QUEUE, 'name', name)

            existing_queue_by_id._name = name

            previous_dir = existing_queue_by_id._resource_directory

            existing_queue_by_id._resource_directory = os.path.join(self._memory_storage_client._request_queues_directory, name)

            await force_rename(previous_dir, existing_queue_by_id._resource_directory)

            # Update timestamps
            await existing_queue_by_id._update_timestamps(has_been_modified=True)

            return existing_queue_by_id._to_resource_info()

    async def delete(self: RequestQueueClient) -> None:
        """Delete the request queue."""
        queue = next((queue for queue in self._memory_storage_client._request_queues_handled if queue._id == self._id), None)

        if queue is not None:
            async with queue._file_operation_lock:
                self._memory_storage_client._request_queues_handled.remove(queue)
                queue._pending_request_count = 0
                queue._handled_request_count = 0
                queue._requests.clear()

                if os.path.exists(queue._resource_directory):
                    await aioshutil.rmtree(queue._resource_directory)

    async def list_head(self: RequestQueueClient, *, limit: int | None = None) -> dict:
        """Retrieve a given number of requests from the beginning of the queue.

        Args:
            limit (int, optional): How many requests to retrieve

        Returns:
            dict: The desired number of requests from the beginning of the queue.
        """
        existing_queue_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self._id)

        async with existing_queue_by_id._file_operation_lock:
            await existing_queue_by_id._update_timestamps(has_been_modified=False)

            items: list[dict] = []

            # Iterate all requests in the queue which have sorted key larger than infinity, which means `orderNo` is not `None`
            # This will iterate them in order of `orderNo`
            for request_key in existing_queue_by_id._requests.irange_key(min_key=-float('inf'), inclusive=(False, True)):
                if len(items) == limit:
                    break

                request = existing_queue_by_id._requests.get(request_key)

                # Check that the request still exists and was not handled,
                # in case something deleted it or marked it as handled concurrenctly
                if request and request['orderNo']:
                    items.append(request)

            return {
                'limit': limit,
                'hadMultipleClients': False,
                'queueModifiedAt': existing_queue_by_id._modified_at,
                'items': [self._json_to_request(item['json']) for item in items],
            }

    async def add_request(self: RequestQueueClient, request: dict, *, forefront: bool | None = None) -> dict:
        """Add a request to the queue.

        Args:
            request (dict): The request to add to the queue
            forefront (bool, optional): Whether to add the request to the head or the end of the queue

        Returns:
            dict: The added request.
        """
        existing_queue_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self._id)

        request_model = self._create_internal_request(request, forefront)

        async with existing_queue_by_id._file_operation_lock:
            existing_request_with_id = existing_queue_by_id._requests.get(request_model['id'])

            # We already have the request present, so we return information about it
            if existing_request_with_id is not None:
                await existing_queue_by_id._update_timestamps(has_been_modified=False)

                return {
                    'requestId': existing_request_with_id['id'],
                    'wasAlreadyHandled': existing_request_with_id['orderNo'] is None,
                    'wasAlreadyPresent': True,
                }

            existing_queue_by_id._requests[request_model['id']] = request_model
            if request_model['orderNo'] is None:
                existing_queue_by_id._handled_request_count += 1
            else:
                existing_queue_by_id._pending_request_count += 1
            await existing_queue_by_id._update_timestamps(has_been_modified=True)
            await update_request_queue_item(
                request=request_model,
                request_id=request_model['id'],
                entity_directory=existing_queue_by_id._resource_directory,
                persist_storage=self._memory_storage_client._persist_storage,
            )

            return {
                'requestId': request_model['id'],
                # We return wasAlreadyHandled: false even though the request may
                # have been added as handled, because that's how API behaves.
                'wasAlreadyHandled': False,
                'wasAlreadyPresent': False,
            }

    async def get_request(self: RequestQueueClient, request_id: str) -> dict | None:
        """Retrieve a request from the queue.

        Args:
            request_id (str): ID of the request to retrieve

        Returns:
            dict, optional: The retrieved request, or None, if it did not exist.
        """
        existing_queue_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self._id)

        async with existing_queue_by_id._file_operation_lock:
            await existing_queue_by_id._update_timestamps(has_been_modified=False)

            request = existing_queue_by_id._requests.get(request_id)
            return self._json_to_request(request['json'] if request is not None else None)

    async def update_request(self: RequestQueueClient, request: dict, *, forefront: bool | None = None) -> dict:
        """Update a request in the queue.

        Args:
            request (dict): The updated request
            forefront (bool, optional): Whether to put the updated request in the beginning or the end of the queue

        Returns:
            dict: The updated request
        """
        existing_queue_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self._id)

        request_model = self._create_internal_request(request, forefront)

        # First we need to check the existing request to be
        # able to return information about its handled state.

        existing_request = existing_queue_by_id._requests.get(request_model['id'])

        # Undefined means that the request is not present in the queue.
        # We need to insert it, to behave the same as API.
        if existing_request is None:
            return await self.add_request(request, forefront=forefront)

        async with existing_queue_by_id._file_operation_lock:
            # When updating the request, we need to make sure that
            # the handled counts are updated correctly in all cases.
            existing_queue_by_id._requests[request_model['id']] = request_model

            pending_count_adjustment = 0
            is_request_handled_state_changing = not isinstance(existing_request['orderNo'], type(request_model['orderNo']))
            request_was_handled_before_update = existing_request['orderNo'] is None

            # We add 1 pending request if previous state was handled
            if is_request_handled_state_changing:
                pending_count_adjustment = 1 if request_was_handled_before_update else -1

            existing_queue_by_id._pending_request_count += pending_count_adjustment
            existing_queue_by_id._handled_request_count -= pending_count_adjustment
            await existing_queue_by_id._update_timestamps(has_been_modified=True)
            await update_request_queue_item(
                request=request_model,
                request_id=request_model['id'],
                entity_directory=existing_queue_by_id._resource_directory,
                persist_storage=self._memory_storage_client._persist_storage,
            )

            return {
                'requestId': request_model['id'],
                'wasAlreadyHandled': request_was_handled_before_update,
                'wasAlreadyPresent': True,
            }

    async def delete_request(self: RequestQueueClient, request_id: str) -> None:
        """Delete a request from the queue.

        Args:
            request_id (str): ID of the request to delete.
        """
        existing_queue_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self._id)

        async with existing_queue_by_id._file_operation_lock:
            request = existing_queue_by_id._requests.get(request_id)

            if request:
                del existing_queue_by_id._requests[request_id]
                if request['orderNo'] is None:
                    existing_queue_by_id._handled_request_count -= 1
                else:
                    existing_queue_by_id._pending_request_count -= 1
                await existing_queue_by_id._update_timestamps(has_been_modified=True)
                await delete_request(entity_directory=existing_queue_by_id._resource_directory, request_id=request_id)

    def _to_resource_info(self: RequestQueueClient) -> dict:
        """Retrieve the request queue store info."""
        return {
            'accessedAt': self._accessed_at,
            'createdAt': self._created_at,
            'hadMultipleClients': False,
            'handledRequestCount': self._handled_request_count,
            'id': self._id,
            'modifiedAt': self._modified_at,
            'name': self._name,
            'pendingRequestCount': self._pending_request_count,
            'stats': {},
            'totalRequestCount': len(self._requests),
            'userId': '1',
        }

    async def _update_timestamps(self: RequestQueueClient, has_been_modified: bool) -> None:  # noqa: FBT001
        self._accessed_at = datetime.now(timezone.utc)

        if has_been_modified:
            self._modified_at = datetime.now(timezone.utc)

        request_queue_info = self._to_resource_info()
        await update_metadata(
            data=request_queue_info,
            entity_directory=self._resource_directory,
            write_metadata=self._memory_storage_client._write_metadata,
        )

    def _json_to_request(self: RequestQueueClient, request_json: str | None) -> dict | None:
        if request_json is None:
            return None
        request = json.loads(request_json)
        return filter_out_none_values_recursively(request)

    def _create_internal_request(self: RequestQueueClient, request: dict, forefront: bool | None) -> dict:
        order_no = self._calculate_order_no(request, forefront)
        id = unique_key_to_request_id(request['uniqueKey'])  # noqa: A001

        if request.get('id') is not None and request['id'] != id:
            raise ValueError('Request ID does not match its unique_key.')

        json_request = json_dumps({**request, 'id': id})
        return {
            'id': id,
            'json': json_request,
            'method': request.get('method'),
            'orderNo': order_no,
            'retryCount': request.get('retryCount', 0),
            'uniqueKey': request['uniqueKey'],
            'url': request['url'],
        }

    def _calculate_order_no(self: RequestQueueClient, request: dict, forefront: bool | None) -> Decimal | None:
        if request.get('handledAt') is not None:
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
    def _get_storages_dir(cls: type[RequestQueueClient], memory_storage_client: MemoryStorageClient) -> str:
        return memory_storage_client._request_queues_directory

    @classmethod
    def _get_storage_client_cache(
        cls: type[RequestQueueClient],
        memory_storage_client: MemoryStorageClient,
    ) -> list[RequestQueueClient]:
        return memory_storage_client._request_queues_handled

    @classmethod
    def _create_from_directory(
        cls: type[RequestQueueClient],
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,  # noqa: A002
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
                    id = metadata['id']  # noqa: A001
                    name = metadata['name']
                    created_at = datetime.fromisoformat(metadata['createdAt'])
                    accessed_at = datetime.fromisoformat(metadata['accessedAt'])
                    modified_at = datetime.fromisoformat(metadata['modifiedAt'])
                    handled_request_count = metadata['handledRequestCount']
                    pending_request_count = metadata['pendingRequestCount']

                    continue

                with open(os.path.join(storage_directory, entry.name), encoding='utf-8') as f:
                    request = json.load(f)
                    if request.get('orderNo'):
                        request['orderNo'] = Decimal(request.get('orderNo'))
                entries.append(request)

        new_client = cls(
            base_storage_directory=memory_storage_client._request_queues_directory,
            memory_storage_client=memory_storage_client,
            id=id,
            name=name,
        )

        # Overwrite properties
        new_client._accessed_at = accessed_at
        new_client._created_at = created_at
        new_client._modified_at = modified_at
        new_client._handled_request_count = handled_request_count
        new_client._pending_request_count = pending_request_count

        for request in entries:
            new_client._requests[request['id']] = request

        return new_client
