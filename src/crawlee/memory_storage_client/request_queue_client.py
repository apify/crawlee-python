from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from logging import getLogger
from typing import TYPE_CHECKING

import aiofiles
import aioshutil
from aiofiles.os import makedirs
from sortedcollections import ValueSortedDict  # type: ignore
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.data_processing import (
    filter_out_none_values_recursively,
    raise_on_duplicate_storage,
    raise_on_non_existing_storage,
)
from crawlee._utils.file import force_remove, force_rename, json_dumps
from crawlee._utils.requests import unique_key_to_request_id
from crawlee.base_storage_client import BaseRequestQueueClient
from crawlee.memory_storage_client._creation_management import (
    find_or_create_client_by_id_or_name_inner,
    persist_metadata_if_enabled,
)
from crawlee.models import (
    BatchRequestsOperationResponse,
    ProcessedRequest,
    ProlongRequestLockResponse,
    Request,
    RequestListResponse,
    RequestQueueHead,
    RequestQueueHeadWithLocks,
    RequestQueueMetadata,
    UnprocessedRequest,
)
from crawlee.types import StorageTypes

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.memory_storage_client import MemoryStorageClient

logger = getLogger(__name__)


class RequestQueueClient(BaseRequestQueueClient):
    """Subclient for manipulating a single request queue."""

    def __init__(
        self,
        *,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,
        name: str | None = None,
        created_at: datetime | None = None,
        accessed_at: datetime | None = None,
        modified_at: datetime | None = None,
        handled_request_count: int = 0,
        pending_request_count: int = 0,
    ) -> None:
        self._memory_storage_client = memory_storage_client
        self.id = id or crypto_random_object_id()
        self.name = name
        self._created_at = created_at or datetime.now(timezone.utc)
        self._accessed_at = accessed_at or datetime.now(timezone.utc)
        self._modified_at = modified_at or datetime.now(timezone.utc)
        self.handled_request_count = handled_request_count
        self.pending_request_count = pending_request_count

        self.requests = ValueSortedDict(lambda request: request.order_no or -float('inf'))
        self.file_operation_lock = asyncio.Lock()
        self._last_used_timestamp = Decimal(0.0)

    @property
    def resource_info(self) -> RequestQueueMetadata:
        """Get the resource info for the request queue client."""
        return RequestQueueMetadata(
            id=self.id,
            name=self.name,
            accessed_at=self._accessed_at,
            created_at=self._created_at,
            modified_at=self._modified_at,
            had_multiple_clients=False,
            handled_request_count=self.handled_request_count,
            pending_request_count=self.pending_request_count,
            stats={},
            total_request_count=len(self.requests),
            user_id='1',
            resource_directory=self.resource_directory,
        )

    @property
    def resource_directory(self) -> str:
        """Get the resource directory for the client."""
        return os.path.join(self._memory_storage_client.request_queues_directory, self.name or self.id)

    @override
    async def get(self) -> RequestQueueMetadata | None:
        found = find_or_create_client_by_id_or_name_inner(
            resource_client_class=RequestQueueClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if found:
            async with found.file_operation_lock:
                await found.update_timestamps(has_been_modified=False)
                return found.resource_info

        return None

    @override
    async def update(self, *, name: str | None = None) -> RequestQueueMetadata:
        # Check by id
        existing_queue_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=RequestQueueClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        # Skip if no changes
        if name is None:
            return existing_queue_by_id.resource_info

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

            previous_dir = existing_queue_by_id.resource_directory
            existing_queue_by_id.name = name

            await force_rename(previous_dir, existing_queue_by_id.resource_directory)

            # Update timestamps
            await existing_queue_by_id.update_timestamps(has_been_modified=True)

            return existing_queue_by_id.resource_info

    @override
    async def delete(self) -> None:
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

    @override
    async def list_head(self, *, limit: int | None = None) -> RequestQueueHead:
        existing_queue_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=RequestQueueClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        async with existing_queue_by_id.file_operation_lock:
            await existing_queue_by_id.update_timestamps(has_been_modified=False)

            requests: list[Request] = []

            # Iterate all requests in the queue which have sorted key larger than infinity, which means
            # `order_no` is not `None`. This will iterate them in order of `order_no`.
            for request_key in existing_queue_by_id.requests.irange_key(
                min_key=-float('inf'),
                inclusive=(False, True),
            ):
                if len(requests) == limit:
                    break

                request = existing_queue_by_id.requests.get(request_key)

                # Check that the request still exists and was not handled,
                # in case something deleted it or marked it as handled concurrenctly
                if request and request.order_no:
                    requests.append(request)

            items = [request for item in requests if (request := self._json_to_request(item.json_))]

            return RequestQueueHead(
                limit=limit,
                had_multiple_clients=False,
                queue_modified_at=existing_queue_by_id._modified_at,  # noqa: SLF001
                items=items,
            )

    @override
    async def list_and_lock_head(self, *, lock_secs: int, limit: int | None = None) -> RequestQueueHeadWithLocks:
        raise NotImplementedError('This method is not supported in memory storage.')

    @override
    async def add_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        existing_queue_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=RequestQueueClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        request_model = await self._create_internal_request(request, forefront)

        async with existing_queue_by_id.file_operation_lock:
            existing_request_with_id = existing_queue_by_id.requests.get(request_model.id)

            # We already have the request present, so we return information about it
            if existing_request_with_id is not None:
                await existing_queue_by_id.update_timestamps(has_been_modified=False)

                return ProcessedRequest(
                    id=request_model.id,
                    unique_key=request_model.unique_key,
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
            return ProcessedRequest(
                id=request_model.id,
                unique_key=request_model.unique_key,
                was_already_present=False,
                was_already_handled=False,
            )

    @override
    async def get_request(self, request_id: str) -> Request | None:
        existing_queue_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=RequestQueueClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        async with existing_queue_by_id.file_operation_lock:
            await existing_queue_by_id.update_timestamps(has_been_modified=False)

            request: Request = existing_queue_by_id.requests.get(request_id)
            return self._json_to_request(request.json_ if request is not None else None)

    @override
    async def update_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        existing_queue_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=RequestQueueClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_queue_by_id is None:
            raise_on_non_existing_storage(StorageTypes.REQUEST_QUEUE, self.id)

        request_model = await self._create_internal_request(request, forefront)

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

            return ProcessedRequest(
                id=request_model.id,
                unique_key=request_model.unique_key,
                was_already_present=True,
                was_already_handled=request_was_handled_before_update,
            )

    @override
    async def delete_request(self, request_id: str) -> None:
        existing_queue_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=RequestQueueClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
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

    @override
    async def prolong_request_lock(
        self,
        request_id: str,
        *,
        forefront: bool = False,
        lock_secs: int,
    ) -> ProlongRequestLockResponse:
        raise NotImplementedError('This method is not supported in memory storage.')

    @override
    async def delete_request_lock(
        self,
        request_id: str,
        *,
        forefront: bool = False,
    ) -> None:
        raise NotImplementedError('This method is not supported in memory storage.')

    @override
    async def batch_add_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> BatchRequestsOperationResponse:
        processed_requests = list[ProcessedRequest]()
        unprocessed_requests = list[UnprocessedRequest]()

        for request in requests:
            try:
                processed_request = await self.add_request(request, forefront=forefront)
                processed_requests.append(
                    ProcessedRequest(
                        id=processed_request.id,
                        unique_key=processed_request.unique_key,
                        was_already_present=processed_request.was_already_present,
                        was_already_handled=processed_request.was_already_handled,
                    )
                )
            except Exception as exc:  # noqa: PERF203
                logger.warning(f'Error adding request to the queue: {exc}')
                unprocessed_requests.append(
                    UnprocessedRequest(
                        unique_key=request.unique_key,
                        url=request.url,
                        method=request.method,
                    )
                )

        return BatchRequestsOperationResponse(
            processed_requests=processed_requests,
            unprocessed_requests=unprocessed_requests,
        )

    @override
    async def batch_delete_requests(self, requests: list[Request]) -> BatchRequestsOperationResponse:
        raise NotImplementedError('This method is not supported in memory storage.')

    @override
    async def list_requests(
        self,
        *,
        limit: int | None = None,
        exclusive_start_id: str | None = None,
    ) -> RequestListResponse:
        raise NotImplementedError('This method is not supported in memory storage.')

    async def update_timestamps(self, *, has_been_modified: bool) -> None:
        """Update the timestamps of the request queue."""
        self._accessed_at = datetime.now(timezone.utc)

        if has_been_modified:
            self._modified_at = datetime.now(timezone.utc)

        await persist_metadata_if_enabled(
            data=self.resource_info.model_dump(),
            entity_directory=self.resource_directory,
            write_metadata=self._memory_storage_client.write_metadata,
        )

    async def _persist_single_request_to_storage(
        self,
        *,
        request: Request,
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
            s = await json_dumps(request.model_dump())
            await f.write(s.encode('utf-8'))

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

    def _json_to_request(self, request_json: str | None) -> Request | None:
        if request_json is None:
            return None
        request_dict = filter_out_none_values_recursively(json.loads(request_json))
        if request_dict is None:
            return None
        return Request.model_validate(request_dict)

    async def _create_internal_request(self, request: Request, forefront: bool | None) -> Request:
        order_no = self._calculate_order_no(request, forefront)
        id = unique_key_to_request_id(request.unique_key)

        if request.id is not None and request.id != id:
            raise ValueError('Request ID does not match its unique_key.')

        json_request = await json_dumps({**(request.model_dump()), 'id': id})
        return Request(
            url=request.url,
            unique_key=request.unique_key,
            id=id,
            method=request.method,
            retry_count=request.retry_count,
            order_no=order_no,
            json_=json_request,
            user_data={},
        )

    def _calculate_order_no(self, request: Request, forefront: bool | None) -> Decimal | None:
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
