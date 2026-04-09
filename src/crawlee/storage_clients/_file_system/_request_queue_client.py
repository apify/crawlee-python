from __future__ import annotations

import json
from logging import getLogger
from typing import TYPE_CHECKING

from crawlee_storage import FileSystemRequestQueueClient as NativeRequestQueueClient
from typing_extensions import Self, override

from crawlee import Request
from crawlee.events._types import Event, EventPersistStateData
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    ProcessedRequest,
    RequestQueueMetadata,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class FileSystemRequestQueueClient(RequestQueueClient):
    """A file system implementation of the request queue client.

    This client persists requests to the file system as individual JSON files, making it suitable for scenarios
    where data needs to survive process restarts. Each request is stored as a separate file in a directory
    structure following the pattern:

    ```
    {STORAGE_DIR}/request_queues/{QUEUE_ID}/{REQUEST_ID}.json
    ```

    This implementation is ideal for long-running crawlers where persistence is important and for situations
    where you need to resume crawling after process termination.

    Backed by the native ``crawlee_storage`` Rust extension for performance.
    """

    def __init__(
        self,
        *,
        native_client: NativeRequestQueueClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemRequestQueueClient.open` class method to create a new instance.
        """
        self._native_client = native_client
        self._event_listener_registered = False

    @property
    def path_to_rq(self) -> Path:
        """The full path to the request queue directory."""
        return self._native_client.path_to_rq

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the request queue metadata file."""
        return self._native_client.path_to_metadata

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        raw = await self._native_client.get_metadata()
        return RequestQueueMetadata(**raw)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        configuration: Configuration,
    ) -> Self:
        """Open or create a file system request queue client.

        This method attempts to open an existing request queue from the file system. If a queue with the specified
        ID or name exists, it loads the metadata and state from the stored files. If no existing queue is found,
        a new one is created.

        Queue state is automatically persisted by the native Rust client to the default key-value store.
        The Python side only needs to trigger ``persist_state`` via the framework event system.

        Args:
            id: The ID of the request queue to open. If provided, searches for existing queue by ID.
            name: The name of the request queue for named (global scope) storages.
            alias: The alias of the request queue for unnamed (run scope) storages.
            configuration: The configuration object containing storage directory settings.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a queue with the specified ID is not found, if metadata is invalid,
                or if both name and alias are provided.
        """
        native_client = await NativeRequestQueueClient.open(
            id=id,
            name=name,
            alias=alias,
            storage_dir=str(configuration.storage_dir),
        )

        client = cls(native_client=native_client)

        # Hook the native client's ``persist_state`` into the Crawlee event
        # system so that state is saved periodically and on shutdown.
        try:
            from crawlee import service_locator  # noqa: PLC0415

            event_manager = service_locator.get_event_manager()
            event_manager.on(event=Event.PERSIST_STATE, listener=client._on_persist_state)
            client._event_listener_registered = True
        except Exception:
            logger.debug('Could not register PERSIST_STATE listener - event manager may not be initialised yet.')

        return client

    async def _on_persist_state(self, _event_data: EventPersistStateData | None = None) -> None:
        """Event handler that persists the native client state."""
        await self._native_client.persist_state()

    @override
    async def drop(self) -> None:
        self._deregister_event_listener()
        await self._native_client.drop_storage()

    @override
    async def purge(self) -> None:
        await self._native_client.purge()

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        # Serialize requests to dicts for the native client.
        request_dicts = [json.loads(r.model_dump_json()) for r in requests]

        raw = await self._native_client.add_batch_of_requests(request_dicts, forefront=forefront)
        return AddRequestsResponse(**raw)

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        raw = await self._native_client.get_request(unique_key)

        if raw is None:
            return None

        return Request.model_validate(raw)

    @override
    async def fetch_next_request(self) -> Request | None:
        raw = await self._native_client.fetch_next_request()

        if raw is None:
            return None

        return Request.model_validate(raw)

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        request_dict = json.loads(request.model_dump_json())
        raw = await self._native_client.mark_request_as_handled(request_dict)

        if raw is None:
            return None

        return ProcessedRequest(**raw)

    @override
    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        request_dict = json.loads(request.model_dump_json())
        raw = await self._native_client.reclaim_request(request_dict, forefront=forefront)

        if raw is None:
            return None

        return ProcessedRequest(**raw)

    @override
    async def is_empty(self) -> bool:
        return await self._native_client.is_empty()

    def _deregister_event_listener(self) -> None:
        """Remove the PERSIST_STATE event listener if it was registered."""
        if not self._event_listener_registered:
            return
        try:
            from crawlee import service_locator  # noqa: PLC0415

            event_manager = service_locator.get_event_manager()
            event_manager.off(event=Event.PERSIST_STATE, listener=self._on_persist_state)
            self._event_listener_registered = False
        except Exception:
            logger.debug('Could not deregister PERSIST_STATE listener.')
