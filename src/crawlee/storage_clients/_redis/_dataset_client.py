from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._client_mixin import RedisClientMixin
from ._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline

logger = getLogger(__name__)


class RedisDatasetClient(DatasetClient, RedisClientMixin):
    """Memory implementation of the dataset client.

    This client stores dataset items in memory using Python lists and dictionaries. No data is persisted
    between process runs, meaning all stored data is lost when the program terminates. This implementation
    is primarily useful for testing, development, and short-lived crawler operations where persistent
    storage is not required.

    The memory implementation provides fast access to data but is limited by available memory and
    does not support data sharing across different processes. It supports all dataset operations including
    sorting, filtering, and pagination, but performs them entirely in memory.
    """

    _DEFAULT_NAME = 'default'

    _MAIN_KEY = 'dataset'

    def __init__(
        self,
        dataset_name: str,
        redis: Redis,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryDatasetClient.open` class method to create a new instance.
        """
        super().__init__(storage_name=dataset_name, redis=redis)

    @override
    async def get_metadata(self) -> DatasetMetadata:
        metadata_dict = await self._get_metadata_by_name(name=self._storage_name, redis=self._redis)
        if metadata_dict is None:
            raise ValueError(f'Dataset with name "{self._storage_name}" does not exist.')
        return DatasetMetadata.model_validate(metadata_dict)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        redis: Redis,
    ) -> RedisDatasetClient:
        """Open or create a new Redis dataset client.

        This method creates a new Redis dataset instance. Unlike persistent storage implementations, Redis
        datasets don't check for existing datasets with the same name or ID since all data exists only in memory
        and is lost when the process terminates.

        Args:
            id: The ID of the dataset. If not provided, a random ID will be generated.
            name: The name of the dataset. If not provided, the dataset will be unnamed.
            redis: Redis client instance.

        Returns:
            An instance for the opened or created storage client.
        """
        if id:
            dataset_name = await cls._get_metadata_name_by_id(id=id, redis=redis)
            if dataset_name is None:
                raise ValueError(f'Dataset with ID "{id}" does not exist.')
        else:
            search_name = name or cls._DEFAULT_NAME
            metadata_data = await cls._get_metadata_by_name(name=search_name, redis=redis)
            dataset_name = search_name if metadata_data is not None else None
        if dataset_name:
            client = cls(dataset_name=dataset_name, redis=redis)
            async with client._get_pipeline() as pipe:
                await client._update_metadata(pipe, update_accessed_at=True)
        else:
            now = datetime.now(timezone.utc)
            metadata = DatasetMetadata(
                id=crypto_random_object_id(),
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
                item_count=0,
            )
            dataset_name = name or cls._DEFAULT_NAME
            client = cls(dataset_name=dataset_name, redis=redis)
            await client._create_metadata_and_storage(metadata.model_dump())
        return client

    @override
    async def _create_storage(self, pipeline: Pipeline) -> None:
        items_key = f'{self._MAIN_KEY}:{self._storage_name}:items'
        await await_redis_response(pipeline.json().set(items_key, '$', []))

    @override
    async def drop(self) -> None:
        storage_id = (await self.get_metadata()).id
        async with self._get_pipeline() as pipe:
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:metadata')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:items')
            await pipe.delete(f'{self._MAIN_KEY}:id_to_name:{storage_id}')

    @override
    async def purge(self) -> None:
        async with self._get_pipeline() as pipe:
            await self._create_storage(pipe)

            await self._update_metadata(
                pipe,
                update_accessed_at=True,
                update_modified_at=True,
                new_item_count=0,
            )

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        if isinstance(data, dict):
            data = [data]

        async with self._get_pipeline() as pipe:
            # Incorrect signature for args type in redis-py
            pipe.json().arrappend(f'{self._MAIN_KEY}:{self._storage_name}:items', '$', *data)  # type: ignore[arg-type]
            delta_item_count = len(data)
            await self._update_metadata(
                pipe, update_accessed_at=True, update_modified_at=True, delta_item_count=delta_item_count
            )

    @override
    async def get_data(
        self,
        *,
        offset: int = 0,
        limit: int | None = 999_999_999_999,
        clean: bool = False,
        desc: bool = False,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: list[str] | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
        flatten: list[str] | None = None,
        view: str | None = None,
    ) -> DatasetItemsListPage:
        # Check for unsupported arguments and log a warning if found
        # When implementing, explore the capabilities of jsonpath to determine what can be done at the Redis level.
        unsupported_args: dict[str, Any] = {
            'clean': clean,
            'fields': fields,
            'omit': omit,
            'unwind': unwind,
            'skip_hidden': skip_hidden,
            'flatten': flatten,
            'view': view,
        }
        unsupported = {k: v for k, v in unsupported_args.items() if v not in (False, None)}

        if unsupported:
            logger.warning(
                f'The arguments {list(unsupported.keys())} of get_data are not supported '
                f'by the {self.__class__.__name__} client.'
            )

        metadata = await self.get_metadata()

        total = metadata.item_count
        items_key = f'{self._MAIN_KEY}:{self._storage_name}:items'
        json_path = '$'

        # Apply sorting and pagination
        if desc:
            if offset and limit is not None:
                json_path += f'[-{offset + limit}:-{offset}]'
            elif limit is not None:
                json_path += f'[-{limit}:]'
            elif offset:
                json_path += f'[:-{offset}]'
        else:  # noqa: PLR5501 # not a mistake, just to please the linter
            if offset and limit is not None:
                json_path += f'[{offset}:{offset + limit}]'
            elif limit is not None:
                json_path += f'[:{limit}]'
            elif offset:
                json_path += f'[{offset}:]'

        if json_path == '$':
            json_path = '$[*]'

        data = await await_redis_response(self._redis.json().get(items_key, json_path))

        if data is None:
            data = []

        if skip_empty:
            data = [item for item in data if item]

        if desc:
            data = list(reversed(data))

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, update_accessed_at=True)

        return DatasetItemsListPage(
            count=len(data),
            offset=offset,
            limit=limit or (total - offset),
            total=total,
            desc=desc,
            items=data,
        )

    @override
    async def iterate_items(
        self,
        *,
        offset: int = 0,
        limit: int | None = None,
        clean: bool = False,
        desc: bool = False,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: list[str] | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        """Iterate over dataset items one by one.

        This method yields items individually instead of loading all items at once,
        which is more memory efficient for large datasets.
        """
        # Log warnings for unsupported arguments
        unsupported_args: dict[str, Any] = {
            'clean': clean,
            'fields': fields,
            'omit': omit,
            'unwind': unwind,
            'skip_hidden': skip_hidden,
        }
        unsupported = {k: v for k, v in unsupported_args.items() if v not in (False, None)}

        if unsupported:
            logger.warning(
                f'The arguments {list(unsupported.keys())} of iterate_items are not supported '
                f'by the {self.__class__.__name__} client.'
            )

        metadata = await self.get_metadata()
        total_items = metadata.item_count
        items_key = f'{self._MAIN_KEY}:{self._storage_name}:items'

        # Calculate actual range based on parameters
        start_idx = offset
        end_idx = min(total_items, offset + limit) if limit is not None else total_items

        # Update accessed_at timestamp
        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, update_accessed_at=True)

        # Process items in batches for better network efficiency
        batch_size = 100

        for batch_start in range(start_idx, end_idx, batch_size):
            batch_end = min(batch_start + batch_size, end_idx)

            # Build JsonPath for batch slice
            if desc:
                # For descending order, we need to reverse the slice calculation
                desc_batch_start = total_items - batch_end
                desc_batch_end = total_items - batch_start
                json_path = f'$[{desc_batch_start}:{desc_batch_end}]'
            else:
                json_path = f'$[{batch_start}:{batch_end}]'

            # Get batch of items
            batch_items = await await_redis_response(self._redis.json().get(items_key, json_path))

            # Handle case where batch_items might be None or not a list
            if batch_items is None:
                continue

            # Reverse batch if desc order (since we got items in normal order but need desc)
            if desc:
                batch_items = list(reversed(batch_items))

            # Yield items from batch
            for item in batch_items:
                # Apply skip_empty filter
                if skip_empty and not item:
                    continue

                yield cast('dict[str, Any]', item)

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, update_accessed_at=True)

    async def _update_metadata(
        self,
        pipeline: Pipeline,
        *,
        new_item_count: int | None = None,
        delta_item_count: int | None = None,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the dataset metadata with current information.

        Args:
            pipeline: The Redis pipeline to use for the update.
            new_item_count: If provided, update the item count to this value.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
            delta_item_count: If provided, increment the item count by this value.
        """
        metadata_key = f'{self._MAIN_KEY}:{self._storage_name}:metadata'
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.accessed_at', now.isoformat(), nx=False, xx=True)
            )
        if update_modified_at:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.modified_at', now.isoformat(), nx=False, xx=True)
            )
        if new_item_count is not None:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.item_count', new_item_count, nx=False, xx=True)
            )
        elif delta_item_count is not None:
            await await_redis_response(pipeline.json().numincrby(metadata_key, '$.item_count', delta_item_count))
