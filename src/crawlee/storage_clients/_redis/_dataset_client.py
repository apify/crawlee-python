from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from typing_extensions import NotRequired, override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._client_mixin import MetadataUpdateParams, RedisClientMixin
from ._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline

logger = getLogger(__name__)


class _DatasetMetadataUpdateParams(MetadataUpdateParams):
    """Parameters for updating dataset metadata."""

    new_item_count: NotRequired[int]
    delta_item_count: NotRequired[int]


class RedisDatasetClient(DatasetClient, RedisClientMixin):
    """Redis implementation of the dataset client.

    This client persists dataset items to Redis using JSON arrays for efficient storage and retrieval.
    Items are stored as JSON objects with automatic ordering preservation through Redis list operations.

    The dataset data is stored in Redis using the following key pattern:
    - `datasets:{name}:items` - Redis JSON array containing all dataset items.
    - `datasets:{name}:metadata` - Redis JSON object containing dataset metadata.

    Items must be JSON-serializable dictionaries. Single items or lists of items can be pushed to the dataset.
    The item ordering is preserved through Redis JSON array operations. All operations provide atomic consistency
    through Redis transactions and pipeline operations.
    """

    _DEFAULT_NAME = 'default'
    """Default Dataset name key prefix when none provided."""

    _MAIN_KEY = 'datasets'
    """Main Redis key prefix for Dataset."""

    _CLIENT_TYPE = 'Dataset'
    """Human-readable client type for error messages."""

    def __init__(self, storage_name: str, storage_id: str, redis: Redis) -> None:
        """Initialize a new instance.

        Preferably use the `RedisDatasetClient.open` class method to create a new instance.

        Args:
            storage_name: Internal storage name used for Redis keys.
            storage_id: Unique identifier for the dataset.
            redis: Redis client instance.
        """
        super().__init__(storage_name=storage_name, storage_id=storage_id, redis=redis)

    @property
    def _items_key(self) -> str:
        """Return the Redis key for the items of this dataset."""
        return f'{self._MAIN_KEY}:{self._storage_name}:items'

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        redis: Redis,
    ) -> RedisDatasetClient:
        """Open or create a new Redis dataset client.

        This method attempts to open an existing dataset from the Redis database. If a dataset with the specified
        ID or name exists, it loads the metadata from the database. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the dataset. If not provided, a random ID will be generated.
            name: The name of the dataset for named (global scope) storages.
            alias: The alias of the dataset for unnamed (run scope) storages.
            redis: Redis client instance.

        Returns:
            An instance for the opened or created storage client.
        """
        return await cls._open(
            id=id,
            name=name,
            alias=alias,
            redis=redis,
            metadata_model=DatasetMetadata,
            extra_metadata_fields={'item_count': 0},
            instance_kwargs={},
        )

    @override
    async def get_metadata(self) -> DatasetMetadata:
        return await self._get_metadata(DatasetMetadata)

    @override
    async def drop(self) -> None:
        await self._drop(extra_keys=[self._items_key])

    @override
    async def purge(self) -> None:
        await self._purge(
            extra_keys=[self._items_key],
            metadata_kwargs=_DatasetMetadataUpdateParams(
                new_item_count=0, update_accessed_at=True, update_modified_at=True
            ),
        )

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        if isinstance(data, dict):
            data = [data]

        async with self._get_pipeline() as pipe:
            pipe.json().arrappend(self._items_key, '$', *data)
            await self._update_metadata(
                pipe,
                **_DatasetMetadataUpdateParams(
                    update_accessed_at=True, update_modified_at=True, delta_item_count=len(data)
                ),
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
        json_path = '$'

        # Apply sorting and pagination
        match (desc, offset, limit):
            case (True, 0, int()):
                json_path += f'[-{limit}:]'
            case (True, int(), None):
                json_path += f'[:-{offset}]'
            case (True, int(), int()):
                json_path += f'[-{offset + limit}:-{offset}]'
            case (False, 0, int()):
                json_path += f'[:{limit}]'
            case (False, int(), None):
                json_path += f'[{offset}:]'
            case (False, int(), int()):
                json_path += f'[{offset}:{offset + limit}]'

        if json_path == '$':
            json_path = '$[*]'

        data = await await_redis_response(self._redis.json().get(self._items_key, json_path))

        if data is None:
            data = []

        if skip_empty:
            data = [item for item in data if item]

        if desc:
            data = list(reversed(data))

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, **_DatasetMetadataUpdateParams(update_accessed_at=True))

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

        # Calculate actual range based on parameters
        start_idx = offset
        end_idx = min(total_items, offset + limit) if limit is not None else total_items

        # Update accessed_at timestamp
        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, **_DatasetMetadataUpdateParams(update_accessed_at=True))

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
            batch_items = await await_redis_response(self._redis.json().get(self._items_key, json_path))

            # Handle case where batch_items might be None or not a list
            if batch_items is None:
                continue

            # Reverse batch if desc order (since we got items in normal order but need desc)
            items_iter = reversed(batch_items) if desc else iter(batch_items)

            # Yield items from batch
            for item in items_iter:
                # Apply skip_empty filter
                if skip_empty and not item:
                    continue

                yield cast('dict[str, Any]', item)

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, **_DatasetMetadataUpdateParams(update_accessed_at=True))

    @override
    async def _create_storage(self, pipeline: Pipeline) -> None:
        """Create the main dataset keys in Redis."""
        # Create an empty JSON array for items
        await await_redis_response(pipeline.json().set(self._items_key, '$', []))

    @override
    async def _specific_update_metadata(
        self,
        pipeline: Pipeline,
        *,
        new_item_count: int | None = None,
        delta_item_count: int | None = None,
        **_kwargs: Any,
    ) -> None:
        """Update the dataset metadata in the database.

        Args:
            pipeline: The Redis pipeline to use for the update.
            new_item_count: If provided, update the item count to this value.
            delta_item_count: If provided, increment the item count by this value.
        """
        if new_item_count is not None:
            await await_redis_response(
                pipeline.json().set(self.metadata_key, '$.item_count', new_item_count, nx=False, xx=True)
            )
        elif delta_item_count is not None:
            await await_redis_response(pipeline.json().numincrby(self.metadata_key, '$.item_count', delta_item_count))
