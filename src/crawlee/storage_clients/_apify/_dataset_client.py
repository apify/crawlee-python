from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar

from apify_client import ApifyClientAsync
from typing_extensions import override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime

    from apify_client.clients import DatasetClientAsync

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class ApifyDatasetClient(DatasetClient):
    """An Apify platform implementation of the dataset client."""

    _cache_by_name: ClassVar[dict[str, ApifyDatasetClient]] = {}
    """A dictionary to cache clients by their names."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        item_count: int,
        api_client: DatasetClientAsync,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `ApifyDatasetClient.open` class method to create a new instance.
        """
        self._metadata = DatasetMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
            item_count=item_count,
        )

        self._api_client = api_client
        """The Apify dataset client for API operations."""

        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

    @override
    @property
    def metadata(self) -> DatasetMetadata:
        return self._metadata

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> ApifyDatasetClient:
        default_name = configuration.default_dataset_id
        token = 'configuration.apify_token'  # TODO: use the real value
        api_url = 'configuration.apify_api_url'  # TODO: use the real value

        name = name or default_name

        # Check if the client is already cached by name.
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata()  # noqa: SLF001
            return client

        # Otherwise, create a new one.
        apify_client_async = ApifyClientAsync(
            token=token,
            api_url=api_url,
            max_retries=8,
            min_delay_between_retries_millis=500,
            timeout_secs=360,
        )

        apify_datasets_client = apify_client_async.datasets()

        metadata = DatasetMetadata.model_validate(
            await apify_datasets_client.get_or_create(name=id if id is not None else name),
        )

        apify_dataset_client = apify_client_async.dataset(dataset_id=metadata.id)

        client = cls(
            id=metadata.id,
            name=metadata.name,
            created_at=metadata.created_at,
            accessed_at=metadata.accessed_at,
            modified_at=metadata.modified_at,
            item_count=metadata.item_count,
            api_client=apify_dataset_client,
        )

        # Cache the client by name.
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        async with self._lock:
            await self._api_client.delete()

            # Remove the client from the cache.
            if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
                del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

    @override
    async def push_data(self, data: list[Any] | dict[str, Any]) -> None:
        async with self._lock:
            await self._api_client.push_items(items=data)
            await self._update_metadata()

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
        unwind: str | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
        flatten: list[str] | None = None,
        view: str | None = None,
    ) -> DatasetItemsListPage:
        response = await self._api_client.list_items(
            offset=offset,
            limit=limit,
            clean=clean,
            desc=desc,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
            flatten=flatten,
            view=view,
        )
        result = DatasetItemsListPage.model_validate(vars(response))
        await self._update_metadata()
        return result

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
        unwind: str | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[dict]:
        async for item in self._api_client.iterate_items(
            offset=offset,
            limit=limit,
            clean=clean,
            desc=desc,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
        ):
            yield item

        await self._update_metadata()

    async def _update_metadata(self) -> None:
        """Update the dataset metadata file with current information."""
        metadata = await self._api_client.get()
        self._metadata = DatasetMetadata.model_validate(metadata)
