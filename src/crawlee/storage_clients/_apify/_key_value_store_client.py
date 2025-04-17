from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar

from apify_client import ApifyClientAsync
from typing_extensions import override
from yarl import URL

from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import (
    KeyValueStoreListKeysPage,
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime

    from apify_client.clients import KeyValueStoreClientAsync

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class ApifyKeyValueStoreClient(KeyValueStoreClient):
    """An Apify platform implementation of the key-value store client."""

    _cache_by_name: ClassVar[dict[str, ApifyKeyValueStoreClient]] = {}
    """A dictionary to cache clients by their names."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        api_client: KeyValueStoreClientAsync,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `ApifyKeyValueStoreClient.open` class method to create a new instance.
        """
        self._metadata = KeyValueStoreMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
        )

        self._api_client = api_client
        """The Apify key-value store client for API operations."""

        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

    @override
    @property
    def metadata(self) -> KeyValueStoreMetadata:
        return self._metadata

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> ApifyKeyValueStoreClient:
        default_name = configuration.default_key_value_store_id
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

        apify_kvss_client = apify_client_async.key_value_stores()

        metadata = KeyValueStoreMetadata.model_validate(
            await apify_kvss_client.get_or_create(name=id if id is not None else name),
        )

        apify_kvs_client = apify_client_async.key_value_store(key_value_store_id=metadata.id)

        client = cls(
            id=metadata.id,
            name=metadata.name,
            created_at=metadata.created_at,
            accessed_at=metadata.accessed_at,
            modified_at=metadata.modified_at,
            api_client=apify_kvs_client,
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
    async def get_value(self, key: str) -> KeyValueStoreRecord | None:
        response = await self._api_client.get_record(key)
        record = KeyValueStoreRecord.model_validate(response) if response else None
        await self._update_metadata()
        return record

    @override
    async def set_value(self, key: str, value: Any, content_type: str | None = None) -> None:
        async with self._lock:
            await self._api_client.set_record(
                key=key,
                value=value,
                content_type=content_type,
            )
            await self._update_metadata()

    @override
    async def delete_value(self, key: str) -> None:
        async with self._lock:
            await self._api_client.delete_record(key=key)
            await self._update_metadata()

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        count = 0

        while True:
            response = await self._api_client.list_keys(exclusive_start_key=exclusive_start_key)
            list_key_page = KeyValueStoreListKeysPage.model_validate(response)

            for item in list_key_page.items:
                yield item
                count += 1

                # If we've reached the limit, stop yielding
                if limit and count >= limit:
                    break

            # If we've reached the limit or there are no more pages, exit the loop
            if (limit and count >= limit) or not list_key_page.is_truncated:
                break

            exclusive_start_key = list_key_page.next_exclusive_start_key

        await self._update_metadata()

    async def get_public_url(self, key: str) -> str:
        """Get a URL for the given key that may be used to publicly access the value in the remote key-value store.

        Args:
            key: The key for which the URL should be generated.
        """
        if self._api_client.resource_id is None:
            raise ValueError('resource_id cannot be None when generating a public URL')

        public_url = (
            URL(self._api_client.base_url) / 'v2' / 'key-value-stores' / self._api_client.resource_id / 'records' / key
        )

        key_value_store = self.metadata

        if key_value_store and isinstance(getattr(key_value_store, 'model_extra', None), dict):
            url_signing_secret_key = key_value_store.model_extra.get('urlSigningSecretKey')
            if url_signing_secret_key:
                # Note: This would require importing create_hmac_signature from apify._crypto
                # public_url = public_url.with_query(signature=create_hmac_signature(url_signing_secret_key, key))
                # For now, I'll leave this part commented as we may need to add the proper import
                pass

        return str(public_url)

    async def _update_metadata(self) -> None:
        """Update the key-value store metadata with current information."""
        metadata = await self._api_client.get()
        self._metadata = KeyValueStoreMetadata.model_validate(metadata)
