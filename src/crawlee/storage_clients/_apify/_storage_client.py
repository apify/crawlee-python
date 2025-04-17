from __future__ import annotations

from typing_extensions import override

from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import ApifyDatasetClient
from ._key_value_store_client import ApifyKeyValueStoreClient
from ._request_queue_client import ApifyRequestQueueClient


class ApifyStorageClient(StorageClient):
    """Apify storage client."""

    @override
    async def open_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> ApifyDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await ApifyDatasetClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start:
            await client.drop()
            client = await ApifyDatasetClient.open(id=id, name=name, configuration=configuration)

        return client

    @override
    async def open_key_value_store_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> ApifyKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await ApifyKeyValueStoreClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start:
            await client.drop()
            client = await ApifyKeyValueStoreClient.open(id=id, name=name, configuration=configuration)

        return client

    @override
    async def open_request_queue_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> ApifyRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await ApifyRequestQueueClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start:
            await client.drop()
            client = await ApifyRequestQueueClient.open(id=id, name=name, configuration=configuration)

        return client
