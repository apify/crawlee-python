from __future__ import annotations

import csv
import io
from typing import TYPE_CHECKING, AsyncIterator

from typing_extensions import override

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.file import json_dumps
from crawlee.storages.base_storage import BaseStorage
from crawlee.storages.key_value_store import KeyValueStore

if TYPE_CHECKING:
    from crawlee.configuration import Configuration
    from crawlee.resource_clients import DatasetClient, DatasetCollectionClient
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storages.models import DatasetItemsListPage, DatasetMetadata
    from crawlee.types import JSONSerializable


class Dataset(BaseStorage):
    """Represents an append-only structured storage, ideal for tabular data akin to database tables.

    Represents a structured data store similar to a table, where each object (row) has consistent attributes (columns).
    Datasets operate on an append-only basis, allowing for the addition of new records without the modification or
    removal of existing ones. This class is typically used for storing crawling results.

    Data can be stored locally or in the cloud, with local storage paths formatted as:
    `{CRAWLEE_LOCAL_STORAGE_DIR}/datasets/{DATASET_ID}/{INDEX}.json`. Here, `{DATASET_ID}` is either "default" or
    a specific dataset ID, and `{INDEX}` represents the zero-based index of the item in the dataset.

    To open a dataset, use the `open` class method with an `id`, `name`, or `config`. If unspecified, the default
    dataset for the current crawler run is used. Opening a non-existent dataset by `id` raises an error, while
    by `name`, it is created.

    Usage:
        dataset = await Dataset.open(id='my_dataset_id')
    """

    _MAX_PAYLOAD_SIZE = ByteSize.from_mb(9)
    """Maximum size for a single payload."""

    _SAFETY_BUFFER_PERCENT = 0.01 / 100  # 0.01%
    """Percentage buffer to reduce payload limit slightly for safety."""

    _EFFECTIVE_LIMIT_SIZE = _MAX_PAYLOAD_SIZE - (_MAX_PAYLOAD_SIZE * _SAFETY_BUFFER_PERCENT)
    """Calculated payload limit considering safety buffer."""

    def __init__(
        self,
        id: str,
        name: str | None,
        configuration: Configuration,
        client: MemoryStorageClient,
    ) -> None:
        super().__init__(id=id, name=name, client=client, configuration=configuration)
        self._dataset_client = client.dataset(self.id)

    @classmethod
    @override
    def _get_human_friendly_label(cls) -> str:
        return 'Dataset'

    @classmethod
    @override
    def _get_default_id(cls, configuration: Configuration) -> str:
        return configuration.default_dataset_id

    @classmethod
    @override
    def _get_single_storage_client(cls, id: str, client: MemoryStorageClient) -> DatasetClient:
        return client.dataset(id)

    @classmethod
    @override
    def _get_storage_collection_client(cls, client: MemoryStorageClient) -> DatasetCollectionClient:
        return client.datasets()

    async def push_data(self, data: JSONSerializable) -> None:
        """Store an object or an array of objects to the dataset.

        The size of the data is limited by the receiving API and therefore `push_data()` will only
        allow objects whose JSON representation is smaller than 9MB. When an array is passed,
        none of the included objects may be larger than 9MB, but the array itself may be of any size.

        Args:
            data: A JSON serializable data structure to be stored in the dataset.
                The JSON representation of each item must be smaller than 9MB.
        """
        # Handle singular items
        if not isinstance(data, list):
            payload = await self._check_and_serialize(data)
            return await self._dataset_client.push_items(payload)

        # Handle lists
        payloads_generator = (await self._check_and_serialize(item, index) for index, item in enumerate(data))

        # Invoke client in series to preserve the order of data
        async for chunk in self._chunk_by_size(payloads_generator):
            await self._dataset_client.push_items(chunk)

        return None

    async def get_data(
        self,
        *,
        offset: int | None = None,
        limit: int | None = None,
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
        """Retrieves dataset items based on filtering, sorting, and pagination parameters.

        This method allows customization of the data retrieval process from a dataset, supporting operations such as
        field selection, ordering, and skipping specific records based on provided parameters.

        Args:
            offset: Skips the specified number of items at the start.
            limit: The maximum number of items to retrieve. Unlimited if None.
            clean: Returns only non-empty items and excludes hidden fields. Shortcut for skip_hidden and skip_empty.
            desc: Set True to sort results in descending order.
            fields: Fields to include in each item. Sorts fields as specified if provided.
            omit: Fields to exclude from each item.
            unwind: Unwinds items by a specified array field, turning each element into a separate item.
            skip_empty: Excludes empty items from the results if True.
            skip_hidden: Excludes fields starting with '#' if True.
            flatten: Fields to be flattened in returned items.
            view: Specifies the dataset view to be used.

        Returns:
            List page containing filtered and paginated dataset items.
        """
        # TODO: Improve error handling here
        # https://github.com/apify/apify-sdk-python/issues/140
        return await self._dataset_client.list_items(
            offset=offset,
            limit=limit,
            desc=desc,
            clean=clean,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
            flatten=flatten,
            view=view,
        )

    async def export_to_json(
        self,
        key: str,
        *,
        to_key_value_store_id: str | None = None,
        to_key_value_store_name: str | None = None,
    ) -> None:
        """Exports a dataset's contents into a single JSON file in a specified key-value store.

        This method consolidates all entries from a specified dataset into one JSON file, which is then saved under a
        given key in a key-value store. Either the dataset's ID or name should be specified, and similarly, either the
        target key-value store's ID or name should be used.

        Args:
            key: The key under which to save the JSON data.
            to_key_value_store_id: ID of the key-value store to save the exported file. Defaults to default store.
            to_key_value_store_name: Name of the key-value store to save the exported file. Use if no store ID provided.

        Note:
            Specify only one of `from_dataset_id` and `from_dataset_name`, and one of `to_key_value_store_id` and
            `to_key_value_store_name`. If both or neither are specified in each pair, defaults are used.
        """
        await self._export_to(
            key,
            to_key_value_store_id=to_key_value_store_id,
            to_key_value_store_name=to_key_value_store_name,
            content_type='application/json',
        )

    async def export_to_csv(
        self,
        key: str,
        *,
        to_key_value_store_id: str | None = None,
        to_key_value_store_name: str | None = None,
    ) -> None:
        """Exports the entire dataset contents into a CSV file stored under a specified key in a key-value store.

        The method exports data from the specified dataset, identifying it by `from_dataset_id` or `from_dataset_name`,
        into a CSV file. This file is then saved in the designated key-value store, determined
        by `to_key_value_store_id` or `to_key_value_store_name`.

        Args:
            key: Key under which to save the CSV data.
            to_key_value_store_id: Optional key-value store ID to save the exported file.
            to_key_value_store_name: Optional key-value store name to save the exported file.

        Note:
            Specify only one dataset source (`from_dataset_id` or `from_dataset_name`) and one storage destination
            (`to_key_value_store_id` or `to_key_value_store_name`). Default settings apply if omitted.
        """
        await self._export_to(
            key,
            to_key_value_store_id=to_key_value_store_id,
            to_key_value_store_name=to_key_value_store_name,
            content_type='text/csv',
        )

    async def get_info(self) -> DatasetMetadata | None:
        """Get an object containing general information about the dataset.

        Returns:
            Object returned by calling the GET dataset API endpoint.
        """
        return await self._dataset_client.get()

    def iterate_items(
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
        """Iterates over dataset items, applying filtering, sorting, and pagination.

        Retrieves dataset items incrementally, allowing fine-grained control over the data fetched. The function
        supports various parameters to filter, sort, and limit the data returned, facilitating tailored dataset
        queries.

        Args:
            offset: Initial number of items to skip.
            limit: Max number of items to return. No limit if None.
            clean: Filters out empty items and hidden fields if True.
            desc: Returns items in reverse order if True.
            fields: Specific fields to include in each item.
            omit: Fields to omit from each item.
            unwind: Field name to unwind items by.
            skip_empty: Omits empty items if True.
            skip_hidden: Excludes fields starting with '#' if True.

        Yields:
            Each item from the dataset as a dictionary.
        """
        return self._dataset_client.iterate_items(
            offset=offset,
            limit=limit,
            clean=clean,
            desc=desc,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
        )

    async def drop(self) -> None:
        """Remove the dataset either from the Apify cloud storage or from the local directory."""
        await self._dataset_client.delete()
        self._remove_from_cache()

    async def _export_to(
        self,
        key: str,
        *,
        to_key_value_store_id: str | None = None,
        to_key_value_store_name: str | None = None,
        content_type: str | None = None,
    ) -> None:
        """Save the entirety of the dataset's contents into one file within a key-value store.

        Args:
            key: The key to save the data under.

            to_key_value_store_id: The id of the key-value store in which the result will be saved.

            to_key_value_store_name: The name of the key-value store in which the result will be saved. You must
                specify only one of `to_key_value_store_id` and `to_key_value_store_name` arguments. If you omit both,
                it uses the default key-value store.

            content_type: Either 'text/csv' or 'application/json'. Defaults to JSON.
        """
        key_value_store = await KeyValueStore.open(id=to_key_value_store_id, name=to_key_value_store_name)
        items: list[dict] = []
        limit = 1000
        offset = 0
        while True:
            list_items = await self._dataset_client.list_items(limit=limit, offset=offset)
            items.extend(list_items.items)
            if list_items.total <= offset + list_items.count:
                break
            offset += list_items.count

        if len(items) == 0:
            raise ValueError('Cannot export an empty dataset')

        if content_type == 'text/csv':
            output = io.StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
            writer.writerows([items[0].keys(), *[item.values() for item in items]])
            value = output.getvalue()
            return await key_value_store.set_value(key, value, content_type)

        if content_type == 'application/json':
            return await key_value_store.set_value(key, items)

        raise ValueError(f'Unsupported content type: {content_type}')

    async def _check_and_serialize(self, item: JSONSerializable, index: int | None = None) -> str:
        """Serializes a given item to JSON, checks its serializability and size against a limit.

        Args:
            item: The item to serialize.
            index: Index of the item, used for error context.

        Returns:
            Serialized JSON string.

        Raises:
            ValueError: If item is not JSON serializable or exceeds size limit.
        """
        s = ' ' if index is None else f' at index {index} '

        try:
            payload = await json_dumps(item)
        except Exception as exc:
            raise ValueError(f'Data item{s}is not serializable to JSON.') from exc

        payload_size = ByteSize(len(payload.encode('utf-8')))
        if payload_size > self._EFFECTIVE_LIMIT_SIZE:
            raise ValueError(f'Data item{s}is too large (size: {payload_size}, limit: {self._EFFECTIVE_LIMIT_SIZE})')

        return payload

    async def _chunk_by_size(self, items: AsyncIterator[str]) -> AsyncIterator[str]:
        """Yields chunks of JSON arrays composed of input strings, respecting a size limit.

        Groups an iterable of JSON string payloads into larger JSON arrays, ensuring the total size
        of each array does not exceed `EFFECTIVE_LIMIT_SIZE`. Each output is a JSON array string that
        contains as many payloads as possible without breaching the size threshold, maintaining the
        order of the original payloads. Assumes individual items are below the size limit.

        Args:
            items: Iterable of JSON string payloads.

        Yields:
            Strings representing JSON arrays of payloads, each staying within the size limit.
        """
        last_chunk_size = ByteSize(2)  # Add 2 bytes for [] wrapper.
        current_chunk = []

        async for payload in items:
            payload_size = ByteSize(len(payload.encode('utf-8')))

            if last_chunk_size + payload_size <= self._EFFECTIVE_LIMIT_SIZE:
                current_chunk.append(payload)
                last_chunk_size += payload_size + ByteSize(1)  # Add 1 byte for ',' separator.
            else:
                yield f'[{",".join(current_chunk)}]'
                current_chunk = [payload]
                last_chunk_size = payload_size + ByteSize(2)  # Add 2 bytes for [] wrapper.

        yield f'[{",".join(current_chunk)}]'
