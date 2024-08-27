from __future__ import annotations

import csv
import io
import json
import logging
from typing import TYPE_CHECKING, AsyncIterator, Literal, TextIO, TypedDict, cast

from typing_extensions import NotRequired, Required, Unpack, override

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.file import json_dumps
from crawlee.base_storage_client._models import DatasetMetadata
from crawlee.storages._base_storage import BaseStorage
from crawlee.storages._key_value_store import KeyValueStore

if TYPE_CHECKING:
    from crawlee._types import JsonSerializable
    from crawlee.base_storage_client import BaseStorageClient
    from crawlee.base_storage_client._models import DatasetItemsListPage
    from crawlee.configuration import Configuration


logger = logging.getLogger(__name__)


class GetDataKwargs(TypedDict):
    """Keyword arguments for dataset's `get_data` method.

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
    """

    offset: NotRequired[int]
    limit: NotRequired[int]
    clean: NotRequired[bool]
    desc: NotRequired[bool]
    fields: NotRequired[list[str]]
    omit: NotRequired[list[str]]
    unwind: NotRequired[str]
    skip_empty: NotRequired[bool]
    skip_hidden: NotRequired[bool]
    flatten: NotRequired[list[str]]
    view: NotRequired[str]


class PushDataKwargs(TypedDict):
    """Keyword arguments for dataset's `push_data` method."""


class ExportToKwargs(TypedDict):
    """Keyword arguments for dataset's `export_to` method.

    Args:
        key: The key under which to save the data.
        content_type: The format in which to export the data. Either 'json' or 'csv'.
        to_key_value_store_id: ID of the key-value store to save the exported file.
        to_key_value_store_name: Name of the key-value store to save the exported file.
    """

    key: Required[str]
    content_type: NotRequired[Literal['json', 'csv']]
    to_key_value_store_id: NotRequired[str]
    to_key_value_store_name: NotRequired[str]


class Dataset(BaseStorage):
    """Represents an append-only structured storage, ideal for tabular data similar to database tables.

    The `Dataset` class is designed to store structured data, where each entry (row) maintains consistent attributes
    (columns) across the dataset. It operates in an append-only mode, allowing new records to be added, but not
    modified or deleted. This makes it particularly useful for storing results from web crawling operations.

    Data can be stored either locally or in the cloud. It depends on the setup of underlying storage client.
    By default a `MemoryStorageClient` is used, but it can be changed to a different one.

    By default, data is stored using the following path structure:
    ```
    {CRAWLEE_STORAGE_DIR}/datasets/{DATASET_ID}/{INDEX}.json
    ```
    - `{CRAWLEE_STORAGE_DIR}`: The root directory for all storage data specified by the environment variable.
    - `{DATASET_ID}`: Specifies the dataset, either "default" or a custom dataset ID.
    - `{INDEX}`: Represents the zero-based index of the record within the dataset.

    To open a dataset, use the `open` class method by specifying an `id`, `name`, or `configuration`. If none are
    provided, the default dataset for the current crawler run is used. Attempting to open a dataset by `id` that does
    not exist will raise an error; however, if accessed by `name`, the dataset will be created if it doesn't already
    exist.

    Usage:
    ```python
    dataset = await Dataset.open(name='my_dataset')
    ```
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
        client: BaseStorageClient,
    ) -> None:
        self._id = id
        self._name = name
        self._configuration = configuration

        # Get resource clients from storage client
        self._resource_client = client.dataset(self._id)
        self._resource_collection_client = client.datasets()

    @override
    @property
    def id(self) -> str:
        return self._id

    @override
    @property
    def name(self) -> str | None:
        return self._name

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: BaseStorageClient | None = None,
    ) -> Dataset:
        from crawlee.storages._creation_management import open_storage

        return await open_storage(
            storage_class=cls,
            id=id,
            name=name,
            configuration=configuration,
            storage_client=storage_client,
        )

    @override
    async def drop(self) -> None:
        from crawlee.storages._creation_management import remove_storage_from_cache

        await self._resource_client.delete()
        remove_storage_from_cache(storage_class=self.__class__, id=self._id, name=self._name)

    async def push_data(self, data: JsonSerializable, **kwargs: Unpack[PushDataKwargs]) -> None:
        """Store an object or an array of objects to the dataset.

        The size of the data is limited by the receiving API and therefore `push_data()` will only
        allow objects whose JSON representation is smaller than 9MB. When an array is passed,
        none of the included objects may be larger than 9MB, but the array itself may be of any size.

        Args:
            data: A JSON serializable data structure to be stored in the dataset. The JSON representation
                of each item must be smaller than 9MB.
            kwargs: Keyword arguments for the storage client method.
        """
        # Handle singular items
        if not isinstance(data, list):
            items = await self._check_and_serialize(data)
            return await self._resource_client.push_items(items, **kwargs)

        # Handle lists
        payloads_generator = (await self._check_and_serialize(item, index) for index, item in enumerate(data))

        # Invoke client in series to preserve the order of data
        async for items in self._chunk_by_size(payloads_generator):
            await self._resource_client.push_items(items, **kwargs)

        return None

    async def get_data(self, **kwargs: Unpack[GetDataKwargs]) -> DatasetItemsListPage:
        """Retrieves dataset items based on filtering, sorting, and pagination parameters.

        This method allows customization of the data retrieval process from a dataset, supporting operations such as
        field selection, ordering, and skipping specific records based on provided parameters.

        Args:
            kwargs: Keyword arguments for the storage client method.

        Returns:
            List page containing filtered and paginated dataset items.
        """
        # TODO: Improve error handling here
        # https://github.com/apify/apify-sdk-python/issues/140
        return await self._resource_client.list_items(**kwargs)

    async def write_to(self, content_type: Literal['json', 'csv'], destination: TextIO) -> None:
        """Exports the entire dataset into an arbitrary stream.

        Args:
            content_type: Specifies the output format
            destination: The stream into which the dataset contents should be written
        """
        items: list[dict] = []
        limit = 1000
        offset = 0

        while True:
            list_items = await self._resource_client.list_items(limit=limit, offset=offset)
            items.extend(list_items.items)
            if list_items.total <= offset + list_items.count:
                break
            offset += list_items.count

        if content_type == 'csv':
            if items:
                writer = csv.writer(destination, quoting=csv.QUOTE_MINIMAL)
                writer.writerows([items[0].keys(), *[item.values() for item in items]])
            else:
                logger.warning('Attempting to export an empty dataset - no file will be created')
        elif content_type == 'json':
            json.dump(items, destination)
        else:
            raise ValueError(f'Unsupported content type: {content_type}')

    async def export_to(self, **kwargs: Unpack[ExportToKwargs]) -> None:
        """Exports the entire dataset into a specified file stored under a key in a key-value store.

        This method consolidates all entries from a specified dataset into one file, which is then saved under a
        given key in a key-value store. The format of the exported file is determined by the `content_type` parameter.
        Either the dataset's ID or name should be specified, and similarly, either the target key-value store's ID or
        name should be used.

        Args:
            kwargs: Keyword arguments for the storage client method.
        """
        key = cast(str, kwargs.get('key'))
        content_type = kwargs.get('content_type', 'json')
        to_key_value_store_id = kwargs.get('to_key_value_store_id', None)
        to_key_value_store_name = kwargs.get('to_key_value_store_name', None)

        key_value_store = await KeyValueStore.open(id=to_key_value_store_id, name=to_key_value_store_name)

        output = io.StringIO()
        await self.write_to(content_type, output)

        if content_type == 'csv':
            await key_value_store.set_value(key, output.getvalue(), 'text/csv')

        if content_type == 'json':
            await key_value_store.set_value(key, output.getvalue(), 'application/json')

    async def get_info(self) -> DatasetMetadata | None:
        """Get an object containing general information about the dataset."""
        metadata = await self._resource_client.get()
        if isinstance(metadata, DatasetMetadata):
            return metadata
        return None

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
        async for item in self._resource_client.iterate_items(  # type: ignore
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

    async def _check_and_serialize(self, item: JsonSerializable, index: int | None = None) -> str:
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
