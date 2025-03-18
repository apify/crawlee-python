from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal, TextIO, TypedDict, cast

from typing_extensions import NotRequired, Required, Unpack, override

from crawlee import service_locator
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.docs import docs_group
from crawlee._utils.file import json_dumps
from crawlee.storage_clients.models import DatasetMetadata, StorageMetadata

from ._base import Storage
from ._key_value_store import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from crawlee._types import JsonSerializable, PushDataKwargs
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients.models import DatasetItemsListPage

logger = logging.getLogger(__name__)


class GetDataKwargs(TypedDict):
    """Keyword arguments for dataset's `get_data` method."""

    offset: NotRequired[int]
    """Skips the specified number of items at the start."""

    limit: NotRequired[int]
    """The maximum number of items to retrieve. Unlimited if None."""

    clean: NotRequired[bool]
    """Return only non-empty items and excludes hidden fields. Shortcut for skip_hidden and skip_empty."""

    desc: NotRequired[bool]
    """Set to True to sort results in descending order."""

    fields: NotRequired[list[str]]
    """Fields to include in each item. Sorts fields as specified if provided."""

    omit: NotRequired[list[str]]
    """Fields to exclude from each item."""

    unwind: NotRequired[str]
    """Unwinds items by a specified array field, turning each element into a separate item."""

    skip_empty: NotRequired[bool]
    """Excludes empty items from the results if True."""

    skip_hidden: NotRequired[bool]
    """Excludes fields starting with '#' if True."""

    flatten: NotRequired[list[str]]
    """Fields to be flattened in returned items."""

    view: NotRequired[str]
    """Specifies the dataset view to be used."""


class ExportToKwargs(TypedDict):
    """Keyword arguments for dataset's `export_to` method."""

    key: Required[str]
    """The key under which to save the data."""

    content_type: NotRequired[Literal['json', 'csv']]
    """The format in which to export the data. Either 'json' or 'csv'."""

    to_key_value_store_id: NotRequired[str]
    """ID of the key-value store to save the exported file."""

    to_key_value_store_name: NotRequired[str]
    """Name of the key-value store to save the exported file."""


class ExportDataJsonKwargs(TypedDict):
    """Keyword arguments for dataset's `export_data_json` method."""

    skipkeys: NotRequired[bool]
    """If True (default: False), dict keys that are not of a basic type (str, int, float, bool, None) will be skipped
    instead of raising a `TypeError`."""

    ensure_ascii: NotRequired[bool]
    """Determines if non-ASCII characters should be escaped in the output JSON string."""

    check_circular: NotRequired[bool]
    """If False (default: True), skips the circular reference check for container types. A circular reference will
    result in a `RecursionError` or worse if unchecked."""

    allow_nan: NotRequired[bool]
    """If False (default: True), raises a ValueError for out-of-range float values (nan, inf, -inf) to strictly comply
    with the JSON specification. If True, uses their JavaScript equivalents (NaN, Infinity, -Infinity)."""

    cls: NotRequired[type[json.JSONEncoder]]
    """Allows specifying a custom JSON encoder."""

    indent: NotRequired[int]
    """Specifies the number of spaces for indentation in the pretty-printed JSON output."""

    separators: NotRequired[tuple[str, str]]
    """A tuple of (item_separator, key_separator). The default is (', ', ': ') if indent is None and (',', ': ')
    otherwise."""

    default: NotRequired[Callable]
    """A function called for objects that can't be serialized otherwise. It should return a JSON-encodable version
    of the object or raise a `TypeError`."""

    sort_keys: NotRequired[bool]
    """Specifies whether the output JSON object should have keys sorted alphabetically."""


class ExportDataCsvKwargs(TypedDict):
    """Keyword arguments for dataset's `export_data_csv` method."""

    dialect: NotRequired[str]
    """Specifies a dialect to be used in CSV parsing and writing."""

    delimiter: NotRequired[str]
    """A one-character string used to separate fields. Defaults to ','."""

    doublequote: NotRequired[bool]
    """Controls how instances of `quotechar` inside a field should be quoted. When True, the character is doubled;
    when False, the `escapechar` is used as a prefix. Defaults to True."""

    escapechar: NotRequired[str]
    """A one-character string used to escape the delimiter if `quoting` is set to `QUOTE_NONE` and the `quotechar`
    if `doublequote` is False. Defaults to None, disabling escaping."""

    lineterminator: NotRequired[str]
    """The string used to terminate lines produced by the writer. Defaults to '\\r\\n'."""

    quotechar: NotRequired[str]
    """A one-character string used to quote fields containing special characters, like the delimiter or quotechar,
    or fields containing new-line characters. Defaults to '\"'."""

    quoting: NotRequired[int]
    """Controls when quotes should be generated by the writer and recognized by the reader. Can take any of
    the `QUOTE_*` constants, with a default of `QUOTE_MINIMAL`."""

    skipinitialspace: NotRequired[bool]
    """When True, spaces immediately following the delimiter are ignored. Defaults to False."""

    strict: NotRequired[bool]
    """When True, raises an exception on bad CSV input. Defaults to False."""


@docs_group('Classes')
class Dataset(Storage):
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

    ### Usage

    ```python
    from crawlee.storages import Dataset

    dataset = await Dataset.open(name='my_dataset')
    ```
    """

    _MAX_PAYLOAD_SIZE = ByteSize.from_mb(9)
    """Maximum size for a single payload."""

    _SAFETY_BUFFER_PERCENT = 0.01 / 100  # 0.01%
    """Percentage buffer to reduce payload limit slightly for safety."""

    _EFFECTIVE_LIMIT_SIZE = _MAX_PAYLOAD_SIZE - (_MAX_PAYLOAD_SIZE * _SAFETY_BUFFER_PERCENT)
    """Calculated payload limit considering safety buffer."""

    def __init__(self, id: str, name: str | None, storage_client: StorageClient) -> None:
        self._id = id
        self._name = name
        datetime_now = datetime.now(timezone.utc)
        self._storage_object = StorageMetadata(
            id=id, name=name, accessed_at=datetime_now, created_at=datetime_now, modified_at=datetime_now
        )

        # Get resource clients from the storage client.
        self._resource_client = storage_client.dataset(self._id)
        self._resource_collection_client = storage_client.datasets()

    @classmethod
    def from_storage_object(cls, storage_client: StorageClient, storage_object: StorageMetadata) -> Dataset:
        """Create a new instance of Dataset from a storage metadata object."""
        dataset = Dataset(
            id=storage_object.id,
            name=storage_object.name,
            storage_client=storage_client,
        )

        dataset.storage_object = storage_object
        return dataset

    @property
    @override
    def id(self) -> str:
        return self._id

    @property
    @override
    def name(self) -> str | None:
        return self._name

    @property
    @override
    def storage_object(self) -> StorageMetadata:
        return self._storage_object

    @storage_object.setter
    @override
    def storage_object(self, storage_object: StorageMetadata) -> None:
        self._storage_object = storage_object

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: StorageClient | None = None,
    ) -> Dataset:
        from crawlee.storages._creation_management import open_storage

        configuration = configuration or service_locator.get_configuration()
        storage_client = storage_client or service_locator.get_storage_client()

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
            items = await self.check_and_serialize(data)
            return await self._resource_client.push_items(items, **kwargs)

        # Handle lists
        payloads_generator = (await self.check_and_serialize(item, index) for index, item in enumerate(data))

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
        return await self._resource_client.list_items(**kwargs)

    async def write_to_csv(self, destination: TextIO, **kwargs: Unpack[ExportDataCsvKwargs]) -> None:
        """Exports the entire dataset into an arbitrary stream.

        Args:
            destination: The stream into which the dataset contents should be written.
            kwargs: Additional keyword arguments for `csv.writer`.
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

        if items:
            writer = csv.writer(destination, **kwargs)
            writer.writerows([items[0].keys(), *[item.values() for item in items]])
        else:
            logger.warning('Attempting to export an empty dataset - no file will be created')

    async def write_to_json(self, destination: TextIO, **kwargs: Unpack[ExportDataJsonKwargs]) -> None:
        """Exports the entire dataset into an arbitrary stream.

        Args:
            destination: The stream into which the dataset contents should be written.
            kwargs: Additional keyword arguments for `json.dump`.
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

        if items:
            json.dump(items, destination, **kwargs)
        else:
            logger.warning('Attempting to export an empty dataset - no file will be created')

    async def export_to(self, **kwargs: Unpack[ExportToKwargs]) -> None:
        """Exports the entire dataset into a specified file stored under a key in a key-value store.

        This method consolidates all entries from a specified dataset into one file, which is then saved under a
        given key in a key-value store. The format of the exported file is determined by the `content_type` parameter.
        Either the dataset's ID or name should be specified, and similarly, either the target key-value store's ID or
        name should be used.

        Args:
            kwargs: Keyword arguments for the storage client method.
        """
        key = cast('str', kwargs.get('key'))
        content_type = kwargs.get('content_type', 'json')
        to_key_value_store_id = kwargs.get('to_key_value_store_id')
        to_key_value_store_name = kwargs.get('to_key_value_store_name')

        key_value_store = await KeyValueStore.open(id=to_key_value_store_id, name=to_key_value_store_name)

        output = io.StringIO()
        if content_type == 'csv':
            await self.write_to_csv(output)
        elif content_type == 'json':
            await self.write_to_json(output)
        else:
            raise ValueError('Unsupported content type, expecting CSV or JSON')

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
        async for item in self._resource_client.iterate_items(
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

    @classmethod
    async def check_and_serialize(cls, item: JsonSerializable, index: int | None = None) -> str:
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
        if payload_size > cls._EFFECTIVE_LIMIT_SIZE:
            raise ValueError(f'Data item{s}is too large (size: {payload_size}, limit: {cls._EFFECTIVE_LIMIT_SIZE})')

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
