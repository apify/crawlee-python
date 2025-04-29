from __future__ import annotations

import logging
from io import StringIO
from typing import TYPE_CHECKING, overload

from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee._utils.file import export_csv_to_stream, export_json_to_stream

from ._base import Storage
from ._key_value_store import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, ClassVar, Literal

    from typing_extensions import Unpack

    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import DatasetClient
    from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

    from ._types import ExportDataCsvKwargs, ExportDataJsonKwargs

logger = logging.getLogger(__name__)


@docs_group('Classes')
class Dataset(Storage):
    """Dataset is a storage for managing structured tabular data.

    The dataset class provides a high-level interface for storing and retrieving structured data
    with consistent schema, similar to database tables or spreadsheets. It abstracts the underlying
    storage implementation details, offering a consistent API regardless of where the data is
    physically stored.

    Dataset operates in an append-only mode, allowing new records to be added but not modified
    or deleted after creation. This makes it particularly suitable for storing crawling results
    and other data that should be immutable once collected.

    The class provides methods for adding data, retrieving data with various filtering options,
    and exporting data to different formats. You can create a dataset using the `open` class method,
    specifying either a name or ID. The underlying storage implementation is determined by
    the configured storage client.

    ### Usage

    ```python
    from crawlee.storages import Dataset

    # Open a dataset
    dataset = await Dataset.open(name='my_dataset')

    # Add data
    await dataset.push_data({'title': 'Example Product', 'price': 99.99})

    # Retrieve filtered data
    results = await dataset.get_data(limit=10, desc=True)

    # Export data
    await dataset.export_to('results.json', content_type='json')
    ```
    """

    _cache: ClassVar[dict[str, Dataset]] = {}
    """A dictionary to cache datasets."""

    def __init__(self, client: DatasetClient, cache_key: str) -> None:
        """Initialize a new instance.

        Preferably use the `Dataset.open` constructor to create a new instance.

        Args:
            client: An instance of a dataset client.
            cache_key: A unique key to identify the dataset in the cache.
        """
        self._client = client
        self._cache_key = cache_key

    @override
    @property
    def id(self) -> str:
        return self._client.metadata.id

    @override
    @property
    def name(self) -> str | None:
        return self._client.metadata.name

    @override
    @property
    def metadata(self) -> DatasetMetadata:
        return self._client.metadata

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
        if id and name:
            raise ValueError('Only one of "id" or "name" can be specified, not both.')

        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client

        cache_key = cls.compute_cache_key(
            id=id,
            name=name,
            configuration=configuration,
            storage_client=storage_client,
        )

        if cache_key in cls._cache:
            return cls._cache[cache_key]

        client = await storage_client.open_dataset_client(
            id=id,
            name=name,
            configuration=configuration,
        )

        dataset = cls(client, cache_key)
        cls._cache[cache_key] = dataset
        return dataset

    @override
    async def drop(self) -> None:
        # Remove from cache before dropping
        if self._cache_key in self._cache:
            del self._cache[self._cache_key]

        await self._client.drop()

    async def push_data(self, data: list[Any] | dict[str, Any]) -> None:
        """Store an object or an array of objects to the dataset.

        The size of the data is limited by the receiving API and therefore `push_data()` will only
        allow objects whose JSON representation is smaller than 9MB. When an array is passed,
        none of the included objects may be larger than 9MB, but the array itself may be of any size.

        Args:
            data: A JSON serializable data structure to be stored in the dataset. The JSON representation
                of each item must be smaller than 9MB.
        """
        await self._client.push_data(data=data)

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
        """Retrieve a paginated list of items from a dataset based on various filtering parameters.

        This method provides the flexibility to filter, sort, and modify the appearance of dataset items
        when listed. Each parameter modifies the result set according to its purpose. The method also
        supports pagination through 'offset' and 'limit' parameters.

        Args:
            offset: Skips the specified number of items at the start.
            limit: The maximum number of items to retrieve. Unlimited if None.
            clean: Return only non-empty items and excludes hidden fields. Shortcut for skip_hidden and skip_empty.
            desc: Set to True to sort results in descending order.
            fields: Fields to include in each item. Sorts fields as specified if provided.
            omit: Fields to exclude from each item.
            unwind: Unwinds items by a specified array field, turning each element into a separate item.
            skip_empty: Excludes empty items from the results if True.
            skip_hidden: Excludes fields starting with '#' if True.
            flatten: Fields to be flattened in returned items.
            view: Specifies the dataset view to be used.

        Returns:
            An object with filtered, sorted, and paginated dataset items plus pagination details.
        """
        return await self._client.get_data(
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

    async def iterate_items(
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
    ) -> AsyncIterator[dict]:
        """Iterate over items in the dataset according to specified filters and sorting.

        This method allows for asynchronously iterating through dataset items while applying various filters such as
        skipping empty items, hiding specific fields, and sorting. It supports pagination via `offset` and `limit`
        parameters, and can modify the appearance of dataset items using `fields`, `omit`, `unwind`, `skip_empty`, and
        `skip_hidden` parameters.

        Args:
            offset: Skips the specified number of items at the start.
            limit: The maximum number of items to retrieve. Unlimited if None.
            clean: Return only non-empty items and excludes hidden fields. Shortcut for skip_hidden and skip_empty.
            desc: Set to True to sort results in descending order.
            fields: Fields to include in each item. Sorts fields as specified if provided.
            omit: Fields to exclude from each item.
            unwind: Unwinds items by a specified array field, turning each element into a separate item.
            skip_empty: Excludes empty items from the results if True.
            skip_hidden: Excludes fields starting with '#' if True.

        Yields:
            An asynchronous iterator of dictionary objects, each representing a dataset item after applying
            the specified filters and transformations.
        """
        async for item in self._client.iterate_items(
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

    async def list_items(
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
    ) -> list[dict]:
        """Retrieve a list of all items from the dataset according to specified filters and sorting.

        This method collects all dataset items into a list while applying various filters such as
        skipping empty items, hiding specific fields, and sorting. It supports pagination via `offset` and `limit`
        parameters, and can modify the appearance of dataset items using `fields`, `omit`, `unwind`, `skip_empty`, and
        `skip_hidden` parameters.

        Args:
            offset: Skips the specified number of items at the start.
            limit: The maximum number of items to retrieve. Unlimited if None.
            clean: Return only non-empty items and excludes hidden fields. Shortcut for skip_hidden and skip_empty.
            desc: Set to True to sort results in descending order.
            fields: Fields to include in each item. Sorts fields as specified if provided.
            omit: Fields to exclude from each item.
            unwind: Unwinds items by a specified array field, turning each element into a separate item.
            skip_empty: Excludes empty items from the results if True.
            skip_hidden: Excludes fields starting with '#' if True.

        Returns:
            A list of dictionary objects, each representing a dataset item after applying
            the specified filters and transformations.
        """
        return [
            item
            async for item in self.iterate_items(
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
        ]

    @overload
    async def export_to(
        self,
        key: str,
        content_type: Literal['json'],
        to_key_value_store_id: str | None = None,
        to_key_value_store_name: str | None = None,
        **kwargs: Unpack[ExportDataJsonKwargs],
    ) -> None: ...

    @overload
    async def export_to(
        self,
        key: str,
        content_type: Literal['csv'],
        to_key_value_store_id: str | None = None,
        to_key_value_store_name: str | None = None,
        **kwargs: Unpack[ExportDataCsvKwargs],
    ) -> None: ...

    async def export_to(
        self,
        key: str,
        content_type: Literal['json', 'csv'] = 'json',
        to_key_value_store_id: str | None = None,
        to_key_value_store_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Export the entire dataset into a specified file stored under a key in a key-value store.

        This method consolidates all entries from a specified dataset into one file, which is then saved under a
        given key in a key-value store. The format of the exported file is determined by the `content_type` parameter.
        Either the dataset's ID or name should be specified, and similarly, either the target key-value store's ID or
        name should be used.

        Args:
            key: The key under which to save the data in the key-value store.
            content_type: The format in which to export the data.
            to_key_value_store_id: ID of the key-value store to save the exported file.
                Specify only one of ID or name.
            to_key_value_store_name: Name of the key-value store to save the exported file.
                Specify only one of ID or name.
            kwargs: Additional parameters for the export operation, specific to the chosen content type.
        """
        kvs = await KeyValueStore.open(id=to_key_value_store_id, name=to_key_value_store_name)
        dst = StringIO()

        if content_type == 'csv':
            await export_csv_to_stream(self.iterate_items(), dst, **kwargs)
            await kvs.set_value(key, dst.getvalue(), 'text/csv')
        elif content_type == 'json':
            await export_json_to_stream(self.iterate_items(), dst, **kwargs)
            await kvs.set_value(key, dst.getvalue(), 'application/json')
        else:
            raise ValueError('Unsupported content type, expecting CSV or JSON')
