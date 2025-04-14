from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, overload

from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee._utils.file import export_csv_to_stream, export_json_to_stream
from crawlee.storage_clients.models import DatasetMetadata

from ._base import Storage
from ._key_value_store import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, ClassVar, Literal

    from typing_extensions import Unpack

    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import DatasetClient
    from crawlee.storage_clients.models import DatasetItemsListPage

    from ._types import ExportDataCsvKwargs, ExportDataJsonKwargs

logger = logging.getLogger(__name__)


@docs_group('Classes')
class Dataset(Storage):
    """Dataset is an append-only structured storage, ideal for tabular data similar to database tables.

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

    _cache_by_id: ClassVar[dict[str, Dataset]] = {}
    """A dictionary to cache datasets by their IDs."""

    _cache_by_name: ClassVar[dict[str, Dataset]] = {}
    """A dictionary to cache datasets by their names."""

    def __init__(self, client: DatasetClient) -> None:
        """Initialize a new instance.

        Preferably use the `Dataset.open` constructor to create a new instance.

        Args:
            client: An instance of a dataset client.
        """
        self._client = client

    @override
    @property
    def id(self) -> str:
        return self._client.id

    @override
    @property
    def name(self) -> str | None:
        return self._client.name

    @override
    @property
    def metadata(self) -> DatasetMetadata:
        return DatasetMetadata(
            id=self._client.id,
            name=self._client.id,
            accessed_at=self._client.accessed_at,
            created_at=self._client.created_at,
            modified_at=self._client.modified_at,
            item_count=self._client.item_count,
        )

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool | None = None,
        storage_dir: Path | None = None,
        configuration: Configuration | None = None,
        storage_client: StorageClient | None = None,
    ) -> Dataset:
        if id and name:
            raise ValueError('Only one of "id" or "name" can be specified, not both.')

        # Check if dataset is already cached by id or name
        if id and id in cls._cache_by_id:
            return cls._cache_by_id[id]
        if name and name in cls._cache_by_name:
            return cls._cache_by_name[name]

        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client
        purge_on_start = configuration.purge_on_start if purge_on_start is None else purge_on_start
        storage_dir = Path(configuration.storage_dir) if storage_dir is None else storage_dir

        client = await storage_client.open_dataset_client(
            id=id,
            name=name,
            purge_on_start=purge_on_start,
            storage_dir=storage_dir,
        )

        dataset = cls(client)

        # Cache the dataset by id and name if available
        if dataset.id:
            cls._cache_by_id[dataset.id] = dataset
        if dataset.name:
            cls._cache_by_name[dataset.name] = dataset

        return dataset

    @override
    async def drop(self) -> None:
        # Remove from cache before dropping
        if self.id in self._cache_by_id:
            del self._cache_by_id[self.id]
        if self.name and self.name in self._cache_by_name:
            del self._cache_by_name[self.name]

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
