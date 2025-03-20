from __future__ import annotations

import csv
import io
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, TextIO, cast

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee.storage_clients.models import DatasetMetadata

from ._key_value_store import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any

    from typing_extensions import Unpack

    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import DatasetClient
    from crawlee.storage_clients.models import DatasetItemsListPage

    from ._types import ExportDataCsvKwargs, ExportDataJsonKwargs, ExportToKwargs

logger = logging.getLogger(__name__)

# TODO:
# - inherit from storage class
# - export methods
# - caching / memoization of both datasets & dataset clients

# Dataset
# - properties:
#   - id
#   - name
#   - metadata
# - methods:
#   - open
#   - drop
#   - push_data
#   - get_data
#   - iterate
#   - export_to_csv
#   - export_to_json


@docs_group('Classes')
class Dataset:
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

    def __init__(self, client: DatasetClient) -> None:
        """Initialize a new instance.

        Preferably use the `Dataset.open` constructor to create a new instance.

        Args:
            client: An instance of a dataset client.
        """
        self._client = client

    @property
    def id(self) -> str:
        return self._client.id

    @property
    def name(self) -> str | None:
        return self._client.name

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

        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client
        purge_on_start = configuration.purge_on_start if purge_on_start is None else purge_on_start
        storage_dir = Path(configuration.storage_dir) if storage_dir is None else storage_dir

        dataset_client = await storage_client.open_dataset_client(
            id=id,
            name=name,
            purge_on_start=purge_on_start,
            storage_dir=storage_dir,
        )

        return cls(dataset_client)

    async def drop(self) -> None:
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
        await self._client.push_data(data)

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

    async def iterate(
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
        async for item in self._client.iterate(
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

    # TODO: update this once KVS is implemented
    async def export_to(self, **kwargs: Unpack[ExportToKwargs]) -> None:
        """Export the entire dataset into a specified file stored under a key in a key-value store.

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

    # TODO: update this once KVS is implemented
    async def write_to_csv(self, destination: TextIO, **kwargs: Unpack[ExportDataCsvKwargs]) -> None:
        """Export the entire dataset into an arbitrary stream.

        Args:
            destination: The stream into which the dataset contents should be written.
            kwargs: Additional keyword arguments for `csv.writer`.
        """
        items: list[dict] = []
        limit = 1000
        offset = 0

        while True:
            list_items = await self._client.get_data(limit=limit, offset=offset)
            items.extend(list_items.items)
            if list_items.total <= offset + list_items.count:
                break
            offset += list_items.count

        if items:
            writer = csv.writer(destination, **kwargs)
            writer.writerows([items[0].keys(), *[item.values() for item in items]])
        else:
            logger.warning('Attempting to export an empty dataset - no file will be created')

    # TODO: update this once KVS is implemented
    async def write_to_json(self, destination: TextIO, **kwargs: Unpack[ExportDataJsonKwargs]) -> None:
        """Export the entire dataset into an arbitrary stream.

        Args:
            destination: The stream into which the dataset contents should be written.
            kwargs: Additional keyword arguments for `json.dump`.
        """
        items: list[dict] = []
        limit = 1000
        offset = 0

        while True:
            list_items = await self._client.get_data(limit=limit, offset=offset)
            items.extend(list_items.items)
            if list_items.total <= offset + list_items.count:
                break
            offset += list_items.count

        if items:
            json.dump(items, destination, **kwargs)
        else:
            logger.warning('Attempting to export an empty dataset - no file will be created')
