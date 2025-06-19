from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager

    from httpx import Response

    from crawlee._types import JsonSerializable
    from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata


@docs_group('Abstract classes')
class DatasetClient(ABC):
    """An abstract class for dataset resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    _LIST_ITEMS_LIMIT = 999_999_999_999
    """This is what API returns in the x-apify-pagination-limit header when no limit query parameter is used."""

    @abstractmethod
    async def get(self) -> DatasetMetadata | None:
        """Get metadata about the dataset being managed by this client.

        Returns:
            An object containing the dataset's details, or None if the dataset does not exist.
        """

    @abstractmethod
    async def update(
        self,
        *,
        name: str | None = None,
    ) -> DatasetMetadata:
        """Update the dataset metadata.

        Args:
            name: New new name for the dataset.

        Returns:
            An object reflecting the updated dataset metadata.
        """

    @abstractmethod
    async def delete(self) -> None:
        """Permanently delete the dataset managed by this client."""

    @abstractmethod
    async def list_items(
        self,
        *,
        offset: int | None = 0,
        limit: int | None = _LIST_ITEMS_LIMIT,
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
            offset: The number of initial items to skip.
            limit: The maximum number of items to return.
            clean: If True, removes empty items and hidden fields, equivalent to 'skip_hidden' and 'skip_empty'.
            desc: If True, items are returned in descending order, i.e., newest first.
            fields: Specifies a subset of fields to include in each item.
            omit: Specifies a subset of fields to exclude from each item.
            unwind: Specifies a field that should be unwound. If it's an array, each element becomes a separate record.
            skip_empty: If True, omits items that are empty after other filters have been applied.
            skip_hidden: If True, omits fields starting with the '#' character.
            flatten: A list of fields to flatten in each item.
            view: The specific view of the dataset to use when retrieving items.

        Returns:
            An object with filtered, sorted, and paginated dataset items plus pagination details.
        """

    @abstractmethod
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
        """Iterate over items in the dataset according to specified filters and sorting.

        This method allows for asynchronously iterating through dataset items while applying various filters such as
        skipping empty items, hiding specific fields, and sorting. It supports pagination via `offset` and `limit`
        parameters, and can modify the appearance of dataset items using `fields`, `omit`, `unwind`, `skip_empty`, and
        `skip_hidden` parameters.

        Args:
            offset: The number of initial items to skip.
            limit: The maximum number of items to iterate over. None means no limit.
            clean: If True, removes empty items and hidden fields, equivalent to 'skip_hidden' and 'skip_empty'.
            desc: If set to True, items are returned in descending order, i.e., newest first.
            fields: Specifies a subset of fields to include in each item.
            omit: Specifies a subset of fields to exclude from each item.
            unwind: Specifies a field that should be unwound into separate items.
            skip_empty: If set to True, omits items that are empty after other filters have been applied.
            skip_hidden: If set to True, omits fields starting with the '#' character from the output.

        Yields:
            An asynchronous iterator of dictionary objects, each representing a dataset item after applying
            the specified filters and transformations.
        """
        # This syntax is to make mypy properly work with abstract AsyncIterator.
        # https://mypy.readthedocs.io/en/stable/more_types.html#asynchronous-iterators
        raise NotImplementedError
        if False:  # type: ignore[unreachable]
            yield 0

    @abstractmethod
    async def get_items_as_bytes(
        self,
        *,
        item_format: str = 'json',
        offset: int | None = None,
        limit: int | None = None,
        desc: bool = False,
        clean: bool = False,
        bom: bool = False,
        delimiter: str | None = None,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: str | None = None,
        skip_empty: bool = False,
        skip_header_row: bool = False,
        skip_hidden: bool = False,
        xml_root: str | None = None,
        xml_row: str | None = None,
        flatten: list[str] | None = None,
    ) -> bytes:
        """Retrieve dataset items as bytes.

        Args:
            item_format: Output format (e.g., 'json', 'csv'); default is 'json'.
            offset: Number of items to skip; default is 0.
            limit: Max number of items to return; no default limit.
            desc: If True, results are returned in descending order.
            clean: If True, filters out empty items and hidden fields.
            bom: Include or exclude UTF-8 BOM; default behavior varies by format.
            delimiter: Delimiter character for CSV; default is ','.
            fields: List of fields to include in the results.
            omit: List of fields to omit from the results.
            unwind: Unwinds a field into separate records.
            skip_empty: If True, skips empty items in the output.
            skip_header_row: If True, skips the header row in CSV.
            skip_hidden: If True, skips hidden fields in the output.
            xml_root: Root element name for XML output; default is 'items'.
            xml_row: Element name for each item in XML output; default is 'item'.
            flatten: List of fields to flatten.

        Returns:
            The dataset items as raw bytes.
        """

    @abstractmethod
    async def stream_items(
        self,
        *,
        item_format: str = 'json',
        offset: int | None = None,
        limit: int | None = None,
        desc: bool = False,
        clean: bool = False,
        bom: bool = False,
        delimiter: str | None = None,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: str | None = None,
        skip_empty: bool = False,
        skip_header_row: bool = False,
        skip_hidden: bool = False,
        xml_root: str | None = None,
        xml_row: str | None = None,
    ) -> AbstractAsyncContextManager[Response | None]:
        """Retrieve dataset items as a streaming response.

        Args:
            item_format: Output format, options include json, jsonl, csv, html, xlsx, xml, rss; default is json.
            offset: Number of items to skip at the start; default is 0.
            limit: Maximum number of items to return; no default limit.
            desc: If True, reverses the order of results.
            clean: If True, filters out empty items and hidden fields.
            bom: Include or exclude UTF-8 BOM; varies by format.
            delimiter: Delimiter for CSV files; default is ','.
            fields: List of fields to include in the output.
            omit: List of fields to omit from the output.
            unwind: Unwinds a field into separate records.
            skip_empty: If True, empty items are omitted.
            skip_header_row: If True, skips the header row in CSV.
            skip_hidden: If True, hides fields starting with the # character.
            xml_root: Custom root element name for XML output; default is 'items'.
            xml_row: Custom element name for each item in XML; default is 'item'.

        Yields:
            The dataset items in a streaming response.
        """

    @abstractmethod
    async def push_items(self, items: JsonSerializable) -> None:
        """Push items to the dataset.

        Args:
            items: The items which to push in the dataset. They must be JSON serializable.
        """
