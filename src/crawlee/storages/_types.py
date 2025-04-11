from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

if TYPE_CHECKING:
    import json
    from collections.abc import Callable
    from datetime import datetime

    from typing_extensions import NotRequired, Required

    from crawlee import Request


class CachedRequest(TypedDict):
    """Represent a cached request in the `RequestQueue`."""

    id: str
    """The ID of the request."""

    was_already_handled: bool
    """Indicates whether the request was already handled."""

    hydrated: Request | None
    """The hydrated request object."""

    lock_expires_at: datetime | None
    """The time at which the lock on the request expires."""

    forefront: bool
    """Indicates whether the request is at the forefront of the queue."""


class IterateKwargs(TypedDict):
    """Keyword arguments for dataset's `iterate` method."""

    offset: NotRequired[int]
    """Skips the specified number of items at the start."""

    limit: NotRequired[int | None]
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


class GetDataKwargs(IterateKwargs):
    """Keyword arguments for dataset's `get_data` method."""

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
