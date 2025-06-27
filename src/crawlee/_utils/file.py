from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, TextIO

    from typing_extensions import Unpack

    from crawlee._types import ExportDataCsvKwargs, ExportDataJsonKwargs

if sys.platform == 'win32':

    def _write_file(path: Path, data: str | bytes) -> None:
        """Windows-specific file write implementation.

        This implementation writes directly to the file without using a temporary file, because
        they are problematic due to permissions issues on Windows.
        """
        if isinstance(data, bytes):
            path.write_bytes(data)
        elif isinstance(data, str):
            path.write_text(data, encoding='utf-8')
        else:
            raise TypeError(f'Unsupported data type: {type(data)}. Expected str or bytes.')
else:

    def _write_file(path: Path, data: str | bytes) -> None:
        """Linux/Unix-specific file write implementation using temporary files."""
        dir_path = path.parent
        fd, tmp_path = tempfile.mkstemp(
            suffix=f'{path.suffix}.tmp',
            prefix=f'{path.name}.',
            dir=str(dir_path),
        )

        if not isinstance(data, (str, bytes)):
            raise TypeError(f'Unsupported data type: {type(data)}. Expected str or bytes.')

        try:
            if isinstance(data, bytes):
                with os.fdopen(fd, 'wb') as tmp_file:
                    tmp_file.write(data)
            else:
                with os.fdopen(fd, 'w', encoding='utf-8') as tmp_file:
                    tmp_file.write(data)

            # Atomically replace the destination file with the temporary file
            Path(tmp_path).replace(path)
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            raise


def infer_mime_type(value: Any) -> str:
    """Infer the MIME content type from the value.

    Args:
        value: The value to infer the content type from.

    Returns:
        The inferred MIME content type.
    """
    # If the value is bytes (or bytearray), return binary content type.
    if isinstance(value, (bytes, bytearray)):
        return 'application/octet-stream'

    # If the value is a dict or list, assume JSON.
    if isinstance(value, (dict, list)):
        return 'application/json; charset=utf-8'

    # If the value is a string, number or boolean, assume plain text.
    if isinstance(value, (str, int, float, bool)):
        return 'text/plain; charset=utf-8'

    # Default fallback.
    return 'application/octet-stream'


async def json_dumps(obj: Any) -> str:
    """Serialize an object to a JSON-formatted string with specific settings.

    Args:
        obj: The object to serialize.

    Returns:
        A string containing the JSON representation of the input object.
    """
    return await asyncio.to_thread(json.dumps, obj, ensure_ascii=False, indent=2, default=str)


@overload
async def atomic_write(
    path: Path,
    data: str,
    *,
    retry_count: int = 0,
) -> None: ...


@overload
async def atomic_write(
    path: Path,
    data: bytes,
    *,
    retry_count: int = 0,
) -> None: ...


async def atomic_write(
    path: Path,
    data: str | bytes,
    *,
    retry_count: int = 0,
) -> None:
    """Write data to a file atomically to prevent data corruption or partial writes.

    This function handles both text and binary data. The binary mode is automatically
    detected based on the data type (bytes = binary, str = text). It ensures atomic
    writing by creating a temporary file and then atomically replacing the target file,
    which prevents data corruption if the process is interrupted during the write operation.

    Args:
        path: The path to the destination file.
        data: The data to write to the file (string or bytes).
        retry_count: Internal parameter to track the number of retry attempts (default: 0).
    """
    max_retries = 3

    try:
        # Use the platform-specific write function resolved at import time.
        await asyncio.to_thread(_write_file, path, data)
    except (FileNotFoundError, PermissionError):
        if retry_count < max_retries:
            return await atomic_write(
                path,
                data,
                retry_count=retry_count + 1,
            )
        # If we reach the maximum number of retries, raise the exception.
        raise


async def export_json_to_stream(
    iterator: AsyncIterator[dict[str, Any]],
    dst: TextIO,
    **kwargs: Unpack[ExportDataJsonKwargs],
) -> None:
    items = [item async for item in iterator]
    json.dump(items, dst, **kwargs)


async def export_csv_to_stream(
    iterator: AsyncIterator[dict[str, Any]],
    dst: TextIO,
    **kwargs: Unpack[ExportDataCsvKwargs],
) -> None:
    writer = csv.writer(dst, **kwargs)
    write_header = True

    # Iterate over the dataset and write to CSV.
    async for item in iterator:
        if not item:
            continue

        if write_header:
            writer.writerow(item.keys())
            write_header = False

        writer.writerow(item.values())
