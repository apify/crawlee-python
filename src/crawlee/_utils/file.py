from __future__ import annotations

import asyncio
import csv
import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, TextIO

    from typing_extensions import Unpack

    from crawlee._types import ExportDataCsvKwargs, ExportDataJsonKwargs


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

    # If the value is a string, assume plain text.
    if isinstance(value, str):
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


async def atomic_write_text(path: Path, data: str) -> None:
    dir_path = path.parent

    def _sync_write_text() -> str:
        # create a temp file in the target dir, return its name
        fd, tmp_path = tempfile.mkstemp(
            suffix=f'{path.suffix}.tmp',
            prefix=f'{path.name}.',
            dir=str(dir_path),
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as tmp_file:
                tmp_file.write(data)
        except:
            Path(tmp_path).unlink(missing_ok=True)
            raise
        return tmp_path

    tmp_path = await asyncio.to_thread(_sync_write_text)

    try:
        await asyncio.to_thread(os.replace, tmp_path, str(path))
    except (FileNotFoundError, PermissionError):
        # fallback if tmp went missing
        await asyncio.to_thread(path.write_text, data, encoding='utf-8')
    finally:
        await asyncio.to_thread(Path(tmp_path).unlink, missing_ok=True)


async def atomic_write_bytes(path: Path, data: bytes) -> None:
    dir_path = path.parent

    def _sync_write_bytes() -> str:
        fd, tmp_path = tempfile.mkstemp(
            suffix=f'{path.suffix}.tmp',
            prefix=f'{path.name}.',
            dir=str(dir_path),
        )
        try:
            with os.fdopen(fd, 'wb') as tmp_file:
                tmp_file.write(data)
        except:
            Path(tmp_path).unlink(missing_ok=True)
            raise
        return tmp_path

    tmp_path = await asyncio.to_thread(_sync_write_bytes)

    try:
        await asyncio.to_thread(os.replace, tmp_path, str(path))
    except (FileNotFoundError, PermissionError):
        # fallback if tmp went missing
        await asyncio.to_thread(path.write_bytes, data)
    finally:
        await asyncio.to_thread(Path(tmp_path).unlink, missing_ok=True)


async def export_json_to_stream(
    iterator: AsyncIterator[dict],
    dst: TextIO,
    **kwargs: Unpack[ExportDataJsonKwargs],
) -> None:
    items = [item async for item in iterator]
    json.dump(items, dst, **kwargs)


async def export_csv_to_stream(
    iterator: AsyncIterator[dict],
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
