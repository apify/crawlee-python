from __future__ import annotations

import asyncio
import contextlib
import io
import json
import mimetypes
import os
import re
import shutil
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


class ContentType(Enum):
    JSON = r'^application/json'
    TEXT = r'^text/'
    XML = r'^application/.*xml$'

    def matches(self, content_type: str) -> bool:
        """Check if the content type matches the enum's pattern."""
        return bool(re.search(self.value, content_type, re.IGNORECASE))


def is_content_type(content_type_enum: ContentType, content_type: str) -> bool:
    """Check if the provided content type string matches the specified ContentType."""
    return content_type_enum.matches(content_type)


async def force_remove(filename: str | Path) -> None:
    """Remove a file, suppressing the FileNotFoundError if it does not exist.

    JS-like rm(filename, { force: true }).

    Args:
        filename: The path to the file to be removed.
    """
    with contextlib.suppress(FileNotFoundError):
        await asyncio.to_thread(os.remove, filename)


async def force_rename(src_dir: str | Path, dst_dir: str | Path) -> None:
    """Rename a directory, ensuring that the destination directory is removed if it exists.

    Args:
        src_dir: The source directory path.
        dst_dir: The destination directory path.
    """
    # Make sure source directory exists
    if await asyncio.to_thread(os.path.exists, src_dir):
        # Remove destination directory if it exists
        if await asyncio.to_thread(os.path.exists, dst_dir):
            await asyncio.to_thread(shutil.rmtree, dst_dir, ignore_errors=True)
        await asyncio.to_thread(os.rename, src_dir, dst_dir)


def determine_file_extension(content_type: str) -> str | None:
    """Determine the file extension for a given MIME content type.

    Args:
        content_type: The MIME content type string.

    Returns:
        A string representing the determined file extension without a leading dot,
            or None if no extension could be determined.
    """
    # e.g. mimetypes.guess_extension('application/json ') does not work...
    actual_content_type = content_type.split(';')[0].strip()

    # mimetypes.guess_extension returns 'xsl' in this case, because 'application/xxx' is "structured"
    # ('text/xml' would be "unstructured" and return 'xml') we have to explicitly override it here
    if actual_content_type == 'application/xml':
        return 'xml'

    # Determine the extension from the mime type
    ext = mimetypes.guess_extension(actual_content_type)

    # Remove the leading dot if extension successfully parsed
    return ext[1:] if ext is not None else ext


def is_file_or_bytes(value: Any) -> bool:
    """Determine if the input value is a file-like object or bytes.

    This function checks whether the provided value is an instance of bytes, bytearray, or io.IOBase (file-like).
    The method is simplified for common use cases and may not cover all edge cases.

    Args:
        value: The value to be checked.

    Returns:
        True if the value is either a file-like object or bytes, False otherwise.
    """
    return isinstance(value, (bytes, bytearray, io.IOBase))


async def json_dumps(obj: Any) -> str:
    """Serialize an object to a JSON-formatted string with specific settings.

    Args:
        obj: The object to serialize.

    Returns:
        A string containing the JSON representation of the input object.
    """
    return await asyncio.to_thread(json.dumps, obj, ensure_ascii=False, indent=2, default=str)
