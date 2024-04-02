from __future__ import annotations

import contextlib
import io
import json
import mimetypes
import os
import re
from typing import Any

import aiofiles
import aioshutil
from aiofiles import ospath
from aiofiles.os import makedirs, remove, rename


async def force_remove(filename: str) -> None:
    """Removes a file, suppressing the FileNotFoundError if it does not exist.

    JS-like rm(filename, { force: true }).

    Args:
        filename: The path to the file to be removed.
    """
    with contextlib.suppress(FileNotFoundError):
        await remove(filename)


async def force_rename(src_dir: str, dst_dir: str) -> None:
    """Renames a directory, ensuring that the destination directory is removed if it exists.

    Args:
        src_dir: The source directory path.
        dst_dir: The destination directory path.
    """
    # Make sure source directory exists
    if await ospath.exists(src_dir):
        # Remove destination directory if it exists
        if await ospath.exists(dst_dir):
            await aioshutil.rmtree(dst_dir, ignore_errors=True)
        await rename(src_dir, dst_dir)


def guess_file_extension(content_type: str) -> str | None:
    """Guess the file extension for a given MIME content type.

    Args:
        content_type: The MIME content type string.

    Returns:
        A string representing the guessed file extension without a leading dot,
            or None if no extension could be determined.
    """
    # e.g. mimetypes.guess_extension('application/json ') does not work...
    actual_content_type = content_type.split(';')[0].strip()

    # mimetypes.guess_extension returns 'xsl' in this case, because 'application/xxx' is "structured"
    # ('text/xml' would be "unstructured" and return 'xml') we have to explicitly override it here
    if actual_content_type == 'application/xml':
        return 'xml'

    # Guess the extension from the mime type
    ext = mimetypes.guess_extension(actual_content_type)

    # Remove the leading dot if extension successfully parsed
    return ext[1:] if ext is not None else ext


def is_content_type_json(content_type: str) -> bool:
    """Check if the provided content type string indicates JSON content.

    Args:
        content_type: The MIME content type string.

    Returns:
        True if the content type is application/json, False otherwise.
    """
    return bool(re.search(r'^application/json', content_type, flags=re.IGNORECASE))


def is_content_type_text(content_type: str) -> bool:
    """Check if the provided content type string indicates plaintext content.

    Args:
        content_type: The MIME content type string.

    Returns:
        True if the content type starts with 'text/', indicating that it is a text format, False otherwise.
    """
    return bool(re.search(r'^text/', content_type, flags=re.IGNORECASE))


def is_content_type_xml(content_type: str) -> bool:
    """Check if the provided content type string indicates XML content.

    Args:
        content_type: The MIME content type string.

    Returns:
        True if the content type is XML-related (application/xml or similar), False otherwise.
    """
    return bool(re.search(r'^application/.*xml$', content_type, flags=re.IGNORECASE))


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


def json_dumps(obj: Any) -> str:
    """Serialize an object to a JSON-formatted string with specific settings.

    Args:
        obj: The object to serialize.

    Returns:
        A string containing the JSON representation of the input object.
    """
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


async def persist_metadata_if_enabled(*, data: dict, entity_directory: str, write_metadata: bool) -> None:
    """Updates or writes metadata to a specified directory.

    The function writes a given metadata dictionary to a JSON file within a specified directory.
    The writing process is skipped if `write_metadata` is False. Before writing, it ensures that
    the target directory exists, creating it if necessary.

    Args:
        data: A dictionary containing metadata to be written.
        entity_directory: The directory path where the metadata file should be stored.
        write_metadata: A boolean flag indicating whether the metadata should be written to file.
    """
    # Skip metadata write; ensure directory exists first
    if not write_metadata:
        return

    # Ensure the directory for the entity exists
    await makedirs(entity_directory, exist_ok=True)

    # Write the metadata to the file
    file_path = os.path.join(entity_directory, '__metadata__.json')
    async with aiofiles.open(file_path, mode='wb') as f:
        await f.write(json_dumps(data).encode('utf-8'))
