from __future__ import annotations

import contextlib
import io
import json
import mimetypes
import os
import re
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn

import aiofiles
import aioshutil
from aiofiles import ospath
from aiofiles.os import makedirs, remove, rename

if TYPE_CHECKING:
    from crawlee.storages.types import StorageTypes


def maybe_parse_bool(val: str | None) -> bool:
    if val in {'true', 'True', '1'}:
        return True
    return False


def is_file_or_bytes(value: Any) -> bool:
    """Check if the input value is a file-like object or bytes.

    The check for IOBase is not ideal, it would be better to use duck typing,
    but then the check would be super complex, judging from how the 'requests' library does it.
    This way should be good enough for the vast majority of use cases, if it causes issues, we can improve it later.
    """
    return isinstance(value, (bytes, bytearray, io.IOBase))


def maybe_extract_enum_member_value(maybe_enum_member: Any) -> Any:
    """Extract the value of an enumeration member if it is an Enum, otherwise return the original value."""
    if isinstance(maybe_enum_member, Enum):
        return maybe_enum_member.value
    return maybe_enum_member


def raise_on_non_existing_storage(client_type: StorageTypes, id_: str | None) -> NoReturn:
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with id "{id_}" does not exist.')


def raise_on_duplicate_storage(client_type: StorageTypes, key_name: str, value: str) -> NoReturn:
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with {key_name} "{value}" already exists.')


def is_content_type_json(content_type: str) -> bool:
    """Check if the given content type is JSON."""
    return bool(re.search(r'^application/json', content_type, flags=re.IGNORECASE))


def is_content_type_xml(content_type: str) -> bool:
    """Check if the given content type is XML."""
    return bool(re.search(r'^application/.*xml$', content_type, flags=re.IGNORECASE))


def is_content_type_text(content_type: str) -> bool:
    """Check if the given content type is text."""
    return bool(re.search(r'^text/', content_type, flags=re.IGNORECASE))


def maybe_parse_body(body: bytes, content_type: str) -> Any:
    if is_content_type_json(content_type):
        return json.loads(body.decode('utf-8'))  # Returns any
    if is_content_type_xml(content_type) or is_content_type_text(content_type):
        return body.decode('utf-8')
    return body


def guess_file_extension(content_type: str) -> str | None:
    """Guess the file extension based on content type."""
    # e.g. mimetypes.guess_extension('application/json ') does not work...
    actual_content_type = content_type.split(';')[0].strip()

    # mimetypes.guess_extension returns 'xsl' in this case, because 'application/xxx' is "structured"
    # ('text/xml' would be "unstructured" and return 'xml')
    # we have to explicitly override it here
    if actual_content_type == 'application/xml':
        return 'xml'

    # Guess the extension from the mime type
    ext = mimetypes.guess_extension(actual_content_type)

    # Remove the leading dot if extension successfully parsed
    return ext[1:] if ext is not None else ext


def json_dumps(obj: Any) -> str:
    """Dump JSON to a string with the correct settings and serializer."""
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


async def force_remove(filename: str) -> None:
    """JS-like rm(filename, { force: true })."""
    with contextlib.suppress(FileNotFoundError):
        await remove(filename)


async def force_rename(src_dir: str, dst_dir: str) -> None:
    """Rename a directory. Checks for existence of soruce directory and removes destination directory if it exists."""
    # Make sure source directory exists
    if await ospath.exists(src_dir):
        # Remove destination directory if it exists
        if await ospath.exists(dst_dir):
            await aioshutil.rmtree(dst_dir, ignore_errors=True)
        await rename(src_dir, dst_dir)


async def update_metadata(*, data: dict, entity_directory: str, write_metadata: bool) -> None:
    # Skip metadata write; ensure directory exists first
    if not write_metadata:
        return

    # Ensure the directory for the entity exists
    await makedirs(entity_directory, exist_ok=True)

    # Write the metadata to the file
    file_path = os.path.join(entity_directory, '__metadata__.json')
    async with aiofiles.open(file_path, mode='wb') as f:
        await f.write(json_dumps(data).encode('utf-8'))


async def _update_dataset_items(
    *,
    data: list[tuple[str, dict]],
    entity_directory: str,
    persist_storage: bool,
) -> None:
    # Skip writing files to the disk if the client has the option set to false
    if not persist_storage:
        return

    # Ensure the directory for the entity exists
    await makedirs(entity_directory, exist_ok=True)

    # Save all the new items to the disk
    for idx, item in data:
        file_path = os.path.join(entity_directory, f'{idx}.json')
        async with aiofiles.open(file_path, mode='wb') as f:
            await f.write(json_dumps(item).encode('utf-8'))


async def update_request_queue_item(
    *,
    request_id: str,
    request: dict,
    entity_directory: str,
    persist_storage: bool,
) -> None:
    # Skip writing files to the disk if the client has the option set to false
    if not persist_storage:
        return

    # Ensure the directory for the entity exists
    await makedirs(entity_directory, exist_ok=True)

    # Write the request to the file
    file_path = os.path.join(entity_directory, f'{request_id}.json')
    async with aiofiles.open(file_path, mode='wb') as f:
        await f.write(json_dumps(request).encode('utf-8'))


async def delete_request(*, request_id: str, entity_directory: str) -> None:
    # Ensure the directory for the entity exists
    await makedirs(entity_directory, exist_ok=True)

    file_path = os.path.join(entity_directory, f'{request_id}.json')
    await force_remove(file_path)
