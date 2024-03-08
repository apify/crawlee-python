from __future__ import annotations

import contextlib
import os

import aiofiles
import aioshutil
from aiofiles.os import makedirs
from apify_shared.utils import json_dumps


async def force_remove(filename: str) -> None:
    """JS-like rm(filename, { force: true })."""
    with contextlib.suppress(FileNotFoundError):
        await aiofiles.remove(filename)


async def force_rename(src_dir: str, dst_dir: str) -> None:
    """Rename a directory. Checks for existence of soruce directory and removes destination directory if it exists."""
    # Make sure source directory exists
    if await aiofiles.ospath.exists(src_dir):
        # Remove destination directory if it exists
        if await aiofiles.ospath.exists(dst_dir):
            await aioshutil.rmtree(dst_dir, ignore_errors=True)
        await aiofiles.os.rename(src_dir, dst_dir)


async def update_metadata(*, data: dict, entity_directory: str, write_metadata: bool) -> None:
    # Skip writing the actual metadata file. This is done after ensuring the directory exists so we have the directory present
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
