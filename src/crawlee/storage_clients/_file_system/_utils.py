from __future__ import annotations

import asyncio
import json
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

METADATA_FILENAME = '__metadata__.json'
"""The name of the metadata file for storage clients."""


async def json_dumps(obj: Any) -> str:
    """Serialize an object to a JSON-formatted string with specific settings.

    Args:
        obj: The object to serialize.

    Returns:
        A string containing the JSON representation of the input object.
    """
    return await asyncio.to_thread(json.dumps, obj, ensure_ascii=False, indent=2, default=str)


async def atomic_write_text(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + '.tmp')
    # write to .tmp
    await asyncio.to_thread(tmp.write_text, data, encoding='utf-8')
    # atomic replace
    await asyncio.to_thread(os.replace, tmp, path)


async def atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + '.tmp')
    # write to .tmp
    await asyncio.to_thread(tmp.write_bytes, data)
    # atomic replace
    await asyncio.to_thread(os.replace, tmp, path)
