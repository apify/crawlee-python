from __future__ import annotations

import asyncio
import json
from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

logger = getLogger(__name__)


async def json_dumps(obj: Any) -> str:
    """Serialize an object to a JSON-formatted string with specific settings.

    Args:
        obj: The object to serialize.

    Returns:
        A string containing the JSON representation of the input object.
    """
    return await asyncio.to_thread(json.dumps, obj, ensure_ascii=False, indent=2, default=str)
