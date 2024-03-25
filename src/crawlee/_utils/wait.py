import asyncio
from datetime import timedelta
from typing import Awaitable, TypeVar

T = TypeVar('T')


async def wait_for(fut: Awaitable[T], timeout: timedelta, timeout_message: str) -> T:
    try:
        return await asyncio.wait_for(fut, timeout.total_seconds())
    except asyncio.TimeoutError as ex:
        raise asyncio.TimeoutError(timeout_message) from ex
