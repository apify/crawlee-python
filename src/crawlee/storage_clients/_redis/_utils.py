from collections.abc import Awaitable
from pathlib import Path
from typing import TypeVar, cast, overload

T = TypeVar('T')


@overload
async def await_redis_response(response: Awaitable[T]) -> T: ...
@overload
async def await_redis_response(response: T) -> T: ...


async def await_redis_response(response: Awaitable[T] | T) -> T:
    """Solve the problem of ambiguous typing for redis."""
    if isinstance(response, Awaitable):
        return cast('T', await response)
    return response


@overload
def expect_bytes(value: bytes | str | None) -> bytes | None: ...
@overload
def expect_bytes(value: list[bytes | str | None]) -> list[bytes | None]: ...


def expect_bytes(value: bytes | str | list[bytes | str | None] | None) -> bytes | list[bytes | None] | None:
    """Narrow a Redis reply to raw bytes, rejecting a client that decodes responses.

    redis-py types every reply as `bytes | str | None`, because a client created with `decode_responses=True` returns
    strings. The storage clients store binary values and need the raw bytes back, so such a client is not supported.

    Raises:
        TypeError: If the reply contains a string, i.e. the Redis client decodes responses.
    """
    values = value if isinstance(value, list) else [value]
    if any(isinstance(item, str) for item in values):
        raise TypeError(
            'The Redis client returned a decoded string instead of raw bytes. The Redis storage client requires '
            'a Redis client created without `decode_responses=True`.'
        )
    return cast('bytes | list[bytes | None] | None', value)


def read_lua_script(script_name: str) -> str:
    """Read a Lua script from a file."""
    file_path = Path(__file__).parent / 'lua_scripts' / script_name
    with file_path.open(mode='r', encoding='utf-8') as file:
        return file.read()
