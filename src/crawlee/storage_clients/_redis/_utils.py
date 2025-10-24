from collections.abc import Awaitable
from pathlib import Path
from typing import TypeVar, overload

T = TypeVar('T')


@overload
async def await_redis_response(response: Awaitable[T]) -> T: ...
@overload
async def await_redis_response(response: T) -> T: ...


async def await_redis_response(response: Awaitable[T] | T) -> T:
    """Solve the problem of ambiguous typing for redis."""
    return await response if isinstance(response, Awaitable) else response


def read_lua_script(script_name: str) -> str:
    """Read a Lua script from a file."""
    file_path = Path(__file__).parent / 'lua_scripts' / script_name
    with file_path.open('r', encoding='utf-8') as file:
        return file.read()
