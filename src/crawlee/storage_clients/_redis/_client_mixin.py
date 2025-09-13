from __future__ import annotations

import asyncio
from abc import ABC
from contextlib import asynccontextmanager
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, TypedDict

from ._utils import await_redis_response, read_lua_script

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline
    from redis.commands.core import AsyncScript
    from typing_extensions import NotRequired


logger = getLogger(__name__)


class MetadataUpdateParams(TypedDict, total=False):
    """Parameters for updating metadata."""

    update_accessed_at: NotRequired[bool]
    update_modified_at: NotRequired[bool]
    force: NotRequired[bool]


class RedisClientMixin(ABC):
    """Mixin class for SQL clients.

    This mixin provides common SQL operations and basic methods for SQL storage clients.
    """

    _DEFAULT_NAME = 'default'

    _MAIN_KEY: ClassVar[str]

    def __init__(self, *, storage_name: str, redis: Redis) -> None:
        self._storage_name = storage_name
        self._redis = redis

    @classmethod
    async def _get_metadata_by_name(cls, name: str, redis: Redis) -> dict | None:
        response = await await_redis_response(redis.json().get(f'{cls._MAIN_KEY}:{name}:metadata'))
        data = response[0] if response is not None and isinstance(response, list) else response
        if data is not None and not isinstance(data, dict):
            raise TypeError('The metadata data was received in an incorrect format.')
        return data

    @classmethod
    async def _get_metadata_name_by_id(cls, id: str, redis: Redis) -> str | None:
        return await await_redis_response(redis.get(f'{cls._MAIN_KEY}:id_to_name:{id}'))

    @asynccontextmanager
    async def _get_pipeline(self, *, with_execute: bool = True) -> AsyncIterator[Pipeline]:
        """Create a new Redis pipeline for this storage."""
        async with self._redis.pipeline() as pipe:
            try:
                pipe.multi()  # type: ignore[no-untyped-call]
                yield pipe
            finally:
                if with_execute:
                    await pipe.execute()

    async def _create_storage(self, pipeline: Pipeline) -> None:
        _pipeline = pipeline  # To avoid unused variable mypy error

    async def _create_script(self, script_name: str) -> AsyncScript:
        """Load a Lua script from a file and return a Script object."""
        script_path = Path(__file__).parent / 'lua_scripts' / script_name
        script_content = await asyncio.to_thread(read_lua_script, script_path)

        return self._redis.register_script(script_content)

    async def _create_metadata_and_storage(self, metadata: dict) -> None:
        metadata_key = f'{self._MAIN_KEY}:{self._storage_name}:metadata'
        index_id_to_name = f'{self._MAIN_KEY}:id_to_name:{metadata["id"]}'
        metadata['created_at'] = metadata['created_at'].isoformat()
        metadata['accessed_at'] = metadata['accessed_at'].isoformat()
        metadata['modified_at'] = metadata['modified_at'].isoformat()
        name = metadata['name'] if metadata['name'] is not None else self._DEFAULT_NAME
        # Use a transaction to ensure atomicity
        async with self._get_pipeline() as pipe:
            await await_redis_response(pipe.json().set(metadata_key, '$', metadata, nx=True))
            await await_redis_response(pipe.set(index_id_to_name, name, nx=True))
            await self._create_storage(pipe)
