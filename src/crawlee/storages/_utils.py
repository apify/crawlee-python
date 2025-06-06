from __future__ import annotations

from typing import Any, Callable, TypeVar, cast

from ._base import Storage

T = TypeVar('T', bound=Storage)


async def open_storage_instance(
    cls: type[T],
    *,
    id: str | None,
    name: str | None,
    configuration: Any,
    cache_by_id: dict[str, T],
    cache_by_name: dict[str, T],
    default_instance_attr: str,
    client_opener: Callable[..., Any],
) -> T:
    if id and name:
        raise ValueError('Only one of "id" or "name" can be specified, not both.')

    default_instance = getattr(cls, default_instance_attr)
    if id is None and name is None and default_instance is not None:
        return cast('T', default_instance)

    if id is not None and id in cache_by_id:
        return cache_by_id[id]
    if name is not None and name in cache_by_name:
        return cache_by_name[name]

    client = await client_opener(id=id, name=name, configuration=configuration)
    instance = cls(client)  # type: ignore[call-arg]
    instance_name = getattr(instance, 'name', None)

    cache_by_id[instance.id] = instance
    if instance_name is not None:
        cache_by_name[instance_name] = instance

    if id is None and name is None:
        setattr(cls, default_instance_attr, instance)

    return instance
