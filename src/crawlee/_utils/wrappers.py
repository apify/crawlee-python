from __future__ import annotations

import functools
from typing import Any, Callable, TypeVar, cast

ImplementationType = TypeVar('ImplementationType', bound=Callable)
MetadataType = TypeVar('MetadataType', bound=Callable)


def wrap_internal(implementation: ImplementationType, metadata_source: MetadataType) -> MetadataType:
    @functools.wraps(metadata_source)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return implementation(*args, **kwargs)

    return cast(MetadataType, wrapper)
