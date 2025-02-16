from __future__ import annotations

import asyncio
from functools import wraps
from typing import Any, Callable, TypeVar

T = TypeVar('T', bound=Callable[..., Any])


def ensure_context(method: T) -> T:
    """Decorator to ensure the (async) context manager is initialized before calling the method.

    Args:
        method: The method to wrap.

    Returns:
        The wrapped method with context checking applied.
    """

    @wraps(method)
    def sync_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not hasattr(self, 'active'):
            raise RuntimeError(f'The {self.__class__.__name__} does not have the "active" attribute.')

        if not self.active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active. Use it within the context.')

        return method(self, *args, **kwargs)

    @wraps(method)
    async def async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not hasattr(self, 'active'):
            raise RuntimeError(f'The {self.__class__.__name__} does not have the "active" attribute.')

        if not self.active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active. Use it within the async context.')

        return await method(self, *args, **kwargs)

    return async_wrapper if asyncio.iscoroutinefunction(method) else sync_wrapper  # type: ignore[return-value]
