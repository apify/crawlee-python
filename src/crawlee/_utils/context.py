from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast

if TYPE_CHECKING:
    from types import TracebackType

T = TypeVar('T', bound=Callable[..., Any])


class Collaborator(Protocol):
    """An async context manager that reports through `active` whether it's entered."""

    @property
    def active(self) -> bool:
        """Whether the object is entered and not exited yet."""

    async def __aenter__(self) -> Any:
        """Prepare the object for use.

        Raises:
            RuntimeError: If the object is already active.
        """

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
        /,
    ) -> Any:
        """Release what the object holds.

        Raises:
            RuntimeError: If the object is not active.
        """


def ensure_context(method: T) -> T:
    """Ensure the (async) context manager is initialized before executing the method.

    This decorator checks if the calling instance has an `active` attribute and verifies that it is set to `True`.
    If the instance is inactive, it raises a `RuntimeError`. Works for both synchronous and asynchronous methods.

    Args:
        method: The method to wrap.

    Returns:
        The wrapped method with context checking applied.

    Raises:
        RuntimeError: If the instance lacks an `active` attribute or is not active.
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

    return cast('T', async_wrapper if inspect.iscoroutinefunction(method) else sync_wrapper)
