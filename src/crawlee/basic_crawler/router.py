from __future__ import annotations

from typing import Awaitable, Callable, Generic, TypeVar

from crawlee.basic_crawler.types import BasicCrawlingContext

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext)
RequestHandler = Callable[[TCrawlingContext], Awaitable]


class Router(Generic[TCrawlingContext]):
    """Dispatches requests to registered handlers based on their labels."""

    def __init__(self: Router) -> None:
        self._default_handler = None
        self._handlers_by_label = dict[str, RequestHandler]()

    def default_handler(self: Router, handler: RequestHandler) -> None:
        """A decorator used to register a default handler.

        The default handler is invoked for requests that have either no label or a label for which we have no matching
        handler.
        """
        if self._default_handler is not None:
            raise RuntimeError('A default handler is already configured')

        self._default_handler = handler

    def handler(self: Router, label: str) -> Callable[[RequestHandler], None]:
        """A decorator used to register a label-based handler.

        The registered will be invoked only for requests with the exact same label.
        """
        if label in self._handlers_by_label:
            raise RuntimeError(f'A handler for label `{label}` is already registered')

        def wrapper(handler: Callable[[TCrawlingContext], Awaitable]) -> None:
            self._handlers_by_label[label] = handler

        return wrapper

    async def __call__(self: Router, context: TCrawlingContext) -> None:
        """Invoke a request handler that matches the request label (or the default)."""
        if context.request.label is None or context.request.label not in self._handlers_by_label:
            if self._default_handler is None:
                raise RuntimeError(
                    f'No handler matches label `{context.request.label}` and no default handler is configured'
                )

            return await self._default_handler(context)

        return await self._handlers_by_label[context.request.label](context)
