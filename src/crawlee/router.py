from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group

__all__ = ['Router']

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext)
RequestHandler = Callable[[TCrawlingContext], Awaitable[None]]


@docs_group('Classes')
class Router(Generic[TCrawlingContext]):
    """A request dispatching system that routes requests to registered handlers based on their labels.

    The `Router` allows you to define and register request handlers for specific labels. When a request is received,
    the router invokes the corresponding `request_handler` based on the request's `label`. If no matching handler
    is found, the default handler is used.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
    from crawlee.router import Router

    router = Router[HttpCrawlingContext]()


    # Handler for requests without a matching label handler
    @router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Request without label {context.request.url} ...')


    # Handler for category requests
    @router.handler(label='category')
    async def category_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Category request {context.request.url} ...')


    # Handler for product requests
    @router.handler(label='product')
    async def product_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Product {context.request.url} ...')


    async def main() -> None:
        crawler = HttpCrawler(request_handler=router)
        await crawler.run()
    """

    def __init__(self) -> None:
        self._default_handler: RequestHandler[TCrawlingContext] | None = None
        self._handlers_by_label = dict[str, RequestHandler[TCrawlingContext]]()

    def default_handler(self: Router, handler: RequestHandler[TCrawlingContext]) -> RequestHandler[TCrawlingContext]:
        """Register a default request handler.

        The default request handler is invoked for requests that have either no label or a label for which we have
        no matching handler.
        """
        if self._default_handler is not None:
            raise RuntimeError('A default handler is already configured')

        self._default_handler = handler

        return handler

    def handler(
        self,
        label: str,
    ) -> Callable[[RequestHandler[TCrawlingContext]], Callable[[TCrawlingContext], Awaitable]]:
        """Register a request handler based on a label.

        This decorator registers a request handler for a specific label. The handler will be invoked only for requests
        that have the exact same label.
        """
        if label in self._handlers_by_label:
            raise RuntimeError(f'A handler for label `{label}` is already registered')

        def wrapper(handler: Callable[[TCrawlingContext], Awaitable]) -> Callable[[TCrawlingContext], Awaitable]:
            self._handlers_by_label[label] = handler
            return handler

        return wrapper

    async def __call__(self, context: TCrawlingContext) -> None:
        """Invoke a request handler that matches the request label (or the default)."""
        if context.request.label is None or context.request.label not in self._handlers_by_label:
            if self._default_handler is None:
                raise RuntimeError(
                    f'No handler matches label `{context.request.label}` and no default handler is configured'
                )

            return await self._default_handler(context)

        handler = self._handlers_by_label[context.request.label]
        return await handler(context)
