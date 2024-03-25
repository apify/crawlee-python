# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/basic-crawler/src/internals/basic-crawler.ts
from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, Awaitable, Callable, Generic

from typing_extensions import TypeVar

from crawlee._utils.unique_key import compute_unique_key
from crawlee._utils.wait import wait_for
from crawlee.autoscaling import AutoscaledPool
from crawlee.autoscaling.snapshotter import Snapshotter
from crawlee.autoscaling.system_status import SystemStatus
from crawlee.basic_crawler.context_pipeline import RequestHandlerError
from crawlee.basic_crawler.router import Router
from crawlee.basic_crawler.types import BasicCrawlingContext, CreateRequestSchema, FinalStatistics, RequestState
from crawlee.config import Configuration
from crawlee.events.local_event_manager import LocalEventManager

if TYPE_CHECKING:
    from crawlee.basic_crawler.context_pipeline import ContextPipeline
    from crawlee.storages.request_provider import RequestProvider


TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
ErrorHandler = Callable[[TCrawlingContext, Exception], Awaitable[TCrawlingContext]]
FailedRequestHandler = Callable[[TCrawlingContext, Exception], Awaitable[None]]

logger = getLogger(__name__)


class UserDefinedErrorHandlerError(Exception):
    """Wraps an exception thrown from an user-defined error handler."""


class BasicCrawler(Generic[TCrawlingContext]):
    """Provides a simple framework for parallel crawling of web pages.

    The URLs to crawl are fed either from a static list of URLs or from a dynamic queue of URLs enabling recursive
    crawling of websites.

    `BasicCrawler` is a low-level tool that requires the user to implement the page download and data extraction
    functionality themselves. If we want a crawler that already facilitates this functionality, we should consider using
    one of its subclasses.
    """

    def __init__(
        self,
        *,
        router: Callable[[TCrawlingContext], Awaitable] | None = None,
        _context_pipeline: ContextPipeline[TCrawlingContext],
        # TODO: make request_provider optional (and instantiate based on configuration if None is supplied)
        request_provider: RequestProvider,
        min_concurrency: int | None = None,
        max_concurrency: int | None = None,
        max_requests_per_minute: int | None = None,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta = timedelta(minutes=1),
    ) -> None:
        if isinstance(router, Router):
            self._router = router
        elif router is not None:
            self._router = None
            self.router.default_handler(router)
        else:
            self._router = None

        self._context_pipeline = _context_pipeline

        self._error_handler: ErrorHandler[BasicCrawlingContext] | None = None
        self._failed_request_handler: FailedRequestHandler[BasicCrawlingContext] | None = None

        pool_kwargs = {}
        if min_concurrency is not None:
            pool_kwargs['min_concurrency'] = min_concurrency
        if max_concurrency is not None:
            pool_kwargs['max_concurrency'] = max_concurrency
        pool_kwargs['max_tasks_per_minute'] = max_requests_per_minute

        self._request_provider = request_provider
        self._configuration = configuration or Configuration()

        self._request_handler_timeout = request_handler_timeout
        self._internal_timeout = (
            self._configuration.internal_timeout
            if self._configuration.internal_timeout is not None
            else max(2 * request_handler_timeout, timedelta(minutes=5))
        )

        self._event_manager = LocalEventManager()  # TODO: switch based on configuration
        self._snapshotter = Snapshotter(self._event_manager)
        self._pool = AutoscaledPool(
            system_status=SystemStatus(self._snapshotter),
            is_finished_function=self.__is_finished_function,
            is_task_ready_function=self.__is_task_ready_function,
            run_task_function=self.__run_task_function,
            **pool_kwargs,
        )

    @property
    def router(self) -> Router[TCrawlingContext]:
        """The router used to handle each individual crawling request."""
        if self._router is None:
            self._router = Router[TCrawlingContext]()

        return self._router

    @router.setter
    def router(self, router: Router[TCrawlingContext]) -> None:
        if self._router is not None:
            raise RuntimeError('A router is already set')

        self._router = router

    def error_handler(self, handler: ErrorHandler[BasicCrawlingContext]) -> ErrorHandler[BasicCrawlingContext]:
        """Decorator for configuring an error handler (called after a request handler error and before retrying)."""
        self._error_handler = handler
        return handler

    def failed_request_handler(
        self, handler: FailedRequestHandler[BasicCrawlingContext]
    ) -> FailedRequestHandler[BasicCrawlingContext]:
        """Decorator for configuring a failed request handler (called after max retries are reached)."""
        self._failed_request_handler = handler
        return handler

    async def add_requests(
        self,
        requests: list[str | CreateRequestSchema],
        *,
        batch_size: int = 1000,
        wait_for_all_requests_to_be_added: bool = False,
        wait_time_between_batches: timedelta = timedelta(0),
    ) -> None:
        """Add requests to the underlying queue."""
        await self._request_provider.add_requests_batched(
            [
                request
                if isinstance(request, CreateRequestSchema)
                else CreateRequestSchema(url=request, unique_key=compute_unique_key(request))
                for request in requests
            ],
            batch_size=batch_size,
            wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
            wait_time_between_batches=wait_time_between_batches,
        )

    async def run(self, requests: list[str | CreateRequestSchema] | None = None) -> FinalStatistics:
        """Run the crawler until all reqeuests are processed."""
        if requests is not None:
            await self.add_requests(requests)

        async with self._event_manager, self._snapshotter:
            await self._pool.run()

        return FinalStatistics()

    async def __is_finished_function(self) -> bool:
        return await self._request_provider.is_finished()

    async def __is_task_ready_function(self) -> bool:
        return not await self._request_provider.is_empty()

    async def __run_task_function(self) -> None:
        request = await wait_for(
            self._request_provider.fetch_next_request(),
            self._internal_timeout,
            f'Fetching next request failed after {self._internal_timeout.total_seconds()} seconds',
        )

        if request is None:
            return

        # TODO: fetch session from the session pool

        crawling_context = BasicCrawlingContext(request=request)

        try:
            request.state = RequestState.REQUEST_HANDLER

            await wait_for(
                self.__run_request_handler(crawling_context),
                self._request_handler_timeout,
                f'Request handler timed out after {self._request_handler_timeout.total_seconds()} seconds',
            )

            await wait_for(
                self._request_provider.mark_request_handled(request),
                self._internal_timeout,
                f'Marking request as handled timed out after {self._internal_timeout.total_seconds()} seconds',
            )  # TODO: retry on failure

            request.state = RequestState.DONE
        except RequestHandlerError[TCrawlingContext] as primary_error:
            try:
                request.state = RequestState.ERROR_HANDLER

                await wait_for(
                    self._handle_request_error(primary_error.crawling_context, primary_error.wrapped_exception),
                    self._internal_timeout,
                    f'Handling request failure timed out after {self._internal_timeout.total_seconds()} seconds',
                )

                request.state = RequestState.DONE
            except UserDefinedErrorHandlerError:
                request.state = RequestState.ERROR
                raise
            except Exception as secondary_error:
                logger.exception(
                    'An exception occurred during handling of failed request. This places the crawler '
                    'and its underlying storages into an unknown state and crawling will be terminated.',
                    exc_info=secondary_error,
                )
                request.state = RequestState.ERROR
                raise
        except Exception as internal_error:
            logger.exception(
                'An exception occurred during handling of a request. This places the crawler '
                'and its underlying storages into an unknown state and crawling will be terminated.',
                exc_info=internal_error,
            )
            raise

    async def __run_request_handler(self, crawling_context: BasicCrawlingContext) -> None:
        await self._context_pipeline(crawling_context, self.router)

    async def _handle_request_error(self, crawling_context: TCrawlingContext, error: Exception) -> None:
        should_retry_request = False  # TODO: handle and track retries

        if should_retry_request:
            if self._error_handler:
                try:
                    await self._error_handler(crawling_context, error)
                except Exception as e:
                    raise UserDefinedErrorHandlerError('Exception thrown in user-defined request error handler') from e

            await self._request_provider.reclaim_request(crawling_context.request)
        else:
            await self._handle_failed_request(crawling_context, error)

    async def _handle_failed_request(self, crawling_context: TCrawlingContext, error: Exception) -> None:
        logger.exception('Request failed and reached maximum retries', exc_info=error)

        if self._failed_request_handler:
            try:
                await self._failed_request_handler(crawling_context, error)
            except Exception as e:
                raise UserDefinedErrorHandlerError('Exception thrown in user-defined failed request handler') from e
