# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/basic-crawler/src/internals/basic-crawler.ts
from __future__ import annotations

from datetime import timedelta
from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING, Awaitable, Callable, Generic, Union, cast

import httpx
from typing_extensions import TypeVar

from crawlee._utils.wait import wait_for
from crawlee.autoscaling import AutoscaledPool, ConcurrencySettings
from crawlee.autoscaling.snapshotter import Snapshotter
from crawlee.autoscaling.system_status import SystemStatus
from crawlee.basic_crawler.context_pipeline import (
    ContextPipeline,
    ContextPipelineInitializationError,
    RequestHandlerError,
)
from crawlee.basic_crawler.router import Router
from crawlee.basic_crawler.types import BasicCrawlingContext, FinalStatistics, SendRequestFunction
from crawlee.configuration import Configuration
from crawlee.events.local_event_manager import LocalEventManager
from crawlee.http_clients.httpx_client import HttpxClient
from crawlee.request import BaseRequestData, Request, RequestState

if TYPE_CHECKING:
    from crawlee.http_clients.base_http_client import BaseHttpClient, HttpResponse
    from crawlee.storages.request_provider import RequestProvider


TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
ErrorHandler = Callable[[TCrawlingContext, Exception], Awaitable[Union[Request, None]]]
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
        request_provider: RequestProvider,
        router: Callable[[TCrawlingContext], Awaitable[None]] | None = None,
        # TODO: make request_provider optional (and instantiate based on configuration if None is supplied)
        # https://github.com/apify/crawlee-py/issues/83
        http_client: BaseHttpClient | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        max_request_retries: int = 3,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta = timedelta(minutes=1),
        _context_pipeline: ContextPipeline[TCrawlingContext] | None = None,
    ) -> None:
        """Initialize the BasicCrawler.

        Args:
            request_provider: Provides requests to be processed
            router: A callable to which request handling is delegated
            http_client: HTTP client to be used for `BasicCrawlingContext.send_request` and HTTP-only crawling.
            concurrency_settings: Allows fine-tuning concurrency levels
            max_request_retries: Maximum amount of attempts at processing a request
            configuration: Crawler configuration
            request_handler_timeout: How long is a single request handler allowed to run
            _context_pipeline: Allows extending the request lifecycle and modifying the crawling context.
                This parameter is meant to be used by child classes, not when BasicCrawler is instantiated directly.
        """
        self._router: Router[TCrawlingContext] | None = None

        if isinstance(cast(Router, router), Router):
            self._router = cast(Router[TCrawlingContext], router)
        elif router is not None:
            self._router = None
            self.router.default_handler(router)

        self._http_client = http_client or HttpxClient()

        self._context_pipeline = _context_pipeline or ContextPipeline()

        self._error_handler: ErrorHandler[TCrawlingContext] | None = None
        self._failed_request_handler: FailedRequestHandler[TCrawlingContext] | None = None

        self._max_request_retries = max_request_retries

        self._request_provider = request_provider
        self._configuration = configuration or Configuration()

        self._request_handler_timeout = request_handler_timeout
        self._internal_timeout = (
            self._configuration.internal_timeout
            if self._configuration.internal_timeout is not None
            else max(2 * request_handler_timeout, timedelta(minutes=5))
        )

        self._event_manager = LocalEventManager()  # TODO: switch based on configuration
        # https://github.com/apify/crawlee-py/issues/83
        self._snapshotter = Snapshotter(self._event_manager)
        self._pool = AutoscaledPool(
            system_status=SystemStatus(self._snapshotter),
            is_finished_function=self.__is_finished_function,
            is_task_ready_function=self.__is_task_ready_function,
            run_task_function=self.__run_task_function,
            concurrency_settings=concurrency_settings,
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

    def error_handler(self, handler: ErrorHandler[TCrawlingContext]) -> ErrorHandler[TCrawlingContext]:
        """Decorator for configuring an error handler (called after a request handler error and before retrying)."""
        self._error_handler = handler
        return handler

    def failed_request_handler(
        self, handler: FailedRequestHandler[TCrawlingContext]
    ) -> FailedRequestHandler[TCrawlingContext]:
        """Decorator for configuring a failed request handler (called after max retries are reached)."""
        self._failed_request_handler = handler
        return handler

    async def add_requests(
        self,
        requests: list[str | BaseRequestData],
        *,
        batch_size: int = 1000,
        wait_for_all_requests_to_be_added: bool = False,
        wait_time_between_batches: timedelta = timedelta(0),
    ) -> None:
        """Add requests to the underlying queue."""
        await self._request_provider.add_requests_batched(
            [
                request if isinstance(request, BaseRequestData) else BaseRequestData.from_url(url=request)
                for request in requests
            ],
            batch_size=batch_size,
            wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
            wait_time_between_batches=wait_time_between_batches,
        )

    async def run(self, requests: list[str | BaseRequestData] | None = None) -> FinalStatistics:
        """Run the crawler until all requests are processed."""
        if requests is not None:
            await self.add_requests(requests)

        async with self._event_manager, self._snapshotter:
            await self._pool.run()

        return FinalStatistics()

    def _should_retry_request(self, crawling_context: BasicCrawlingContext) -> bool:
        max_request_retries = crawling_context.request.max_retries
        if max_request_retries is None:
            max_request_retries = self._max_request_retries

        return (
            not crawling_context.request.no_retry and (crawling_context.request.retry_count + 1) < max_request_retries
        )

    async def _handle_request_error(self, crawling_context: TCrawlingContext, error: Exception) -> None:
        if self._should_retry_request(crawling_context):
            request = crawling_context.request
            request.retry_count += 1

            if self._error_handler:
                try:
                    new_request = await self._error_handler(crawling_context, error)
                except Exception as e:
                    raise UserDefinedErrorHandlerError('Exception thrown in user-defined request error handler') from e
                else:
                    if new_request is not None:
                        request = new_request

            await self._request_provider.reclaim_request(request)
        else:
            await wait_for(
                lambda: self._request_provider.mark_request_as_handled(crawling_context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=logger,
                max_retries=3,
            )
            await self._handle_failed_request(crawling_context, error)

    async def _handle_failed_request(self, crawling_context: TCrawlingContext, error: Exception) -> None:
        logger.exception('Request failed and reached maximum retries', exc_info=error)

        if self._failed_request_handler:
            try:
                await self._failed_request_handler(crawling_context, error)
            except Exception as e:
                raise UserDefinedErrorHandlerError('Exception thrown in user-defined failed request handler') from e

    def _prepare_send_request_function(self) -> SendRequestFunction:
        async def send_request(url: str, *, method: str = 'get', headers: dict[str, str] | None = None) -> HttpResponse:
            return await self._http_client.send_request(url, method=method, headers=httpx.Headers(headers))

        return send_request

    async def __is_finished_function(self) -> bool:
        return await self._request_provider.is_finished()

    async def __is_task_ready_function(self) -> bool:
        return not await self._request_provider.is_empty()

    async def __run_task_function(self) -> None:
        request = await wait_for(
            lambda: self._request_provider.fetch_next_request(),
            timeout=self._internal_timeout,
            timeout_message=f'Fetching next request failed after {self._internal_timeout.total_seconds()} seconds',
            logger=logger,
            max_retries=3,
        )

        if request is None:
            return

        # TODO: fetch session from the session pool
        # https://github.com/apify/crawlee-py/issues/110

        crawling_context = BasicCrawlingContext(request=request, send_request=self._prepare_send_request_function())

        try:
            request.state = RequestState.REQUEST_HANDLER

            await wait_for(
                lambda: self.__run_request_handler(crawling_context),
                timeout=self._request_handler_timeout,
                timeout_message='Request handler timed out after '
                f'{self._request_handler_timeout.total_seconds()} seconds',
                logger=logger,
            )

            await wait_for(
                lambda: self._request_provider.mark_request_as_handled(crawling_context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=logger,
                max_retries=3,
            )

            request.state = RequestState.DONE
        except RequestHandlerError as primary_error:
            primary_error = cast(
                RequestHandlerError[TCrawlingContext], primary_error
            )  # valid thanks to ContextPipeline

            try:
                request.state = RequestState.ERROR_HANDLER

                await wait_for(
                    partial(
                        self._handle_request_error, primary_error.crawling_context, primary_error.wrapped_exception
                    ),
                    timeout=self._internal_timeout,
                    timeout_message='Handling request failure timed out after '
                    f'{self._internal_timeout.total_seconds()} seconds',
                    logger=logger,
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
        except ContextPipelineInitializationError as initialization_error:
            if self._should_retry_request(crawling_context):
                logger.debug(
                    'An exception occured during the initialization of crawling context, a retry is in order',
                    exc_info=initialization_error,
                )

                request = crawling_context.request
                request.retry_count += 1
                request.state = RequestState.DONE
                await self._request_provider.reclaim_request(request)
            else:
                logger.exception('Request failed and reached maximum retries', exc_info=initialization_error)

                await wait_for(
                    lambda: self._request_provider.mark_request_as_handled(crawling_context.request),
                    timeout=self._internal_timeout,
                    timeout_message='Marking request as handled timed out after '
                    f'{self._internal_timeout.total_seconds()} seconds',
                    logger=logger,
                    max_retries=3,
                )
        except Exception as internal_error:
            logger.exception(
                'An exception occurred during handling of a request. This places the crawler '
                'and its underlying storages into an unknown state and crawling will be terminated.',
                exc_info=internal_error,
            )
            raise

    async def __run_request_handler(self, crawling_context: BasicCrawlingContext) -> None:
        await self._context_pipeline(crawling_context, self.router)
