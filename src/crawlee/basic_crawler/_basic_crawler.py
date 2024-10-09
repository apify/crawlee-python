# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/basic-crawler/src/internals/basic-crawler.ts
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import tempfile
from asyncio import CancelledError
from collections.abc import AsyncGenerator, Awaitable, Sequence
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncContextManager, Callable, Generic, Literal, Union, cast
from urllib.parse import ParseResult, urlparse

from tldextract import TLDExtract
from typing_extensions import NotRequired, TypedDict, TypeVar, Unpack, assert_never

from crawlee import EnqueueStrategy, Glob, service_container
from crawlee._autoscaling import AutoscaledPool
from crawlee._autoscaling.snapshotter import Snapshotter
from crawlee._autoscaling.system_status import SystemStatus
from crawlee._log_config import configure_logger, get_configured_log_level
from crawlee._request import BaseRequestData, Request, RequestState
from crawlee._types import BasicCrawlingContext, HttpHeaders, RequestHandlerRunResult, SendRequestFunction
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.http import is_status_code_client_error
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee._utils.wait import wait_for
from crawlee.basic_crawler._context_pipeline import ContextPipeline
from crawlee.errors import (
    ContextPipelineInitializationError,
    ContextPipelineInterruptedError,
    HttpStatusCodeError,
    RequestHandlerError,
    SessionError,
    UserDefinedErrorHandlerError,
)
from crawlee.http_clients import HttpxHttpClient
from crawlee.router import Router
from crawlee.sessions import SessionPool
from crawlee.statistics import Statistics
from crawlee.storages import Dataset, KeyValueStore, RequestQueue

if TYPE_CHECKING:
    import re

    from crawlee._types import ConcurrencySettings, HttpMethod, JsonSerializable
    from crawlee.base_storage_client._models import DatasetItemsListPage
    from crawlee.configuration import Configuration
    from crawlee.events._event_manager import EventManager
    from crawlee.http_clients import BaseHttpClient, HttpResponse
    from crawlee.proxy_configuration import ProxyConfiguration, ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import FinalStatistics, StatisticsState
    from crawlee.storages._dataset import GetDataKwargs, PushDataKwargs
    from crawlee.storages._request_provider import RequestProvider

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
ErrorHandler = Callable[[TCrawlingContext, Exception], Awaitable[Union[Request, None]]]
FailedRequestHandler = Callable[[TCrawlingContext, Exception], Awaitable[None]]


class BasicCrawlerOptions(TypedDict, Generic[TCrawlingContext]):
    """Copy of the parameter types of `BasicCrawler.__init__` meant for typing forwarded __init__ args in subclasses."""

    request_provider: NotRequired[RequestProvider]
    request_handler: NotRequired[Callable[[TCrawlingContext], Awaitable[None]]]
    http_client: NotRequired[BaseHttpClient]
    concurrency_settings: NotRequired[ConcurrencySettings]
    max_request_retries: NotRequired[int]
    max_requests_per_crawl: NotRequired[int | None]
    max_session_rotations: NotRequired[int]
    configuration: NotRequired[Configuration]
    request_handler_timeout: NotRequired[timedelta]
    session_pool: NotRequired[SessionPool]
    use_session_pool: NotRequired[bool]
    retry_on_blocked: NotRequired[bool]
    proxy_configuration: NotRequired[ProxyConfiguration]
    statistics: NotRequired[Statistics[StatisticsState]]
    event_manager: NotRequired[EventManager]
    configure_logging: NotRequired[bool]
    _context_pipeline: NotRequired[ContextPipeline[TCrawlingContext]]
    _additional_context_managers: NotRequired[Sequence[AsyncContextManager]]
    _logger: NotRequired[logging.Logger]


class BasicCrawler(Generic[TCrawlingContext]):
    """A versatile web crawler for parallel URL fetching with extensive features for web scraping.

    BasicCrawler is a highly customizable and efficient web crawling solution that provides:
    - Automatic scaling based on available system resources and target website's requirements.
    - Smart request routing and handling of different types of URLs.
    - Session management for maintaining persistent connections.
    - Proxy integration and rotation for avoiding IP-based blocking.
    - Automatic retries for failed requests with customizable settings.
    - Built-in statistics tracking for monitoring crawler performance.
    - Event system for fine-grained control over the crawling process.

    Basic usage:
    ```python
    async def handle_request(context: Context) -> None:
        url = context.request.url
        response = await context.send_request()
        # Process the response here
    crawler = BasicCrawler(
        request_handler=handle_request,
    )
    await crawler.run()
    ```

    Advanced features:
    - Configure concurrency to control parallel processing.
    - Set up automatic retry mechanisms for failed or blocked requests.
    - Utilize session pools for efficient connection management.
    - Integrate proxy services for IP rotation.
    - Monitor detailed statistics about the crawling process.

    Args:
        request_provider: The provider for requests to be crawled.
        request_handler: The function to handle individual requests.
        http_client: The HTTP client for making web requests.
        concurrency_settings: Settings for controlling concurrency.
        max_request_retries: Maximum retry attempts for failed requests.
        max_requests_per_crawl: Maximum number of requests per crawl.
        max_session_rotations: Maximum session rotations per request.
        configuration: Crawler configuration settings.
        request_handler_timeout: Request handler timeout.
        use_session_pool: Whether to use session pool.
        session_pool: Preconfigured session pool.
        retry_on_blocked: Whether to retry on blocked requests.
        proxy_configuration: Proxy configuration.
        statistics: Statistics object.
        event_manager: Event manager.
        configure_logging: Whether to configure logging.
        _context_pipeline: Internal context pipeline.
        _additional_context_managers: Additional context managers.
        _logger: Logger instance.
    """

    def __init__(
        self,
        *,
        request_provider: RequestProvider | None = None,
        request_handler: Callable[[TCrawlingContext], Awaitable[None]] | None = None,
        http_client: BaseHttpClient | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        max_request_retries: int = 3,
        max_requests_per_crawl: int | None = None,
        max_session_rotations: int = 10,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta = timedelta(minutes=1),
        session_pool: SessionPool | None = None,
        use_session_pool: bool = True,
        retry_on_blocked: bool = True,
        proxy_configuration: ProxyConfiguration | None = None,
        statistics: Statistics | None = None,
        event_manager: EventManager | None = None,
        configure_logging: bool = True,
        _context_pipeline: ContextPipeline[TCrawlingContext] | None = None,
        _additional_context_managers: Sequence[AsyncContextManager] | None = None,
        _logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the BasicCrawler.

        Args:
            request_provider: Request provider.
            request_handler: Request handler function.
            http_client: HTTP client.
            concurrency_settings: Concurrency settings.
            max_request_retries: Maximum retries.
            max_requests_per_crawl: Maximum requests per crawl.
            max_session_rotations: Maximum session rotations.
            configuration: Crawler configuration.
            request_handler_timeout: Request handler timeout.
            use_session_pool: Use session pool.
            session_pool: Preconfigured session pool.
            retry_on_blocked: Retry on blocked requests.
            proxy_configuration: Proxy configuration.
            statistics: Statistics object.
            event_manager: Event manager.
            configure_logging: Configure logging.
            _context_pipeline: Internal context pipeline.
            _additional_context_managers: Additional context managers.
            _logger: Logger instance.
        """
        self._router: Router[TCrawlingContext] | None = None

        if isinstance(cast(Router, request_handler), Router):
            self._router = cast(Router[TCrawlingContext], request_handler)
        elif request_handler is not None:
            self._router = None
            self.router.default_handler(request_handler)

        self._http_client = http_client or HttpxHttpClient()

        self._context_pipeline = (_context_pipeline or ContextPipeline()).compose(self._check_url_after_redirects)

        self._error_handler: ErrorHandler[TCrawlingContext | BasicCrawlingContext] | None = None
        self._failed_request_handler: FailedRequestHandler[TCrawlingContext | BasicCrawlingContext] | None = None

        self._max_request_retries = max_request_retries
        self._max_requests_per_crawl = max_requests_per_crawl
        self._max_session_rotations = max_session_rotations

        self._request_provider = request_provider
        self._configuration = configuration or service_container.get_configuration()

        self._request_handler_timeout = request_handler_timeout
        self._internal_timeout = (
            self._configuration.internal_timeout
            if self._configuration.internal_timeout is not None
            else max(2 * request_handler_timeout, timedelta(minutes=5))
        )

        self._tld_extractor = TLDExtract(cache_dir=tempfile.TemporaryDirectory().name)

        self._event_manager = event_manager or service_container.get_event_manager()
        self._snapshotter = Snapshotter(
            self._event_manager,
            max_memory_size=ByteSize.from_mb(self._configuration.memory_mbytes)
            if self._configuration.memory_mbytes
            else None,
            available_memory_ratio=self._configuration.available_memory_ratio,
        )
        self._pool = AutoscaledPool(
            system_status=SystemStatus(self._snapshotter),
            is_finished_function=self.__is_finished_function,
            is_task_ready_function=self.__is_task_ready_function,
            run_task_function=self.__run_task_function,
            concurrency_settings=concurrency_settings,
        )

        self._use_session_pool = use_session_pool
        self._session_pool = session_pool or SessionPool()

        self._retry_on_blocked = retry_on_blocked

        if configure_logging:
            root_logger = logging.getLogger()
            configure_logger(root_logger, self._configuration, remove_old_handlers=True)

            # Silence HTTPX logger
            httpx_logger = logging.getLogger('httpx')
            httpx_logger.setLevel(
                logging.DEBUG if get_configured_log_level(self._configuration) <= logging.DEBUG else logging.WARNING
            )

        if not _logger:
            _logger = logging.getLogger(__name__)

        self._logger = _logger

        self._proxy_configuration = proxy_configuration
        self._statistics = statistics or Statistics(
            event_manager=self._event_manager,
            periodic_message_logger=self._logger,
            log_message='Current request statistics:',
        )
        self._additional_context_managers = _additional_context_managers or []

        self._running = False
        self._has_finished_before = False

    @property
    def log(self) -> logging.Logger:
        """The logger used by the crawler for debugging and info-level messages."""
        return self._logger

    @property
    def router(self) -> Router[TCrawlingContext]:
        """Handle individual crawling requests. Initialize a router if none is set."""
        if self._router is None:
            self._router = Router[TCrawlingContext]()

        return self._router

    @router.setter
    def router(self, router: Router[TCrawlingContext]) -> None:
        """Set the router, ensuring only one instance is allowed."""
        if self._router is not None:
            raise RuntimeError('A router is already set')

        self._router = router

    @property
    def statistics(self) -> Statistics[StatisticsState]:
        """Return statistics about the crawler's current or last run."""
        return self._statistics

    @property
    def _max_requests_count_exceeded(self) -> bool:
        """Check if the max number of crawl request has been reached."""
        if self._max_requests_per_crawl is None:
            return False

        return self._statistics.state.requests_finished >= self._max_requests_per_crawl

    async def _get_session(self) -> Session | None:
        """Fetch a session from the pool if session pooling is enabled."""
        if not self._use_session_pool:
            return None

        return await wait_for(
            self._session_pool.get_session,
            timeout=self._internal_timeout,
            timeout_message='Fetching a session from the pool timed out after '
            f'{self._internal_timeout.total_seconds()} seconds',
            max_retries=3,
            logger=self._logger,
        )

    async def _get_proxy_info(self, request: Request, session: Session | None) -> ProxyInfo | None:
        """Return a ProxyInfo object for a request based on the crawlers proxy settings."""
        if not self._proxy_configuration:
            return None

        return await self._proxy_configuration.new_proxy_info(
            session_id=session.id if session else None,
            request=request,
            proxy_tier=None,
        )

    async def get_request_provider(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> RequestProvider:
        """Return the configured request provider or open the default request queue."""
        if not self._request_provider:
            self._request_provider = await RequestQueue.open(id=id, name=name, configuration=self._configuration)

        return self._request_provider

    async def get_dataset(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Dataset:
        """Open and return the dataset by ID or name, or open the default dataset if none is provided."""
        return await Dataset.open(id=id, name=name, configuration=self._configuration)

    async def get_key_value_store(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> KeyValueStore:
        """Open and return a key-value store by ID or name, or open the default one if none is provided."""
        return await KeyValueStore.open(id=id, name=name, configuration=self._configuration)

    def error_handler(
        self, handler: ErrorHandler[TCrawlingContext | BasicCrawlingContext]
    ) -> ErrorHandler[TCrawlingContext]:
        """Set a custom error handler for request retries in case of errors."""
        self._error_handler = handler
        return handler

    def failed_request_handler(
        self, handler: FailedRequestHandler[TCrawlingContext | BasicCrawlingContext]
    ) -> FailedRequestHandler[TCrawlingContext]:
        """Set a handler for requests that fail after reaching max retries."""
        self._failed_request_handler = handler
        return handler

    async def run(
        self,
        requests: Sequence[str | Request] | None = None,
        *,
        purge_request_queue: bool = True,
    ) -> FinalStatistics:
        """Run the crawler until all requests are processed.

        Args:
            requests: A list of requests to enqueue before the crawl starts.
            purge_request_queue: Whether to clear the request queue before starting the crawler run.

        Returns:
            The final statistics about the crawler run.
        """
        if self._running:
            raise RuntimeError(
                'This crawler instance is already running, you can add more requests to it via `crawler.add_requests()`'
            )

        self._running = True

        if self._has_finished_before:
            await self._statistics.reset()

            if self._use_session_pool:
                await self._session_pool.reset_store()

            request_provider = await self.get_request_provider()
            if purge_request_queue and isinstance(request_provider, RequestQueue):
                await request_provider.drop()
                self._request_provider = await RequestQueue.open(configuration=self._configuration)

        if requests is not None:
            await self.add_requests(requests)

        interrupted = False

        def sigint_handler() -> None:
            nonlocal interrupted

            if not interrupted:
                interrupted = True
                self._logger.info('Pausing... Press CTRL+C again to force exit.')

            run_task.cancel()

        run_task = asyncio.create_task(self._run_crawler())

        with suppress(NotImplementedError):  # event loop signal handlers are not supported on Windows
            asyncio.get_running_loop().add_signal_handler(signal.SIGINT, sigint_handler)

        try:
            await run_task
        except CancelledError:
            pass
        finally:
            with suppress(NotImplementedError):
                asyncio.get_running_loop().remove_signal_handler(signal.SIGINT)

        if self._statistics.error_tracker.total > 0:
            self._logger.info(
                'Error analysis:'
                f' total_errors={self._statistics.error_tracker.total}'
                f' unique_errors={self._statistics.error_tracker.unique_error_count}'
            )

        if interrupted:
            self._logger.info(
                f'The crawl was interrupted. To resume, do: CRAWLEE_PURGE_ON_START=0 python {sys.argv[0]}'
            )

        self._running = False
        self._has_finished_before = True

        final_statistics = self._statistics.calculate()
        self._logger.info(f'Final request statistics:\n{final_statistics.to_table()}')

        return final_statistics

    async def _run_crawler(self) -> None:
        async with AsyncExitStack() as exit_stack:
            await exit_stack.enter_async_context(self._event_manager)
            await exit_stack.enter_async_context(self._snapshotter)
            await exit_stack.enter_async_context(self._statistics)

            if self._use_session_pool:
                await exit_stack.enter_async_context(self._session_pool)

            for context_manager in self._additional_context_managers:
                await exit_stack.enter_async_context(context_manager)

            await self._pool.run()

    async def add_requests(
        self,
        requests: Sequence[str | Request],
        *,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(0),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        """Add requests to the underlying request provider in batches.

        Args:
            requests: A list of requests to add to the queue.
            batch_size: The number of requests to add in one batch.
            wait_time_between_batches: Time to wait between adding batches.
            wait_for_all_requests_to_be_added: If True, wait for all requests to be added before returning.
            wait_for_all_requests_to_be_added_timeout: Timeout for waiting for all requests to be added.
        """
        request_provider = await self.get_request_provider()

        await request_provider.add_requests_batched(
            requests=requests,
            batch_size=batch_size,
            wait_time_between_batches=wait_time_between_batches,
            wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
            wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
        )

    async def get_data(
        self,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[GetDataKwargs],
    ) -> DatasetItemsListPage:
        """Retrieve data from a dataset.

        This helper method simplifies the process of retrieving data from a dataset. It opens the specified
        dataset and then retrieves the data based on the provided parameters.

        Args:
            dataset_id: The ID of the dataset.
            dataset_name: The name of the dataset.
            kwargs: Keyword arguments to be passed to the dataset's `get_data` method.

        Returns:
            The retrieved data.
        """
        dataset = await Dataset.open(id=dataset_id, name=dataset_name)
        return await dataset.get_data(**kwargs)

    async def export_data(
        self,
        path: str | Path,
        content_type: Literal['json', 'csv'] | None = None,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
    ) -> None:
        """Export data from a dataset.

        This helper method simplifies the process of exporting data from a dataset. It opens the specified
        dataset and then exports the data based on the provided parameters.

        Args:
            path: The destination path
            content_type: The output format
            dataset_id: The ID of the dataset.
            dataset_name: The name of the dataset.
        """
        dataset = await self.get_dataset(id=dataset_id, name=dataset_name)
        path = path if isinstance(path, Path) else Path(path)

        if content_type is None:
            content_type = 'csv' if path.suffix == '.csv' else 'json'

        return await dataset.write_to(content_type, path.open('w', newline=''))

    async def _push_data(
        self,
        data: JsonSerializable,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[PushDataKwargs],
    ) -> None:
        """Push data to a dataset.

        This helper method simplifies the process of pushing data to a dataset. It opens the specified
        dataset and then pushes the provided data to it.

        Args:
            data: The data to push to the dataset.
            dataset_id: The ID of the dataset.
            dataset_name: The name of the dataset.
            kwargs: Keyword arguments to be passed to the dataset's `push_data` method.
        """
        dataset = await self.get_dataset(id=dataset_id, name=dataset_name)
        await dataset.push_data(data, **kwargs)

    def _should_retry_request(self, crawling_context: BasicCrawlingContext, error: Exception) -> bool:
        if crawling_context.request.no_retry:
            return False

        # Do not retry on client errors.
        if isinstance(error, HttpStatusCodeError) and is_status_code_client_error(error.status_code):
            return False

        if isinstance(error, SessionError):
            return ((crawling_context.request.session_rotation_count or 0) + 1) < self._max_session_rotations

        max_request_retries = crawling_context.request.max_retries
        if max_request_retries is None:
            max_request_retries = self._max_request_retries

        return (crawling_context.request.retry_count + 1) < max_request_retries

    async def _check_url_after_redirects(
        self, crawling_context: TCrawlingContext
    ) -> AsyncGenerator[TCrawlingContext, None]:
        """Invoked at the end of the context pipeline to make sure that the `loaded_url` still matches enqueue_strategy.

        This is done to filter out links that redirect outside of the crawled domain.
        """
        if crawling_context.request.loaded_url is not None and not self._check_enqueue_strategy(
            crawling_context.request.enqueue_strategy,
            origin_url=urlparse(crawling_context.request.url),
            target_url=urlparse(crawling_context.request.loaded_url),
        ):
            raise ContextPipelineInterruptedError(
                f'Skipping URL {crawling_context.request.loaded_url} (redirected from {crawling_context.request.url})'
            )

        yield crawling_context

    def _check_enqueue_strategy(
        self,
        strategy: EnqueueStrategy,
        *,
        target_url: ParseResult,
        origin_url: ParseResult,
    ) -> bool:
        """Check if a URL matches the enqueue_strategy."""
        if strategy == EnqueueStrategy.SAME_HOSTNAME:
            return target_url.hostname == origin_url.hostname

        if strategy == EnqueueStrategy.SAME_DOMAIN:
            if origin_url.hostname is None or target_url.hostname is None:
                raise ValueError('Both origin and target URLs must have a hostname')

            origin_domain = self._tld_extractor.extract_str(origin_url.hostname).domain
            target_domain = self._tld_extractor.extract_str(target_url.hostname).domain
            return origin_domain == target_domain

        if strategy == EnqueueStrategy.SAME_ORIGIN:
            return target_url.hostname == origin_url.hostname and target_url.scheme == origin_url.scheme

        if strategy == EnqueueStrategy.ALL:
            return True

        assert_never(strategy)

    def _check_url_patterns(
        self,
        target_url: str,
        include: Sequence[re.Pattern[Any] | Glob] | None,
        exclude: Sequence[re.Pattern[Any] | Glob] | None,
    ) -> bool:
        """Check if a URL matches configured include/exclude patterns."""
        # If the URL matches any `exclude` pattern, reject it
        for pattern in exclude or ():
            if isinstance(pattern, Glob):
                pattern = pattern.regexp  # noqa: PLW2901

            if pattern.match(target_url) is not None:
                return False

        # If there are no `include` patterns and the URL passed all `exclude` patterns, accept the URL
        if include is None:
            return True

        # If the URL matches any `include` pattern, accept it
        for pattern in include:
            if isinstance(pattern, Glob):
                pattern = pattern.regexp  # noqa: PLW2901

            if pattern.match(target_url) is not None:
                return True

        # The URL does not match any `include` pattern - reject it
        return False

    async def _handle_request_retries(
        self,
        crawling_context: TCrawlingContext | BasicCrawlingContext,
        error: Exception,
    ) -> None:
        request_provider = await self.get_request_provider()
        request = crawling_context.request

        if self._should_retry_request(crawling_context, error):
            request.retry_count += 1
            self._statistics.error_tracker.add(error)

            if self._error_handler:
                try:
                    new_request = await self._error_handler(crawling_context, error)
                except Exception as e:
                    raise UserDefinedErrorHandlerError('Exception thrown in user-defined request error handler') from e
                else:
                    if new_request is not None:
                        request = new_request

            await request_provider.reclaim_request(request)
        else:
            await wait_for(
                lambda: request_provider.mark_request_as_handled(crawling_context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
                max_retries=3,
            )
            await self._handle_failed_request(crawling_context, error)
            self._statistics.record_request_processing_failure(request.id or request.unique_key)

    async def _handle_request_error(
        self,
        crawling_context: TCrawlingContext | BasicCrawlingContext,
        error: Exception,
    ) -> None:
        try:
            crawling_context.request.state = RequestState.ERROR_HANDLER

            await wait_for(
                partial(self._handle_request_retries, crawling_context, error),
                timeout=self._internal_timeout,
                timeout_message='Handling request failure timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
            )

            crawling_context.request.state = RequestState.DONE
        except UserDefinedErrorHandlerError:
            crawling_context.request.state = RequestState.ERROR
            raise
        except Exception as secondary_error:
            self._logger.exception(
                'An exception occurred during handling of failed request. This places the crawler '
                'and its underlying storages into an unknown state and crawling will be terminated.',
                exc_info=secondary_error,
            )
            crawling_context.request.state = RequestState.ERROR
            raise

        if crawling_context.session:
            crawling_context.session.mark_bad()

    async def _handle_failed_request(
        self, crawling_context: TCrawlingContext | BasicCrawlingContext, error: Exception
    ) -> None:
        self._logger.exception('Request failed and reached maximum retries', exc_info=error)
        self._statistics.error_tracker.add(error)

        if self._failed_request_handler:
            try:
                await self._failed_request_handler(crawling_context, error)
            except Exception as e:
                raise UserDefinedErrorHandlerError('Exception thrown in user-defined failed request handler') from e

    def _prepare_send_request_function(
        self,
        session: Session | None,
        proxy_info: ProxyInfo | None,
    ) -> SendRequestFunction:
        async def send_request(
            url: str,
            *,
            method: HttpMethod = 'GET',
            headers: HttpHeaders | None = None,
        ) -> HttpResponse:
            return await self._http_client.send_request(
                url=url,
                method=method,
                headers=headers,
                session=session,
                proxy_info=proxy_info,
            )

        return send_request

    async def _commit_request_handler_result(
        self, context: BasicCrawlingContext, result: RequestHandlerRunResult
    ) -> None:
        request_provider = await self.get_request_provider()
        origin = context.request.loaded_url or context.request.url

        for call in result.add_requests_calls:
            requests = list[Request]()

            for request in call['requests']:
                if (limit := call.get('limit')) is not None and len(requests) >= limit:
                    break

                # If the request is a string, convert it to Request object.
                if isinstance(request, str):
                    if is_url_absolute(request):
                        dst_request = Request.from_url(request)

                    # If the request URL is relative, make it absolute using the origin URL.
                    else:
                        base_url = call['base_url'] if call.get('base_url') else origin
                        absolute_url = convert_to_absolute_url(base_url, request)
                        dst_request = Request.from_url(absolute_url)

                # If the request is a BaseRequestData, convert it to Request object.
                elif isinstance(request, BaseRequestData):
                    dst_request = Request.from_base_request_data(request)

                if self._check_enqueue_strategy(
                    call.get('strategy', EnqueueStrategy.ALL),
                    target_url=urlparse(dst_request.url),
                    origin_url=urlparse(origin),
                ) and self._check_url_patterns(
                    dst_request.url,
                    call.get('include', None),
                    call.get('exclude', None),
                ):
                    requests.append(dst_request)

            await request_provider.add_requests_batched(requests)

    async def __is_finished_function(self) -> bool:
        request_provider = await self.get_request_provider()
        is_finished = await request_provider.is_finished()

        if self._max_requests_count_exceeded:
            self._logger.info(
                f'The crawler has reached its limit of {self._max_requests_per_crawl} requests per crawl. '
                f'All ongoing requests have now completed. Total requests processed: '
                f'{self._statistics.state.requests_finished}. The crawler will now shut down.'
            )
            return True

        return is_finished

    async def __is_task_ready_function(self) -> bool:
        if self._max_requests_count_exceeded:
            self._logger.info(
                f'The crawler has reached its limit of {self._max_requests_per_crawl} requests per crawl. '
                f'The crawler will soon shut down. Ongoing requests will be allowed to complete.'
            )
            return False

        request_provider = await self.get_request_provider()
        return not await request_provider.is_empty()

    async def __run_task_function(self) -> None:
        request_provider = await self.get_request_provider()

        request = await wait_for(
            lambda: request_provider.fetch_next_request(),
            timeout=self._internal_timeout,
            timeout_message=f'Fetching next request failed after {self._internal_timeout.total_seconds()} seconds',
            logger=self._logger,
            max_retries=3,
        )

        if request is None:
            return

        session = await self._get_session()
        proxy_info = await self._get_proxy_info(request, session)
        result = RequestHandlerRunResult()

        crawling_context = BasicCrawlingContext(
            request=request,
            session=session,
            proxy_info=proxy_info,
            send_request=self._prepare_send_request_function(session, proxy_info),
            add_requests=result.add_requests,
            push_data=self._push_data,
            log=self._logger,
        )

        statistics_id = request.id or request.unique_key
        self._statistics.record_request_processing_start(statistics_id)

        try:
            request.state = RequestState.REQUEST_HANDLER

            await wait_for(
                lambda: self.__run_request_handler(crawling_context),
                timeout=self._request_handler_timeout,
                timeout_message='Request handler timed out after '
                f'{self._request_handler_timeout.total_seconds()} seconds',
                logger=self._logger,
            )

            await self._commit_request_handler_result(crawling_context, result)

            await wait_for(
                lambda: request_provider.mark_request_as_handled(crawling_context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
                max_retries=3,
            )

            request.state = RequestState.DONE

            if crawling_context.session:
                crawling_context.session.mark_good()

            self._statistics.record_request_processing_finish(statistics_id)

        except RequestHandlerError as primary_error:
            primary_error = cast(
                RequestHandlerError[TCrawlingContext], primary_error
            )  # valid thanks to ContextPipeline

            self._logger.debug(
                'An exception occurred in the user-defined request handler',
                exc_info=primary_error.wrapped_exception,
            )
            await self._handle_request_error(primary_error.crawling_context, primary_error.wrapped_exception)

        except SessionError as session_error:
            if not crawling_context.session:
                raise RuntimeError('SessionError raised in a crawling context without a session') from session_error

            if self._error_handler:
                await self._error_handler(crawling_context, session_error)

            if self._should_retry_request(crawling_context, session_error):
                self._logger.warning('Encountered a session error, rotating session and retrying')

                crawling_context.session.retire()

                if crawling_context.request.session_rotation_count is None:
                    crawling_context.request.session_rotation_count = 0
                crawling_context.request.session_rotation_count += 1

                await request_provider.reclaim_request(request)
                self._statistics.error_tracker_retry.add(session_error)
            else:
                self._logger.exception('Request failed and reached maximum retries', exc_info=session_error)

                await wait_for(
                    lambda: request_provider.mark_request_as_handled(crawling_context.request),
                    timeout=self._internal_timeout,
                    timeout_message='Marking request as handled timed out after '
                    f'{self._internal_timeout.total_seconds()} seconds',
                    logger=self._logger,
                    max_retries=3,
                )

                self._statistics.record_request_processing_failure(statistics_id)
                self._statistics.error_tracker.add(session_error)

        except ContextPipelineInterruptedError as interrupted_error:
            self._logger.debug('The context pipeline was interrupted', exc_info=interrupted_error)

            await wait_for(
                lambda: request_provider.mark_request_as_handled(crawling_context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
                max_retries=3,
            )

        except ContextPipelineInitializationError as initialization_error:
            self._logger.debug(
                'An exception occurred during the initialization of crawling context',
                exc_info=initialization_error,
            )
            await self._handle_request_error(crawling_context, initialization_error.wrapped_exception)

        except Exception as internal_error:
            self._logger.exception(
                'An exception occurred during handling of a request. This places the crawler '
                'and its underlying storages into an unknown state and crawling will be terminated.',
                exc_info=internal_error,
            )
            raise

    async def __run_request_handler(self, crawling_context: BasicCrawlingContext) -> None:
        await self._context_pipeline(crawling_context, self.router)
