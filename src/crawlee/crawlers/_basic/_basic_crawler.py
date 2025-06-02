# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/basic-crawler/src/internals/basic-crawler.ts
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import tempfile
import threading
import traceback
from asyncio import CancelledError
from collections.abc import AsyncGenerator, Awaitable, Iterable, Sequence
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal, Union, cast
from urllib.parse import ParseResult, urlparse
from weakref import WeakKeyDictionary

from cachetools import LRUCache
from tldextract import TLDExtract
from typing_extensions import NotRequired, TypedDict, TypeVar, Unpack, assert_never
from yarl import URL

from crawlee import EnqueueStrategy, Glob, service_locator
from crawlee._autoscaling import AutoscaledPool, Snapshotter, SystemStatus
from crawlee._log_config import configure_logger, get_configured_log_level
from crawlee._request import Request, RequestState
from crawlee._types import (
    BasicCrawlingContext,
    GetKeyValueStoreFromRequestHandlerFunction,
    HttpHeaders,
    HttpPayload,
    RequestHandlerRunResult,
    SendRequestFunction,
    SkippedReason,
)
from crawlee._utils.docs import docs_group
from crawlee._utils.robots import RobotsTxtFile
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee._utils.wait import wait_for
from crawlee._utils.web import is_status_code_client_error, is_status_code_server_error
from crawlee.errors import (
    ContextPipelineInitializationError,
    ContextPipelineInterruptedError,
    HttpClientStatusCodeError,
    HttpStatusCodeError,
    RequestCollisionError,
    RequestHandlerError,
    SessionError,
    UserDefinedErrorHandlerError,
)
from crawlee.http_clients import HttpxHttpClient
from crawlee.router import Router
from crawlee.sessions import SessionPool
from crawlee.statistics import Statistics, StatisticsState
from crawlee.storages import Dataset, KeyValueStore, RequestQueue

from ._context_pipeline import ContextPipeline
from ._logging_utils import (
    get_one_line_error_summary_if_possible,
    reduce_asyncio_timeout_error_to_relevant_traceback_parts,
)

if TYPE_CHECKING:
    import re
    from contextlib import AbstractAsyncContextManager

    from crawlee._types import ConcurrencySettings, HttpMethod, JsonSerializable
    from crawlee.configuration import Configuration
    from crawlee.events import EventManager
    from crawlee.http_clients import HttpClient, HttpResponse
    from crawlee.proxy_configuration import ProxyConfiguration, ProxyInfo
    from crawlee.request_loaders import RequestManager
    from crawlee.sessions import Session
    from crawlee.statistics import FinalStatistics
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients.models import DatasetItemsListPage
    from crawlee.storages._dataset import ExportDataCsvKwargs, ExportDataJsonKwargs, GetDataKwargs, PushDataKwargs

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)
ErrorHandler = Callable[[TCrawlingContext, Exception], Awaitable[Union[Request, None]]]
FailedRequestHandler = Callable[[TCrawlingContext, Exception], Awaitable[None]]
SkippedRequestCallback = Callable[[str, SkippedReason], Awaitable[None]]


class _BasicCrawlerOptions(TypedDict):
    """Non-generic options the `BasicCrawler` constructor."""

    configuration: NotRequired[Configuration]
    """The `Configuration` instance. Some of its properties are used as defaults for the crawler."""

    event_manager: NotRequired[EventManager]
    """The event manager for managing events for the crawler and all its components."""

    storage_client: NotRequired[StorageClient]
    """The storage client for managing storages for the crawler and all its components."""

    request_manager: NotRequired[RequestManager]
    """Manager of requests that should be processed by the crawler."""

    session_pool: NotRequired[SessionPool]
    """A custom `SessionPool` instance, allowing the use of non-default configuration."""

    proxy_configuration: NotRequired[ProxyConfiguration]
    """HTTP proxy configuration used when making requests."""

    http_client: NotRequired[HttpClient]
    """HTTP client used by `BasicCrawlingContext.send_request` method."""

    max_request_retries: NotRequired[int]
    """Specifies the maximum number of retries allowed for a request if its processing fails.
    This includes retries due to navigation errors or errors thrown from user-supplied functions
    (`request_handler`, `pre_navigation_hooks` etc.).

    This limit does not apply to retries triggered by session rotation (see `max_session_rotations`)."""

    max_requests_per_crawl: NotRequired[int | None]
    """Maximum number of pages to open during a crawl. The crawl stops upon reaching this limit.
    Setting this value can help avoid infinite loops in misconfigured crawlers. `None` means no limit.
    Due to concurrency settings, the actual number of pages visited may slightly exceed this value."""

    max_session_rotations: NotRequired[int]
    """Maximum number of session rotations per request. The crawler rotates the session if a proxy error occurs
    or if the website blocks the request.

    The session rotations are not counted towards the `max_request_retries` limit.
    """

    max_crawl_depth: NotRequired[int | None]
    """Specifies the maximum crawl depth. If set, the crawler will stop processing links beyond this depth.
    The crawl depth starts at 0 for initial requests and increases with each subsequent level of links.
    Requests at the maximum depth will still be processed, but no new links will be enqueued from those requests.
    If not set, crawling continues without depth restrictions.
    """

    use_session_pool: NotRequired[bool]
    """Enable the use of a session pool for managing sessions during crawling."""

    retry_on_blocked: NotRequired[bool]
    """If True, the crawler attempts to bypass bot protections automatically."""

    concurrency_settings: NotRequired[ConcurrencySettings]
    """Settings to fine-tune concurrency levels."""

    request_handler_timeout: NotRequired[timedelta]
    """Maximum duration allowed for a single request handler to run."""

    abort_on_error: NotRequired[bool]
    """If True, the crawler stops immediately when any request handler error occurs."""

    configure_logging: NotRequired[bool]
    """If True, the crawler will set up logging infrastructure automatically."""

    statistics_log_format: NotRequired[Literal['table', 'inline']]
    """If 'table', displays crawler statistics as formatted tables in logs. If 'inline', outputs statistics as plain
    text log messages.
    """

    keep_alive: NotRequired[bool]
    """Flag that can keep crawler running even when there are no requests in queue."""

    additional_http_error_status_codes: NotRequired[Iterable[int]]
    """Additional HTTP status codes to treat as errors, triggering automatic retries when encountered."""

    ignore_http_error_status_codes: NotRequired[Iterable[int]]
    """HTTP status codes that are typically considered errors but should be treated as successful responses."""

    _additional_context_managers: NotRequired[Sequence[AbstractAsyncContextManager]]
    """Additional context managers used throughout the crawler lifecycle. Intended for use by
    subclasses rather than direct instantiation of `BasicCrawler`."""

    _logger: NotRequired[logging.Logger]
    """A logger instance, typically provided by a subclass, for consistent logging labels. Intended for use by
    subclasses rather than direct instantiation of `BasicCrawler`."""

    respect_robots_txt_file: NotRequired[bool]
    """If set to `True`, the crawler will automatically try to fetch the robots.txt file for each domain,
    and skip those that are not allowed. This also prevents disallowed URLs to be added via `EnqueueLinksFunction`."""


class _BasicCrawlerOptionsGeneric(Generic[TCrawlingContext, TStatisticsState], TypedDict):
    """Generic options the `BasicCrawler` constructor."""

    request_handler: NotRequired[Callable[[TCrawlingContext], Awaitable[None]]]
    """A callable responsible for handling requests."""

    _context_pipeline: NotRequired[ContextPipeline[TCrawlingContext]]
    """Enables extending the request lifecycle and modifying the crawling context. Intended for use by
    subclasses rather than direct instantiation of `BasicCrawler`."""

    statistics: NotRequired[Statistics[TStatisticsState]]
    """A custom `Statistics` instance, allowing the use of non-default configuration."""


@docs_group('Data structures')
class BasicCrawlerOptions(
    Generic[TCrawlingContext, TStatisticsState],
    _BasicCrawlerOptions,
    _BasicCrawlerOptionsGeneric[TCrawlingContext, TStatisticsState],
):
    """Arguments for the `BasicCrawler` constructor.

    It is intended for typing forwarded `__init__` arguments in the subclasses.
    """


@docs_group('Classes')
class BasicCrawler(Generic[TCrawlingContext, TStatisticsState]):
    """A basic web crawler providing a framework for crawling websites.

    The `BasicCrawler` provides a low-level functionality for crawling websites, allowing users to define their
    own page download and data extraction logic. It is designed mostly to be subclassed by crawlers with specific
    purposes. In most cases, you will want to use a more specialized crawler, such as `HttpCrawler`,
    `BeautifulSoupCrawler`, `ParselCrawler`, or `PlaywrightCrawler`. If you are an advanced user and want full
    control over the crawling process, you can subclass the `BasicCrawler` and implement the request-handling logic
    yourself.

    The crawling process begins with URLs provided by a `RequestProvider` instance. Each request is then
    handled by a user-defined `request_handler` function, which processes the page and extracts the data.

    The `BasicCrawler` includes several common features for crawling, such as:
        - automatic scaling based on the system resources,
        - retries for failed requests,
        - session management,
        - statistics tracking,
        - request routing via labels,
        - proxy rotation,
        - direct storage interaction helpers,
        - and more.
    """

    _CRAWLEE_STATE_KEY = 'CRAWLEE_STATE'
    _request_handler_timeout_text = 'Request handler timed out after'

    def __init__(
        self,
        *,
        configuration: Configuration | None = None,
        event_manager: EventManager | None = None,
        storage_client: StorageClient | None = None,
        request_manager: RequestManager | None = None,
        session_pool: SessionPool | None = None,
        proxy_configuration: ProxyConfiguration | None = None,
        http_client: HttpClient | None = None,
        request_handler: Callable[[TCrawlingContext], Awaitable[None]] | None = None,
        max_request_retries: int = 3,
        max_requests_per_crawl: int | None = None,
        max_session_rotations: int = 10,
        max_crawl_depth: int | None = None,
        use_session_pool: bool = True,
        retry_on_blocked: bool = True,
        additional_http_error_status_codes: Iterable[int] | None = None,
        ignore_http_error_status_codes: Iterable[int] | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        request_handler_timeout: timedelta = timedelta(minutes=1),
        statistics: Statistics[TStatisticsState] | None = None,
        abort_on_error: bool = False,
        keep_alive: bool = False,
        configure_logging: bool = True,
        statistics_log_format: Literal['table', 'inline'] = 'table',
        respect_robots_txt_file: bool = False,
        _context_pipeline: ContextPipeline[TCrawlingContext] | None = None,
        _additional_context_managers: Sequence[AbstractAsyncContextManager] | None = None,
        _logger: logging.Logger | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            configuration: The `Configuration` instance. Some of its properties are used as defaults for the crawler.
            event_manager: The event manager for managing events for the crawler and all its components.
            storage_client: The storage client for managing storages for the crawler and all its components.
            request_manager: Manager of requests that should be processed by the crawler.
            session_pool: A custom `SessionPool` instance, allowing the use of non-default configuration.
            proxy_configuration: HTTP proxy configuration used when making requests.
            http_client: HTTP client used by `BasicCrawlingContext.send_request` method.
            request_handler: A callable responsible for handling requests.
            max_request_retries: Specifies the maximum number of retries allowed for a request if its processing fails.
                This includes retries due to navigation errors or errors thrown from user-supplied functions
                (`request_handler`, `pre_navigation_hooks` etc.).

                This limit does not apply to retries triggered by session rotation (see `max_session_rotations`).
            max_requests_per_crawl: Maximum number of pages to open during a crawl. The crawl stops upon reaching
                this limit. Setting this value can help avoid infinite loops in misconfigured crawlers. `None` means
                no limit. Due to concurrency settings, the actual number of pages visited may slightly exceed
                this value. If used together with `keep_alive`, then the crawler will be kept alive only until
                `max_requests_per_crawl` is achieved.
            max_session_rotations: Maximum number of session rotations per request. The crawler rotates the session
                if a proxy error occurs or if the website blocks the request.

                The session rotations are not counted towards the `max_request_retries` limit.
            max_crawl_depth: Specifies the maximum crawl depth. If set, the crawler will stop processing links beyond
                this depth. The crawl depth starts at 0 for initial requests and increases with each subsequent level
                of links. Requests at the maximum depth will still be processed, but no new links will be enqueued
                from those requests. If not set, crawling continues without depth restrictions.
            use_session_pool: Enable the use of a session pool for managing sessions during crawling.
            retry_on_blocked: If True, the crawler attempts to bypass bot protections automatically.
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors,
                triggering automatic retries when encountered.
            ignore_http_error_status_codes: HTTP status codes that are typically considered errors but should be treated
                as successful responses.
            concurrency_settings: Settings to fine-tune concurrency levels.
            request_handler_timeout: Maximum duration allowed for a single request handler to run.
            statistics: A custom `Statistics` instance, allowing the use of non-default configuration.
            abort_on_error: If True, the crawler stops immediately when any request handler error occurs.
            keep_alive: If True, it will keep crawler alive even if there are no requests in queue.
                Use `crawler.stop()` to exit the crawler.
            configure_logging: If True, the crawler will set up logging infrastructure automatically.
            statistics_log_format: If 'table', displays crawler statistics as formatted tables in logs. If 'inline',
                outputs statistics as plain text log messages.
            respect_robots_txt_file: If set to `True`, the crawler will automatically try to fetch the robots.txt file
                for each domain, and skip those that are not allowed. This also prevents disallowed URLs to be added
                via `EnqueueLinksFunction`
            _context_pipeline: Enables extending the request lifecycle and modifying the crawling context.
                Intended for use by subclasses rather than direct instantiation of `BasicCrawler`.
            _additional_context_managers: Additional context managers used throughout the crawler lifecycle.
                Intended for use by subclasses rather than direct instantiation of `BasicCrawler`.
            _logger: A logger instance, typically provided by a subclass, for consistent logging labels.
                Intended for use by subclasses rather than direct instantiation of `BasicCrawler`.
        """
        if configuration:
            service_locator.set_configuration(configuration)
        if storage_client:
            service_locator.set_storage_client(storage_client)
        if event_manager:
            service_locator.set_event_manager(event_manager)

        config = service_locator.get_configuration()

        # Core components
        self._request_manager = request_manager
        self._session_pool = session_pool or SessionPool()
        self._proxy_configuration = proxy_configuration

        self._additional_http_error_status_codes = (
            set(additional_http_error_status_codes) if additional_http_error_status_codes else set()
        )
        self._ignore_http_error_status_codes = (
            set(ignore_http_error_status_codes) if ignore_http_error_status_codes else set()
        )

        self._http_client = http_client or HttpxHttpClient()

        # Request router setup
        self._router: Router[TCrawlingContext] | None = None
        if isinstance(cast('Router', request_handler), Router):
            self._router = cast('Router[TCrawlingContext]', request_handler)
        elif request_handler is not None:
            self._router = None
            self.router.default_handler(request_handler)

        # Error, failed & skipped request handlers
        self._error_handler: ErrorHandler[TCrawlingContext | BasicCrawlingContext] | None = None
        self._failed_request_handler: FailedRequestHandler[TCrawlingContext | BasicCrawlingContext] | None = None
        self._on_skipped_request: SkippedRequestCallback | None = None
        self._abort_on_error = abort_on_error

        # Context of each request with matching result of request handler.
        # Inheritors can use this to override the result of individual request handler runs in `_run_request_handler`.
        self._context_result_map = WeakKeyDictionary[BasicCrawlingContext, RequestHandlerRunResult]()

        # Context pipeline
        self._context_pipeline = (_context_pipeline or ContextPipeline()).compose(self._check_url_after_redirects)

        # Crawl settings
        self._max_request_retries = max_request_retries
        self._max_requests_per_crawl = max_requests_per_crawl
        self._max_session_rotations = max_session_rotations
        self._max_crawl_depth = max_crawl_depth
        self._respect_robots_txt_file = respect_robots_txt_file

        # Timeouts
        self._request_handler_timeout = request_handler_timeout
        self._internal_timeout = (
            config.internal_timeout
            if config.internal_timeout is not None
            else max(2 * request_handler_timeout, timedelta(minutes=5))
        )

        # Retry and session settings
        self._use_session_pool = use_session_pool
        self._retry_on_blocked = retry_on_blocked

        # Logging setup
        if configure_logging:
            root_logger = logging.getLogger()
            configure_logger(root_logger, remove_old_handlers=True)
            httpx_logger = logging.getLogger('httpx')  # Silence HTTPX logger
            httpx_logger.setLevel(logging.DEBUG if get_configured_log_level() <= logging.DEBUG else logging.WARNING)
        self._logger = _logger or logging.getLogger(__name__)
        self._statistics_log_format = statistics_log_format

        # Statistics
        self._statistics = statistics or cast(
            'Statistics[TStatisticsState]',
            Statistics.with_default_state(
                periodic_message_logger=self._logger,
                statistics_log_format=self._statistics_log_format,
                log_message='Current request statistics:',
            ),
        )

        # Additional context managers to enter and exit
        self._additional_context_managers = _additional_context_managers or []

        # Internal, not explicitly configurable components
        self._robots_txt_file_cache: LRUCache[str, RobotsTxtFile] = LRUCache(maxsize=1000)
        self._robots_txt_lock = asyncio.Lock()
        self._tld_extractor = TLDExtract(cache_dir=tempfile.TemporaryDirectory().name)
        self._snapshotter = Snapshotter.from_config(config)
        self._autoscaled_pool = AutoscaledPool(
            system_status=SystemStatus(self._snapshotter),
            concurrency_settings=concurrency_settings,
            is_finished_function=self.__is_finished_function,
            is_task_ready_function=self.__is_task_ready_function,
            run_task_function=self.__run_task_function,
        )

        # State flags
        self._keep_alive = keep_alive
        self._running = False
        self._has_finished_before = False

        self._failed = False

        self._unexpected_stop = False

    @property
    def log(self) -> logging.Logger:
        """The logger used by the crawler."""
        return self._logger

    @property
    def router(self) -> Router[TCrawlingContext]:
        """The `Router` used to handle each individual crawling request."""
        if self._router is None:
            self._router = Router[TCrawlingContext]()

        return self._router

    @router.setter
    def router(self, router: Router[TCrawlingContext]) -> None:
        if self._router is not None:
            raise RuntimeError('A router is already set')

        self._router = router

    @property
    def statistics(self) -> Statistics[TStatisticsState]:
        """Statistics about the current (or last) crawler run."""
        return self._statistics

    def stop(self, reason: str = 'Stop was called externally.') -> None:
        """Set flag to stop crawler.

        This stops current crawler run regardless of whether all requests were finished.

        Args:
            reason: Reason for stopping that will be used in logs.
        """
        self._logger.info(f'Crawler.stop() was called with following reason: {reason}.')
        self._unexpected_stop = True

    def _stop_if_max_requests_count_exceeded(self) -> None:
        """Call `stop` when the maximum number of requests to crawl has been reached."""
        if self._max_requests_per_crawl is None:
            return

        if self._statistics.state.requests_finished >= self._max_requests_per_crawl:
            self.stop(
                reason=f'The crawler has reached its limit of {self._max_requests_per_crawl} requests per crawl. '
            )

    async def _get_session(self) -> Session | None:
        """If session pool is being used, try to take a session from it."""
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

    async def _get_session_by_id(self, session_id: str | None) -> Session | None:
        """If session pool is being used, try to take a session by id from it."""
        if not self._use_session_pool or not session_id:
            return None

        return await wait_for(
            partial(self._session_pool.get_session_by_id, session_id),
            timeout=self._internal_timeout,
            timeout_message='Fetching a session from the pool timed out after '
            f'{self._internal_timeout.total_seconds()} seconds',
            max_retries=3,
            logger=self._logger,
        )

    async def _get_proxy_info(self, request: Request, session: Session | None) -> ProxyInfo | None:
        """Retrieve a new ProxyInfo object based on crawler configuration and the current request and session."""
        if not self._proxy_configuration:
            return None

        return await self._proxy_configuration.new_proxy_info(
            session_id=session.id if session else None,
            request=request,
            proxy_tier=None,
        )

    async def get_request_manager(self) -> RequestManager:
        """Return the configured request manager. If none is configured, open and return the default request queue."""
        if not self._request_manager:
            self._request_manager = await RequestQueue.open()

        return self._request_manager

    async def get_dataset(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Dataset:
        """Return the `Dataset` with the given ID or name. If none is provided, return the default one."""
        return await Dataset.open(id=id, name=name)

    async def get_key_value_store(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> KeyValueStore:
        """Return the `KeyValueStore` with the given ID or name. If none is provided, return the default KVS."""
        return await KeyValueStore.open(id=id, name=name)

    def error_handler(
        self, handler: ErrorHandler[TCrawlingContext | BasicCrawlingContext]
    ) -> ErrorHandler[TCrawlingContext]:
        """Register a function to handle errors occurring in request handlers.

        The error handler is invoked after a request handler error occurs and before a retry attempt.
        """
        self._error_handler = handler
        return handler

    def failed_request_handler(
        self, handler: FailedRequestHandler[TCrawlingContext | BasicCrawlingContext]
    ) -> FailedRequestHandler[TCrawlingContext]:
        """Register a function to handle requests that exceed the maximum retry limit.

        The failed request handler is invoked when a request has failed all retry attempts.
        """
        self._failed_request_handler = handler
        return handler

    def on_skipped_request(self, callback: SkippedRequestCallback) -> SkippedRequestCallback:
        """Register a function to handle skipped requests.

        The skipped request handler is invoked when a request is skipped due to a collision or other reasons.
        """
        self._on_skipped_request = callback
        return callback

    async def run(
        self,
        requests: Sequence[str | Request] | None = None,
        *,
        purge_request_queue: bool = True,
    ) -> FinalStatistics:
        """Run the crawler until all requests are processed.

        Args:
            requests: The requests to be enqueued before the crawler starts.
            purge_request_queue: If this is `True` and the crawler is not being run for the first time, the default
                request queue will be purged.
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

            request_manager = await self.get_request_manager()
            if purge_request_queue and isinstance(request_manager, RequestQueue):
                await request_manager.drop()
                self._request_manager = await RequestQueue.open()

        if requests is not None:
            await self.add_requests(requests)

        interrupted = False

        def sigint_handler() -> None:
            nonlocal interrupted

            if not interrupted:
                interrupted = True
                self._logger.info('Pausing... Press CTRL+C again to force exit.')

            run_task.cancel()

        run_task = asyncio.create_task(self._run_crawler(), name='run_crawler_task')

        if threading.current_thread() is threading.main_thread():  # `add_signal_handler` works only in the main thread
            with suppress(NotImplementedError):  # event loop signal handlers are not supported on Windows
                asyncio.get_running_loop().add_signal_handler(signal.SIGINT, sigint_handler)

        try:
            await run_task
        except CancelledError:
            pass
        finally:
            if threading.current_thread() is threading.main_thread():
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

        await self._save_crawler_state()

        final_statistics = self._statistics.calculate()
        if self._statistics_log_format == 'table':
            self._logger.info(f'Final request statistics:\n{final_statistics.to_table()}')
        else:
            self._logger.info('Final request statistics:', extra=final_statistics.to_dict())
        return final_statistics

    async def _run_crawler(self) -> None:
        event_manager = service_locator.get_event_manager()

        # Collect the context managers to be entered. Context managers that are already active are excluded,
        # as they were likely entered by the caller, who will also be responsible for exiting them.
        contexts_to_enter = [
            cm
            for cm in (
                event_manager,
                self._snapshotter,
                self._statistics,
                self._session_pool if self._use_session_pool else None,
                *self._additional_context_managers,
            )
            if cm and getattr(cm, 'active', False) is False
        ]

        async with AsyncExitStack() as exit_stack:
            for context in contexts_to_enter:
                await exit_stack.enter_async_context(context)  # type: ignore[arg-type]

            await self._autoscaled_pool.run()

    async def add_requests(
        self,
        requests: Sequence[str | Request],
        *,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(0),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        """Add requests to the underlying request manager in batches.

        Args:
            requests: A list of requests to add to the queue.
            batch_size: The number of requests to add in one batch.
            wait_time_between_batches: Time to wait between adding batches.
            wait_for_all_requests_to_be_added: If True, wait for all requests to be added before returning.
            wait_for_all_requests_to_be_added_timeout: Timeout for waiting for all requests to be added.
        """
        allowed_requests = []
        skipped = []

        for request in requests:
            check_url = request.url if isinstance(request, Request) else request
            if await self._is_allowed_based_on_robots_txt_file(check_url):
                allowed_requests.append(request)
            else:
                skipped.append(request)

        if skipped:
            skipped_tasks = [
                asyncio.create_task(self._handle_skipped_request(request, 'robots_txt')) for request in skipped
            ]
            await asyncio.gather(*skipped_tasks)
            self._logger.warning('Some requests were skipped because they were disallowed based on the robots.txt file')

        request_manager = await self.get_request_manager()

        await request_manager.add_requests_batched(
            requests=allowed_requests,
            batch_size=batch_size,
            wait_time_between_batches=wait_time_between_batches,
            wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
            wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
        )

    async def _use_state(self, default_value: dict[str, JsonSerializable] | None = None) -> dict[str, JsonSerializable]:
        store = await self.get_key_value_store()
        return await store.get_auto_saved_value(self._CRAWLEE_STATE_KEY, default_value)

    async def _save_crawler_state(self) -> None:
        store = await self.get_key_value_store()
        await store.persist_autosaved_values()

    async def get_data(
        self,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[GetDataKwargs],
    ) -> DatasetItemsListPage:
        """Retrieve data from a `Dataset`.

        This helper method simplifies the process of retrieving data from a `Dataset`. It opens the specified
        one and then retrieves the data based on the provided parameters.

        Args:
            dataset_id: The ID of the `Dataset`.
            dataset_name: The name of the `Dataset`.
            kwargs: Keyword arguments to be passed to the `Dataset.get_data()` method.

        Returns:
            The retrieved data.
        """
        dataset = await Dataset.open(id=dataset_id, name=dataset_name)
        return await dataset.get_data(**kwargs)

    async def export_data(
        self,
        path: str | Path,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
    ) -> None:
        """Export data from a `Dataset`.

        This helper method simplifies the process of exporting data from a `Dataset`. It opens the specified
        one and then exports the data based on the provided parameters. If you need to pass options
        specific to the output format, use the `export_data_csv` or `export_data_json` method instead.

        Args:
            path: The destination path.
            dataset_id: The ID of the `Dataset`.
            dataset_name: The name of the `Dataset`.
        """
        dataset = await self.get_dataset(id=dataset_id, name=dataset_name)

        path = path if isinstance(path, Path) else Path(path)
        destination = path.open('w', newline='')

        if path.suffix == '.csv':
            await dataset.write_to_csv(destination)
        elif path.suffix == '.json':
            await dataset.write_to_json(destination)
        else:
            raise ValueError(f'Unsupported file extension: {path.suffix}')

    async def export_data_csv(
        self,
        path: str | Path,
        *,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[ExportDataCsvKwargs],
    ) -> None:
        """Export data from a `Dataset` to a CSV file.

        This helper method simplifies the process of exporting data from a `Dataset` in csv format. It opens
        the specified one and then exports the data based on the provided parameters.

        Args:
            path: The destination path.
            content_type: The output format.
            dataset_id: The ID of the `Dataset`.
            dataset_name: The name of the `Dataset`.
            kwargs: Extra configurations for dumping/writing in csv format.
        """
        dataset = await self.get_dataset(id=dataset_id, name=dataset_name)
        path = path if isinstance(path, Path) else Path(path)

        return await dataset.write_to_csv(path.open('w', newline=''), **kwargs)

    async def export_data_json(
        self,
        path: str | Path,
        *,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[ExportDataJsonKwargs],
    ) -> None:
        """Export data from a `Dataset` to a JSON file.

        This helper method simplifies the process of exporting data from a `Dataset` in json format. It opens the
        specified one and then exports the data based on the provided parameters.

        Args:
            path: The destination path
            dataset_id: The ID of the `Dataset`.
            dataset_name: The name of the `Dataset`.
            kwargs: Extra configurations for dumping/writing in json format.
        """
        dataset = await self.get_dataset(id=dataset_id, name=dataset_name)
        path = path if isinstance(path, Path) else Path(path)

        return await dataset.write_to_json(path.open('w', newline=''), **kwargs)

    async def _push_data(
        self,
        data: JsonSerializable,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[PushDataKwargs],
    ) -> None:
        """Push data to a `Dataset`.

        This helper method simplifies the process of pushing data to a `Dataset`. It opens the specified
        one and then pushes the provided data to it.

        Args:
            data: The data to push to the `Dataset`.
            dataset_id: The ID of the `Dataset`.
            dataset_name: The name of the `Dataset`.
            kwargs: Keyword arguments to be passed to the `Dataset.push_data()` method.
        """
        dataset = await self.get_dataset(id=dataset_id, name=dataset_name)
        await dataset.push_data(data, **kwargs)

    def _should_retry_request(self, context: BasicCrawlingContext, error: Exception) -> bool:
        if context.request.no_retry:
            return False

        # Do not retry on client errors.
        if isinstance(error, HttpClientStatusCodeError):
            return False

        if isinstance(error, SessionError):
            return ((context.request.session_rotation_count or 0) + 1) < self._max_session_rotations

        max_request_retries = context.request.max_retries
        if max_request_retries is None:
            max_request_retries = self._max_request_retries

        return (context.request.retry_count + 1) < max_request_retries

    async def _check_url_after_redirects(self, context: TCrawlingContext) -> AsyncGenerator[TCrawlingContext, None]:
        """Ensure that the `loaded_url` still matches the enqueue strategy after redirects.

        Filter out links that redirect outside of the crawled domain.
        """
        if context.request.loaded_url is not None and not self._check_enqueue_strategy(
            context.request.enqueue_strategy,
            origin_url=urlparse(context.request.url),
            target_url=urlparse(context.request.loaded_url),
        ):
            raise ContextPipelineInterruptedError(
                f'Skipping URL {context.request.loaded_url} (redirected from {context.request.url})'
            )

        yield context

    def _check_enqueue_strategy(
        self,
        strategy: EnqueueStrategy,
        *,
        target_url: ParseResult,
        origin_url: ParseResult,
    ) -> bool:
        """Check if a URL matches the enqueue_strategy."""
        if strategy == 'same-hostname':
            return target_url.hostname == origin_url.hostname

        if strategy == 'same-domain':
            if origin_url.hostname is None or target_url.hostname is None:
                raise ValueError('Both origin and target URLs must have a hostname')

            origin_domain = self._tld_extractor.extract_str(origin_url.hostname).domain
            target_domain = self._tld_extractor.extract_str(target_url.hostname).domain
            return origin_domain == target_domain

        if strategy == 'same-origin':
            return (
                target_url.hostname == origin_url.hostname
                and target_url.scheme == origin_url.scheme
                and target_url.port == origin_url.port
            )

        if strategy == 'all':
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
        context: TCrawlingContext | BasicCrawlingContext,
        error: Exception,
    ) -> None:
        request_manager = await self.get_request_manager()
        request = context.request

        if self._abort_on_error:
            self._logger.exception('Aborting crawler run due to error (abort_on_error=True)', exc_info=error)
            self._failed = True

        if self._should_retry_request(context, error):
            request.retry_count += 1
            self.log.warning(
                f'Retrying request to {context.request.url} due to: {error} \n'
                f'{get_one_line_error_summary_if_possible(error)}'
            )
            await self._statistics.error_tracker.add(error=error, context=context)

            if self._error_handler:
                try:
                    new_request = await self._error_handler(context, error)
                except Exception as e:
                    raise UserDefinedErrorHandlerError('Exception thrown in user-defined request error handler') from e
                else:
                    if new_request is not None:
                        request = new_request

            await request_manager.reclaim_request(request)
        else:
            await wait_for(
                lambda: request_manager.mark_request_as_handled(context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
                max_retries=3,
            )
            await self._handle_failed_request(context, error)
            self._statistics.record_request_processing_failure(request.id or request.unique_key)

    async def _handle_request_error(self, context: TCrawlingContext | BasicCrawlingContext, error: Exception) -> None:
        try:
            context.request.state = RequestState.ERROR_HANDLER

            await wait_for(
                partial(self._handle_request_retries, context, error),
                timeout=self._internal_timeout,
                timeout_message='Handling request failure timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
            )

            context.request.state = RequestState.DONE
        except UserDefinedErrorHandlerError:
            context.request.state = RequestState.ERROR
            raise
        except Exception as secondary_error:
            self._logger.exception(
                'An exception occurred during handling of failed request. This places the crawler '
                'and its underlying storages into an unknown state and crawling will be terminated.',
                exc_info=secondary_error,
            )
            context.request.state = RequestState.ERROR
            raise

        if context.session:
            context.session.mark_bad()

    async def _handle_failed_request(self, context: TCrawlingContext | BasicCrawlingContext, error: Exception) -> None:
        self._logger.error(
            f'Request to {context.request.url} failed and reached maximum retries\n '
            f'{self._get_message_from_error(error)}'
        )
        await self._statistics.error_tracker.add(error=error, context=context)

        if self._failed_request_handler:
            try:
                await self._failed_request_handler(context, error)
            except Exception as e:
                raise UserDefinedErrorHandlerError('Exception thrown in user-defined failed request handler') from e

    async def _handle_skipped_request(
        self, request: Request | str, reason: SkippedReason, *, need_mark: bool = False
    ) -> None:
        if need_mark and isinstance(request, Request):
            request_manager = await self.get_request_manager()

            await wait_for(
                lambda: request_manager.mark_request_as_handled(request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
                max_retries=3,
            )
            request.state = RequestState.SKIPPED

        url = request.url if isinstance(request, Request) else request

        if self._on_skipped_request:
            try:
                await self._on_skipped_request(url, reason)
            except Exception as e:
                raise UserDefinedErrorHandlerError('Exception thrown in user-defined skipped request callback') from e

    def _get_message_from_error(self, error: Exception) -> str:
        """Get error message summary from exception.

        Custom processing to reduce the irrelevant traceback clutter in some cases.
        """
        traceback_parts = traceback.format_exception(type(error), value=error, tb=error.__traceback__, chain=True)
        used_traceback_parts = traceback_parts

        if (
            isinstance(error, asyncio.exceptions.TimeoutError)
            and self._request_handler_timeout_text in traceback_parts[-1]
        ):
            used_traceback_parts = reduce_asyncio_timeout_error_to_relevant_traceback_parts(error)
            used_traceback_parts.append(traceback_parts[-1])

        return ''.join(used_traceback_parts).strip('\n')

    def _get_only_inner_most_exception(self, error: BaseException) -> BaseException:
        """Get innermost exception by following __cause__ and __context__ attributes of exception."""
        if error.__cause__:
            return self._get_only_inner_most_exception(error.__cause__)
        if error.__context__:
            return self._get_only_inner_most_exception(error.__context__)
        # No __cause__ and no __context__, this is as deep as it can get.
        return error

    def _prepare_send_request_function(
        self,
        session: Session | None,
        proxy_info: ProxyInfo | None,
    ) -> SendRequestFunction:
        async def send_request(
            url: str,
            *,
            method: HttpMethod = 'GET',
            payload: HttpPayload | None = None,
            headers: HttpHeaders | dict[str, str] | None = None,
        ) -> HttpResponse:
            return await self._http_client.send_request(
                url=url,
                method=method,
                payload=payload,
                headers=headers,
                session=session,
                proxy_info=proxy_info,
            )

        return send_request

    async def _commit_request_handler_result(self, context: BasicCrawlingContext) -> None:
        """Commit request handler result for the input `context`. Result is taken from `_context_result_map`."""
        result = self._context_result_map[context]

        request_manager = await self.get_request_manager()
        origin = context.request.loaded_url or context.request.url

        for add_requests_call in result.add_requests_calls:
            requests = list[Request]()

            for request in add_requests_call['requests']:
                if (limit := add_requests_call.get('limit')) is not None and len(requests) >= limit:
                    break

                # If the request is a Request object, keep it as it is
                if isinstance(request, Request):
                    dst_request = request
                # If the request is a string, convert it to Request object.
                if isinstance(request, str):
                    if is_url_absolute(request):
                        dst_request = Request.from_url(request)

                    # If the request URL is relative, make it absolute using the origin URL.
                    else:
                        base_url = url if (url := add_requests_call.get('base_url')) else origin
                        absolute_url = convert_to_absolute_url(base_url, request)
                        dst_request = Request.from_url(absolute_url)

                # Update the crawl depth of the request.
                dst_request.crawl_depth = context.request.crawl_depth + 1

                if (
                    (self._max_crawl_depth is None or dst_request.crawl_depth <= self._max_crawl_depth)
                    and self._check_enqueue_strategy(
                        add_requests_call.get('strategy', 'all'),
                        target_url=urlparse(dst_request.url),
                        origin_url=urlparse(context.request.url),
                    )
                    and self._check_url_patterns(
                        dst_request.url,
                        add_requests_call.get('include', None),
                        add_requests_call.get('exclude', None),
                    )
                ):
                    requests.append(dst_request)

            await request_manager.add_requests_batched(requests)

        for push_data_call in result.push_data_calls:
            await self._push_data(**push_data_call)

        await self._commit_key_value_store_changes(result, get_kvs=self.get_key_value_store)

    @staticmethod
    async def _commit_key_value_store_changes(
        result: RequestHandlerRunResult, get_kvs: GetKeyValueStoreFromRequestHandlerFunction
    ) -> None:
        """Store key value store changes recorded in result."""
        for (id, name), changes in result.key_value_store_changes.items():
            store = await get_kvs(id=id, name=name)
            for key, value in changes.updates.items():
                await store.set_value(key, value.content, value.content_type)

    async def __is_finished_function(self) -> bool:
        self._stop_if_max_requests_count_exceeded()
        if self._unexpected_stop:
            self._logger.info('The crawler will finish any remaining ongoing requests and shut down.')
            return True

        if self._abort_on_error and self._failed:
            self._failed = False
            return True

        if self._keep_alive:
            return False

        request_manager = await self.get_request_manager()
        return await request_manager.is_finished()

    async def __is_task_ready_function(self) -> bool:
        self._stop_if_max_requests_count_exceeded()
        if self._unexpected_stop:
            self._logger.info(
                'No new requests are allowed because crawler `stop` method was called. '
                'Ongoing requests will be allowed to complete.'
            )
            return False

        request_manager = await self.get_request_manager()
        return not await request_manager.is_empty()

    async def __run_task_function(self) -> None:
        request_manager = await self.get_request_manager()

        request = await wait_for(
            lambda: request_manager.fetch_next_request(),
            timeout=self._internal_timeout,
            timeout_message=f'Fetching next request failed after {self._internal_timeout.total_seconds()} seconds',
            logger=self._logger,
            max_retries=3,
        )

        if request is None:
            return

        if not (await self._is_allowed_based_on_robots_txt_file(request.url)):
            self._logger.warning(
                f'Skipping request {request.url} ({request.id}) because it is disallowed based on robots.txt'
            )

            await self._handle_skipped_request(request, 'robots_txt', need_mark=True)
            return

        if request.session_id:
            session = await self._get_session_by_id(request.session_id)
        else:
            session = await self._get_session()
        proxy_info = await self._get_proxy_info(request, session)
        result = RequestHandlerRunResult(key_value_store_getter=self.get_key_value_store)

        context = BasicCrawlingContext(
            request=request,
            session=session,
            proxy_info=proxy_info,
            send_request=self._prepare_send_request_function(session, proxy_info),
            add_requests=result.add_requests,
            push_data=result.push_data,
            get_key_value_store=result.get_key_value_store,
            use_state=self._use_state,
            log=self._logger,
        )
        self._context_result_map[context] = result

        statistics_id = request.id or request.unique_key
        self._statistics.record_request_processing_start(statistics_id)

        try:
            request.state = RequestState.REQUEST_HANDLER

            self._check_request_collision(context.request, context.session)

            try:
                await self._run_request_handler(context=context)
            except asyncio.TimeoutError as e:
                raise RequestHandlerError(e, context) from e

            await self._commit_request_handler_result(context)
            await wait_for(
                lambda: request_manager.mark_request_as_handled(context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=self._logger,
                max_retries=3,
            )

            request.state = RequestState.DONE

            if context.session and context.session.is_usable:
                context.session.mark_good()

            self._statistics.record_request_processing_finish(statistics_id)

        except RequestCollisionError as request_error:
            context.request.no_retry = True
            await self._handle_request_error(context, request_error)

        except RequestHandlerError as primary_error:
            primary_error = cast(
                'RequestHandlerError[TCrawlingContext]', primary_error
            )  # valid thanks to ContextPipeline

            self._logger.debug(
                'An exception occurred in the user-defined request handler',
                exc_info=primary_error.wrapped_exception,
            )
            await self._handle_request_error(primary_error.crawling_context, primary_error.wrapped_exception)

        except SessionError as session_error:
            if not context.session:
                raise RuntimeError('SessionError raised in a crawling context without a session') from session_error

            if self._error_handler:
                await self._error_handler(context, session_error)

            if self._should_retry_request(context, session_error):
                self._logger.warning('Encountered a session error, rotating session and retrying')

                context.session.retire()

                # Increment session rotation count.
                context.request.session_rotation_count = (context.request.session_rotation_count or 0) + 1

                await request_manager.reclaim_request(request)
                await self._statistics.error_tracker_retry.add(error=session_error, context=context)
            else:
                await wait_for(
                    lambda: request_manager.mark_request_as_handled(context.request),
                    timeout=self._internal_timeout,
                    timeout_message='Marking request as handled timed out after '
                    f'{self._internal_timeout.total_seconds()} seconds',
                    logger=self._logger,
                    max_retries=3,
                )

                await self._handle_failed_request(context, session_error)
                self._statistics.record_request_processing_failure(statistics_id)

        except ContextPipelineInterruptedError as interrupted_error:
            self._logger.debug('The context pipeline was interrupted', exc_info=interrupted_error)

            await wait_for(
                lambda: request_manager.mark_request_as_handled(context.request),
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
            await self._handle_request_error(context, initialization_error.wrapped_exception)

        except Exception as internal_error:
            self._logger.exception(
                'An exception occurred during handling of a request. This places the crawler '
                'and its underlying storages into an unknown state and crawling will be terminated.',
                exc_info=internal_error,
            )
            raise

    async def _run_request_handler(self, context: BasicCrawlingContext) -> None:
        await wait_for(
            lambda: self._context_pipeline(context, self.router),
            timeout=self._request_handler_timeout,
            timeout_message=f'{self._request_handler_timeout_text}'
            f' {self._request_handler_timeout.total_seconds()} seconds',
            logger=self._logger,
        )

    def _raise_for_error_status_code(self, status_code: int) -> None:
        """Raise an exception if the given status code is considered an error.

        Args:
            status_code: The HTTP status code to check.

        Raises:
            HttpStatusCodeError: If the status code represents a server error or is explicitly configured as an error.
            HttpClientStatusCodeError: If the status code represents a client error.
        """
        is_ignored_status = status_code in self._ignore_http_error_status_codes
        is_explicit_error = status_code in self._additional_http_error_status_codes

        if is_explicit_error:
            raise HttpStatusCodeError('Error status code (user-configured) returned.', status_code)

        if is_status_code_client_error(status_code) and not is_ignored_status:
            raise HttpClientStatusCodeError('Client error status code returned', status_code)

        if is_status_code_server_error(status_code) and not is_ignored_status:
            raise HttpStatusCodeError('Error status code returned', status_code)

    def _raise_for_session_blocked_status_code(self, session: Session | None, status_code: int) -> None:
        """Raise an exception if the given status code indicates the session is blocked.

        Args:
            session: The session used for the request. If None, no check is performed.
            status_code: The HTTP status code to check.

        Raises:
            SessionError: If the status code indicates the session is blocked.
        """
        if session is not None and session.is_blocked_status_code(
            status_code=status_code,
            ignore_http_error_status_codes=self._ignore_http_error_status_codes,
        ):
            raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')

    def _check_request_collision(self, request: Request, session: Session | None) -> None:
        """Raise an exception if a request cannot access required resources.

        Args:
            request: The `Request` that might require specific resources (like a session).
            session: The `Session` that was retrieved for the request, or `None` if not available.

        Raises:
            RequestCollisionError: If the `Session` referenced by the `Request` is not available.
        """
        if self._use_session_pool and request.session_id and not session:
            raise RequestCollisionError(
                f'The Session (id: {request.session_id}) bound to the Request is no longer available in SessionPool'
            )

    async def _is_allowed_based_on_robots_txt_file(self, url: str) -> bool:
        """Check if the URL is allowed based on the robots.txt file.

        Args:
            url: The URL to verify against robots.txt rules. Returns True if crawling this URL is permitted.
        """
        if not self._respect_robots_txt_file:
            return True
        robots_txt_file = await self._get_robots_txt_file_for_url(url)
        return not robots_txt_file or robots_txt_file.is_allowed(url)

    async def _get_robots_txt_file_for_url(self, url: str) -> RobotsTxtFile | None:
        """Get the RobotsTxtFile for a given URL.

        Args:
            url: The URL whose domain will be used to locate and fetch the corresponding robots.txt file.
        """
        if not self._respect_robots_txt_file:
            return None
        origin_url = str(URL(url).origin())
        robots_txt_file = self._robots_txt_file_cache.get(origin_url)
        if robots_txt_file:
            return robots_txt_file

        async with self._robots_txt_lock:
            # Check again if the robots.txt file is already cached after acquiring the lock
            robots_txt_file = self._robots_txt_file_cache.get(origin_url)
            if robots_txt_file:
                return robots_txt_file

            # If not cached, fetch the robots.txt file
            robots_txt_file = await self._find_txt_file_for_url(url)
            self._robots_txt_file_cache[origin_url] = robots_txt_file
            return robots_txt_file

    async def _find_txt_file_for_url(self, url: str) -> RobotsTxtFile:
        """Find the robots.txt file for a given URL.

        Args:
            url: The URL whose domain will be used to locate and fetch the corresponding robots.txt file.
        """
        return await RobotsTxtFile.find(url, self._http_client)
