# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/basic-crawler/src/internals/basic-crawler.ts
from __future__ import annotations

import tempfile
from contextlib import AsyncExitStack
from datetime import timedelta
from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable, Generic, Sequence, Union, cast

import httpx
from tldextract import TLDExtract
from typing_extensions import NotRequired, TypedDict, TypeVar, assert_never

from crawlee import Glob
from crawlee._utils.wait import wait_for
from crawlee.autoscaling import AutoscaledPool, ConcurrencySettings
from crawlee.autoscaling.snapshotter import Snapshotter
from crawlee.autoscaling.system_status import SystemStatus
from crawlee.basic_crawler.context_pipeline import (
    ContextPipeline,
)
from crawlee.basic_crawler.errors import (
    ContextPipelineInitializationError,
    ContextPipelineInterruptedError,
    RequestHandlerError,
    SessionError,
    UserDefinedErrorHandlerError,
)
from crawlee.basic_crawler.router import Router
from crawlee.basic_crawler.types import (
    BasicCrawlingContext,
    FinalStatistics,
    RequestHandlerRunResult,
    SendRequestFunction,
)
from crawlee.configuration import Configuration
from crawlee.enqueue_strategy import EnqueueStrategy
from crawlee.events.local_event_manager import LocalEventManager
from crawlee.http_clients.httpx_client import HttpxClient
from crawlee.models import BaseRequestData, Request, RequestState
from crawlee.sessions import SessionPool
from crawlee.storages.request_queue import RequestQueue

if TYPE_CHECKING:
    import re

    from crawlee.http_clients.base_http_client import BaseHttpClient, HttpResponse
    from crawlee.sessions.session import Session
    from crawlee.storages.request_provider import RequestProvider

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
ErrorHandler = Callable[[TCrawlingContext, Exception], Awaitable[Union[Request, None]]]
FailedRequestHandler = Callable[[TCrawlingContext, Exception], Awaitable[None]]

logger = getLogger(__name__)


class BasicCrawlerOptions(TypedDict, Generic[TCrawlingContext]):
    """Copy of the parameter types of `BasicCrawler.__init__` meant for typing forwarded __init__ args in subclasses."""

    request_provider: NotRequired[RequestProvider]
    request_handler: NotRequired[Callable[[TCrawlingContext], Awaitable[None]]]
    http_client: NotRequired[BaseHttpClient]
    concurrency_settings: NotRequired[ConcurrencySettings]
    max_request_retries: NotRequired[int]
    max_session_rotations: NotRequired[int]
    configuration: NotRequired[Configuration]
    request_handler_timeout: NotRequired[timedelta]
    session_pool: NotRequired[SessionPool]
    use_session_pool: NotRequired[bool]
    retry_on_blocked: NotRequired[bool]
    _context_pipeline: NotRequired[ContextPipeline[TCrawlingContext]]


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
        request_provider: RequestProvider | None = None,
        request_handler: Callable[[TCrawlingContext], Awaitable[None]] | None = None,
        http_client: BaseHttpClient | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        max_request_retries: int = 3,
        max_session_rotations: int = 10,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta = timedelta(minutes=1),
        session_pool: SessionPool | None = None,
        use_session_pool: bool = True,
        retry_on_blocked: bool = True,
        _context_pipeline: ContextPipeline[TCrawlingContext] | None = None,
    ) -> None:
        """Initialize the BasicCrawler.

        Args:
            request_provider: Provides requests to be processed
            request_handler: A callable to which request handling is delegated
            http_client: HTTP client to be used for `BasicCrawlingContext.send_request` and HTTP-only crawling.
            concurrency_settings: Allows fine-tuning concurrency levels
            max_request_retries: Maximum amount of attempts at processing a request
            max_session_rotations: Maximum number of session rotations per request.
                The crawler will automatically rotate the session in case of a proxy error or if it gets blocked by
                the website.
            configuration: Crawler configuration
            request_handler_timeout: How long is a single request handler allowed to run
            use_session_pool: Enables using the session pool for crawling
            session_pool: A preconfigured SessionPool instance if you wish to use non-default configuration
            retry_on_blocked: If set to True, the crawler will try to automatically bypass any detected bot protection
            _context_pipeline: Allows extending the request lifecycle and modifying the crawling context.
                This parameter is meant to be used by child classes, not when BasicCrawler is instantiated directly.
        """
        self._router: Router[TCrawlingContext] | None = None

        if isinstance(cast(Router, request_handler), Router):
            self._router = cast(Router[TCrawlingContext], request_handler)
        elif request_handler is not None:
            self._router = None
            self.router.default_handler(request_handler)

        self._http_client = http_client or HttpxClient()

        self._context_pipeline = (_context_pipeline or ContextPipeline()).compose(self._check_url_after_redirects)

        self._error_handler: ErrorHandler[TCrawlingContext] | None = None
        self._failed_request_handler: FailedRequestHandler[TCrawlingContext] | None = None

        self._max_request_retries = max_request_retries
        self._max_session_rotations = max_session_rotations

        self._request_provider = request_provider
        self._configuration = configuration or Configuration()

        self._request_handler_timeout = request_handler_timeout
        self._internal_timeout = (
            self._configuration.internal_timeout
            if self._configuration.internal_timeout is not None
            else max(2 * request_handler_timeout, timedelta(minutes=5))
        )

        self._tld_extractor = TLDExtract(cache_dir=tempfile.TemporaryDirectory().name)

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

        self._use_session_pool = use_session_pool
        self._session_pool: SessionPool = session_pool or SessionPool()

        self._retry_on_blocked = retry_on_blocked

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
            logger=logger,
        )

    async def get_request_provider(self) -> RequestProvider:
        """Return the configured request provider. If none is configured, open and return the default request queue."""
        if not self._request_provider:
            self._request_provider = await RequestQueue.open()

        return self._request_provider

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
        request_provider = await self.get_request_provider()
        await request_provider.add_requests_batched(
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

        async with AsyncExitStack() as exit_stack:
            await exit_stack.enter_async_context(self._event_manager)
            await exit_stack.enter_async_context(self._snapshotter)

            if self._use_session_pool:
                await exit_stack.enter_async_context(self._session_pool)

            await self._pool.run()

        return FinalStatistics()

    def _should_retry_request(self, crawling_context: BasicCrawlingContext, error: Exception) -> bool:
        if crawling_context.request.no_retry:
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
            origin_url=httpx.URL(crawling_context.request.url),
            target_url=httpx.URL(crawling_context.request.loaded_url),
        ):
            raise ContextPipelineInterruptedError(
                f'Skipping URL {crawling_context.request.loaded_url} (redirected from {crawling_context.request.url})'
            )

        yield crawling_context

    def _check_enqueue_strategy(
        self, strategy: EnqueueStrategy, *, target_url: httpx.URL, origin_url: httpx.URL
    ) -> bool:
        """Check if a URL matches the enqueue_strategy."""
        if strategy == EnqueueStrategy.SAME_HOSTNAME:
            return target_url.host == origin_url.host

        if strategy == EnqueueStrategy.SAME_DOMAIN:
            origin_domain = self._tld_extractor.extract_str(origin_url.host).domain
            target_domain = self._tld_extractor.extract_str(target_url.host).domain
            return origin_domain == target_domain

        if strategy == EnqueueStrategy.SAME_ORIGIN:
            return target_url.host == origin_url.host and target_url.scheme == origin_url.scheme

        if strategy == EnqueueStrategy.ALL:
            return True

        assert_never()

    def _check_url_patterns(
        self,
        target_url: httpx.URL,
        include: Sequence[re.Pattern | Glob] | None,
        exclude: Sequence[re.Pattern | Glob] | None,
    ) -> bool:
        """Check if a URL matches configured include/exclude patterns."""
        # If the URL matches any `exclude` pattern, reject it
        for pattern in exclude or ():
            if isinstance(pattern, Glob):
                pattern = pattern.regexp  # noqa: PLW2901

            if pattern.match(str(target_url)) is not None:
                return False

        # If there are no `include` patterns and the URL passed all `exclude` patterns, accept the URL
        if include is None:
            return True

        # If the URL matches any `include` pattern, accept it
        for pattern in include:
            if isinstance(pattern, Glob):
                pattern = pattern.regexp  # noqa: PLW2901

            if pattern.match(str(target_url)) is not None:
                return True

        # The URL does not match any `include` pattern - reject it
        return False

    async def _handle_request_error(self, crawling_context: TCrawlingContext, error: Exception) -> None:
        request_provider = await self.get_request_provider()

        if self._should_retry_request(crawling_context, error):
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

            await request_provider.reclaim_request(request)
        else:
            await wait_for(
                lambda: request_provider.mark_request_as_handled(crawling_context.request),
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

    def _prepare_send_request_function(self, session: Session | None) -> SendRequestFunction:
        async def send_request(
            url: str,
            *,
            method: str = 'get',
            headers: dict[str, str] | None = None,
        ) -> HttpResponse:
            return await self._http_client.send_request(
                url, method=method, headers=httpx.Headers(headers), session=session
            )

        return send_request

    async def _commit_request_handler_result(
        self, context: BasicCrawlingContext, result: RequestHandlerRunResult
    ) -> None:
        request_provider = await self.get_request_provider()
        origin = httpx.URL(context.request.loaded_url or context.request.url)

        for call in result.add_requests_calls:
            requests = list[BaseRequestData]()

            for request in call['requests']:
                if (limit := call.get('limit')) is not None and len(requests) >= limit:
                    break

                request_model = request if isinstance(request, BaseRequestData) else BaseRequestData.from_url(request)
                destination = httpx.URL(request_model.url)
                if destination.is_relative_url:
                    base_url = httpx.URL(call.get('base_url', origin))
                    request_model.url = str(base_url.join(destination))

                if self._check_enqueue_strategy(
                    call.get('strategy', EnqueueStrategy.ALL), target_url=destination, origin_url=origin
                ) and self._check_url_patterns(destination, call.get('include', None), call.get('exclude', None)):
                    requests.append(request_model)

            await request_provider.add_requests_batched(
                requests=requests,
                wait_for_all_requests_to_be_added=False,
            )

    async def __is_finished_function(self) -> bool:
        request_provider = await self.get_request_provider()
        return await request_provider.is_finished()

    async def __is_task_ready_function(self) -> bool:
        request_provider = await self.get_request_provider()
        return not await request_provider.is_empty()

    async def __run_task_function(self) -> None:  # noqa: PLR0912
        request_provider = await self.get_request_provider()

        request = await wait_for(
            lambda: request_provider.fetch_next_request(),
            timeout=self._internal_timeout,
            timeout_message=f'Fetching next request failed after {self._internal_timeout.total_seconds()} seconds',
            logger=logger,
            max_retries=3,
        )

        if request is None:
            return

        session = await self._get_session()
        result = RequestHandlerRunResult()

        crawling_context = BasicCrawlingContext(
            request=request,
            session=session,
            send_request=self._prepare_send_request_function(session),
            add_requests=result.add_requests,
        )

        try:
            request.state = RequestState.REQUEST_HANDLER

            await wait_for(
                lambda: self.__run_request_handler(crawling_context),
                timeout=self._request_handler_timeout,
                timeout_message='Request handler timed out after '
                f'{self._request_handler_timeout.total_seconds()} seconds',
                logger=logger,
            )

            await self._commit_request_handler_result(crawling_context, result)

            await wait_for(
                lambda: request_provider.mark_request_as_handled(crawling_context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=logger,
                max_retries=3,
            )

            request.state = RequestState.DONE

            if crawling_context.session:
                crawling_context.session.mark_good()
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

            if crawling_context.session:
                crawling_context.session.mark_bad()
        except SessionError as session_error:
            if not crawling_context.session:
                raise RuntimeError('SessionError raised in a crawling context without a session') from session_error

            if self._should_retry_request(crawling_context, session_error):
                logger.warning('Encountered a session error, rotating session and retrying')

                crawling_context.session.retire()

                if crawling_context.request.session_rotation_count is None:
                    crawling_context.request.session_rotation_count = 0
                crawling_context.request.session_rotation_count += 1

                await request_provider.reclaim_request(request)
            else:
                logger.exception('Request failed and reached maximum retries', exc_info=session_error)

                await wait_for(
                    lambda: request_provider.mark_request_as_handled(crawling_context.request),
                    timeout=self._internal_timeout,
                    timeout_message='Marking request as handled timed out after '
                    f'{self._internal_timeout.total_seconds()} seconds',
                    logger=logger,
                    max_retries=3,
                )
        except ContextPipelineInterruptedError as interruped_error:
            logger.debug('The context pipeline was interrupted', exc_info=interruped_error)

            await wait_for(
                lambda: request_provider.mark_request_as_handled(crawling_context.request),
                timeout=self._internal_timeout,
                timeout_message='Marking request as handled timed out after '
                f'{self._internal_timeout.total_seconds()} seconds',
                logger=logger,
                max_retries=3,
            )
        except ContextPipelineInitializationError as initialization_error:
            if self._should_retry_request(crawling_context, initialization_error):
                logger.debug(
                    'An exception occurred during the initialization of crawling context, a retry is in order',
                    exc_info=initialization_error,
                )

                request = crawling_context.request
                request.retry_count += 1
                request.state = RequestState.DONE
                await request_provider.reclaim_request(request)
            else:
                logger.exception('Request failed and reached maximum retries', exc_info=initialization_error)

                await wait_for(
                    lambda: request_provider.mark_request_as_handled(crawling_context.request),
                    timeout=self._internal_timeout,
                    timeout_message='Marking request as handled timed out after '
                    f'{self._internal_timeout.total_seconds()} seconds',
                    logger=logger,
                    max_retries=3,
                )

            if crawling_context.session:
                crawling_context.session.mark_bad()
        except Exception as internal_error:
            logger.exception(
                'An exception occurred during handling of a request. This places the crawler '
                'and its underlying storages into an unknown state and crawling will be terminated.',
                exc_info=internal_error,
            )
            raise

    async def __run_request_handler(self, crawling_context: BasicCrawlingContext) -> None:
        await self._context_pipeline(crawling_context, self.router)
