from __future__ import annotations

import asyncio
import logging
import sys
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock, patch

import pytest

from crawlee._request import Request
from crawlee._types import BasicCrawlingContext, RequestHandlerRunResult
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    RenderingType,
    RenderingTypePrediction,
    RenderingTypePredictor,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler import SubCrawlerRun
from crawlee.errors import ContextPipelineInitializationError, ProxyError, RequestHandlerError, SessionError

if TYPE_CHECKING:
    from crawlee._types import UseStateFunction


class _HttpClientTransportError(Exception):
    """Stands in for transport errors from HTTP client libraries that don't subclass builtin connection errors."""


@pytest.fixture
def crawling_context() -> BasicCrawlingContext:
    return BasicCrawlingContext(
        request=Request.from_url(url='https://example.com/test'),
        session=Mock(),
        proxy_info=Mock(),
        send_request=AsyncMock(),
        add_requests=AsyncMock(),
        push_data=AsyncMock(),
        get_key_value_store=AsyncMock(),
        use_state=AsyncMock(return_value={}),
        log=logging.getLogger('test'),
        register_deferred_cleanup=lambda _: None,
    )


def _make_crawler_and_predictor() -> tuple[AdaptivePlaywrightCrawler, Mock]:
    predictor = Mock(spec=RenderingTypePredictor)
    predictor.predict.return_value = RenderingTypePrediction(
        rendering_type='client only', detection_probability_recommendation=1.0
    )
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(rendering_type_predictor=predictor)
    return crawler, predictor


@pytest.mark.parametrize(
    'optional_module_name',
    [
        pytest.param('playwright', id='playwright'),
        pytest.param('rapidfuzz', id='rapidfuzz'),
        pytest.param('sklearn', id='sklearn'),
    ],
)
@pytest.mark.usefixtures('_restore_crawlers_binding')
def test_import_error_handled(optional_module_name: str) -> None:
    # Block the package and all its cached submodules to prevent submodule cache entries
    # from bypassing the blocked top-level package.
    blocked = {
        mod_name: None
        for mod_name in sys.modules
        if mod_name == optional_module_name or mod_name.startswith(f'{optional_module_name}.')
    }

    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name == 'crawlee.crawlers' or mod_name.startswith('crawlee.crawlers._adaptive_playwright'):
                sys.modules.pop(mod_name, None)

        with pytest.raises(ImportError):
            from crawlee.crawlers import AdaptivePlaywrightCrawler  # noqa: F401 PLC0415


@pytest.mark.parametrize(
    'transient_exception',
    [
        pytest.param(SessionError('Session blocked'), id='session-error'),
        pytest.param(ProxyError('Proxy connection failed'), id='proxy-error'),
        pytest.param(ConnectionResetError('Connection reset by peer'), id='connection-error'),
        pytest.param(TimeoutError('Request timed out'), id='timeout-error'),
        pytest.param(asyncio.TimeoutError('Asyncio timeout'), id='asyncio-timeout-error'),
        pytest.param(
            ContextPipelineInitializationError(
                ConnectionResetError('Connection reset by peer'),
                Mock(spec=BasicCrawlingContext),
            ),
            id='pipeline-initialization-connection-error',
        ),
        pytest.param(
            ContextPipelineInitializationError(
                _HttpClientTransportError('Read failed'),
                Mock(spec=BasicCrawlingContext),
            ),
            id='http-client-transport-error',
        ),
    ],
)
async def test_transient_failure_skips_rendering_type_detection(
    transient_exception: Exception,
    crawling_context: BasicCrawlingContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient infrastructure error in the static sub-crawler skips training the predictor."""
    crawler, predictor = _make_crawler_and_predictor()
    pw_result = RequestHandlerRunResult(key_value_store_getter=AsyncMock())

    async def mock_crawl_one(
        rendering_type: RenderingType,
        context: BasicCrawlingContext,  # noqa: ARG001
        use_state: UseStateFunction | None = None,  # noqa: ARG001
    ) -> SubCrawlerRun:
        if rendering_type == 'client only':
            return SubCrawlerRun(result=pw_result)
        return SubCrawlerRun(exception=transient_exception)

    monkeypatch.setattr(crawler, '_crawl_one', mock_crawl_one)

    async with crawler.statistics:
        await crawler._run_request_handler(crawling_context)

    predictor.store_result.assert_not_called()


@pytest.mark.parametrize(
    'handler_exception',
    [
        pytest.param(AttributeError("'NoneType' object has no attribute 'text'"), id='attribute-error'),
        pytest.param(KeyError('price'), id='key-error'),
        pytest.param(
            RequestHandlerError(
                AttributeError("'NoneType' object has no attribute 'text'"),
                Mock(spec=BasicCrawlingContext),
            ),
            id='request-handler-error',
        ),
    ],
)
async def test_handler_error_labels_client_only(
    handler_exception: Exception,
    crawling_context: BasicCrawlingContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A handler error in the static sub-crawler trains the predictor with 'client only'."""
    crawler, predictor = _make_crawler_and_predictor()
    pw_result = RequestHandlerRunResult(key_value_store_getter=AsyncMock())

    async def mock_crawl_one(
        rendering_type: RenderingType,
        context: BasicCrawlingContext,  # noqa: ARG001
        use_state: UseStateFunction | None = None,  # noqa: ARG001
    ) -> SubCrawlerRun:
        if rendering_type == 'client only':
            return SubCrawlerRun(result=pw_result)
        return SubCrawlerRun(exception=handler_exception)

    monkeypatch.setattr(crawler, '_crawl_one', mock_crawl_one)

    async with crawler.statistics:
        await crawler._run_request_handler(crawling_context)

    predictor.store_result.assert_called_once_with(crawling_context.request, 'client only')


async def test_static_success_matching_browser_labels_static(
    crawling_context: BasicCrawlingContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful static sub-crawler run matching the browser run trains the predictor with 'static'."""
    crawler, predictor = _make_crawler_and_predictor()
    pw_result = RequestHandlerRunResult(key_value_store_getter=AsyncMock())
    await pw_result.push_data({'data': 'foo'})
    static_result = RequestHandlerRunResult(key_value_store_getter=AsyncMock())
    await static_result.push_data({'data': 'foo'})

    async def mock_crawl_one(
        rendering_type: RenderingType,
        context: BasicCrawlingContext,  # noqa: ARG001
        use_state: UseStateFunction | None = None,  # noqa: ARG001
    ) -> SubCrawlerRun:
        if rendering_type == 'client only':
            return SubCrawlerRun(result=pw_result)
        return SubCrawlerRun(result=static_result)

    monkeypatch.setattr(crawler, '_crawl_one', mock_crawl_one)

    async with crawler.statistics:
        await crawler._run_request_handler(crawling_context)

    predictor.store_result.assert_called_once_with(crawling_context.request, 'static')


async def test_static_success_differing_from_browser_labels_client_only(
    crawling_context: BasicCrawlingContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful static sub-crawler run with differing results trains the predictor with 'client only'."""
    crawler, predictor = _make_crawler_and_predictor()
    pw_result = RequestHandlerRunResult(key_value_store_getter=AsyncMock())
    await pw_result.push_data({'data': 'from_pw'})
    static_result = RequestHandlerRunResult(key_value_store_getter=AsyncMock())
    await static_result.push_data({'data': 'from_static'})

    async def mock_crawl_one(
        rendering_type: RenderingType,
        context: BasicCrawlingContext,  # noqa: ARG001
        use_state: UseStateFunction | None = None,  # noqa: ARG001
    ) -> SubCrawlerRun:
        if rendering_type == 'client only':
            return SubCrawlerRun(result=pw_result)
        return SubCrawlerRun(result=static_result)

    monkeypatch.setattr(crawler, '_crawl_one', mock_crawl_one)

    async with crawler.statistics:
        await crawler._run_request_handler(crawling_context)

    predictor.store_result.assert_called_once_with(crawling_context.request, 'client only')
