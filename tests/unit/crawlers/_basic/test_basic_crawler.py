# ruff: noqa: ARG001
from __future__ import annotations

import asyncio
import concurrent
import json
import logging
import os
import re
import sys
import time
from asyncio import Future
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from itertools import product
from typing import TYPE_CHECKING, Any, Literal, cast
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from crawlee import ConcurrencySettings, Glob, service_locator
from crawlee._request import Request, RequestState
from crawlee._types import BasicCrawlingContext, EnqueueLinksKwargs, HttpMethod
from crawlee._utils.robots import RobotsTxtFile
from crawlee.configuration import Configuration
from crawlee.crawlers import BasicCrawler
from crawlee.errors import RequestCollisionError, SessionError, UserDefinedErrorHandlerError
from crawlee.events import Event, EventCrawlerStatusData
from crawlee.events._local_event_manager import LocalEventManager
from crawlee.request_loaders import RequestList, RequestManagerTandem
from crawlee.sessions import Session, SessionPool
from crawlee.statistics import FinalStatistics
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore, RequestQueue

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from yarl import URL

    from crawlee._types import JsonSerializable
    from crawlee.statistics import StatisticsState


async def test_processes_requests_from_explicit_queue() -> None:
    queue = await RequestQueue.open()
    await queue.add_requests(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    crawler = BasicCrawler(request_manager=queue)
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run()

    assert calls == ['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com']


async def test_processes_requests_from_request_source_tandem() -> None:
    request_queue = await RequestQueue.open()
    await request_queue.add_requests(
        ['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com']
    )

    request_list = RequestList(['https://a.placeholder.com', 'https://d.placeholder.com', 'https://e.placeholder.com'])

    crawler = BasicCrawler(request_manager=RequestManagerTandem(request_list, request_queue))
    calls = set[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.add(context.request.url)

    await crawler.run()

    assert calls == {
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://c.placeholder.com',
        'https://d.placeholder.com',
        'https://e.placeholder.com',
    }


async def test_processes_requests_from_run_args() -> None:
    crawler = BasicCrawler()
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    assert calls == ['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com']


async def test_allows_multiple_run_calls() -> None:
    crawler = BasicCrawler()
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])
    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    assert calls == [
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://c.placeholder.com',
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://c.placeholder.com',
    ]


async def test_retries_failed_requests() -> None:
    crawler = BasicCrawler()
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

        if context.request.url == 'https://b.placeholder.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    assert calls == [
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://c.placeholder.com',
        'https://b.placeholder.com',
        'https://b.placeholder.com',
        'https://b.placeholder.com',
    ]


async def test_respects_no_retry() -> None:
    crawler = BasicCrawler(max_request_retries=2)
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)
        raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run(
        [
            'https://a.placeholder.com',
            'https://b.placeholder.com',
            Request.from_url(url='https://c.placeholder.com', no_retry=True),
        ]
    )

    assert calls == [
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://c.placeholder.com',
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://a.placeholder.com',
        'https://b.placeholder.com',
    ]


async def test_respects_request_specific_max_retries() -> None:
    crawler = BasicCrawler(max_request_retries=0)
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)
        raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run(
        [
            'https://a.placeholder.com',
            'https://b.placeholder.com',
            Request.from_url(url='https://c.placeholder.com', user_data={'__crawlee': {'maxRetries': 1}}),
        ]
    )

    assert calls == [
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://c.placeholder.com',
        'https://c.placeholder.com',
    ]


async def test_calls_error_handler() -> None:
    # Data structure to better track the calls to the error handler.
    @dataclass(frozen=True)
    class Call:
        url: str
        error: Exception

    # List to store the information of calls to the error handler.
    calls = list[Call]()

    crawler = BasicCrawler(max_request_retries=2)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'https://b.placeholder.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> Request:
        # Append the current call information.
        calls.append(Call(context.request.url, error))
        return context.request

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    # Verify that the error handler was called twice
    assert len(calls) == 2

    # Check calls
    for error_call in calls:
        assert error_call.url == 'https://b.placeholder.com'
        assert isinstance(error_call.error, RuntimeError)


async def test_calls_error_handler_for_session_errors() -> None:
    crawler = BasicCrawler(
        max_session_rotations=1,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        raise SessionError('Arbitrary session error for testing purposes')

    error_handler_mock = AsyncMock()

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> None:
        await error_handler_mock(context, error)

    await crawler.run(['https://crawlee.dev'])

    assert error_handler_mock.call_count == 1


async def test_handles_error_in_error_handler() -> None:
    crawler = BasicCrawler(max_request_retries=3)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'https://b.placeholder.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in error handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])


async def test_calls_failed_request_handler() -> None:
    crawler = BasicCrawler(max_request_retries=3)
    calls = list[tuple[BasicCrawlingContext, Exception]]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'https://b.placeholder.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        calls.append((context, error))

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    assert len(calls) == 1
    assert calls[0][0].request.url == 'https://b.placeholder.com'
    assert isinstance(calls[0][1], RuntimeError)


@pytest.mark.parametrize('handler', ['failed_request_handler', 'error_handler'])
async def test_handlers_use_context_helpers(tmp_path: Path, handler: str) -> None:
    """Test that context helpers used in `failed_request_handler` and in `error_handler` have effect."""
    # Prepare crawler
    storage_client = FileSystemStorageClient()
    crawler = BasicCrawler(
        max_request_retries=1, storage_client=storage_client, configuration=Configuration(storage_dir=str(tmp_path))
    )
    # Test data
    rq_alias = 'other'
    test_data = {'some': 'data'}
    test_key = 'key'
    test_value = 'value'
    test_request = Request.from_url('https://d.placeholder.com')

    # Request handler with injected error
    @crawler.router.default_handler
    async def request_handler(context: BasicCrawlingContext) -> None:
        raise RuntimeError('Arbitrary crash for testing purposes')

    # Apply one of the handlers
    @getattr(crawler, handler)  # type: ignore[untyped-decorator]
    async def handler_implementation(context: BasicCrawlingContext, error: Exception) -> None:
        await context.push_data(test_data)
        await context.add_requests(requests=[test_request], rq_alias=rq_alias)
        kvs = await context.get_key_value_store()
        await kvs.set_value(test_key, test_value)

    await crawler.run(['https://b.placeholder.com'])

    # Verify that the context helpers used in handlers had effect on used storages
    dataset = await Dataset.open(storage_client=storage_client)
    kvs = await KeyValueStore.open(storage_client=storage_client)
    rq = await RequestQueue.open(alias=rq_alias, storage_client=storage_client)

    assert test_value == await kvs.get_value(test_key)
    assert [test_data] == (await dataset.get_data()).items
    assert test_request == await rq.fetch_next_request()


async def test_handles_error_in_failed_request_handler() -> None:
    crawler = BasicCrawler(max_request_retries=3)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'https://b.placeholder.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in failed request handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])


@pytest.mark.parametrize(
    ('method', 'path', 'payload'),
    [
        pytest.param('GET', 'get', None, id='get send_request'),
        pytest.param('POST', 'post', b'Hello, world!', id='post send_request'),
    ],
)
async def test_send_request_works(server_url: URL, method: HttpMethod, path: str, payload: None | bytes) -> None:
    response_data: dict[str, Any] = {}

    crawler = BasicCrawler(max_request_retries=3)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        response = await context.send_request(str(server_url / path), method=method, payload=payload)

        response_data['body'] = json.loads(await response.read())
        response_data['headers'] = response.headers

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    response_body = response_data.get('body')
    assert response_body is not None
    assert response_body.get('data') == (payload.decode() if payload else None)

    response_headers = response_data.get('headers')
    assert response_headers is not None
    content_type = response_headers.get('content-type')
    assert content_type is not None
    assert content_type == 'application/json'


@dataclass
class AddRequestsTestInput:
    start_url: str
    loaded_url: str
    requests: Sequence[str | Request]
    expected_urls: Sequence[str]
    kwargs: EnqueueLinksKwargs


STRATEGY_TEST_URLS = (
    'https://someplace.com/',
    'http://someplace.com/index.html',
    'https://blog.someplace.com/index.html',
    'https://redirect.someplace.com',
    'https://other.place.com/index.html',
    'https://someplace.jp/',
)

INCLUDE_TEST_URLS = (
    'https://someplace.com/',
    'https://someplace.com/blog/category/cats',
    'https://someplace.com/blog/category/boots',
    'https://someplace.com/blog/archive/index.html',
    'https://someplace.com/blog/archive/cats',
)


@pytest.mark.parametrize(
    'test_input',
    argvalues=[
        # Basic use case
        pytest.param(
            AddRequestsTestInput(
                start_url='https://a.placeholder.com',
                loaded_url='https://a.placeholder.com',
                requests=[
                    'https://a.placeholder.com',
                    Request.from_url('https://b.placeholder.com'),
                    'https://c.placeholder.com',
                ],
                kwargs={},
                expected_urls=['https://b.placeholder.com', 'https://c.placeholder.com'],
            ),
            id='basic',
        ),
        # Enqueue strategy
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[0],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(),
                expected_urls=STRATEGY_TEST_URLS[1:],
            ),
            id='enqueue_strategy_default',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[0],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='all'),
                expected_urls=STRATEGY_TEST_URLS[1:],
            ),
            id='enqueue_strategy_all',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[0],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='same-domain'),
                expected_urls=STRATEGY_TEST_URLS[1:4],
            ),
            id='enqueue_strategy_same_domain',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[0],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='same-hostname'),
                expected_urls=[STRATEGY_TEST_URLS[1]],
            ),
            id='enqueue_strategy_same_hostname',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[0],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='same-origin'),
                expected_urls=[],
            ),
            id='enqueue_strategy_same_origin',
        ),
        # Enqueue strategy with redirect
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[3],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(),
                expected_urls=STRATEGY_TEST_URLS[:3] + STRATEGY_TEST_URLS[4:],
            ),
            id='redirect_enqueue_strategy_default',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[3],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='all'),
                expected_urls=STRATEGY_TEST_URLS[:3] + STRATEGY_TEST_URLS[4:],
            ),
            id='redirect_enqueue_strategy_all',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[3],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='same-domain'),
                expected_urls=STRATEGY_TEST_URLS[:3],
            ),
            id='redirect_enqueue_strategy_same_domain',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[3],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='same-hostname'),
                expected_urls=[],
            ),
            id='redirect_enqueue_strategy_same_hostname',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[3],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS,
                kwargs=EnqueueLinksKwargs(strategy='same-origin'),
                expected_urls=[],
            ),
            id='redirect_enqueue_strategy_same_origin',
        ),
        # Include/exclude
        pytest.param(
            AddRequestsTestInput(
                start_url=INCLUDE_TEST_URLS[0],
                loaded_url=INCLUDE_TEST_URLS[0],
                requests=INCLUDE_TEST_URLS,
                kwargs=EnqueueLinksKwargs(include=[Glob('https://someplace.com/**/cats')]),
                expected_urls=[INCLUDE_TEST_URLS[1], INCLUDE_TEST_URLS[4]],
            ),
            id='include_exclude_1',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=INCLUDE_TEST_URLS[0],
                loaded_url=INCLUDE_TEST_URLS[0],
                requests=INCLUDE_TEST_URLS,
                kwargs=EnqueueLinksKwargs(exclude=[Glob('https://someplace.com/**/cats')]),
                expected_urls=[INCLUDE_TEST_URLS[2], INCLUDE_TEST_URLS[3]],
            ),
            id='include_exclude_2',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=INCLUDE_TEST_URLS[0],
                loaded_url=INCLUDE_TEST_URLS[0],
                requests=INCLUDE_TEST_URLS,
                kwargs=EnqueueLinksKwargs(
                    include=[Glob('https://someplace.com/**/cats')], exclude=[Glob('https://**/archive/**')]
                ),
                expected_urls=[INCLUDE_TEST_URLS[1]],
            ),
            id='include_exclude_3',
        ),
    ],
)
async def test_enqueue_strategy(test_input: AddRequestsTestInput) -> None:
    visit = Mock()

    crawler = BasicCrawler()

    @crawler.router.handler('start')
    async def start_handler(context: BasicCrawlingContext) -> None:
        # Assign test value to loaded_url - BasicCrawler does not do any navigation by itself
        context.request.loaded_url = test_input.loaded_url
        await context.add_requests(
            test_input.requests,
            **test_input.kwargs,
        )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        visit(context.request.url)

    await crawler.run([Request.from_url(test_input.start_url, label='start')])

    visited = {call[0][0] for call in visit.call_args_list}
    assert visited == set(test_input.expected_urls)


async def test_session_rotation(server_url: URL) -> None:
    session_ids: list[str | None] = []

    crawler = BasicCrawler(
        max_session_rotations=7,
        max_request_retries=1,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        session_ids.append(context.session.id if context.session else None)
        raise SessionError('Test error')

    await crawler.run([str(server_url)])

    # exactly 7 handler calls happened
    assert len(session_ids) == 7

    # all session ids are not None
    assert None not in session_ids

    # and each was a different session
    assert len(set(session_ids)) == 7


async def test_final_statistics() -> None:
    crawler = BasicCrawler(max_request_retries=2)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        id_param = context.request.get_query_param_from_url('id')
        assert id_param is not None
        id = int(id_param)

        await asyncio.sleep(0.001)

        if context.request.retry_count == 0 and id % 2 == 0:
            raise RuntimeError('First crash')

        if context.request.retry_count == 1 and id % 3 == 0:
            raise RuntimeError('Second crash')

        if context.request.retry_count == 2 and id % 4 == 0:
            raise RuntimeError('Third crash')

    final_statistics = await crawler.run(
        [Request.from_url(f'https://someplace.com/?id={id}', label='start') for id in range(50)]
    )

    assert final_statistics.requests_total == 50
    assert final_statistics.requests_finished == 45
    assert final_statistics.requests_failed == 5

    assert final_statistics.retry_histogram == [25, 16, 9]

    assert final_statistics.request_avg_finished_duration is not None
    assert final_statistics.request_avg_finished_duration > timedelta()

    assert final_statistics.request_avg_failed_duration is not None
    assert final_statistics.request_avg_failed_duration > timedelta()

    assert final_statistics.request_total_duration > timedelta()

    assert final_statistics.crawler_runtime > timedelta()

    assert final_statistics.requests_finished_per_minute > 0
    assert final_statistics.requests_failed_per_minute > 0


async def test_crawler_get_storages() -> None:
    crawler = BasicCrawler()

    rp = await crawler.get_request_manager()
    assert isinstance(rp, RequestQueue)

    dataset = await crawler.get_dataset()
    assert isinstance(dataset, Dataset)

    kvs = await crawler.get_key_value_store()
    assert isinstance(kvs, KeyValueStore)


async def test_crawler_run_requests() -> None:
    crawler = BasicCrawler()
    seen_urls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        seen_urls.append(context.request.url)

    start_urls = [
        'http://test.io/1',
        'http://test.io/2',
        'http://test.io/3',
    ]
    stats = await crawler.run(start_urls)

    assert seen_urls == start_urls
    assert stats.requests_total == 3
    assert stats.requests_finished == 3


async def test_context_push_and_get_data() -> None:
    crawler = BasicCrawler()
    dataset = await Dataset.open()

    await dataset.push_data({'a': 1})
    assert (await crawler.get_data()).items == [{'a': 1}]

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data({'b': 2})

    await dataset.push_data({'c': 3})
    assert (await crawler.get_data()).items == [{'a': 1}, {'c': 3}]

    stats = await crawler.run(['http://test.io/1'])

    assert (await crawler.get_data()).items == [{'a': 1}, {'c': 3}, {'b': 2}]
    assert stats.requests_total == 1
    assert stats.requests_finished == 1


async def test_context_push_and_get_data_handler_error() -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data({'b': 2})
        raise RuntimeError('Watch me crash')

    stats = await crawler.run(['https://a.placeholder.com'])

    assert (await crawler.get_data()).items == []
    assert stats.requests_total == 1
    assert stats.requests_finished == 0
    assert stats.requests_failed == 1


async def test_crawler_push_and_export_data(tmp_path: Path) -> None:
    crawler = BasicCrawler()
    dataset = await Dataset.open()

    await dataset.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
    await dataset.push_data({'id': 2, 'test': 'test'})

    await crawler.export_data(path=tmp_path / 'dataset.json')
    await crawler.export_data(path=tmp_path / 'dataset.csv')

    assert json.load((tmp_path / 'dataset.json').open()) == [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]

    # On Windows, text mode file writes convert \n to \r\n, resulting in \r\n line endings.
    # On Unix/Linux, \n remains as \n.
    if sys.platform == 'win32':
        assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\r\n0,test\r\n1,test\r\n2,test\r\n'
    else:
        assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\n0,test\n1,test\n2,test\n'


async def test_crawler_export_data_additional_kwargs(tmp_path: Path) -> None:
    crawler = BasicCrawler()
    dataset = await Dataset.open()

    await dataset.push_data({'z': 1, 'a': 2})

    json_path = tmp_path / 'dataset.json'
    csv_path = tmp_path / 'dataset.csv'

    await crawler.export_data(path=json_path, sort_keys=True, separators=(',', ':'))
    await crawler.export_data(path=csv_path, delimiter=';', lineterminator='\n')

    assert json_path.read_text() == '[{"a":2,"z":1}]'
    assert csv_path.read_text() == 'z;a\n1;2\n'


async def test_context_push_and_export_data(tmp_path: Path) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
        await context.push_data({'id': 2, 'test': 'test'})

    await crawler.run(['http://test.io/1'])

    await crawler.export_data(path=tmp_path / 'dataset.json')
    await crawler.export_data(path=tmp_path / 'dataset.csv')

    assert json.load((tmp_path / 'dataset.json').open()) == [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]

    # On Windows, text mode file writes convert \n to \r\n, resulting in \r\n line endings.
    # On Unix/Linux, \n remains as \n.
    if sys.platform == 'win32':
        assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\r\n0,test\r\n1,test\r\n2,test\r\n'
    else:
        assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\n0,test\n1,test\n2,test\n'


async def test_context_update_kv_store() -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        store = await context.get_key_value_store()
        await store.set_value('foo', 'bar')

    await crawler.run(['https://hello.world'])

    store = await crawler.get_key_value_store()
    assert (await store.get_value('foo')) == 'bar'


async def test_context_use_state() -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.use_state({'hello': 'world'})

    await crawler.run(['https://hello.world'])

    kvs = await crawler.get_key_value_store()
    value = await kvs.get_value(BasicCrawler._CRAWLEE_STATE_KEY)

    assert value == {'hello': 'world'}


async def test_context_handlers_use_state(key_value_store: KeyValueStore) -> None:
    state_in_handler_one: dict[str, JsonSerializable] = {}
    state_in_handler_two: dict[str, JsonSerializable] = {}
    state_in_handler_three: dict[str, JsonSerializable] = {}

    crawler = BasicCrawler()

    @crawler.router.handler('one')
    async def handler_one(context: BasicCrawlingContext) -> None:
        state = await context.use_state({'hello': 'world'})
        state_in_handler_one.update(state)
        state['hello'] = 'new_world'
        await context.add_requests([Request.from_url('https://crawlee.dev/docs/quick-start', label='two')])

    @crawler.router.handler('two')
    async def handler_two(context: BasicCrawlingContext) -> None:
        state = await context.use_state({'hello': 'world'})
        state_in_handler_two.update(state)
        state['hello'] = 'last_world'

    @crawler.router.handler('three')
    async def handler_three(context: BasicCrawlingContext) -> None:
        state = await context.use_state({'hello': 'world'})
        state_in_handler_three.update(state)

    await crawler.run([Request.from_url('https://crawlee.dev/', label='one')])
    await crawler.run([Request.from_url('https://crawlee.dev/docs/examples', label='three')])

    # The state in handler_one must match the default state
    assert state_in_handler_one == {'hello': 'world'}

    # The state in handler_two must match the state updated in handler_one
    assert state_in_handler_two == {'hello': 'new_world'}

    # The state in handler_three must match the final state updated in previous run
    assert state_in_handler_three == {'hello': 'last_world'}

    store = await crawler.get_key_value_store()

    # The state in the KVS must match with the last set state
    assert (await store.get_value(BasicCrawler._CRAWLEE_STATE_KEY)) == {'hello': 'last_world'}


async def test_max_requests_per_crawl() -> None:
    start_urls = [
        'http://test.io/1',
        'http://test.io/2',
        'http://test.io/3',
        'http://test.io/4',
        'http://test.io/5',
    ]
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = BasicCrawler(
        concurrency_settings=ConcurrencySettings(desired_concurrency=1, max_concurrency=1),
        max_requests_per_crawl=3,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        processed_urls.append(context.request.url)

    stats = await crawler.run(start_urls)

    # Verify that only 3 out of the 5 provided URLs were made
    assert len(processed_urls) == 3
    assert stats.requests_total == 3
    assert stats.requests_finished == 3


async def test_max_crawl_depth() -> None:
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = BasicCrawler(
        concurrency_settings=ConcurrencySettings(desired_concurrency=1, max_concurrency=1),
        max_crawl_depth=2,
    )

    @crawler.router.handler('start')
    async def start_handler(context: BasicCrawlingContext) -> None:
        processed_urls.append(context.request.url)
        await context.add_requests(['https://someplace.com/too-deep'])

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        processed_urls.append(context.request.url)

    start_request = Request.from_url('https://someplace.com/', label='start')
    start_request.crawl_depth = 2

    stats = await crawler.run([start_request])

    assert len(processed_urls) == 1
    assert stats.requests_total == 1
    assert stats.requests_finished == 1


@pytest.mark.parametrize(
    ('total_requests', 'fail_at_request', 'expected_starts', 'expected_finished'),
    [
        (3, None, 3, 3),
        (3, 2, 2, 1),
    ],
    ids=[
        'all_requests_successful',
        'abort_on_second_request',
    ],
)
async def test_abort_on_error(
    total_requests: int, fail_at_request: int | None, expected_starts: int, expected_finished: int
) -> None:
    starts_urls = []

    crawler = BasicCrawler(
        concurrency_settings=ConcurrencySettings(desired_concurrency=1, max_concurrency=1),
        abort_on_error=True,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        starts_urls.append(context.request.url)

        if context.request.user_data.get('n_request') == fail_at_request:
            raise ValueError('Error request')

    stats = await crawler.run(
        [
            Request.from_url('https://crawlee.dev', always_enqueue=True, user_data={'n_request': i + 1})
            for i in range(total_requests)
        ]
    )

    assert len(starts_urls) == expected_starts
    assert stats.requests_finished == expected_finished


def test_crawler_log() -> None:
    crawler = BasicCrawler()
    assert isinstance(crawler.log, logging.Logger)
    crawler.log.info('Test log message')


async def test_consecutive_runs_purge_request_queue() -> None:
    crawler = BasicCrawler()
    visit = Mock()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        visit(context.request.url)

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])
    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])
    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    counter = Counter(args[0][0] for args in visit.call_args_list)
    assert counter == {
        'https://a.placeholder.com': 3,
        'https://b.placeholder.com': 3,
        'https://c.placeholder.com': 3,
    }


@pytest.mark.skipif(os.name == 'nt' and 'CI' in os.environ, reason='Skipped in Windows CI')
@pytest.mark.parametrize(
    ('statistics_log_format'),
    [
        pytest.param('table', id='With table for logs'),
        pytest.param('inline', id='With inline logs'),
    ],
)
async def test_logs_final_statistics(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, statistics_log_format: Literal['table', 'inline']
) -> None:
    # Set the log level to INFO to capture the final statistics log.
    caplog.set_level(logging.INFO)

    crawler = BasicCrawler(configure_logging=False, statistics_log_format=statistics_log_format)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data({'something': 'something'})

    fake_statistics = FinalStatistics(
        requests_finished=4,
        requests_failed=33,
        retry_histogram=[1, 4, 8],
        request_avg_failed_duration=timedelta(seconds=99),
        request_avg_finished_duration=timedelta(milliseconds=483),
        requests_finished_per_minute=0.33,
        requests_failed_per_minute=0.1,
        request_total_duration=timedelta(minutes=12),
        requests_total=37,
        crawler_runtime=timedelta(minutes=5),
    )

    monkeypatch.setattr(crawler._statistics, 'calculate', lambda: fake_statistics)

    result = await crawler.run()
    assert result is fake_statistics

    final_statistics = next(
        (record for record in caplog.records if record.msg.startswith('Final')),
        None,
    )

    assert final_statistics is not None
    if statistics_log_format == 'table':
        assert final_statistics.msg.splitlines() == [
            'Final request statistics:',
            '┌───────────────────────────────┬────────────┐',
            '│ requests_finished             │ 4          │',
            '│ requests_failed               │ 33         │',
            '│ retry_histogram               │ [1, 4, 8]  │',
            '│ request_avg_failed_duration   │ 1min 39.0s │',
            '│ request_avg_finished_duration │ 483.0ms    │',
            '│ requests_finished_per_minute  │ 0.33       │',
            '│ requests_failed_per_minute    │ 0.1        │',
            '│ request_total_duration        │ 12min      │',
            '│ requests_total                │ 37         │',
            '│ crawler_runtime               │ 5min       │',
            '└───────────────────────────────┴────────────┘',
        ]
    else:
        assert final_statistics.msg == 'Final request statistics:'

        # ignore[attr-defined] since `extra` parameters are not defined for `LogRecord`
        assert final_statistics.requests_finished == 4  # type: ignore[attr-defined]
        assert final_statistics.requests_failed == 33  # type: ignore[attr-defined]
        assert final_statistics.retry_histogram == [1, 4, 8]  # type: ignore[attr-defined]
        assert final_statistics.request_avg_failed_duration == 99.0  # type: ignore[attr-defined]
        assert final_statistics.request_avg_finished_duration == 0.483  # type: ignore[attr-defined]
        assert final_statistics.requests_finished_per_minute == 0.33  # type: ignore[attr-defined]
        assert final_statistics.requests_failed_per_minute == 0.1  # type: ignore[attr-defined]
        assert final_statistics.request_total_duration == 720.0  # type: ignore[attr-defined]
        assert final_statistics.requests_total == 37  # type: ignore[attr-defined]
        assert final_statistics.crawler_runtime == 300.0  # type: ignore[attr-defined]


async def test_crawler_manual_stop() -> None:
    """Test that no new requests are handled after crawler.stop() is called."""
    start_urls = [
        'http://test.io/1',
        'http://test.io/2',
        'http://test.io/3',
    ]
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing urls are visited one by one in order.
    crawler = BasicCrawler(concurrency_settings=ConcurrencySettings(desired_concurrency=1, max_concurrency=1))

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        processed_urls.append(context.request.url)
        if context.request.url == start_urls[1]:
            crawler.stop()

    stats = await crawler.run(start_urls)

    # Verify that only 2 out of the 3 provided URLs were made
    assert len(processed_urls) == 2
    assert stats.requests_total == 2
    assert stats.requests_finished == 2


@pytest.mark.skipif(sys.version_info[:3] < (3, 11), reason='asyncio.Barrier was introduced in Python 3.11.')
async def test_crawler_multiple_stops_in_parallel() -> None:
    """Test that no new requests are handled after crawler.stop() is called, but ongoing requests can still finish."""

    start_urls = [
        'http://test.io/1',
        'http://test.io/2',
        'http://test.io/3',
    ]
    processed_urls = []

    # Set concurrency to 2 to ensure two urls are being visited in parallel.
    crawler = BasicCrawler(concurrency_settings=ConcurrencySettings(desired_concurrency=2, max_concurrency=2))

    both_handlers_started = asyncio.Barrier(2)  # type:ignore[attr-defined]  # Test is skipped in older Python versions.
    only_one_handler_at_a_time = asyncio.Semaphore(1)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await both_handlers_started.wait()  # Block until both handlers are started.

        async with only_one_handler_at_a_time:
            # Reliably create situation where one handler called `crawler.stop()`, while other handler is still running.
            crawler.stop(reason=f'Stop called on {context.request.url}')
            processed_urls.append(context.request.url)

    stats = await crawler.run(start_urls)

    # Verify that only 2 out of the 3 provided URLs were made
    assert len(processed_urls) == 2
    assert stats.requests_total == 2
    assert stats.requests_finished == 2


async def test_services_no_side_effect_on_crawler_init() -> None:
    custom_configuration = Configuration()
    custom_event_manager = LocalEventManager.from_config(custom_configuration)
    custom_storage_client = MemoryStorageClient()

    _ = BasicCrawler(
        configuration=custom_configuration,
        event_manager=custom_event_manager,
        storage_client=custom_storage_client,
    )

    assert service_locator.get_configuration() is not custom_configuration
    assert service_locator.get_event_manager() is not custom_event_manager
    assert service_locator.get_storage_client() is not custom_storage_client


async def test_crawler_uses_default_services() -> None:
    custom_configuration = Configuration()
    service_locator.set_configuration(custom_configuration)

    custom_event_manager = LocalEventManager.from_config(custom_configuration)
    service_locator.set_event_manager(custom_event_manager)

    custom_storage_client = MemoryStorageClient()
    service_locator.set_storage_client(custom_storage_client)

    basic_crawler = BasicCrawler()

    assert basic_crawler._service_locator.get_configuration() is custom_configuration
    assert basic_crawler._service_locator.get_event_manager() is custom_event_manager
    assert basic_crawler._service_locator.get_storage_client() is custom_storage_client


async def test_services_crawlers_can_use_different_services() -> None:
    custom_configuration_1 = Configuration()
    custom_event_manager_1 = LocalEventManager.from_config(custom_configuration_1)
    custom_storage_client_1 = MemoryStorageClient()

    custom_configuration_2 = Configuration()
    custom_event_manager_2 = LocalEventManager.from_config(custom_configuration_2)
    custom_storage_client_2 = MemoryStorageClient()

    _ = BasicCrawler(
        configuration=custom_configuration_1,
        event_manager=custom_event_manager_1,
        storage_client=custom_storage_client_1,
    )

    _ = BasicCrawler(
        configuration=custom_configuration_2,
        event_manager=custom_event_manager_2,
        storage_client=custom_storage_client_2,
    )


async def test_crawler_uses_default_storages(tmp_path: Path) -> None:
    configuration = Configuration(
        storage_dir=str(tmp_path),
        purge_on_start=True,
    )
    service_locator.set_configuration(configuration)

    dataset = await Dataset.open()
    kvs = await KeyValueStore.open()
    rq = await RequestQueue.open()

    crawler = BasicCrawler()

    assert dataset is await crawler.get_dataset()
    assert kvs is await crawler.get_key_value_store()
    assert rq is await crawler.get_request_manager()


async def test_crawler_can_use_other_storages(tmp_path: Path) -> None:
    configuration = Configuration(
        storage_dir=str(tmp_path),
        purge_on_start=True,
    )
    service_locator.set_configuration(configuration)

    dataset = await Dataset.open()
    kvs = await KeyValueStore.open()
    rq = await RequestQueue.open()

    crawler = BasicCrawler(storage_client=MemoryStorageClient())

    assert dataset is not await crawler.get_dataset()
    assert kvs is not await crawler.get_key_value_store()
    assert rq is not await crawler.get_request_manager()


async def test_crawler_can_use_other_storages_of_same_type(tmp_path: Path) -> None:
    """Test that crawler can use non-global storage of the same type as global storage without conflicts"""
    a_path = tmp_path / 'a'
    b_path = tmp_path / 'b'
    a_path.mkdir()
    b_path.mkdir()
    expected_paths = {
        path / storage
        for path, storage in product({a_path, b_path}, {'datasets', 'key_value_stores', 'request_queues'})
    }

    configuration_a = Configuration(
        storage_dir=str(a_path),
        purge_on_start=True,
    )
    configuration_b = Configuration(
        storage_dir=str(b_path),
        purge_on_start=True,
    )

    # Set global configuration
    service_locator.set_configuration(configuration_a)
    service_locator.set_storage_client(FileSystemStorageClient())
    # Create storages based on the global services
    dataset = await Dataset.open()
    kvs = await KeyValueStore.open()
    rq = await RequestQueue.open()

    # Set the crawler to use different storage client
    crawler = BasicCrawler(storage_client=FileSystemStorageClient(), configuration=configuration_b)

    # Assert that the storages are different
    assert dataset is not await crawler.get_dataset()
    assert kvs is not await crawler.get_key_value_store()
    assert rq is not await crawler.get_request_manager()

    # Assert that all storages exists on the filesystem
    for path in expected_paths:
        assert path.is_dir()


async def test_allows_storage_client_overwrite_before_run(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_storage_client = MemoryStorageClient()

    crawler = BasicCrawler(
        storage_client=custom_storage_client,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data({'foo': 'bar'})

    other_storage_client = MemoryStorageClient()
    service_locator.set_storage_client(other_storage_client)

    with monkeypatch.context() as monkey:
        spy = Mock(wraps=service_locator.get_storage_client)
        monkey.setattr(service_locator, 'get_storage_client', spy)
        await crawler.run(['https://does-not-matter.com'])
        assert spy.call_count >= 1

    dataset = await crawler.get_dataset()
    data = await dataset.get_data()
    assert data.items == [{'foo': 'bar'}]


@pytest.mark.skipif(sys.version_info[:3] < (3, 11), reason='asyncio.Barrier was introduced in Python 3.11.')
async def test_context_use_state_race_condition_in_handlers(key_value_store: KeyValueStore) -> None:
    """Two parallel handlers increment global variable obtained by `use_state` method.

    Result should be incremented by 2.
    Method `use_state` must be implemented in a way that prevents race conditions in such scenario."""
    # Test is skipped in older Python versions.
    from asyncio import Barrier  # type:ignore[attr-defined] # noqa: PLC0415

    crawler = BasicCrawler()
    store = await crawler.get_key_value_store()
    await store.set_value(BasicCrawler._CRAWLEE_STATE_KEY, {'counter': 0})
    handler_barrier = Barrier(2)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        state = cast('dict[str, int]', await context.use_state())
        await handler_barrier.wait()  # Block until both handlers get the state.
        state['counter'] += 1
        await handler_barrier.wait()  # Block until both handlers increment the state.

    await crawler.run(['https://crawlee.dev/', 'https://crawlee.dev/docs/quick-start'])

    store = await crawler.get_key_value_store()
    # Ensure that local state is pushed back to kvs.
    await store.persist_autosaved_values()
    assert (await store.get_value(BasicCrawler._CRAWLEE_STATE_KEY))['counter'] == 2


@pytest.mark.run_alone
@pytest.mark.skipif(sys.version_info[:3] < (3, 11), reason='asyncio.timeout was introduced in Python 3.11.')
@pytest.mark.parametrize(
    'sleep_type',
    [
        pytest.param('async_sleep'),
        pytest.param('sync_sleep', marks=pytest.mark.skip(reason='https://github.com/apify/crawlee-python/issues/908')),
    ],
)
async def test_timeout_in_handler(sleep_type: str) -> None:
    """Test that timeout from request handler is treated the same way as exception thrown in request handler.

    Handler should be able to time out even if the code causing the timeout is blocking sync code.
    Crawler should attempt to retry it.
    This test creates situation where the request handler times out twice, on third retry it does not time out."""
    # Test is skipped in older Python versions.
    from asyncio import timeout  # type:ignore[attr-defined] # noqa: PLC0415

    handler_timeout = timedelta(seconds=1)
    max_request_retries = 3
    double_handler_timeout_s = handler_timeout.total_seconds() * 2
    handler_sleep = iter([double_handler_timeout_s, double_handler_timeout_s, 0])

    crawler = BasicCrawler(request_handler_timeout=handler_timeout, max_request_retries=max_request_retries)

    mocked_handler_before_sleep = Mock()
    mocked_handler_after_sleep = Mock()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        mocked_handler_before_sleep()

        if sleep_type == 'async_sleep':
            await asyncio.sleep(next(handler_sleep))
        else:
            time.sleep(next(handler_sleep))  # noqa:ASYNC251  # Using blocking sleep in async function is the test.

        # This will not execute if timeout happens.
        mocked_handler_after_sleep()

    # Timeout in pytest, because previous implementation would run crawler until following:
    # "The request queue seems to be stuck for 300.0s, resetting internal state."
    async with timeout(max_request_retries * double_handler_timeout_s):
        await crawler.run(['https://a.placeholder.com'])

    assert crawler.statistics.state.requests_finished == 1
    assert mocked_handler_before_sleep.call_count == max_request_retries
    assert mocked_handler_after_sleep.call_count == 1


@pytest.mark.parametrize(
    ('keep_alive', 'max_requests_per_crawl', 'expected_handled_requests_count'),
    [
        pytest.param(True, 2, 2, id='keep_alive, 2 requests'),
        pytest.param(True, 1, 1, id='keep_alive, but max_requests_per_crawl achieved after 1 request'),
        pytest.param(False, 2, 0, id='Crawler without keep_alive (default), crawler finished before adding requests'),
    ],
)
async def test_keep_alive(
    *, keep_alive: bool, max_requests_per_crawl: int, expected_handled_requests_count: int
) -> None:
    """Test that crawler can be kept alive without any requests and stopped with `crawler.stop()`.

    Crawler should stop if `max_requests_per_crawl` is reached regardless of the `keep_alive` flag."""
    additional_urls = ['https://a.placeholder.com', 'https://b.placeholder.com']
    expected_handler_calls = [call(url) for url in additional_urls[:expected_handled_requests_count]]

    crawler = BasicCrawler(
        keep_alive=keep_alive,
        max_requests_per_crawl=max_requests_per_crawl,
        # If more request can run in parallel, then max_requests_per_crawl is not deterministic.
        concurrency_settings=ConcurrencySettings(desired_concurrency=1, max_concurrency=1),
    )
    mocked_handler = Mock()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        mocked_handler(context.request.url)
        if context.request == additional_urls[-1]:
            crawler.stop()

    crawler_run_task = asyncio.create_task(crawler.run())

    # Give some time to crawler to finish(or be in keep_alive state) and add new request.
    # TODO: Replace sleep time by waiting for specific crawler state.
    # https://github.com/apify/crawlee-python/issues/925
    await asyncio.sleep(1)
    assert crawler_run_task.done() != keep_alive
    add_request_task = asyncio.create_task(crawler.add_requests(additional_urls))

    await asyncio.gather(crawler_run_task, add_request_task)

    mocked_handler.assert_has_calls(expected_handler_calls)


@pytest.mark.parametrize(
    ('retire'),
    [
        pytest.param(False, id='without retire'),
        pytest.param(True, id='with retire'),
    ],
)
async def test_session_retire_in_user_handler(*, retire: bool) -> None:
    crawler = BasicCrawler(session_pool=SessionPool(max_pool_size=1))
    sessions = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.session:
            sessions.append(context.session.id)

            context.session.retire() if retire else None

        await context.add_requests(['https://b.placeholder.com'])

    await crawler.run(['https://a.placeholder.com'])

    # The session should differ if `retire` was called and match otherwise since pool size == 1
    if retire:
        assert sessions[1] != sessions[0]
    else:
        assert sessions[1] == sessions[0]


async def test_bound_session_to_request() -> None:
    async with SessionPool() as session_pool:
        check_session: Session = await session_pool.get_session()
        used_sessions = list[str]()
        crawler = BasicCrawler(session_pool=session_pool)

        @crawler.router.default_handler
        async def handler(context: BasicCrawlingContext) -> None:
            if context.session:
                used_sessions.append(context.session.id)

        requests = [
            Request.from_url('https://a.placeholder.com', session_id=check_session.id, always_enqueue=True)
            for _ in range(10)
        ]

        await crawler.run(requests)

        assert len(used_sessions) == 10
        assert set(used_sessions) == {check_session.id}


async def test_bound_sessions_to_same_request() -> None:
    # Use a custom function to avoid errors due to random Session retrieval
    def create_session_function() -> Callable[[], Session]:
        counter = -1

        def create_session() -> Session:
            nonlocal counter
            counter += 1
            return Session(id=str(counter))

        return create_session

    check_sessions = [str(session_id) for session_id in range(10)]
    used_sessions = list[str]()
    crawler = BasicCrawler(session_pool=SessionPool(create_session_function=create_session_function()))

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.session:
            used_sessions.append(context.session.id)

    requests = [
        Request.from_url('https://a.placeholder.com', session_id=str(session_id), use_extended_unique_key=True)
        for session_id in range(10)
    ]

    await crawler.run(requests)

    assert len(used_sessions) == 10
    assert set(used_sessions) == set(check_sessions)


async def test_error_bound_session_to_request() -> None:
    crawler = BasicCrawler(request_handler=AsyncMock())

    requests = [Request.from_url('https://a.placeholder.com', session_id='1', always_enqueue=True) for _ in range(10)]

    stats = await crawler.run(requests)

    assert stats.requests_total == 10
    assert stats.requests_failed == 10
    assert stats.retry_histogram == [10]


async def test_handle_error_bound_session_to_request() -> None:
    error_handler_mock = AsyncMock()
    crawler = BasicCrawler(request_handler=AsyncMock())

    @crawler.failed_request_handler
    async def error_req_hook(context: BasicCrawlingContext, error: Exception) -> None:
        if isinstance(error, RequestCollisionError):
            await error_handler_mock(context, error)

    requests = [Request.from_url('https://a.placeholder.com', session_id='1')]

    await crawler.run(requests)

    assert error_handler_mock.call_count == 1


async def test_handles_session_error_in_failed_request_handler() -> None:
    crawler = BasicCrawler(max_session_rotations=1)
    handler_requests = set()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        raise SessionError('blocked')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        handler_requests.add(context.request.url)

    requests = ['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com']

    await crawler.run(requests)

    assert set(requests) == handler_requests


async def test_lock_with_get_robots_txt_file_for_url(server_url: URL) -> None:
    crawler = BasicCrawler(respect_robots_txt_file=True)

    with patch('crawlee.crawlers._basic._basic_crawler.RobotsTxtFile.find', wraps=RobotsTxtFile.find) as spy:
        await asyncio.gather(
            *[asyncio.create_task(crawler._get_robots_txt_file_for_url(str(server_url))) for _ in range(10)]
        )

        # Check that the lock was acquired only once
        assert spy.call_count == 1


async def test_reduced_logs_from_timed_out_request_handler(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    crawler = BasicCrawler(
        configure_logging=False,
        max_request_retries=1,
        request_handler_timeout=timedelta(seconds=1),
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        # Intentionally add a delay longer than the timeout to trigger the timeout mechanism
        await asyncio.sleep(10)  # INJECTED DELAY

    # Capture all logs from the 'crawlee' logger at INFO level or higher
    with caplog.at_level(logging.INFO, logger='crawlee'):
        await crawler.run([Request.from_url('https://a.placeholder.com')])

    # Check for the timeout message in any of the logs
    found_timeout_message = False
    for record in caplog.records:
        if record.message and 'timed out after 1.0 seconds' in record.message:
            full_message = (record.message or '') + (record.exc_text or '')
            assert '\n' not in full_message
            assert '# INJECTED DELAY' in full_message
            found_timeout_message = True
            break

    assert found_timeout_message, 'Expected log message about request handler error was not found.'


async def test_reduced_logs_from_time_out_in_request_handler(caplog: pytest.LogCaptureFixture) -> None:
    crawler = BasicCrawler(configure_logging=False, max_request_retries=1)

    @crawler.router.default_handler
    async def default_handler(_: BasicCrawlingContext) -> None:
        await asyncio.wait_for(Future(), timeout=1)

    # Capture all logs from the 'crawlee' logger at INFO level or higher
    with caplog.at_level(logging.INFO, logger='crawlee'):
        await crawler.run([Request.from_url('https://a.placeholder.com')])

    # Check for 1 line summary message
    found_timeout_message = False
    for record in caplog.records:
        if re.match(
            r'Retrying request to .* due to: Timeout raised by user defined handler\. File .*, line .*,'
            r' in default_handler,     await asyncio.wait_for\(Future\(\), timeout=1\)',
            record.message,
        ):
            found_timeout_message = True
            break

    assert found_timeout_message, 'Expected log message about request handler error was not found.'


async def test_status_message_callback() -> None:
    """Test that status message callback is called with the correct message."""
    status_message_callback = AsyncMock()
    states: list[dict[str, StatisticsState | None]] = []

    async def status_callback(
        state: StatisticsState, previous_state: StatisticsState | None, message: str
    ) -> str | None:
        await status_message_callback(message)
        states.append({'state': state, 'previous_state': previous_state})
        return message

    crawler = BasicCrawler(
        status_message_callback=status_callback, status_message_logging_interval=timedelta(seconds=0.01)
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await asyncio.sleep(0.1)  # Simulate some processing time

    await crawler.run(['https://a.placeholder.com'])

    assert status_message_callback.called

    assert len(states) > 1

    first_call = states[0]
    second_call = states[1]

    # For the first call, `previous_state` is None
    assert first_call['state'] is not None
    assert first_call['previous_state'] is None

    # For second call, `previous_state` is the first state
    assert second_call['state'] is not None
    assert second_call['previous_state'] is not None
    assert second_call['previous_state'] == first_call['state']


async def test_status_message_emit() -> None:
    event_manager = service_locator.get_event_manager()

    status_message_listener = Mock()

    def listener(event_data: EventCrawlerStatusData) -> None:
        status_message_listener(event_data)

    event_manager.on(event=Event.CRAWLER_STATUS, listener=listener)

    crawler = BasicCrawler(request_handler=AsyncMock())

    await crawler.run(['https://a.placeholder.com'])

    event_manager.off(event=Event.CRAWLER_STATUS, listener=listener)

    assert status_message_listener.called


@pytest.mark.parametrize(
    ('queue_name', 'queue_alias', 'by_id'),
    [
        pytest.param('named-queue', None, False, id='with rq_name'),
        pytest.param(None, 'alias-queue', False, id='with rq_alias'),
        pytest.param('id-queue', None, True, id='with rq_id'),
    ],
)
async def test_add_requests_with_rq_param(queue_name: str | None, queue_alias: str | None, *, by_id: bool) -> None:
    crawler = BasicCrawler()
    rq = await RequestQueue.open(name=queue_name, alias=queue_alias)
    if by_id:
        queue_id = rq.id
        queue_name = None
    else:
        queue_id = None
    visit_urls = set()

    check_requests = [
        Request.from_url('https://a.placeholder.com'),
        Request.from_url('https://b.placeholder.com'),
        Request.from_url('https://c.placeholder.com'),
    ]

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        visit_urls.add(context.request.url)
        await context.add_requests(check_requests, rq_id=queue_id, rq_name=queue_name, rq_alias=queue_alias)

    await crawler.run(['https://start.placeholder.com'])

    requests_from_queue = []
    while request := await rq.fetch_next_request():
        requests_from_queue.append(request)

    assert requests_from_queue == check_requests
    assert visit_urls == {'https://start.placeholder.com'}

    await rq.drop()


@pytest.mark.parametrize(
    ('queue_name', 'queue_alias', 'queue_id'),
    [
        pytest.param('named-queue', 'alias-queue', None, id='rq_name and rq_alias'),
        pytest.param('named-queue', None, 'id-queue', id='rq_name and rq_id'),
        pytest.param(None, 'alias-queue', 'id-queue', id='rq_alias and rq_id'),
        pytest.param('named-queue', 'alias-queue', 'id-queue', id='rq_name and rq_alias and rq_id'),
    ],
)
async def test_add_requests_error_with_multi_params(
    queue_id: str | None, queue_name: str | None, queue_alias: str | None
) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        with pytest.raises(ValueError, match='Only one of `rq_id`, `rq_name` or `rq_alias` can be set'):
            await context.add_requests(
                [Request.from_url('https://a.placeholder.com')],
                rq_id=queue_id,
                rq_name=queue_name,
                rq_alias=queue_alias,
            )

    await crawler.run(['https://start.placeholder.com'])


async def test_crawler_purge_request_queue_uses_same_storage_client() -> None:
    """Make sure that purge on start does not replace the storage client in the underlying storage manager"""

    # Set some different storage_client globally and different for Crawlee.
    service_locator.set_storage_client(FileSystemStorageClient())
    unrelated_rq = await RequestQueue.open()
    unrelated_request = Request.from_url('https://x.placeholder.com')
    await unrelated_rq.add_request(unrelated_request)

    crawler = BasicCrawler(storage_client=MemoryStorageClient())

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        context.log.info(context.request.url)

    for _ in (1, 2):
        await crawler.run(requests=[Request.from_url('https://a.placeholder.com')], purge_request_queue=True)
        assert crawler.statistics.state.requests_finished == 1

    # Crawler should not fall back to the default storage after the purge
    assert await unrelated_rq.fetch_next_request() == unrelated_request


async def _run_crawler(requests: list[str], storage_dir: str) -> StatisticsState:
    """Run crawler and return its statistics state.

    Must be defined like this to be pickable for ProcessPoolExecutor."""
    service_locator.set_configuration(
        Configuration(
            storage_dir=storage_dir,
            purge_on_start=False,
        )
    )

    async def request_handler(context: BasicCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    crawler = BasicCrawler(
        request_handler=request_handler,
        concurrency_settings=ConcurrencySettings(max_concurrency=1, desired_concurrency=1),
    )

    await crawler.run(requests)
    return crawler.statistics.state


def _process_run_crawler(requests: list[str], storage_dir: str) -> StatisticsState:
    return asyncio.run(_run_crawler(requests=requests, storage_dir=storage_dir))


async def test_crawler_statistics_persistence(tmp_path: Path) -> None:
    """Test that crawler statistics persist and are loaded correctly.

    This test simulates starting the crawler process twice, and checks that the statistics include first run."""

    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Crawl 2 requests in the first run and automatically persist the state.
        first_run_state = executor.submit(
            _process_run_crawler,
            requests=['https://a.placeholder.com', 'https://b.placeholder.com'],
            storage_dir=str(tmp_path),
        ).result()
        assert first_run_state.requests_finished == 2

    # Do not reuse the executor to simulate a fresh process to avoid modified class attributes.
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Crawl 1 additional requests in the second run, but use previously automatically persisted state.
        second_run_state = executor.submit(
            _process_run_crawler, requests=['https://c.placeholder.com'], storage_dir=str(tmp_path)
        ).result()
        assert second_run_state.requests_finished == 3

    assert first_run_state.crawler_started_at == second_run_state.crawler_started_at
    assert first_run_state.crawler_finished_at
    assert second_run_state.crawler_finished_at

    assert first_run_state.crawler_finished_at < second_run_state.crawler_finished_at
    assert first_run_state.crawler_runtime < second_run_state.crawler_runtime


async def test_crawler_intermediate_statistics() -> None:
    """Test that crawler statistics are correctly updating total runtime on every calculate call."""
    crawler = BasicCrawler()
    check_time = timedelta(seconds=0.1)

    async def wait_for_statistics_initialization() -> None:
        while not crawler.statistics.active:  # noqa: ASYNC110 # It is ok for tests.
            await asyncio.sleep(0.1)

    @crawler.router.default_handler
    async def handler(_: BasicCrawlingContext) -> None:
        await asyncio.sleep(check_time.total_seconds() * 5)

    # Start crawler and wait until statistics are initialized.
    crawler_task = asyncio.create_task(crawler.run(['https://a.placeholder.com']))
    await wait_for_statistics_initialization()

    # Wait some time and check that runtime is updated.
    await asyncio.sleep(check_time.total_seconds())
    crawler.statistics.calculate()
    assert crawler.statistics.state.crawler_runtime >= check_time

    # Wait for crawler to finish
    await crawler_task


async def test_protect_request_in_run_handlers() -> None:
    """Test that request in crawling context are protected in run handlers."""
    request_queue = await RequestQueue.open(name='state-test')

    request = Request.from_url('https://test.url/', user_data={'request_state': ['initial']})

    crawler = BasicCrawler(request_manager=request_queue, max_request_retries=0)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if isinstance(context.request.user_data['request_state'], list):
            context.request.user_data['request_state'].append('modified')
        raise ValueError('Simulated error after modifying request')

    await crawler.run([request])

    check_request = await request_queue.get_request(request.unique_key)
    assert check_request is not None
    assert check_request.user_data['request_state'] == ['initial']

    await request_queue.drop()


async def test_new_request_error_handler() -> None:
    """Test that error in new_request_handler is handled properly."""
    queue = await RequestQueue.open()
    crawler = BasicCrawler(
        request_manager=queue,
    )

    request = Request.from_url('https://a.placeholder.com')

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if '|test' in context.request.unique_key:
            return
        raise ValueError('This error should not be handled by error handler')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> Request | None:
        return Request.from_url(
            context.request.url,
            unique_key=f'{context.request.unique_key}|test',
        )

    await crawler.run([request])

    original_request = await queue.get_request(request.unique_key)
    error_request = await queue.get_request(f'{request.unique_key}|test')

    assert original_request is not None
    assert original_request.state == RequestState.ERROR_HANDLER
    assert original_request.was_already_handled

    assert error_request is not None
    assert error_request.state == RequestState.DONE
    assert error_request.was_already_handled
