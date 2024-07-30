# ruff: noqa: ARG001
from __future__ import annotations

import asyncio
from collections import Counter
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest
from httpx import Headers, Response

from crawlee import Glob
from crawlee.autoscaling import ConcurrencySettings
from crawlee.basic_crawler import BasicCrawler
from crawlee.basic_crawler.errors import SessionError, UserDefinedErrorHandlerError
from crawlee.basic_crawler.types import AddRequestsKwargs, BasicCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy
from crawlee.models import BaseRequestData, Request
from crawlee.storages import Dataset, KeyValueStore, RequestList, RequestQueue

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    import respx


async def test_processes_requests() -> None:
    crawler = BasicCrawler(request_provider=RequestList(['http://a.com/', 'http://b.com/', 'http://c.com/']))
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run()

    assert calls == ['http://a.com/', 'http://b.com/', 'http://c.com/']


async def test_processes_requests_from_run_args() -> None:
    crawler = BasicCrawler(request_provider=RequestList())
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    assert calls == ['http://a.com/', 'http://b.com/', 'http://c.com/']


async def test_allows_multiple_run_calls() -> None:
    crawler = BasicCrawler(request_provider=RequestList())
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])
    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    assert calls == [
        'http://a.com/',
        'http://b.com/',
        'http://c.com/',
        'http://a.com/',
        'http://b.com/',
        'http://c.com/',
    ]


async def test_retries_failed_requests() -> None:
    crawler = BasicCrawler(request_provider=RequestList(['http://a.com/', 'http://b.com/', 'http://c.com/']))
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run()

    assert calls == [
        'http://a.com/',
        'http://b.com/',
        'http://c.com/',
        'http://b.com/',
        'http://b.com/',
    ]


async def test_respects_no_retry() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(
            ['http://a.com/', 'http://b.com/', Request.from_url(url='http://c.com/', no_retry=True)]
        ),
        max_request_retries=3,
    )
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)
        raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run()

    assert calls == [
        'http://a.com/',
        'http://b.com/',
        'http://c.com/',
        'http://a.com/',
        'http://b.com/',
        'http://a.com/',
        'http://b.com/',
    ]


async def test_respects_request_specific_max_retries() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(
            [
                'http://a.com/',
                'http://b.com/',
                Request.from_url(url='http://c.com/', user_data={'__crawlee': {'maxRetries': 4}}),
            ]
        ),
        max_request_retries=1,
    )
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)
        raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run()

    assert calls == [
        'http://a.com/',
        'http://b.com/',
        'http://c.com/',
        'http://c.com/',
        'http://c.com/',
        'http://c.com/',
    ]


async def test_calls_error_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com/', 'http://b.com/', 'http://c.com/']),
        max_request_retries=3,
    )
    calls = list[tuple[BasicCrawlingContext, Exception, int]]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> Request:
        headers = context.request.headers or {}
        custom_retry_count = int(headers.get('custom_retry_count', '0'))
        calls.append((context, error, custom_retry_count))

        request = context.request.model_dump()
        request['headers']['custom_retry_count'] = str(custom_retry_count + 1)

        return Request.model_validate(request)

    await crawler.run()

    assert len(calls) == 2  # error handler should be called for each retryable request
    assert calls[0][0].request.url == 'http://b.com/'
    assert isinstance(calls[0][1], RuntimeError)

    # Check the contents of the `custom_retry_count` header added by the error handler
    assert calls[0][2] == 0
    assert calls[1][2] == 1


async def test_handles_error_in_error_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com/', 'http://b.com/', 'http://c.com/']),
        max_request_retries=3,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in error handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run()


async def test_calls_failed_request_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com/', 'http://b.com/', 'http://c.com/']),
        max_request_retries=3,
    )
    calls = list[tuple[BasicCrawlingContext, Exception]]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        calls.append((context, error))

    await crawler.run()

    assert len(calls) == 1
    assert calls[0][0].request.url == 'http://b.com/'
    assert isinstance(calls[0][1], RuntimeError)


async def test_handles_error_in_failed_request_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com/', 'http://b.com/', 'http://c.com/']),
        max_request_retries=3,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in failed request handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run()


async def test_send_request_works(respx_mock: respx.MockRouter) -> None:
    respx_mock.get('http://b.com/', name='test_endpoint').return_value = Response(
        status_code=200, json={'hello': 'world'}
    )

    response_body: Any = None
    response_headers: Headers | None = None

    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com/']),
        max_request_retries=3,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        nonlocal response_body, response_headers

        response = await context.send_request('http://b.com/')
        response_body = response.read()
        response_headers = response.headers

    await crawler.run()
    assert respx_mock['test_endpoint'].called

    assert json.loads(response_body) == {'hello': 'world'}

    assert response_headers is not None
    assert response_headers.get('content-type').endswith('/json')


@dataclass
class AddRequestsTestInput:
    start_url: str
    requests: Sequence[str | BaseRequestData]
    expected_urls: Sequence[str]
    kwargs: AddRequestsKwargs


STRATEGY_TEST_URLS = (
    'https://someplace.com/index.html',
    'http://someplace.com/index.html',
    'https://blog.someplace.com/index.html',
    'https://other.place.com/index.html',
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
        AddRequestsTestInput(
            start_url='https://a.com/',
            requests=[
                'https://a.com/',
                BaseRequestData.from_url('http://b.com/'),
                'http://c.com/',
            ],
            kwargs={},
            expected_urls=['https://a.com/', 'http://b.com/', 'http://c.com/'],
        ),
        # Enqueue strategy
        AddRequestsTestInput(
            start_url=STRATEGY_TEST_URLS[0],
            requests=STRATEGY_TEST_URLS,
            kwargs=AddRequestsKwargs(),
            expected_urls=STRATEGY_TEST_URLS,
        ),
        AddRequestsTestInput(
            start_url=STRATEGY_TEST_URLS[0],
            requests=STRATEGY_TEST_URLS,
            kwargs=AddRequestsKwargs(strategy=EnqueueStrategy.ALL),
            expected_urls=STRATEGY_TEST_URLS,
        ),
        AddRequestsTestInput(
            start_url=STRATEGY_TEST_URLS[0],
            requests=STRATEGY_TEST_URLS,
            kwargs=AddRequestsKwargs(strategy=EnqueueStrategy.SAME_DOMAIN),
            expected_urls=STRATEGY_TEST_URLS[:3],
        ),
        AddRequestsTestInput(
            start_url=STRATEGY_TEST_URLS[0],
            requests=STRATEGY_TEST_URLS,
            kwargs=AddRequestsKwargs(strategy=EnqueueStrategy.SAME_HOSTNAME),
            expected_urls=STRATEGY_TEST_URLS[:2],
        ),
        AddRequestsTestInput(
            start_url=STRATEGY_TEST_URLS[0],
            requests=STRATEGY_TEST_URLS,
            kwargs=AddRequestsKwargs(strategy=EnqueueStrategy.SAME_ORIGIN),
            expected_urls=STRATEGY_TEST_URLS[:1],
        ),
        # Include/exclude
        AddRequestsTestInput(
            start_url=INCLUDE_TEST_URLS[0],
            requests=INCLUDE_TEST_URLS,
            kwargs=AddRequestsKwargs(include=[Glob('https://someplace.com/**/cats')]),
            expected_urls=[INCLUDE_TEST_URLS[1], INCLUDE_TEST_URLS[4]],
        ),
        AddRequestsTestInput(
            start_url=INCLUDE_TEST_URLS[0],
            requests=INCLUDE_TEST_URLS,
            kwargs=AddRequestsKwargs(exclude=[Glob('https://someplace.com/**/cats')]),
            expected_urls=[INCLUDE_TEST_URLS[0], INCLUDE_TEST_URLS[2], INCLUDE_TEST_URLS[3]],
        ),
        AddRequestsTestInput(
            start_url=INCLUDE_TEST_URLS[0],
            requests=INCLUDE_TEST_URLS,
            kwargs=AddRequestsKwargs(
                include=[Glob('https://someplace.com/**/cats')], exclude=[Glob('https://**/archive/**')]
            ),
            expected_urls=[INCLUDE_TEST_URLS[1]],
        ),
    ],
    ids=[
        'basic',
        'enqueue_strategy_1',
        'enqueue_strategy_2',
        'enqueue_strategy_3',
        'enqueue_strategy_4',
        'enqueue_strategy_5',
        'include_exclude_1',
        'include_exclude_2',
        'include_exclude_3',
    ],
)
async def test_enqueue_strategy(test_input: AddRequestsTestInput) -> None:
    visit = Mock()
    crawler = BasicCrawler(request_provider=RequestList([Request.from_url('https://someplace.com/', label='start')]))

    @crawler.router.handler('start')
    async def default_handler(context: BasicCrawlingContext) -> None:
        await context.add_requests(
            test_input.requests,
            **test_input.kwargs,
        )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        visit(context.request.url)

    await crawler.run()

    visited = {call[0][0] for call in visit.call_args_list}
    assert visited == set(test_input.expected_urls)


async def test_session_rotation() -> None:
    track_session_usage = Mock()
    crawler = BasicCrawler(
        request_provider=RequestList([Request.from_url('https://someplace.com/', label='start')]),
        max_session_rotations=7,
        max_request_retries=1,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        track_session_usage(context.session.id if context.session else None)
        raise SessionError('Test error')

    await crawler.run()
    assert track_session_usage.call_count == 7

    session_ids = {call[0][0] for call in track_session_usage.call_args_list}
    assert len(session_ids) == 7
    assert None not in session_ids


async def test_final_statistics() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(
            [Request.from_url(f'https://someplace.com/?id={id}', label='start') for id in range(50)]
        ),
        max_request_retries=3,
    )

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

    final_statistics = await crawler.run()

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

    rp = await crawler.get_request_provider()
    assert isinstance(rp, RequestQueue)

    dataset = await crawler.get_dataset()
    assert isinstance(dataset, Dataset)

    kvs = await crawler.get_key_value_store()
    assert isinstance(kvs, KeyValueStore)


async def test_crawler_run_requests(httpbin: str) -> None:
    crawler = BasicCrawler()
    seen_urls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        seen_urls.append(context.request.url)

    stats = await crawler.run([f'{httpbin}/1', f'{httpbin}/2', f'{httpbin}/3'])

    assert seen_urls == [f'{httpbin}/1', f'{httpbin}/2', f'{httpbin}/3']
    assert stats.requests_total == 3
    assert stats.requests_finished == 3


async def test_context_push_and_get_data(httpbin: str) -> None:
    crawler = BasicCrawler()
    dataset = await Dataset.open()

    await dataset.push_data('{"a": 1}')
    assert (await crawler.get_data()).items == [{'a': 1}]

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data('{"b": 2}')

    await dataset.push_data('{"c": 3}')
    assert (await crawler.get_data()).items == [{'a': 1}, {'c': 3}]

    stats = await crawler.run([f'{httpbin}/1'])

    assert (await crawler.get_data()).items == [{'a': 1}, {'c': 3}, {'b': 2}]
    assert stats.requests_total == 1
    assert stats.requests_finished == 1


async def test_crawler_push_and_export_data(tmp_path: Path) -> None:
    crawler = BasicCrawler()
    dataset = await Dataset.open()

    await dataset.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
    await dataset.push_data({'id': 2, 'test': 'test'})

    await crawler.export_data(tmp_path / 'dataset.json')
    await crawler.export_data(tmp_path / 'dataset.csv')

    assert json.load((tmp_path / 'dataset.json').open()) == [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]
    assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\r\n0,test\r\n1,test\r\n2,test\r\n'


async def test_context_push_and_export_data(httpbin: str, tmp_path: Path) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
        await context.push_data({'id': 2, 'test': 'test'})

    await crawler.run([f'{httpbin}/1'])

    await crawler.export_data(tmp_path / 'dataset.json')
    await crawler.export_data(tmp_path / 'dataset.csv')

    assert json.load((tmp_path / 'dataset.json').open()) == [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]

    assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\r\n0,test\r\n1,test\r\n2,test\r\n'


async def test_max_requests_per_crawl(httpbin: str) -> None:
    start_urls = [f'{httpbin}/1', f'{httpbin}/2', f'{httpbin}/3', f'{httpbin}/4', f'{httpbin}/5']
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = BasicCrawler(
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
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

    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])
    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])
    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])

    counter = Counter(args[0][0] for args in visit.call_args_list)
    assert counter == {
        'http://a.com': 3,
        'http://b.com': 3,
        'http://c.com': 3,
    }
