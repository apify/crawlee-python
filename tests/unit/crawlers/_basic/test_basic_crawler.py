# ruff: noqa: ARG001
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from crawlee import ConcurrencySettings, Glob, service_locator
from crawlee._request import Request
from crawlee._types import BasicCrawlingContext, EnqueueLinksKwargs, HttpHeaders, HttpMethod
from crawlee._utils.robots import RobotsTxtFile
from crawlee.configuration import Configuration
from crawlee.crawlers import BasicCrawler
from crawlee.errors import RequestCollisionError, SessionError, UserDefinedErrorHandlerError
from crawlee.events._local_event_manager import LocalEventManager
from crawlee.request_loaders import RequestList, RequestManagerTandem
from crawlee.sessions import Session, SessionPool
from crawlee.statistics import FinalStatistics
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore, RequestQueue

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from yarl import URL

    from crawlee._types import JsonSerializable
    from crawlee.storage_clients._memory import DatasetClient


async def test_processes_requests_from_explicit_queue() -> None:
    queue = await RequestQueue.open()
    await queue.add_requests_batched(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    crawler = BasicCrawler(request_manager=queue)
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run()

    assert calls == ['http://a.com/', 'http://b.com/', 'http://c.com/']


async def test_processes_requests_from_request_source_tandem() -> None:
    request_queue = await RequestQueue.open()
    await request_queue.add_requests_batched(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    request_list = RequestList(['http://a.com/', 'http://d.com', 'http://e.com'])

    crawler = BasicCrawler(request_manager=RequestManagerTandem(request_list, request_queue))
    calls = set[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.add(context.request.url)

    await crawler.run()

    assert calls == {'http://a.com/', 'http://b.com/', 'http://c.com/', 'http://d.com', 'http://e.com'}


async def test_processes_requests_from_run_args() -> None:
    crawler = BasicCrawler()
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    assert calls == ['http://a.com/', 'http://b.com/', 'http://c.com/']


async def test_allows_multiple_run_calls() -> None:
    crawler = BasicCrawler()
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
    crawler = BasicCrawler()
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    assert calls == [
        'http://a.com/',
        'http://b.com/',
        'http://c.com/',
        'http://b.com/',
        'http://b.com/',
    ]


async def test_respects_no_retry() -> None:
    crawler = BasicCrawler(max_request_retries=3)
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)
        raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run(['http://a.com/', 'http://b.com/', Request.from_url(url='http://c.com/', no_retry=True)])

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
    crawler = BasicCrawler(max_request_retries=1)
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)
        raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run(
        [
            'http://a.com/',
            'http://b.com/',
            Request.from_url(url='http://c.com/', user_data={'__crawlee': {'maxRetries': 4}}),
        ]
    )

    assert calls == [
        'http://a.com/',
        'http://b.com/',
        'http://c.com/',
        'http://c.com/',
        'http://c.com/',
        'http://c.com/',
    ]


async def test_calls_error_handler() -> None:
    # Data structure to better track the calls to the error handler.
    @dataclass(frozen=True)
    class Call:
        url: str
        error: Exception
        custom_retry_count: int

    # List to store the information of calls to the error handler.
    calls = list[Call]()

    crawler = BasicCrawler(max_request_retries=3)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> Request:
        # Retrieve or initialize the headers, and extract the current custom retry count.
        headers = context.request.headers or HttpHeaders()
        custom_retry_count = int(headers.get('custom_retry_count', '0'))

        # Append the current call information.
        calls.append(Call(context.request.url, error, custom_retry_count))

        # Update the request to include an incremented custom retry count in the headers and return it.
        request = context.request.model_dump()
        request['headers'] = HttpHeaders({'custom_retry_count': str(custom_retry_count + 1)})
        return Request.model_validate(request)

    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    # Verify that the error handler was called twice
    assert len(calls) == 2

    # Check the first call...
    first_call = calls[0]
    assert first_call.url == 'http://b.com/'
    assert isinstance(first_call.error, RuntimeError)
    assert first_call.custom_retry_count == 0

    # Check the second call...
    second_call = calls[1]
    assert second_call.url == 'http://b.com/'
    assert isinstance(second_call.error, RuntimeError)
    assert second_call.custom_retry_count == 1


async def test_calls_error_handler_for_sesion_errors() -> None:
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
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in error handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])


async def test_calls_failed_request_handler() -> None:
    crawler = BasicCrawler(max_request_retries=3)
    calls = list[tuple[BasicCrawlingContext, Exception]]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        calls.append((context, error))

    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])

    assert len(calls) == 1
    assert calls[0][0].request.url == 'http://b.com/'
    assert isinstance(calls[0][1], RuntimeError)


async def test_handles_error_in_failed_request_handler() -> None:
    crawler = BasicCrawler(max_request_retries=3)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com/':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in failed request handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])


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

        response_data['body'] = json.loads(response.read())
        response_data['headers'] = response.headers

    await crawler.run(['http://a.com/', 'http://b.com/', 'http://c.com/'])

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
                start_url='https://a.com/',
                loaded_url='https://a.com/',
                requests=[
                    'https://a.com/',
                    Request.from_url('http://b.com/'),
                    'http://c.com/',
                ],
                kwargs={},
                expected_urls=['http://b.com/', 'http://c.com/'],
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
                requests=STRATEGY_TEST_URLS[:4],
                kwargs=EnqueueLinksKwargs(strategy='same-domain'),
                expected_urls=STRATEGY_TEST_URLS[1:4],
            ),
            id='enqueue_strategy_same_domain',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[0],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS[:4],
                kwargs=EnqueueLinksKwargs(strategy='same-hostname'),
                expected_urls=[STRATEGY_TEST_URLS[1]],
            ),
            id='enqueue_strategy_same_hostname',
        ),
        pytest.param(
            AddRequestsTestInput(
                start_url=STRATEGY_TEST_URLS[0],
                loaded_url=STRATEGY_TEST_URLS[0],
                requests=STRATEGY_TEST_URLS[:4],
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


async def test_session_rotation() -> None:
    track_session_usage = Mock()

    crawler = BasicCrawler(
        max_session_rotations=7,
        max_request_retries=1,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        track_session_usage(context.session.id if context.session else None)
        raise SessionError('Test error')

    await crawler.run([Request.from_url('https://someplace.com/', label='start')])
    assert track_session_usage.call_count == 7

    session_ids = {call[0][0] for call in track_session_usage.call_args_list}
    assert len(session_ids) == 7
    assert None not in session_ids


async def test_final_statistics() -> None:
    crawler = BasicCrawler(max_request_retries=3)

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

    await dataset.push_data('{"a": 1}')
    assert (await crawler.get_data()).items == [{'a': 1}]

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data('{"b": 2}')

    await dataset.push_data('{"c": 3}')
    assert (await crawler.get_data()).items == [{'a': 1}, {'c': 3}]

    stats = await crawler.run(['http://test.io/1'])

    assert (await crawler.get_data()).items == [{'a': 1}, {'c': 3}, {'b': 2}]
    assert stats.requests_total == 1
    assert stats.requests_finished == 1


async def test_context_push_and_get_data_handler_error() -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data('{"b": 2}')
        raise RuntimeError('Watch me crash')

    stats = await crawler.run(['https://a.com'])

    assert (await crawler.get_data()).items == []
    assert stats.requests_total == 1
    assert stats.requests_finished == 0
    assert stats.requests_failed == 1


async def test_crawler_push_and_export_data(tmp_path: Path) -> None:
    crawler = BasicCrawler()
    dataset = await Dataset.open()

    await dataset.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
    await dataset.push_data({'id': 2, 'test': 'test'})

    await crawler.export_data_json(path=tmp_path / 'dataset.json')
    await crawler.export_data_csv(path=tmp_path / 'dataset.csv')

    assert json.load((tmp_path / 'dataset.json').open()) == [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]
    assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\r\n0,test\r\n1,test\r\n2,test\r\n'


async def test_context_push_and_export_data(tmp_path: Path) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
        await context.push_data({'id': 2, 'test': 'test'})

    await crawler.run(['http://test.io/1'])

    await crawler.export_data_json(path=tmp_path / 'dataset.json')
    await crawler.export_data_csv(path=tmp_path / 'dataset.csv')

    assert json.load((tmp_path / 'dataset.json').open()) == [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]

    assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\r\n0,test\r\n1,test\r\n2,test\r\n'


async def test_crawler_push_and_export_data_and_json_dump_parameter(tmp_path: Path) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
        await context.push_data({'id': 2, 'test': 'test'})

    await crawler.run(['http://test.io/1'])

    await crawler.export_data_json(path=tmp_path / 'dataset.json', indent=3)

    with (tmp_path / 'dataset.json').open() as json_file:
        exported_json_str = json_file.read()

    # Expected data in JSON format with 3 spaces indent
    expected_data = [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]
    expected_json_str = json.dumps(expected_data, indent=3)

    # Assert that the exported JSON string matches the expected JSON string
    assert exported_json_str == expected_json_str


async def test_crawler_push_data_over_limit() -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        # Push a roughly 15MB payload - this should be enough to break the 9MB limit
        await context.push_data({'hello': 'world' * 3 * 1024 * 1024})

    stats = await crawler.run(['http://example.tld/1'])
    assert stats.requests_failed == 1


async def test_context_update_kv_store() -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        store = await context.get_key_value_store()
        await store.set_value('foo', 'bar')

    await crawler.run(['https://hello.world'])

    store = await crawler.get_key_value_store()
    assert (await store.get_value('foo')) == 'bar'


async def test_context_use_state(key_value_store: KeyValueStore) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.use_state({'hello': 'world'})

    await crawler.run(['https://hello.world'])

    store = await crawler.get_key_value_store()

    assert (await store.get_value(BasicCrawler._CRAWLEE_STATE_KEY)) == {'hello': 'world'}


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


async def test_max_crawl_depth() -> None:
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = BasicCrawler(
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
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

    crawler = BasicCrawler(concurrency_settings=ConcurrencySettings(max_concurrency=1), abort_on_error=True)

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

    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])
    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])
    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])

    counter = Counter(args[0][0] for args in visit.call_args_list)
    assert counter == {
        'http://a.com': 3,
        'http://b.com': 3,
        'http://c.com': 3,
    }


async def test_respects_no_persist_storage() -> None:
    configuration = Configuration(persist_storage=False)
    crawler = BasicCrawler(configuration=configuration)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data({'something': 'something'})

    datasets_path = Path(configuration.storage_dir) / 'datasets' / 'default'
    assert not datasets_path.exists() or list(datasets_path.iterdir()) == []


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
            '┌───────────────────────────────┬───────────┐',
            '│ requests_finished             │ 4         │',
            '│ requests_failed               │ 33        │',
            '│ retry_histogram               │ [1, 4, 8] │',
            '│ request_avg_failed_duration   │ 99.0      │',
            '│ request_avg_finished_duration │ 0.483     │',
            '│ requests_finished_per_minute  │ 0.33      │',
            '│ requests_failed_per_minute    │ 0.1       │',
            '│ request_total_duration        │ 720.0     │',
            '│ requests_total                │ 37        │',
            '│ crawler_runtime               │ 300.0     │',
            '└───────────────────────────────┴───────────┘',
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
    crawler = BasicCrawler(concurrency_settings=ConcurrencySettings(max_concurrency=1))

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

    # Set max_concurrency to 2 to ensure two urls are being visited in parallel.
    crawler = BasicCrawler(concurrency_settings=ConcurrencySettings(max_concurrency=2))

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


async def test_sets_services() -> None:
    custom_configuration = Configuration()
    custom_event_manager = LocalEventManager.from_config(custom_configuration)
    custom_storage_client = MemoryStorageClient.from_config(custom_configuration)

    crawler = BasicCrawler(
        configuration=custom_configuration,
        event_manager=custom_event_manager,
        storage_client=custom_storage_client,
    )

    assert service_locator.get_configuration() is custom_configuration
    assert service_locator.get_event_manager() is custom_event_manager
    assert service_locator.get_storage_client() is custom_storage_client

    dataset = await crawler.get_dataset(name='test')
    assert cast('DatasetClient', dataset._resource_client)._memory_storage_client is custom_storage_client


async def test_allows_storage_client_overwrite_before_run(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_storage_client = MemoryStorageClient.from_config()

    crawler = BasicCrawler(
        storage_client=custom_storage_client,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data({'foo': 'bar'})

    other_storage_client = MemoryStorageClient.from_config()
    service_locator.set_storage_client(other_storage_client)

    with monkeypatch.context() as monkey:
        spy = Mock(wraps=service_locator.get_storage_client)
        monkey.setattr(service_locator, 'get_storage_client', spy)
        await crawler.run(['https://does-not-matter.com'])
        assert spy.call_count >= 1

    dataset = await crawler.get_dataset()
    assert cast('DatasetClient', dataset._resource_client)._memory_storage_client is other_storage_client

    data = await dataset.get_data()
    assert data.items == [{'foo': 'bar'}]


@pytest.mark.skipif(sys.version_info[:3] < (3, 11), reason='asyncio.Barrier was introduced in Python 3.11.')
async def test_context_use_state_race_condition_in_handlers(key_value_store: KeyValueStore) -> None:
    """Two parallel handlers increment global variable obtained by `use_state` method.

    Result should be incremented by 2.
    Method `use_state` must be implemented in a way that prevents race conditions in such scenario."""
    from asyncio import Barrier  # type:ignore[attr-defined]  # Test is skipped in older Python versions.

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
    from asyncio import timeout  # type:ignore[attr-defined]  # Test is skipped in older Python versions.

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
        await crawler.run(['http://a.com/'])

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
    additional_urls = ['http://a.com/', 'http://b.com/']
    expected_handler_calls = [call(url) for url in additional_urls[:expected_handled_requests_count]]

    crawler = BasicCrawler(
        keep_alive=keep_alive,
        max_requests_per_crawl=max_requests_per_crawl,
        # If more request can run in parallel, then max_requests_per_crawl is not deterministic.
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
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

        await context.add_requests(['http://b.com/'])

    await crawler.run(['http://a.com/'])

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
            Request.from_url('http://a.com/', session_id=check_session.id, always_enqueue=True) for _ in range(10)
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
        Request.from_url('http://a.com/', session_id=str(session_id), use_extended_unique_key=True)
        for session_id in range(10)
    ]

    await crawler.run(requests)

    assert len(used_sessions) == 10
    assert set(used_sessions) == set(check_sessions)


async def test_error_bound_session_to_request() -> None:
    crawler = BasicCrawler(request_handler=AsyncMock())

    requests = [Request.from_url('http://a.com/', session_id='1', always_enqueue=True) for _ in range(10)]

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

    requests = [Request.from_url('http://a.com/', session_id='1')]

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

    requests = ['http://a.com/', 'http://b.com/', 'http://c.com/']

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


async def test_reduced_logs_from_timed_out_request_handler(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    crawler = BasicCrawler(configure_logging=False, request_handler_timeout=timedelta(seconds=1))

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await asyncio.sleep(10)  # INJECTED DELAY

    await crawler.run([Request.from_url('http://a.com/')])

    for record in caplog.records:
        if record.funcName == '_handle_failed_request':
            full_message = (record.message or '') + (record.exc_text or '')
            assert Counter(full_message)['\n'] < 10
            assert '# INJECTED DELAY' in full_message
            break
    else:
        raise AssertionError('Expected log message about request handler error was not found.')
