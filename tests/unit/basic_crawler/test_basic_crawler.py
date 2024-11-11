# ruff: noqa: ARG001
from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from crawlee import ConcurrencySettings, EnqueueStrategy, Glob
from crawlee._request import BaseRequestData, Request
from crawlee._types import AddRequestsKwargs, BasicCrawlingContext, HttpHeaders
from crawlee.basic_crawler import BasicCrawler
from crawlee.configuration import Configuration
from crawlee.errors import SessionError, UserDefinedErrorHandlerError
from crawlee.statistics import FinalStatistics
from crawlee.storages import Dataset, KeyValueStore, RequestList, RequestQueue

if TYPE_CHECKING:
    from collections.abc import Sequence

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
    # Data structure to better track the calls to the error handler.
    @dataclass(frozen=True)
    class Call:
        url: str
        error: Exception
        custom_retry_count: int

    # List to store the information of calls to the error handler.
    calls = list[Call]()

    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com/', 'http://b.com/', 'http://c.com/']),
        max_request_retries=3,
    )

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

    await crawler.run()

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
    respx_mock.get('http://b.com/', name='test_endpoint').return_value = httpx.Response(
        status_code=200, json={'hello': 'world'}
    )

    response_body: Any = None
    response_headers: HttpHeaders | None = None

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
    content_type = response_headers.get('content-type')
    assert content_type is not None
    assert content_type.endswith('/json')


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
    async def start_handler(context: BasicCrawlingContext) -> None:
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


async def test_context_push_and_export_data(httpbin: str, tmp_path: Path) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
        await context.push_data({'id': 2, 'test': 'test'})

    await crawler.run([f'{httpbin}/1'])

    await crawler.export_data_json(path=tmp_path / 'dataset.json')
    await crawler.export_data_csv(path=tmp_path / 'dataset.csv')

    assert json.load((tmp_path / 'dataset.json').open()) == [
        {'id': 0, 'test': 'test'},
        {'id': 1, 'test': 'test'},
        {'id': 2, 'test': 'test'},
    ]

    assert (tmp_path / 'dataset.csv').read_bytes() == b'id,test\r\n0,test\r\n1,test\r\n2,test\r\n'


async def test_crawler_push_and_export_data_and_json_dump_parameter(httpbin: str, tmp_path: Path) -> None:
    crawler = BasicCrawler()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data([{'id': 0, 'test': 'test'}, {'id': 1, 'test': 'test'}])
        await context.push_data({'id': 2, 'test': 'test'})

    await crawler.run([f'{httpbin}/1'])

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


async def test_max_crawl_depth(httpbin: str) -> None:
    processed_urls = []

    start_request = Request.from_url('https://someplace.com/', label='start')
    start_request.crawl_depth = 2

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = BasicCrawler(
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
        max_crawl_depth=2,
        request_provider=RequestList([start_request]),
    )

    @crawler.router.handler('start')
    async def start_handler(context: BasicCrawlingContext) -> None:
        processed_urls.append(context.request.url)
        await context.add_requests(['https://someplace.com/too-deep'])

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        processed_urls.append(context.request.url)

    stats = await crawler.run()

    assert len(processed_urls) == 1
    assert stats.requests_total == 1
    assert stats.requests_finished == 1


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


async def test_passes_configuration_to_storages() -> None:
    configuration = Configuration(persist_storage=False, purge_on_start=True)

    crawler = BasicCrawler(configuration=configuration)

    dataset = await crawler.get_dataset()
    assert dataset._configuration is configuration

    key_value_store = await crawler.get_key_value_store()
    assert key_value_store._configuration is configuration

    request_provider = await crawler.get_request_provider()
    assert isinstance(request_provider, RequestQueue)
    assert request_provider._configuration is configuration


async def test_respects_no_persist_storage() -> None:
    configuration = Configuration(persist_storage=False)
    crawler = BasicCrawler(configuration=configuration)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        await context.push_data({'something': 'something'})

    datasets_path = Path(configuration.storage_dir) / 'datasets' / 'default'
    assert not datasets_path.exists() or list(datasets_path.iterdir()) == []


async def test_logs_final_statistics(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # Set the log level to INFO to capture the final statistics log.
    caplog.set_level(logging.INFO)

    crawler = BasicCrawler(configure_logging=False)

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
