import pytest

from crawlee.basic_crawler.basic_crawler import BasicCrawler, UserDefinedErrorHandlerError
from crawlee.basic_crawler.types import BasicCrawlingContext, RequestData
from crawlee.storages.request_list import RequestList


async def test_processes_requests() -> None:
    crawler = BasicCrawler(request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']))
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run()

    assert calls == ['http://a.com', 'http://b.com', 'http://c.com']


async def test_processes_requests_from_run_args() -> None:
    crawler = BasicCrawler(request_provider=RequestList())
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])

    assert calls == ['http://a.com', 'http://b.com', 'http://c.com']


async def test_allows_multiple_run_calls() -> None:
    crawler = BasicCrawler(request_provider=RequestList())
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])
    await crawler.run(['http://a.com', 'http://b.com', 'http://c.com'])

    assert calls == [
        'http://a.com',
        'http://b.com',
        'http://c.com',
        'http://a.com',
        'http://b.com',
        'http://c.com',
    ]


async def test_retries_failed_requests() -> None:
    crawler = BasicCrawler(request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']))
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

        if context.request.url == 'http://b.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    await crawler.run()

    assert calls == [
        'http://a.com',
        'http://b.com',
        'http://c.com',
        'http://b.com',
        'http://b.com',
    ]


async def test_respects_no_retry() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(
            ['http://a.com', 'http://b.com', RequestData(url='http://c.com', unique_key='', id='', no_retry=True)]
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
        'http://a.com',
        'http://b.com',
        'http://c.com',
        'http://a.com',
        'http://b.com',
        'http://a.com',
        'http://b.com',
    ]


async def test_respects_request_specific_max_retries() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(
            [
                'http://a.com',
                'http://b.com',
                RequestData(url='http://c.com', unique_key='', id='', user_data={'__crawlee': {'maxRetries': 4}}),
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
        'http://a.com',
        'http://b.com',
        'http://c.com',
        'http://c.com',
        'http://c.com',
        'http://c.com',
    ]


async def test_calls_error_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']),
        max_request_retries=3,
    )
    calls = list[tuple[BasicCrawlingContext, Exception, int]]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> RequestData:
        headers = context.request.headers or {}
        custom_retry_count = int(headers.get('custom_retry_count', '0'))
        calls.append((context, error, custom_retry_count))

        return RequestData.model_validate(
            context.request.model_dump() | {'headers': headers | {'custom_retry_count': str(custom_retry_count + 1)}}
        )

    await crawler.run()

    assert len(calls) == 2  # error handler should be called for each retryable request
    assert calls[0][0].request.url == 'http://b.com'
    assert isinstance(calls[0][1], RuntimeError)

    # Check the contents of the `custom_retry_count` header added by the error handler
    assert calls[0][2] == 0
    assert calls[1][2] == 1


async def test_handles_error_in_error_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']),
        max_request_retries=3,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in error handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run()


async def test_calls_failed_request_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']),
        max_request_retries=3,
    )
    calls = list[tuple[BasicCrawlingContext, Exception]]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        calls.append((context, error))

    await crawler.run()

    assert len(calls) == 1
    assert calls[0][0].request.url == 'http://b.com'
    assert isinstance(calls[0][1], RuntimeError)


async def test_handles_error_in_failed_request_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']),
        max_request_retries=3,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.failed_request_handler
    async def failed_request_handler(context: BasicCrawlingContext, error: Exception) -> None:
        raise RuntimeError('Crash in failed request handler')

    with pytest.raises(UserDefinedErrorHandlerError):
        await crawler.run()
