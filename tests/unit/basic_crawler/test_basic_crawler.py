from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.types import BasicCrawlingContext
from crawlee.storages.request_list import RequestList


async def test_processes_requests() -> None:
    crawler = BasicCrawler(request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']))
    calls = list[str]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        calls.append(context.request.url)

    await crawler.run()

    assert calls == ['http://a.com', 'http://b.com', 'http://c.com']


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


async def test_calls_error_handler() -> None:
    crawler = BasicCrawler(
        request_provider=RequestList(['http://a.com', 'http://b.com', 'http://c.com']),
        max_request_retries=3,
    )
    calls = list[tuple[BasicCrawlingContext, Exception]]()

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        if context.request.url == 'http://b.com':
            raise RuntimeError('Arbitrary crash for testing purposes')

    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> BasicCrawlingContext:
        calls.append((context, error))
        return context

    await crawler.run()

    assert len(calls) == 2  # error handler should be called for each retryable request
    assert calls[0][0].request.url == 'http://b.com'
    assert isinstance(calls[0][1], RuntimeError)


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
