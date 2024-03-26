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
    pass
