import asyncio
from copy import deepcopy
from pyexpat import features

from bs4 import BeautifulSoup

from crawlee import service_locator
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.adaptive_crawler._adaptive_crawler import AdaptiveCrawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import RequestQueue
from tests.unit.browsers.test_playwright_browser_controller import playwright


class AdaptivePlayWrightCrawler(AdaptiveCrawler):

    @classmethod
    async def create_with_default_settings(cls, **crawler_kwargs):
        primary_request_provider = await RequestQueue.open(name=crypto_random_object_id())
        secondary_request_provider = await RequestQueue.open(name=crypto_random_object_id())

        # TODO: set subcrawlers log level higher than INFO. So far good for development
        beautifulsoup_crawler = BeautifulSoupCrawler(request_provider=primary_request_provider)
        playwright_crawler = PlaywrightCrawler(request_provider=secondary_request_provider)

        adaptive_crawler = cls(primary_crawler=beautifulsoup_crawler,
                               secondary_crawler=playwright_crawler,
                               **crawler_kwargs)


        @beautifulsoup_crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawler) -> None:
            context.log.info(f'Processing with BS: {context.request.url} ...')
            adaptive_crawler.coordinator.set_result(beautifulsoup_crawler, context.request.id, context)

        @playwright_crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing with PW: {context.request.url} ...')
            adaptive_crawler.coordinator.set_result(playwright_crawler, context.request.id,
                                                    BeautifulSoup(await context.page.content(), features="lxml"))

        return adaptive_crawler


