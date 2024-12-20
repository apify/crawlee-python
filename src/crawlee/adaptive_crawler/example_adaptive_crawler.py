import asyncio

from crawlee import service_locator
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.adaptive_crawler._adaptive_crawler import AdaptiveCrawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import RequestQueue


async def main():
    # Create two non-default memory storages for each of the subcrawlers to hold their request providers


    service_locator.set_storage_client(MemoryStorageClient.from_config())
    """
    MemoryStorageClient(write_metadata=False, persist_storage=False, storage_dir="primary", default_request_queue_id="primary", default_key_value_store_id="primary", default_dataset_id="primary")
    MemoryStorageClient(write_metadata=False, persist_storage=False, storage_dir="secondary",
                        default_request_queue_id="secondary", default_key_value_store_id="secondary",
                        default_dataset_id="secondary")
    """
    primary_request_provider = await RequestQueue.open(name=crypto_random_object_id())
    secondary_request_provider = await RequestQueue.open(name=crypto_random_object_id())
    bs_crawler = BeautifulSoupCrawler(request_provider=primary_request_provider)
    pw_crawler = PlaywrightCrawler(request_provider=secondary_request_provider)
    adaptive_crawler = AdaptiveCrawler(primary_crawler=bs_crawler, secondary_crawler=pw_crawler)

    @bs_crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawler) -> None:
        context.log.info(f'Processing with BS: {context.request.url} ...')
        result = {"BSUrl":context.request.url}
        adaptive_crawler.coordinator.set_result(bs_crawler, context.request.id ,result)
        #await context.push_data(result)

    @pw_crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing with PW: {context.request.url} ...')
        result = {"PWUrl":context.request.url}
        adaptive_crawler.coordinator.set_result(pw_crawler, context.request.id, result)
        #await context.push_data(result)

    @adaptive_crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawler) -> None:
        context.log.info(f'Processing with Top adaptive_crawler: {context.request.url} ...')
        #  Custom logic to route to other crawlers
        await context.push_data({"Url": context.request.url})

    await adaptive_crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
