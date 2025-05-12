from crawlee import Glob
from crawlee.crawlers._beautifulsoup import *
from crawlee import EnqueueStrategy
from crawlee.configuration import Configuration
from crawlee.storages import RequestQueue

crawler = BeautifulSoupCrawler(
    configuration=Configuration(
        persist_storage=False,
        purge_on_start=True,
        verbose_log=True,
    ),
)

@crawler.router.default_handler
async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
    context.log.info(f'Processing {context.request.url} ...')
    await context.enqueue_links(
        strategy=EnqueueStrategy.SAME_HOSTNAME,
    )

    results = await crawler.run(['https://11-19-inject-broken-links.docs-7kl.pages.dev'])
    results

    request_provider = await RequestQueue.open()
    await request_provider.drop()
