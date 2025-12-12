"""Base crawler implementation for tax documentation."""

from crawlee.crawlers import BeautifulSoupCrawler
from crawlee._autoscaling.autoscaled_pool import ConcurrencySettings

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.models.tax_document import TaxDocument


class TaxDataCrawler:
    """Base crawler for scraping Canadian tax documentation."""

    def __init__(self, settings: Settings = None):
        """Initialize the crawler with settings.

        Args:
            settings: Application settings. If None, uses default settings.
        """
        self.settings = settings or Settings()
        self.crawler = BeautifulSoupCrawler(
            max_requests_per_crawl=self.settings.MAX_REQUESTS_PER_CRAWL,
            concurrency_settings=ConcurrencySettings(
                max_concurrency=self.settings.MAX_CONCURRENCY,
                desired_concurrency=self.settings.MAX_CONCURRENCY,
            ),
        )
        self._setup_handlers()

    def _setup_handlers(self):
        """Set up request handlers for the crawler."""

        @self.crawler.router.default_handler
        async def default_handler(context):
            context.log.info(f'Processing {context.request.url}')

            # Extract basic data
            doc = TaxDocument(
                url=str(context.request.url),
                title=context.soup.title.string if context.soup.title else "",
                content=context.soup.get_text()[:1000],  # First 1000 chars
            )

            await context.push_data(doc.model_dump())

    async def run(self, start_urls: list[str]):
        """Run the crawler with the given start URLs.

        Args:
            start_urls: List of URLs to start crawling from.
        """
        await self.crawler.run(start_urls)
