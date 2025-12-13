"""Base crawler implementation for tax documentation."""

from crawlee.crawlers import BeautifulSoupCrawler
from crawlee._autoscaling.autoscaled_pool import ConcurrencySettings
from crawlee.configuration import Configuration
from crawlee.http_clients import HttpxHttpClient

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.models.tax_document import TaxDocument
from tax_rag_scraper.handlers.base_handler import BaseHandler
from tax_rag_scraper.utils.stats_tracker import CrawlStats
from tax_rag_scraper.utils.user_agents import get_random_user_agent
from tax_rag_scraper.utils.robots import RobotsChecker


class TaxDataCrawler:
    """Base crawler for scraping Canadian tax documentation."""

    def __init__(self, settings: Settings = None):
        """Initialize the crawler with settings.

        Args:
            settings: Application settings. If None, uses default settings.
        """
        self.settings = settings or Settings()

        # Initialize robots.txt checker
        self.robots_checker = RobotsChecker() if self.settings.RESPECT_ROBOTS_TXT else None

        # Configure retries and timeouts
        config = Configuration(
            max_request_retries=3,
            request_handler_timeout_secs=60,
        )

        # Create HTTP client with custom headers and user-agent rotation
        http_client = HttpxHttpClient(
            headers={
                'User-Agent': get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',  # Do Not Track
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            },
            timeout=self.settings.REQUEST_TIMEOUT,
        )

        # Add statistics tracker
        self.stats = CrawlStats()

        self.crawler = BeautifulSoupCrawler(
            max_requests_per_crawl=self.settings.MAX_REQUESTS_PER_CRAWL,
            concurrency_settings=ConcurrencySettings(
                max_concurrency=self.settings.MAX_CONCURRENCY,
                desired_concurrency=self.settings.MAX_CONCURRENCY,
            ),
            configuration=config,
            http_client=http_client,
            # Rate limiting
            max_requests_per_minute=self.settings.MAX_REQUESTS_PER_MINUTE,
        )
        self._setup_handlers()

    def _setup_handlers(self):
        """Set up request handlers for the crawler."""

        # Create a concrete implementation of BaseHandler
        class DefaultHandler(BaseHandler):
            def __init__(self, stats: CrawlStats, robots_checker: RobotsChecker = None):
                self.stats = stats
                self.robots_checker = robots_checker

            async def handle(self, context):
                # Check robots.txt before processing
                if self.robots_checker and not self.robots_checker.can_fetch(context.request.url):
                    context.log.warning(f'Blocked by robots.txt: {context.request.url}')
                    return None

                # Call parent handle method (from BaseHandler)
                return await super().handle(context)

            async def _extract_data(self, context):
                # Same extraction logic as Stage 1
                doc = TaxDocument(
                    url=str(context.request.url),
                    title=context.soup.title.string if context.soup.title else "",
                    content=context.soup.get_text()[:1000],
                )
                return doc

        handler = DefaultHandler(self.stats, self.robots_checker)

        @self.crawler.router.default_handler
        async def default_handler(context):
            try:
                result = await handler.handle(context)
                if result:
                    self.stats.record_success()
                else:
                    self.stats.record_failure()
            except Exception:
                self.stats.record_failure()
                raise  # Re-raise to let Crawlee handle retries

    async def run(self, start_urls: list[str]):
        """Run the crawler with the given start URLs.

        Args:
            start_urls: List of URLs to start crawling from.
        """
        await self.crawler.run(start_urls)

        # Print statistics after crawl completes
        summary = self.stats.summary()
        print("\n" + "="*50)
        print("CRAWL STATISTICS")
        print("="*50)
        for key, value in summary.items():
            if key == 'success_rate':
                print(f"{key}: {value:.2f}%")
            elif key == 'duration_seconds':
                print(f"{key}: {value:.2f}s")
            else:
                print(f"{key}: {value}")
        print("="*50 + "\n")
