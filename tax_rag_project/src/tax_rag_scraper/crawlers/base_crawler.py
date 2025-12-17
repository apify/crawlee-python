"""Base crawler implementation for tax documentation."""

from datetime import timedelta

from crawlee.crawlers import BeautifulSoupCrawler
from crawlee._autoscaling.autoscaled_pool import ConcurrencySettings
from crawlee.http_clients import HttpxHttpClient

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.models.tax_document import TaxDocument
from tax_rag_scraper.handlers.base_handler import BaseHandler
from tax_rag_scraper.utils.stats_tracker import CrawlStats
from tax_rag_scraper.utils.user_agents import get_random_user_agent
from tax_rag_scraper.utils.robots import RobotsChecker
from tax_rag_scraper.crawlers.site_router import SiteRouter
from tax_rag_scraper.utils.link_extractor import LinkExtractor


class TaxDataCrawler:
    """Base crawler for scraping Canadian tax documentation."""

    def __init__(self, settings: Settings = None, max_depth: int = 2):
        """Initialize the crawler with settings.

        Args:
            settings: Application settings. If None, uses default settings.
            max_depth: Maximum crawl depth for link discovery. Default is 2.
        """
        self.settings = settings or Settings()

        # Initialize robots.txt checker
        self.robots_checker = RobotsChecker() if self.settings.RESPECT_ROBOTS_TXT else None

        # Initialize site router and link extractor (NEW)
        self.site_router = SiteRouter()
        self.link_extractor = LinkExtractor(max_depth=max_depth)

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
                # Rate limiting: max tasks per minute
                max_tasks_per_minute=self.settings.MAX_REQUESTS_PER_MINUTE,
            ),
            http_client=http_client,
            # Retry configuration (passed directly to crawler, not Configuration)
            max_request_retries=3,
            # Timeout configuration (uses timedelta)
            request_handler_timeout=timedelta(seconds=60),
        )
        self._setup_handlers()

    def _setup_handlers(self):
        """Set up request handlers for the crawler."""

        @self.crawler.router.default_handler
        async def default_handler(context):
            # Check robots.txt if enabled (from Stage 3)
            if self.robots_checker and not self.robots_checker.can_fetch(context.request.url):
                context.log.warning(f'Blocked by robots.txt: {context.request.url}')
                self.stats.record_failure()
                return

            # Get appropriate handler for this URL domain (NEW)
            handler = self.site_router.get_handler(context.request.url)

            # Process the page
            try:
                result = await handler.handle(context)
                if result:
                    self.stats.record_success()
                else:
                    self.stats.record_failure()
            except Exception:
                self.stats.record_failure()
                raise

            # Extract and enqueue links for deep crawling (NEW)
            current_depth = context.request.user_data.get('depth', 0)

            if current_depth < self.link_extractor.max_depth:
                context.log.info(f'Extracting links from depth {current_depth}')

                links = self.link_extractor.extract_links(
                    context.soup,
                    context.request.url,
                    current_depth
                )

                context.log.info(f'Found {len(links)} valid links at depth {current_depth}')

                # Enqueue discovered links
                await context.add_requests(
                    [link for link in links],
                    user_data={'depth': current_depth + 1}
                )

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
