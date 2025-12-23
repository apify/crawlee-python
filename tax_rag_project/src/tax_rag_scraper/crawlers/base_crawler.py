"""Base crawler implementation for tax documentation."""

import logging
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from crawlee._autoscaling.autoscaled_pool import ConcurrencySettings
from crawlee.crawlers import BeautifulSoupCrawler
from crawlee.http_clients import HttpxHttpClient
from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.site_router import SiteRouter
from tax_rag_scraper.storage.qdrant_client import TaxDataQdrantClient
from tax_rag_scraper.utils.embeddings import EmbeddingService
from tax_rag_scraper.utils.link_extractor import LinkExtractor
from tax_rag_scraper.utils.robots import RobotsChecker
from tax_rag_scraper.utils.stats_tracker import CrawlStats
from tax_rag_scraper.utils.user_agents import get_random_user_agent

if TYPE_CHECKING:
    from crawlee.beautifulsoup_crawler import BeautifulSoupCrawlingContext

logger = logging.getLogger(__name__)

HTTP_OK = 200


class TaxDataCrawler:
    """Base crawler for scraping Canadian tax documentation."""

    def __init__(
        self,
        settings: Settings | None = None,
        max_depth: int = 2,
        *,
        use_qdrant: bool = False,
        qdrant_url: str | None = None,
        qdrant_api_key: str | None = None,
    ) -> None:
        """Initialize the crawler with settings.

        Args:
            settings: Application settings. If None, uses default settings.
            max_depth: Maximum crawl depth for link discovery. Default is 2.
            use_qdrant: Enable Qdrant Cloud vector database integration.
            qdrant_url: Qdrant Cloud URL (e.g., 'https://xyz.cloud.qdrant.io').
            qdrant_api_key: API key for Qdrant Cloud authentication.
        """
        self.settings = settings or Settings()

        # Initialize robots.txt checker
        self.robots_checker = RobotsChecker() if self.settings.RESPECT_ROBOTS_TXT else None

        # Initialize site router and link extractor
        self.site_router = SiteRouter()
        self.link_extractor = LinkExtractor(max_depth=max_depth)

        # Initialize Qdrant Cloud integration
        self.use_qdrant = use_qdrant

        if use_qdrant:
            if not qdrant_url or not qdrant_api_key:
                raise ValueError(
                    'Qdrant Cloud credentials required. '
                    'Set QDRANT_URL and QDRANT_API_KEY environment variables. '
                    'Get credentials at https://cloud.qdrant.io'
                )

            logger.info('Initializing Qdrant Cloud integration...')
            self.qdrant_client = TaxDataQdrantClient(
                url=qdrant_url,
                api_key=qdrant_api_key,
                collection_name=self.settings.QDRANT_COLLECTION,
                vector_size=1536,
            )
            self.embedding_service = EmbeddingService(
                model_name=self.settings.EMBEDDING_MODEL, api_key=self.settings.OPENAI_API_KEY
            )
            logger.info('✓ Qdrant Cloud integration ready')
        else:
            self.qdrant_client = None
            self.embedding_service = None

        # Batch storage for efficiency (NEW)
        self.document_batch = []
        self.batch_size = self.settings.EMBEDDING_BATCH_SIZE if use_qdrant else 0

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

    def _setup_handlers(self) -> None:
        """Set up request handlers for the crawler."""

        @self.crawler.router.default_handler
        async def default_handler(context: 'BeautifulSoupCrawlingContext') -> None:
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

                    # Add to Qdrant batch if enabled (NEW)
                    if self.use_qdrant:
                        # Convert TaxDocument to dict for batching
                        doc_dict = result.model_dump() if hasattr(result, 'model_dump') else result
                        await self._store_document(doc_dict)
                else:
                    self.stats.record_failure()
            except Exception:
                self.stats.record_failure()
                raise

            # Extract and enqueue links for deep crawling (NEW)
            current_depth = context.request.user_data.get('depth', 0)

            if current_depth < self.link_extractor.max_depth:
                context.log.info(f'Extracting links from depth {current_depth}')

                links = self.link_extractor.extract_links(context.soup, context.request.url, current_depth)

                context.log.info(f'Found {len(links)} valid links at depth {current_depth}')

                # Enqueue discovered links
                await context.add_requests(list(links), user_data={'depth': current_depth + 1})

    async def _store_document(self, doc_data: dict) -> None:
        """Store document (batch for Qdrant, immediate for filesystem).

        Args:
            doc_data: Document dictionary to store.
        """
        if not self.use_qdrant:
            return

        self.document_batch.append(doc_data)

        # Flush batch when it reaches batch_size
        if len(self.document_batch) >= self.batch_size:
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        """Flush document batch to Qdrant."""
        if not self.document_batch:
            return

        try:
            logger.info(f'Flushing batch of {len(self.document_batch)} documents to Qdrant')

            # Generate embeddings for all documents in batch
            embeddings = await self.embedding_service.embed_documents(self.document_batch)

            # Store in Qdrant
            await self.qdrant_client.store_documents(self.document_batch, embeddings)

            logger.info('✓ Batch flushed successfully')

            # Clear batch
            self.document_batch = []

        except Exception:
            logger.exception('Error flushing batch')
            # Don't re-raise - we don't want to stop the crawler
            # Documents are still saved to filesystem

    async def run(self, start_urls: list[str], crawl_type: str = 'standard') -> None:
        """Run the crawler with the given start URLs.

        Args:
            start_urls: List of URLs to start crawling from.
            crawl_type: Type of crawl for metrics tracking ('daily', 'weekly-deep', 'standard')
        """
        await self.crawler.run(start_urls)

        # Flush any remaining documents in batch (NEW)
        if self.use_qdrant and self.document_batch:
            logger.info(f'Flushing final batch of {len(self.document_batch)} documents')
            await self._flush_batch()

        # Log statistics after crawl completes
        summary = self.stats.summary()
        logger.info('\n%s', '=' * 50)
        logger.info('CRAWL STATISTICS')
        logger.info('=' * 50)
        for key, value in summary.items():
            if key == 'success_rate':
                logger.info('%s: %.2f%%', key, value)
            elif key == 'duration_seconds':
                logger.info('%s: %.2fs', key, value)
            else:
                logger.info('%s: %s', key, value)

        # Log Qdrant statistics if enabled (NEW)
        if self.use_qdrant and self.qdrant_client:
            doc_count = self.qdrant_client.count_documents()
            logger.info('\nQdrant Documents: %d', doc_count)

        logger.info('%s\n', '=' * 50)

        # Write metrics to JSONL file for GitHub Actions artifact upload
        self._write_metrics(crawl_type)

    def _write_metrics(self, crawl_type: str) -> None:
        """Write crawl metrics to a JSONL file for persistent tracking.

        Args:
            crawl_type: Type of crawl ('daily', 'weekly-deep', 'standard')
        """
        try:
            # Determine metrics file path based on storage directory
            storage_dir = Path(self.settings.STORAGE_DIR)
            metrics_dir = storage_dir / 'datasets' / 'default'
            metrics_dir.mkdir(parents=True, exist_ok=True)

            metrics_file = metrics_dir / 'metrics.jsonl'

            # Generate JSONL line
            metrics_line = self.stats.to_jsonl(crawl_type)

            # Append to metrics file (creates if doesn't exist)
            with metrics_file.open('a') as f:
                f.write(metrics_line + '\n')

            logger.info('[OK] Metrics written to %s', metrics_file)

        except Exception:
            logger.exception('[ERROR] Failed to write metrics file')
            # Don't raise - metrics writing shouldn't fail the crawl
