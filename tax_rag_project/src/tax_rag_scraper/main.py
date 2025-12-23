"""Main entry point for the tax documentation crawler."""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Canadian Tax Documentation Crawler with Qdrant Cloud integration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard daily crawl (100 requests, depth 2)
  python main.py

  # Deep weekly crawl (500 requests, depth 3)
  python main.py --deep

  # Custom depth
  python main.py --max-depth 4

Environment Variables Required:
  QDRANT_URL        - Your Qdrant Cloud cluster URL
  QDRANT_API_KEY    - Your Qdrant Cloud API key
  OPENAI_API_KEY    - Your OpenAI API key for embeddings

Get credentials:
  Qdrant Cloud: https://cloud.qdrant.io
  OpenAI API:   https://platform.openai.com/api-keys
        """,
    )

    parser.add_argument(
        '--deep',
        action='store_true',
        help='Enable deep crawl mode (increases max depth to 3, may override --max-depth)',
    )

    parser.add_argument(
        '--max-depth', type=int, default=None, help='Maximum crawl depth (default: 2, or 3 if --deep is used)'
    )

    return parser.parse_args()


async def main() -> None:
    """Run the tax documentation crawler with Qdrant Cloud integration."""
    # Parse command line arguments
    args = parse_arguments()

    # Load environment variables
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        logger.info('[OK] Loaded environment from .env')
    else:
        logger.warning('[WARNING] .env file not found, using environment variables')

    # Validate required Qdrant Cloud credentials
    qdrant_url = os.getenv('QDRANT_URL')
    qdrant_api_key = os.getenv('QDRANT_API_KEY')

    if not qdrant_url:
        logger.error('\n[ERROR] QDRANT_URL environment variable not set')
        logger.error('\nTo use Qdrant Cloud:')
        logger.error('  1. Visit https://cloud.qdrant.io')
        logger.error('  2. Create a free account (1GB storage included)')
        logger.error('  3. Create a new cluster')
        logger.error('  4. Copy your cluster URL')
        logger.error('  5. Add to .env file or GitHub Secrets:')
        logger.error('     QDRANT_URL=https://your-cluster.cloud.qdrant.io')
        logger.error('     QDRANT_API_KEY=your-api-key')
        sys.exit(1)

    if not qdrant_api_key:
        logger.error('\n[ERROR] QDRANT_API_KEY environment variable not set')
        logger.error('\nGet your API key from https://cloud.qdrant.io')
        logger.error('Add to .env file or GitHub Secrets: QDRANT_API_KEY=your-api-key')
        sys.exit(1)

    # Validate required OpenAI API key
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        logger.error('\n[ERROR] OPENAI_API_KEY environment variable not set')
        logger.error('\nGet your API key from https://platform.openai.com/api-keys')
        logger.error('Add to .env file or GitHub Secrets: OPENAI_API_KEY=sk-proj-...')
        sys.exit(1)

    logger.info('[OK] Qdrant Cloud URL: %s', qdrant_url)
    logger.info('[OK] OpenAI API key configured')

    # Configure settings
    settings = Settings()

    # Determine crawl depth based on arguments
    if args.max_depth is not None:
        # Explicit max-depth takes precedence
        crawl_depth = args.max_depth
        logger.info('[INFO] Using custom max depth: %d', crawl_depth)
    elif args.deep:
        # Deep mode uses depth 3
        crawl_depth = 3
        logger.info('[INFO] Deep crawl mode enabled - max depth: %d', crawl_depth)
    else:
        # Default to settings value
        crawl_depth = settings.MAX_CRAWL_DEPTH
        logger.info('[INFO] Using default max depth: %d', crawl_depth)

    # Override settings with crawl depth
    settings.MAX_CRAWL_DEPTH = crawl_depth

    # Test with CRA website (Canadian Revenue Agency - public info pages)
    test_urls = ['https://www.canada.ca/en/revenue-agency/services/forms-publications.html']

    logger.info('\n[INFO] Starting crawler with Qdrant Cloud integration')
    logger.info('[INFO] Mode: %s', 'Deep Crawl' if args.deep else 'Standard Crawl')
    logger.info('[INFO] Collection: %s', settings.QDRANT_COLLECTION)
    logger.info('[INFO] Max requests: %d', settings.MAX_REQUESTS_PER_CRAWL)
    logger.info('[INFO] Max depth: %d', settings.MAX_CRAWL_DEPTH)
    logger.info('[INFO] Concurrency: %d', settings.MAX_CONCURRENCY)
    logger.info('[INFO] Start URL: %s\n', test_urls[0])

    # Create crawler with Qdrant Cloud integration
    crawler = TaxDataCrawler(
        settings=settings,
        use_qdrant=settings.USE_QDRANT,
        qdrant_url=qdrant_url,
        qdrant_api_key=qdrant_api_key,
    )

    await crawler.run(test_urls)

    logger.info('\n[OK] Crawler complete. Check storage/datasets/default/ for results.')
    logger.info('[OK] View your data in Qdrant Cloud: https://cloud.qdrant.io')


if __name__ == '__main__':
    asyncio.run(main())
