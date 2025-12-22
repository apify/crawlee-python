"""Main entry point for the tax documentation crawler."""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler


def parse_arguments():
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


async def main():
    """Run the tax documentation crawler with Qdrant Cloud integration."""
    # Parse command line arguments
    args = parse_arguments()

    # Load environment variables
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print('[OK] Loaded environment from .env')
    else:
        print('[WARNING] .env file not found, using environment variables')

    # Validate required Qdrant Cloud credentials
    qdrant_url = os.getenv('QDRANT_URL')
    qdrant_api_key = os.getenv('QDRANT_API_KEY')

    if not qdrant_url:
        print('\n[ERROR] QDRANT_URL environment variable not set')
        print('\nTo use Qdrant Cloud:')
        print('  1. Visit https://cloud.qdrant.io')
        print('  2. Create a free account (1GB storage included)')
        print('  3. Create a new cluster')
        print('  4. Copy your cluster URL')
        print('  5. Add to .env file or GitHub Secrets:')
        print('     QDRANT_URL=https://your-cluster.cloud.qdrant.io')
        print('     QDRANT_API_KEY=your-api-key')
        sys.exit(1)

    if not qdrant_api_key:
        print('\n[ERROR] QDRANT_API_KEY environment variable not set')
        print('\nGet your API key from https://cloud.qdrant.io')
        print('Add to .env file or GitHub Secrets: QDRANT_API_KEY=your-api-key')
        sys.exit(1)

    # Validate required OpenAI API key
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        print('\n[ERROR] OPENAI_API_KEY environment variable not set')
        print('\nGet your API key from https://platform.openai.com/api-keys')
        print('Add to .env file or GitHub Secrets: OPENAI_API_KEY=sk-proj-...')
        sys.exit(1)

    print(f'[OK] Qdrant Cloud URL: {qdrant_url}')
    print('[OK] OpenAI API key configured')

    # Configure settings
    settings = Settings()

    # Determine crawl depth based on arguments
    if args.max_depth is not None:
        # Explicit max-depth takes precedence
        crawl_depth = args.max_depth
        print(f'[INFO] Using custom max depth: {crawl_depth}')
    elif args.deep:
        # Deep mode uses depth 3
        crawl_depth = 3
        print(f'[INFO] Deep crawl mode enabled - max depth: {crawl_depth}')
    else:
        # Default to settings value
        crawl_depth = settings.MAX_CRAWL_DEPTH
        print(f'[INFO] Using default max depth: {crawl_depth}')

    # Override settings with crawl depth
    settings.MAX_CRAWL_DEPTH = crawl_depth

    # Test with CRA website (Canadian Revenue Agency - public info pages)
    test_urls = ['https://www.canada.ca/en/revenue-agency/services/forms-publications.html']

    print('\n[INFO] Starting crawler with Qdrant Cloud integration')
    print(f'[INFO] Mode: {"Deep Crawl" if args.deep else "Standard Crawl"}')
    print(f'[INFO] Collection: {settings.QDRANT_COLLECTION}')
    print(f'[INFO] Max requests: {settings.MAX_REQUESTS_PER_CRAWL}')
    print(f'[INFO] Max depth: {settings.MAX_CRAWL_DEPTH}')
    print(f'[INFO] Concurrency: {settings.MAX_CONCURRENCY}')
    print(f'[INFO] Start URL: {test_urls[0]}\n')

    # Create crawler with Qdrant Cloud integration
    crawler = TaxDataCrawler(
        settings=settings,
        use_qdrant=settings.USE_QDRANT,
        qdrant_url=qdrant_url,
        qdrant_api_key=qdrant_api_key,
    )

    await crawler.run(test_urls)

    print('\n[OK] Crawler complete. Check storage/datasets/default/ for results.')
    print('[OK] View your data in Qdrant Cloud: https://cloud.qdrant.io')


if __name__ == '__main__':
    asyncio.run(main())
