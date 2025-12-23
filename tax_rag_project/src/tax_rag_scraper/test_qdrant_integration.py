"""Test full crawler integration with Qdrant and OpenAI embeddings."""

import asyncio
import logging
import os
import traceback
from pathlib import Path

from dotenv import load_dotenv

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler

logger = logging.getLogger(__name__)

# Load environment variables from .env
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
    logger.info('[OK] Loaded environment from .env')
else:
    logger.warning('[WARNING] Warning: .env file not found')


async def main() -> None:
    """Test full crawler with Qdrant Cloud + OpenAI integration."""
    logger.info('=' * 50)
    logger.info('QDRANT CLOUD INTEGRATION TEST')
    logger.info('=' * 50)

    # Check OpenAI API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error('\n[ERROR] OPENAI_API_KEY not set')
        logger.error('Add to .env file: OPENAI_API_KEY=sk-proj-...')
        return

    logger.info('[OK] OpenAI API key found: %s...', api_key[:20])

    # Check Qdrant Cloud credentials
    qdrant_url = os.getenv('QDRANT_URL')
    qdrant_api_key = os.getenv('QDRANT_API_KEY')

    if not qdrant_url or not qdrant_api_key:
        logger.error('\n[ERROR] Qdrant Cloud credentials not set')
        logger.error('Get credentials at https://cloud.qdrant.io')
        logger.error('Add to .env file:')
        logger.error('  QDRANT_URL=https://your-cluster.cloud.qdrant.io')
        logger.error('  QDRANT_API_KEY=your-api-key')
        return

    logger.info('[OK] Qdrant Cloud URL found: %s', qdrant_url)

    # Configure crawler
    settings = Settings(
        MAX_REQUESTS_PER_CRAWL=5,
        MAX_CONCURRENCY=2,
        MAX_REQUESTS_PER_MINUTE=30,
        RESPECT_ROBOTS_TXT=True,
        USE_QDRANT=True,
        QDRANT_URL=qdrant_url,
        QDRANT_API_KEY=qdrant_api_key,
        QDRANT_COLLECTION='tax_documents',
        EMBEDDING_MODEL='text-embedding-3-small',
        EMBEDDING_BATCH_SIZE=3,
        OPENAI_API_KEY=api_key,
    )

    test_urls = [
        'https://www.canada.ca/en/revenue-agency/services/tax/individuals/topics/about-your-tax-return.html',
    ]

    logger.info('\nConfiguration:')
    logger.info('  Max requests: %d', settings.MAX_REQUESTS_PER_CRAWL)
    logger.info('  Embedding model: %s', settings.EMBEDDING_MODEL)
    logger.info('  Collection: %s', settings.QDRANT_COLLECTION)
    logger.info('  Batch size: %d', settings.EMBEDDING_BATCH_SIZE)
    logger.info('  Vector dimensions: 1536 (text-embedding-3-small)')
    logger.info('\nTest URL: %s', test_urls[0])
    logger.info('\nStarting crawl with Qdrant integration...\n')

    # Create and run crawler
    try:
        crawler = TaxDataCrawler(
            settings=settings,
            max_depth=1,  # Shallow crawl for testing
            use_qdrant=True,
            qdrant_url=settings.QDRANT_URL,
            qdrant_api_key=settings.QDRANT_API_KEY,
        )

        await crawler.run(test_urls)

        logger.info('\n%s', '=' * 50)
        logger.info('[OK] Integration test complete')
        logger.info('=' * 50)

    except Exception:
        logger.exception('\n[ERROR] Integration test failed')
        traceback.print_exc()
        return

    logger.info('\nVerification Steps:')
    logger.info('  1. Check Qdrant Cloud dashboard: https://cloud.qdrant.io')
    logger.info("     - Look for 'tax_documents' collection")
    logger.info('     - Verify vector dimension is 1536')
    logger.info('     - Check document count matches crawled pages')
    logger.info('\n  2. Expected behavior:')
    logger.info('     - Documents scraped and saved to filesystem')
    logger.info('     - Embeddings generated in batches of 3')
    logger.info('     - Documents stored in Qdrant Cloud with 1536-dim vectors')
    logger.info('     - Final statistics show Qdrant document count')
    logger.info('\n  3. Cost estimate:')
    logger.info('     - ~$0.001-0.01 depending on pages crawled')
    logger.info('     - OpenAI text-embedding-3-small: $0.00002/1K tokens')
    logger.info('     - Qdrant Cloud free tier: 1GB storage included')


if __name__ == '__main__':
    asyncio.run(main())
