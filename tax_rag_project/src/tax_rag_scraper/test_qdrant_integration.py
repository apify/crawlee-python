"""Test full crawler integration with Qdrant and OpenAI embeddings"""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler

# Load environment variables from .env
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print('[OK] Loaded environment from .env')
else:
    print('[WARNING] Warning: .env file not found')


async def main():
    """Test full crawler with Qdrant Cloud + OpenAI integration"""
    print('=' * 50)
    print('QDRANT CLOUD INTEGRATION TEST')
    print('=' * 50)

    # Check OpenAI API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print('\n[ERROR] OPENAI_API_KEY not set')
        print('Add to .env file: OPENAI_API_KEY=sk-proj-...')
        return

    print(f'[OK] OpenAI API key found: {api_key[:20]}...')

    # Check Qdrant Cloud credentials
    qdrant_url = os.getenv('QDRANT_URL')
    qdrant_api_key = os.getenv('QDRANT_API_KEY')

    if not qdrant_url or not qdrant_api_key:
        print('\n[ERROR] Qdrant Cloud credentials not set')
        print('Get credentials at https://cloud.qdrant.io')
        print('Add to .env file:')
        print('  QDRANT_URL=https://your-cluster.cloud.qdrant.io')
        print('  QDRANT_API_KEY=your-api-key')
        return

    print(f'[OK] Qdrant Cloud URL found: {qdrant_url}')

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

    print('\nConfiguration:')
    print(f'  Max requests: {settings.MAX_REQUESTS_PER_CRAWL}')
    print(f'  Embedding model: {settings.EMBEDDING_MODEL}')
    print(f'  Collection: {settings.QDRANT_COLLECTION}')
    print(f'  Batch size: {settings.EMBEDDING_BATCH_SIZE}')
    print('  Vector dimensions: 1536 (text-embedding-3-small)')
    print(f'\nTest URL: {test_urls[0]}')
    print('\nStarting crawl with Qdrant integration...\n')

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

        print('\n' + '=' * 50)
        print('[OK] Integration test complete')
        print('=' * 50)

    except Exception as e:
        print(f'\n[ERROR] Integration test failed: {e}')
        import traceback

        traceback.print_exc()
        return

    print('\nVerification Steps:')
    print('  1. Check Qdrant Cloud dashboard: https://cloud.qdrant.io')
    print("     - Look for 'tax_documents' collection")
    print('     - Verify vector dimension is 1536')
    print('     - Check document count matches crawled pages')
    print('\n  2. Expected behavior:')
    print('     - Documents scraped and saved to filesystem')
    print('     - Embeddings generated in batches of 3')
    print('     - Documents stored in Qdrant Cloud with 1536-dim vectors')
    print('     - Final statistics show Qdrant document count')
    print('\n  3. Cost estimate:')
    print('     - ~$0.001-0.01 depending on pages crawled')
    print('     - OpenAI text-embedding-3-small: $0.00002/1K tokens')
    print('     - Qdrant Cloud free tier: 1GB storage included')


if __name__ == '__main__':
    asyncio.run(main())
