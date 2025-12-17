"""Test full crawler integration with Qdrant and OpenAI embeddings"""
import asyncio
import os
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler
from tax_rag_scraper.config.settings import Settings


async def main():
    """Test full crawler with Qdrant + OpenAI integration"""

    print("="*50)
    print("QDRANT INTEGRATION TEST")
    print("="*50)

    # Check API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("\n✗ OPENAI_API_KEY not set")
        print("Add to .env file: OPENAI_API_KEY=sk-proj-...")
        return

    print(f"✓ API key found: {api_key[:20]}...")

    # Configure crawler
    settings = Settings(
        MAX_REQUESTS_PER_CRAWL=5,
        MAX_CONCURRENCY=2,
        MAX_REQUESTS_PER_MINUTE=30,
        RESPECT_ROBOTS_TXT=True,
        USE_QDRANT=True,
        QDRANT_HOST="localhost",
        QDRANT_PORT=6333,
        QDRANT_COLLECTION="tax_documents",
        EMBEDDING_MODEL="text-embedding-3-small",
        EMBEDDING_BATCH_SIZE=3,
        OPENAI_API_KEY=api_key,
    )

    test_urls = [
        'https://www.canada.ca/en/revenue-agency/services/tax/individuals/topics/about-your-tax-return.html',
    ]

    print(f"\nConfiguration:")
    print(f"  Max requests: {settings.MAX_REQUESTS_PER_CRAWL}")
    print(f"  Embedding model: {settings.EMBEDDING_MODEL}")
    print(f"  Collection: {settings.QDRANT_COLLECTION}")
    print(f"  Batch size: {settings.EMBEDDING_BATCH_SIZE}")
    print(f"  Vector dimensions: 1536 (text-embedding-3-small)")
    print(f"\nTest URL: {test_urls[0]}")
    print(f"\nStarting crawl with Qdrant integration...\n")

    # Create and run crawler
    try:
        crawler = TaxDataCrawler(
            settings=settings,
            max_depth=1,  # Shallow crawl for testing
            use_qdrant=True,
            qdrant_host=settings.QDRANT_HOST,
            qdrant_port=settings.QDRANT_PORT,
        )

        await crawler.run(test_urls)

        print("\n" + "="*50)
        print("✓ Integration test complete")
        print("="*50)

    except Exception as e:
        print(f"\n✗ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\nVerification Steps:")
    print("  1. Check Qdrant dashboard: http://localhost:6333/dashboard")
    print("     - Look for 'tax_documents' collection")
    print("     - Verify vector dimension is 1536")
    print("     - Check document count matches crawled pages")
    print("\n  2. Check Docker logs:")
    print("     docker-compose logs qdrant | grep -i 'upsert'")
    print("\n  3. Expected behavior:")
    print("     - Documents scraped and saved to filesystem")
    print("     - Embeddings generated in batches of 3")
    print("     - Documents stored in Qdrant with 1536-dim vectors")
    print("     - Final statistics show Qdrant document count")
    print("\n  4. Cost estimate:")
    print("     - ~$0.001-0.01 depending on pages crawled")
    print("     - OpenAI text-embedding-3-small: $0.00002/1K tokens")


if __name__ == '__main__':
    asyncio.run(main())
