"""Main entry point for the tax documentation crawler."""

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler
from tax_rag_scraper.config.settings import Settings


async def main():
    """Run the tax documentation crawler with Qdrant Cloud integration."""

    # Load environment variables
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[OK] Loaded environment from .env")
    else:
        print("[WARNING] .env file not found, using environment variables")

    # Validate Qdrant Cloud credentials
    qdrant_url = os.getenv('QDRANT_URL')
    qdrant_api_key = os.getenv('QDRANT_API_KEY')

    if not qdrant_url:
        print("\n[ERROR] QDRANT_URL environment variable not set")
        print("\nTo use Qdrant Cloud:")
        print("  1. Visit https://cloud.qdrant.io")
        print("  2. Create a free account (1GB storage included)")
        print("  3. Create a new cluster")
        print("  4. Copy your cluster URL")
        print("  5. Add to .env file:")
        print("     QDRANT_URL=https://your-cluster.cloud.qdrant.io")
        print("     QDRANT_API_KEY=your-api-key")
        sys.exit(1)

    if not qdrant_api_key:
        print("\n[ERROR] QDRANT_API_KEY environment variable not set")
        print("\nGet your API key from https://cloud.qdrant.io")
        print("Add to .env file: QDRANT_API_KEY=your-api-key")
        sys.exit(1)

    # Validate OpenAI API key if using Qdrant
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        print("\n[ERROR] OPENAI_API_KEY environment variable not set")
        print("\nGet your API key from https://platform.openai.com/api-keys")
        print("Add to .env file: OPENAI_API_KEY=sk-proj-...")
        sys.exit(1)

    print(f"[OK] Qdrant Cloud URL: {qdrant_url}")
    print(f"[OK] OpenAI API key configured")

    # Configure settings
    settings = Settings()

    # Test with CRA website (Canadian Revenue Agency - public info pages)
    test_urls = [
        'https://www.canada.ca/en/revenue-agency/services/forms-publications.html'
    ]

    print(f"\n[INFO] Starting crawler with Qdrant Cloud integration")
    print(f"[INFO] Collection: {settings.QDRANT_COLLECTION}")
    print(f"[INFO] Max requests: {settings.MAX_REQUESTS_PER_CRAWL}")
    print(f"[INFO] Start URL: {test_urls[0]}\n")

    # Create crawler with Qdrant Cloud integration
    crawler = TaxDataCrawler(
        settings=settings,
        use_qdrant=settings.USE_QDRANT,
        qdrant_url=qdrant_url,
        qdrant_api_key=qdrant_api_key,
    )

    await crawler.run(test_urls)

    print("\n[OK] Crawler complete. Check storage/datasets/default/ for results.")
    print("[OK] View your data in Qdrant Cloud: https://cloud.qdrant.io")


if __name__ == '__main__':
    asyncio.run(main())
