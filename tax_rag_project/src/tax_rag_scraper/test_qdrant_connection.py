"""Test Qdrant connection with OpenAI embeddings"""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from tax_rag_scraper.storage.qdrant_client import TaxDataQdrantClient
from tax_rag_scraper.utils.embeddings import EmbeddingService

# Load environment variables from .env
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print("[OK] Loaded environment from .env")
else:
    print("[WARNING] .env file not found")


async def main():
    """Test Qdrant Cloud connection and OpenAI embeddings"""

    print("=" * 50)
    print("QDRANT CLOUD + OPENAI CONNECTION TEST")
    print("=" * 50)

    # Check OpenAI API key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("\n[ERROR] OPENAI_API_KEY not set")
        print("Set it in .env file or environment:")
        print("  export OPENAI_API_KEY=sk-proj-...")
        return

    print(f"[OK] OpenAI API key found: {api_key[:20]}...")

    # Load Qdrant Cloud configuration from environment
    qdrant_url = os.getenv("QDRANT_URL")
    qdrant_api_key = os.getenv("QDRANT_API_KEY")

    # Validate Qdrant credentials
    if not qdrant_url:
        print("\n[ERROR] QDRANT_URL not set")
        print("Get credentials at https://cloud.qdrant.io")
        print("Set in .env file:")
        print("  QDRANT_URL=https://your-cluster.cloud.qdrant.io")
        print("  QDRANT_API_KEY=your-api-key")
        return

    if not qdrant_api_key:
        print("\n[ERROR] QDRANT_API_KEY not set")
        print("Get credentials at https://cloud.qdrant.io")
        print("Set in .env file:")
        print("  QDRANT_API_KEY=your-api-key")
        return

    # Test 1: Qdrant Cloud connection
    print(f"\n1. Testing Qdrant Cloud connection to {qdrant_url}...")
    try:
        client = TaxDataQdrantClient(
            url=qdrant_url,
            api_key=qdrant_api_key,
            collection_name="test_collection",
            vector_size=1536,
        )
        print("[OK] Connected to Qdrant Cloud successfully")
        print("[OK] Collection 'test_collection' ready (1536 dimensions)")
    except Exception as e:
        print(f"[ERROR] Failed to connect: {e}")
        print("\nCheck your Qdrant Cloud credentials in .env:")
        print("  QDRANT_URL=https://your-cluster.cloud.qdrant.io")
        print("  QDRANT_API_KEY=your-api-key")
        print("\nGet credentials at https://cloud.qdrant.io")
        return

    # Test 2: Embedding service
    print("\n2. Testing OpenAI embedding service...")
    try:
        embedding_service = EmbeddingService(model_name="text-embedding-3-small", api_key=api_key)
        print("[OK] OpenAI embedding service initialized")
        print(f"  Model: {embedding_service.model_name}")
        print(f"  Vector size: {embedding_service.vector_size}")
    except Exception as e:
        print(f"[ERROR] Failed to initialize embedding service: {e}")
        return

    # Test 3: Generate embeddings
    print("\n3. Testing embedding generation...")
    try:
        test_docs = [
            {
                "title": "Income Tax Act",
                "content": "The Income Tax Act governs taxation in Canada.",
                "url": "https://test.example.com/doc1",
            },
            {
                "title": "GST/HST Guide",
                "content": "Guide to Goods and Services Tax and Harmonized Sales Tax.",
                "url": "https://test.example.com/doc2",
            },
        ]

        embeddings = await embedding_service.embed_documents(test_docs)
        print(f"[OK] Generated embeddings for {len(embeddings)} documents")
        print(f"  Embedding dimensions: {len(embeddings[0])}")

        if len(embeddings[0]) != 1536:
            print(f"[WARNING] Expected 1536 dimensions, got {len(embeddings[0])}")
    except Exception as e:
        print(f"[ERROR] Failed to generate embeddings: {e}")
        return

    # Test 4: Store in Qdrant
    print("\n4. Testing document storage...")
    try:
        await client.store_documents(test_docs, embeddings)
        print(f"[OK] Stored {len(test_docs)} documents in Qdrant")

        doc_count = client.count_documents()
        print(f"  Total documents in collection: {doc_count}")
    except Exception as e:
        print(f"[ERROR] Failed to store documents: {e}")
        return

    # Test 5: Similarity search
    print("\n5. Testing similarity search...")
    try:
        query = "Canadian tax regulations"
        query_embedding = await embedding_service.embed_query(query)

        results = client.search(query_vector=query_embedding, limit=2)

        print(f"[OK] Search completed for query: '{query}'")
        print(f"  Found {len(results)} results:")

        for i, result in enumerate(results, 1):
            print(f"\n  Result {i}:")
            print(f"    Score: {result.score:.4f}")
            print(f"    Title: {result.payload['title']}")
            print(f"    URL: {result.payload['url']}")
    except Exception as e:
        print(f"[ERROR] Failed to search: {e}")
        return

    # Test 6: Cleanup
    print("\n6. Cleaning up test collection...")
    try:
        client.delete_collection()
        print("[OK] Test collection deleted")
    except Exception as e:
        print(f"[WARNING] Failed to delete test collection: {e}")

    print("\n" + "=" * 50)
    print("ALL TESTS PASSED")
    print("=" * 50)
    print("\nQdrant Cloud is ready for use!")
    print("Next steps:")
    print("  - Run integration test: python src/tax_rag_scraper/test_qdrant_integration.py")
    print("  - Check Qdrant Cloud dashboard at https://cloud.qdrant.io")
    print("  - View your collections and monitor usage")


if __name__ == "__main__":
    asyncio.run(main())
