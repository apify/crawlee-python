"""Test Qdrant connection with OpenAI embeddings"""
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
from tax_rag_scraper.storage.qdrant_client import TaxDataQdrantClient
from tax_rag_scraper.utils.embeddings import EmbeddingService

# Load environment variables from .env
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"[OK] Loaded environment from .env")
else:
    print("[WARNING] .env file not found")


async def main():
    """Test Qdrant connection and OpenAI embeddings"""

    print("="*50)
    print("QDRANT + OPENAI CONNECTION TEST")
    print("="*50)

    # Check API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("\n[ERROR] OPENAI_API_KEY not set")
        print("Set it in .env file or environment:")
        print("  export OPENAI_API_KEY=sk-proj-...")
        return

    print(f"[OK] API key found: {api_key[:20]}...")

    # Load Qdrant configuration from environment
    qdrant_host = os.getenv('QDRANT_HOST', 'localhost')
    qdrant_port = int(os.getenv('QDRANT_PORT', '6333'))
    qdrant_api_key = os.getenv('QDRANT_API_KEY', '')
    qdrant_use_https = os.getenv('QDRANT_USE_HTTPS', 'false').lower() == 'true'

    # Test 1: Qdrant connection
    print(f"\n1. Testing Qdrant connection to {qdrant_host}...")
    try:
        client = TaxDataQdrantClient(
            host=qdrant_host,
            port=qdrant_port,
            api_key=qdrant_api_key if qdrant_api_key else None,
            use_https=qdrant_use_https,
            collection_name="test_collection",
            vector_size=1536,
        )
        connection_type = "Qdrant Cloud" if qdrant_api_key else "Local Qdrant"
        print(f"[OK] Connected to {connection_type} successfully")
        print(f"[OK] Collection 'test_collection' ready (1536 dimensions)")
    except Exception as e:
        print(f"[ERROR] Failed to connect: {e}")
        if qdrant_api_key:
            print("\nCheck your Qdrant Cloud credentials in .env.local:")
            print("  QDRANT_HOST=your-cluster.cloud.qdrant.io")
            print("  QDRANT_API_KEY=your-api-key")
        else:
            print("\nMake sure Qdrant is running:")
            print("  docker-compose up -d")
        return

    # Test 2: Embedding service
    print("\n2. Testing OpenAI embedding service...")
    try:
        embedding_service = EmbeddingService(
            model_name='text-embedding-3-small',
            api_key=api_key
        )
        print(f"[OK] OpenAI embedding service initialized")
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
                "url": "https://test.example.com/doc1"
            },
            {
                "title": "GST/HST Guide",
                "content": "Guide to Goods and Services Tax and Harmonized Sales Tax.",
                "url": "https://test.example.com/doc2"
            }
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

        results = client.search(
            query_vector=query_embedding,
            limit=2
        )

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

    print("\n" + "="*50)
    print("ALL TESTS PASSED")
    print("="*50)
    print("\nQdrant is ready for use!")
    print("Next steps:")
    print("  - Run integration test: python src/tax_rag_scraper/test_qdrant_integration.py")
    print("  - Check dashboard: http://localhost:6333/dashboard")
    print("  - Check Docker logs: docker-compose logs qdrant")


if __name__ == '__main__':
    asyncio.run(main())
