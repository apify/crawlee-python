"""Test Qdrant connection with OpenAI embeddings"""
import asyncio
import os
from tax_rag_scraper.storage.qdrant_client import TaxDataQdrantClient
from tax_rag_scraper.utils.embeddings import EmbeddingService


async def main():
    """Test Qdrant connection and OpenAI embeddings"""

    print("="*50)
    print("QDRANT + OPENAI CONNECTION TEST")
    print("="*50)

    # Check API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("\n✗ OPENAI_API_KEY not set")
        print("Set it in .env file or environment:")
        print("  export OPENAI_API_KEY=sk-proj-...")
        return

    print(f"✓ API key found: {api_key[:20]}...")

    # Test 1: Qdrant connection
    print("\n1. Testing Qdrant connection...")
    try:
        client = TaxDataQdrantClient(
            host="localhost",
            port=6333,
            collection_name="test_collection",
            vector_size=1536,
        )
        print("✓ Connected to Qdrant successfully")
        print(f"✓ Collection 'test_collection' ready (1536 dimensions)")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
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
        print(f"✓ OpenAI embedding service initialized")
        print(f"  Model: {embedding_service.model_name}")
        print(f"  Vector size: {embedding_service.vector_size}")
    except Exception as e:
        print(f"✗ Failed to initialize embedding service: {e}")
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
        print(f"✓ Generated embeddings for {len(embeddings)} documents")
        print(f"  Embedding dimensions: {len(embeddings[0])}")

        if len(embeddings[0]) != 1536:
            print(f"⚠ Warning: Expected 1536 dimensions, got {len(embeddings[0])}")
    except Exception as e:
        print(f"✗ Failed to generate embeddings: {e}")
        return

    # Test 4: Store in Qdrant
    print("\n4. Testing document storage...")
    try:
        await client.store_documents(test_docs, embeddings)
        print(f"✓ Stored {len(test_docs)} documents in Qdrant")

        doc_count = client.count_documents()
        print(f"  Total documents in collection: {doc_count}")
    except Exception as e:
        print(f"✗ Failed to store documents: {e}")
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

        print(f"✓ Search completed for query: '{query}'")
        print(f"  Found {len(results)} results:")

        for i, result in enumerate(results, 1):
            print(f"\n  Result {i}:")
            print(f"    Score: {result.score:.4f}")
            print(f"    Title: {result.payload['title']}")
            print(f"    URL: {result.payload['url']}")
    except Exception as e:
        print(f"✗ Failed to search: {e}")
        return

    # Test 6: Cleanup
    print("\n6. Cleaning up test collection...")
    try:
        client.delete_collection()
        print("✓ Test collection deleted")
    except Exception as e:
        print(f"⚠ Failed to delete test collection: {e}")

    print("\n" + "="*50)
    print("ALL TESTS PASSED ✓")
    print("="*50)
    print("\nQdrant is ready for use!")
    print("Next steps:")
    print("  - Run integration test: python src/tax_rag_scraper/test_qdrant_integration.py")
    print("  - Check dashboard: http://localhost:6333/dashboard")
    print("  - Check Docker logs: docker-compose logs qdrant")


if __name__ == '__main__':
    asyncio.run(main())
