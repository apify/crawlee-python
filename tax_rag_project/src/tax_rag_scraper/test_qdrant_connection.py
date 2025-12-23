"""Test Qdrant connection with OpenAI embeddings."""

import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from tax_rag_scraper.storage.qdrant_client import TaxDataQdrantClient
from tax_rag_scraper.utils.embeddings import EmbeddingService

logger = logging.getLogger(__name__)

# Expected embedding dimension for OpenAI text-embedding-3-small model
EXPECTED_EMBEDDING_DIM = 1536

# Load environment variables from .env
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
    logger.info('[OK] Loaded environment from .env')
else:
    logger.warning('[WARNING] .env file not found')


async def main() -> None:
    """Test Qdrant Cloud connection and OpenAI embeddings."""
    logger.info('=' * 50)
    logger.info('QDRANT CLOUD + OPENAI CONNECTION TEST')
    logger.info('=' * 50)

    # Check OpenAI API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error('\n[ERROR] OPENAI_API_KEY not set')
        logger.error('Set it in .env file or environment:')
        logger.error('  export OPENAI_API_KEY=sk-proj-...')
        return

    logger.info('[OK] OpenAI API key found: %s...', api_key[:20])

    # Load Qdrant Cloud configuration from environment
    qdrant_url = os.getenv('QDRANT_URL')
    qdrant_api_key = os.getenv('QDRANT_API_KEY')

    # Validate Qdrant credentials
    if not qdrant_url:
        logger.error('\n[ERROR] QDRANT_URL not set')
        logger.error('Get credentials at https://cloud.qdrant.io')
        logger.error('Set in .env file:')
        logger.error('  QDRANT_URL=https://your-cluster.cloud.qdrant.io')
        logger.error('  QDRANT_API_KEY=your-api-key')
        return

    if not qdrant_api_key:
        logger.error('\n[ERROR] QDRANT_API_KEY not set')
        logger.error('Get credentials at https://cloud.qdrant.io')
        logger.error('Set in .env file:')
        logger.error('  QDRANT_API_KEY=your-api-key')
        return

    # Test 1: Qdrant Cloud connection
    logger.info('\n1. Testing Qdrant Cloud connection to %s...', qdrant_url)
    try:
        client = TaxDataQdrantClient(
            url=qdrant_url,
            api_key=qdrant_api_key,
            collection_name='test_collection',
            vector_size=1536,
        )
        logger.info('[OK] Connected to Qdrant Cloud successfully')
        logger.info("[OK] Collection 'test_collection' ready (%d dimensions)", EXPECTED_EMBEDDING_DIM)
    except Exception:
        logger.exception(
            '[ERROR] Failed to connect\n'
            'Check your Qdrant Cloud credentials in .env:\n'
            '  QDRANT_URL=https://your-cluster.cloud.qdrant.io\n'
            '  QDRANT_API_KEY=your-api-key\n'
            'Get credentials at https://cloud.qdrant.io'
        )
        return

    # Test 2: Embedding service
    logger.info('\n2. Testing OpenAI embedding service...')
    try:
        embedding_service = EmbeddingService(model_name='text-embedding-3-small', api_key=api_key)
        logger.info('[OK] OpenAI embedding service initialized')
        logger.info('  Model: %s', embedding_service.model_name)
        logger.info('  Vector size: %d', embedding_service.vector_size)
    except Exception:
        logger.exception('[ERROR] Failed to initialize embedding service')
        return

    # Test 3: Generate embeddings
    logger.info('\n3. Testing embedding generation...')
    try:
        test_docs = [
            {
                'title': 'Income Tax Act',
                'content': 'The Income Tax Act governs taxation in Canada.',
                'url': 'https://test.example.com/doc1',
            },
            {
                'title': 'GST/HST Guide',
                'content': 'Guide to Goods and Services Tax and Harmonized Sales Tax.',
                'url': 'https://test.example.com/doc2',
            },
        ]

        embeddings = await embedding_service.embed_documents(test_docs)
        logger.info('[OK] Generated embeddings for %d documents', len(embeddings))
        logger.info('  Embedding dimensions: %d', len(embeddings[0]))

        if len(embeddings[0]) != EXPECTED_EMBEDDING_DIM:
            logger.warning(
                '[WARNING] Expected %d dimensions, got %d',
                EXPECTED_EMBEDDING_DIM,
                len(embeddings[0]),
            )
    except Exception:
        logger.exception('[ERROR] Failed to generate embeddings')
        return

    # Test 4: Store in Qdrant
    logger.info('\n4. Testing document storage...')
    try:
        await client.store_documents(test_docs, embeddings)
        logger.info('[OK] Stored %d documents in Qdrant', len(test_docs))

        doc_count = client.count_documents()
        logger.info('  Total documents in collection: %d', doc_count)
    except Exception:
        logger.exception('[ERROR] Failed to store documents')
        return

    # Test 5: Similarity search
    logger.info('\n5. Testing similarity search...')
    try:
        query = 'Canadian tax regulations'
        query_embedding = await embedding_service.embed_query(query)

        results = client.search(query_vector=query_embedding, limit=2)

        logger.info("[OK] Search completed for query: '%s'", query)
        logger.info('  Found %d results:', len(results))

        for i, result in enumerate(results, 1):
            logger.info('\n  Result %d:', i)
            logger.info('    Score: %.4f', result.score)
            logger.info('    Title: %s', result.payload['title'])
            logger.info('    URL: %s', result.payload['url'])
    except Exception:
        logger.exception('[ERROR] Failed to search')
        return

    # Test 6: Cleanup
    logger.info('\n6. Cleaning up test collection...')
    try:
        client.delete_collection()
        logger.info('[OK] Test collection deleted')
    except Exception:
        logger.exception('[WARNING] Failed to delete test collection')

    logger.info('\n%s', '=' * 50)
    logger.info('ALL TESTS PASSED')
    logger.info('=' * 50)
    logger.info('\nQdrant Cloud is ready for use!')
    logger.info('Next steps:')
    logger.info('  - Run integration test: python src/tax_rag_scraper/test_qdrant_integration.py')
    logger.info('  - Check Qdrant Cloud dashboard at https://cloud.qdrant.io')
    logger.info('  - View your collections and monitor usage')


if __name__ == '__main__':
    asyncio.run(main())
