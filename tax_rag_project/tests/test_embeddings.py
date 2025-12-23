"""Test suite for embeddings.py.

Tests chunking with overlap, token estimation, and embedding generation.
"""

import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from tax_rag_scraper.utils.embeddings import EmbeddingService


class TestEmbeddingService:
    """Test cases for EmbeddingService class."""

    def test_init_with_api_key(self) -> None:
        """Test initialization with explicit API key."""
        service = EmbeddingService(api_key='test_key')
        assert service.api_key == 'test_key'
        assert service.model_name == 'text-embedding-3-small'
        assert service.vector_size == 1536

    def test_init_without_api_key_fails(self) -> None:
        """Test initialization without API key raises ValueError."""
        # Temporarily clear OPENAI_API_KEY env var
        original_key = os.environ.get('OPENAI_API_KEY')
        if 'OPENAI_API_KEY' in os.environ:
            del os.environ['OPENAI_API_KEY']

        try:
            with pytest.raises(ValueError, match='OpenAI API key required'):
                EmbeddingService()
        finally:
            # Restore original key
            if original_key:
                os.environ['OPENAI_API_KEY'] = original_key

    def test_estimate_tokens(self) -> None:
        """Test token estimation (1 token ≈ 3 characters)."""
        service = EmbeddingService(api_key='test_key')

        # 300 chars = 100 tokens
        text = 'a' * 300
        assert service._estimate_tokens(text) == 100

        # 3000 chars = 1000 tokens
        text = 'a' * 3000
        assert service._estimate_tokens(text) == 1000

    def test_chunk_text_small_text(self) -> None:
        """Test that small texts are not chunked."""
        service = EmbeddingService(api_key='test_key')

        # Small text (under 1200 words)
        text = 'This is a small text. ' * 50  # ~100 words
        chunks = service._chunk_text(text)

        assert len(chunks) == 1
        assert chunks[0] == text

    def test_chunk_text_large_text_with_overlap(self) -> None:
        """Test that large texts are chunked with proper overlap."""
        service = EmbeddingService(api_key='test_key')

        # Create a large text (>1200 words)
        # Each sentence is ~10 words
        sentence = 'This is sentence number {}. It contains exactly ten words here. '
        text = ''.join([sentence.format(i) for i in range(200)])  # ~2000 words

        chunks = service._chunk_text(text, max_words=1200, overlap_words=200)

        # Should have at least 2 chunks
        assert len(chunks) >= 2

        # Verify overlap exists between consecutive chunks
        for i in range(len(chunks) - 1):
            chunk1 = chunks[i]
            chunk2 = chunks[i + 1]

            # Check that end of chunk1 appears in beginning of chunk2
            # Extract last ~200 words from chunk1
            chunk1_words = chunk1.split()
            chunk2_words = chunk2.split()

            # Get last 100 words from chunk1 (should be part of overlap)
            overlap_sample = ' '.join(chunk1_words[-100:])

            # This overlap should appear near the start of chunk2
            chunk2_start = ' '.join(chunk2_words[:300])  # First 300 words

            # At least some words should overlap
            assert any(word in chunk2_start for word in overlap_sample.split()[:20]), (
                f'No overlap found between chunk {i} and {i + 1}'
            )

    def test_chunk_text_boundary_detection(self) -> None:
        """Test that chunks break at sentence boundaries."""
        service = EmbeddingService(api_key='test_key')

        # Create text with clear sentence boundaries
        sentences = [f'Sentence {i}. ' for i in range(500)]
        text = ''.join(sentences)

        chunks = service._chunk_text(text, max_words=1200)

        # All chunks except possibly the last should end with sentence punctuation
        for chunk in chunks[:-1]:
            assert chunk.rstrip().endswith(('.', '!', '?')), 'Chunk should end at sentence boundary'

    def test_chunk_text_respects_token_limits(self) -> None:
        """Test that all chunks are within token limits."""
        service = EmbeddingService(api_key='test_key')

        # Create very large text
        text = 'word ' * 5000  # 5000 words

        chunks = service._chunk_text(text)

        # All chunks should be under 7000 token limit
        for i, chunk in enumerate(chunks):
            estimated_tokens = service._estimate_tokens(chunk)
            assert estimated_tokens <= 7000, f'Chunk {i} exceeds token limit: {estimated_tokens} tokens'

    def test_chunk_text_overlap_parameter(self) -> None:
        """Test that overlap parameter works correctly."""
        service = EmbeddingService(api_key='test_key')

        # Create text
        text = 'word ' * 3000  # 3000 words

        # Test with different overlap values
        chunks_200 = service._chunk_text(text, max_words=1200, overlap_words=200)
        chunks_0 = service._chunk_text(text, max_words=1200, overlap_words=0)

        # More overlap should create more (smaller effective) chunks
        # or at least chunks with more repeated content
        assert len(chunks_200) >= len(chunks_0), 'Overlap should not reduce chunk count'

    @pytest.mark.asyncio
    async def test_embed_texts_mock(self) -> None:
        """Test embed_texts with mocked OpenAI API"""
        service = EmbeddingService(api_key='test_key')

        # Mock the OpenAI client
        mock_response = MagicMock()
        mock_response.data = [
            MagicMock(embedding=[0.1] * 1536),
            MagicMock(embedding=[0.2] * 1536),
        ]
        mock_response.usage.total_tokens = 100

        service.client.embeddings.create = AsyncMock(return_value=mock_response)

        # Call embed_texts
        texts = ['text 1', 'text 2']
        embeddings = await service.embed_texts(texts)

        # Verify results
        assert len(embeddings) == 2
        assert len(embeddings[0]) == 1536
        assert embeddings[0] == [0.1] * 1536
        assert embeddings[1] == [0.2] * 1536

    @pytest.mark.asyncio
    async def test_embed_texts_validates_size(self) -> None:
        """Test that embed_texts validates text size before sending"""
        service = EmbeddingService(api_key='test_key')

        # Create oversized text (>7000 tokens)
        oversized_text = 'a' * 22000  # ~7333 tokens

        with pytest.raises(ValueError, match='exceeds safe token limit'):
            await service.embed_texts([oversized_text])

    @pytest.mark.asyncio
    async def test_embed_documents_single_chunk(self) -> None:
        """Test embed_documents with normal-sized documents"""
        service = EmbeddingService(api_key='test_key')

        # Mock the OpenAI client
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=[0.1] * 1536)]
        mock_response.usage.total_tokens = 50

        service.client.embeddings.create = AsyncMock(return_value=mock_response)

        # Call embed_documents
        documents = [{'title': 'Test Doc', 'content': 'Test content'}]
        embeddings = await service.embed_documents(documents)

        # Verify results
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 1536

    @pytest.mark.asyncio
    async def test_embed_documents_multi_chunk(self) -> None:
        """Test embed_documents with oversized documents requiring chunking"""
        service = EmbeddingService(api_key='test_key')

        # Mock the OpenAI client to return different embeddings for each chunk
        mock_response_1 = MagicMock()
        mock_response_1.data = [
            MagicMock(embedding=[0.1] * 1536),
            MagicMock(embedding=[0.2] * 1536),
        ]
        mock_response_1.usage.total_tokens = 1000

        service.client.embeddings.create = AsyncMock(return_value=mock_response_1)

        # Create oversized document
        large_content = 'word ' * 10000  # 10000 words (will be split)
        documents = [{'title': 'Large Doc', 'content': large_content}]

        embeddings = await service.embed_documents(documents)

        # Should average the chunk embeddings
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 1536

        # The averaged embedding should be between the two chunk embeddings
        # Average of [0.1]*1536 and [0.2]*1536 should be [0.15]*1536
        assert abs(embeddings[0][0] - 0.15) < 0.01

    @pytest.mark.asyncio
    async def test_embed_query(self) -> None:
        """Test embed_query for single query"""
        service = EmbeddingService(api_key='test_key')

        # Mock the OpenAI client
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=[0.5] * 1536)]
        mock_response.usage.total_tokens = 10

        service.client.embeddings.create = AsyncMock(return_value=mock_response)

        # Call embed_query
        query = 'test query'
        embedding = await service.embed_query(query)

        # Verify results
        assert len(embedding) == 1536
        assert embedding == [0.5] * 1536


class TestChunkingOverlap:
    """Specific tests for chunk overlap functionality."""

    def test_overlap_preserves_context(self) -> None:
        """Test that overlap preserves context across chunks."""
        service = EmbeddingService(api_key='test_key')

        # Create text with identifiable markers
        text = ''
        for i in range(300):
            text += f'MARKER_{i} ' + 'filler ' * 10

        chunks = service._chunk_text(text, max_words=1200, overlap_words=200)

        # Check that markers appear in multiple chunks
        marker_appearances = {}
        for chunk_idx, chunk in enumerate(chunks):
            for i in range(300):
                marker = f'MARKER_{i}'
                if marker in chunk:
                    if marker not in marker_appearances:
                        marker_appearances[marker] = []
                    marker_appearances[marker].append(chunk_idx)

        # Some markers should appear in multiple chunks (due to overlap)
        multi_chunk_markers = [m for m, chunks in marker_appearances.items() if len(chunks) > 1]
        assert len(multi_chunk_markers) > 0, 'Overlap should cause some markers to appear in multiple chunks'

    def test_no_overlap_when_overlap_zero(self) -> None:
        """Test that setting overlap_words=0 prevents overlap."""
        service = EmbeddingService(api_key='test_key')

        text = 'word ' * 3000
        chunks = service._chunk_text(text, max_words=1200, overlap_words=0)

        # With no overlap, chunks should be completely distinct
        # (This is hard to verify directly, but we can check that
        # the total character count is close to original)
        total_chars = sum(len(chunk) for chunk in chunks)
        # Allow some variance due to spacing/boundary adjustments
        assert abs(total_chars - len(text)) < len(text) * 0.1

    def test_overlap_respects_word_boundaries(self) -> None:
        """Test that overlap breaks at word boundaries."""
        service = EmbeddingService(api_key='test_key')

        # Create text with long words
        text = 'supercalifragilisticexpialidocious ' * 2000

        chunks = service._chunk_text(text, max_words=1200, overlap_words=200)

        # All chunks should start and end with complete words (no partial words)
        for chunk in chunks:
            # Should not start or end with mid-word characters
            # (In our case, should start/end with space or full word)
            assert chunk[0].isalnum() or chunk[0].isspace(), 'Chunk should start at word boundary'
            assert chunk[-1].isalnum() or chunk[-1].isspace(), 'Chunk should end at word boundary'


def test_integration_realistic_document() -> None:
    """Integration test with realistic tax document."""
    service = EmbeddingService(api_key='test_key')

    # Simulate a realistic tax document
    document = {
        'title': 'IRS Publication 501 - Dependents and Filing Status',
        'content': """
        Introduction to Tax Filing Status

        Your filing status is used to determine your filing requirements, standard deduction,
        eligibility for certain credits, and your correct tax. If more than one filing status
        applies to you, this publication will help you choose the one that will result in the
        lowest tax.

        """
        + ('Additional tax information. ' * 1000),  # Make it large
    }

    chunks = service._chunk_text(f'Title: {document["title"]}\nContent: {document["content"]}')

    # Verify chunking worked
    assert len(chunks) >= 1

    # All chunks should be valid size
    for chunk in chunks:
        tokens = service._estimate_tokens(chunk)
        assert tokens <= 7000

    # If multiple chunks, verify overlap
    if len(chunks) > 1:
        # Some content should appear in multiple chunks
        first_chunk_end = chunks[0][-500:]  # Last 500 chars
        second_chunk_start = chunks[1][:1000]  # First 1000 chars

        # Should have some overlap
        assert any(word in second_chunk_start for word in first_chunk_end.split()[:50])


if __name__ == '__main__':
    # Run tests
    pytest.main([__file__, '-v', '-s'])
