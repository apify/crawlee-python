import asyncio
import logging
import os

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

# Token limits for OpenAI embeddings API
MAX_SAFE_TOKEN_LIMIT = 7000  # Safety margin from 8,192 limit
TOKEN_WARNING_THRESHOLD = 6000  # Warn when approaching limit


class EmbeddingService:
    """Service for generating text embeddings using OpenAI's API."""

    def __init__(self, model_name: str = 'text-embedding-3-small', api_key: str | None = None) -> None:
        """Initialize OpenAI embedding service.

        Args:
            model_name: OpenAI embedding model name.
                - text-embedding-3-small: 1536 dimensions, cost-effective (default)
                - text-embedding-3-large: 3072 dimensions, higher quality
            api_key: OpenAI API key (if not provided, reads from OPENAI_API_KEY env var).

        Raises:
            ValueError: If API key is not provided or found in environment.
        """
        self.model_name = model_name
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')

        if not self.api_key:
            raise ValueError('OpenAI API key required. Set OPENAI_API_KEY environment variable.')

        self.client = AsyncOpenAI(api_key=self.api_key)
        self.vector_size = 1536  # text-embedding-3-small

        logger.info(f'Initialized OpenAI embeddings: {self.model_name}, {self.vector_size} dims')

    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count for a text string.

        Uses conservative ratio: 1 token ≈ 3 characters.

        Args:
            text: Text to estimate tokens for.

        Returns:
            Estimated token count.
        """
        return len(text) // 3

    def _chunk_text(self, text: str, max_words: int = 1200, overlap_words: int = 200) -> list[str]:
        """Split text into chunks that fit within token limits with overlap.

        Uses word-based chunking with conservative token estimation.

        Strategy:
            - Max 1200 words per chunk (~8,400 chars, ~2,800 tokens)
            - 200-word overlap between chunks (~1,400 chars) for context preservation
            - Conservative estimate: 1 token ≈ 3 characters
            - Target max: 7,000 tokens per chunk (safety margin from 8,192 limit)

        Args:
            text: Text to chunk.
            max_words: Maximum words per chunk (default 1200).
            overlap_words: Number of words to overlap between chunks (default 200).

        Returns:
            List of text chunks with overlap, each guaranteed under token limit.
        """
        # Calculate max characters based on word limit
        # Average 7 characters per word (includes spaces/punctuation)
        max_chars = max_words * 7
        overlap_chars = overlap_words * 7

        if len(text) <= max_chars:
            # Validate even small texts don't exceed token limit
            estimated_tokens = self._estimate_tokens(text)
            if estimated_tokens > MAX_SAFE_TOKEN_LIMIT:
                # Recursively split if too large
                logger.warning(f'Text too large ({estimated_tokens} tokens), splitting recursively')
                return self._chunk_text(text, max_words=max_words // 2, overlap_words=overlap_words // 2)
            return [text]

        chunks = []
        current_pos = 0

        while current_pos < len(text):
            chunk_end = current_pos + max_chars

            # Try to break at sentence boundary
            if chunk_end < len(text):
                # Look for sentence ending within last 500 chars
                search_start = max(current_pos, chunk_end - 500)
                sentence_ends = ['.', '!', '?', '\n\n']
                best_break = chunk_end

                for i in range(chunk_end, search_start, -1):
                    if text[i : i + 1] in sentence_ends:
                        best_break = i + 1
                        break

                chunk_end = best_break

            chunk = text[current_pos:chunk_end].strip()

            # Validate chunk size before adding
            estimated_tokens = self._estimate_tokens(chunk)
            if estimated_tokens > MAX_SAFE_TOKEN_LIMIT:
                # Chunk still too large, split it further
                logger.warning(f'Chunk too large ({estimated_tokens} tokens), splitting further')
                sub_chunks = self._chunk_text(chunk, max_words=max_words // 2, overlap_words=overlap_words // 2)
                chunks.extend(sub_chunks)
            else:
                chunks.append(chunk)

            # Move position forward, but step back by overlap amount for next chunk
            # Only apply overlap if there's more text to process
            if chunk_end < len(text):
                # Find a good overlap point (prefer word boundary)
                overlap_start = max(current_pos, chunk_end - overlap_chars)

                # Try to start overlap at a word boundary
                # Look backwards from chunk_end for a space
                for i in range(chunk_end - overlap_chars, chunk_end):
                    if text[i : i + 1] == ' ':
                        overlap_start = i + 1
                        break

                current_pos = overlap_start
            else:
                current_pos = chunk_end

        return chunks

    async def embed_texts(self, texts: list[str], max_retries: int = 3) -> list[list[float]]:
        """Generate embeddings for a list of texts with automatic retry on rate limits.

        Includes pre-flight validation to catch oversized chunks.

        Args:
            texts: List of text strings to embed.
            max_retries: Maximum number of retry attempts for rate limits.

        Returns:
            List of embedding vectors (each is a list of floats).
        """
        if not texts:
            return []

        # Pre-flight validation: check all texts are under token limit
        for i, text in enumerate(texts):
            estimated_tokens = self._estimate_tokens(text)
            if estimated_tokens > MAX_SAFE_TOKEN_LIMIT:
                logger.error(
                    f'Text {i} too large: {estimated_tokens} estimated tokens '
                    f'({len(text)} chars). Max safe limit: {MAX_SAFE_TOKEN_LIMIT} tokens'
                )
                raise ValueError(
                    f'Text chunk exceeds safe token limit: {estimated_tokens} > {MAX_SAFE_TOKEN_LIMIT}. '
                    f'This should have been caught by chunking logic.'
                )
            if estimated_tokens > TOKEN_WARNING_THRESHOLD:
                logger.warning(f'Text {i} approaching limit: {estimated_tokens} estimated tokens ({len(text)} chars)')

        for attempt in range(max_retries):
            try:
                response = await self.client.embeddings.create(
                    model=self.model_name, input=texts, encoding_format='float'
                )

                embeddings = [item.embedding for item in response.data]
                logger.info(f'Generated {len(embeddings)} embeddings, {response.usage.total_tokens} tokens')

                return embeddings  # noqa: TRY300

            except Exception as e:  # noqa: PERF203
                error_msg = str(e)

                # Check if it's a token limit error
                if 'maximum context length' in error_msg or ('requested' in error_msg and 'tokens' in error_msg):
                    logger.exception('Token limit exceeded despite validation')
                    # Log diagnostic info before raising
                    logger.info(f'Text lengths: {[len(t) for t in texts]}')
                    logger.info(f'Estimated tokens: {[self._estimate_tokens(t) for t in texts]}')
                    raise

                # Check if it's a rate limit error
                if 'rate_limit_exceeded' in error_msg or '429' in error_msg:
                    wait_time = 2**attempt * 3  # 3s, 6s, 12s
                    logger.warning(f'Rate limit hit, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})')
                    await asyncio.sleep(wait_time)

                    if attempt == max_retries - 1:
                        logger.exception('Max retries reached for rate limit')
                        raise
                else:
                    logger.exception('Error generating embeddings')
                    raise

        return []

    async def embed_documents(self, documents: list[dict]) -> list[list[float]]:
        """Generate embeddings from document dictionaries.

        Handles oversized documents by chunking and averaging embeddings.
        Combines title and content for richer semantic representation.

        Args:
            documents: List of document dicts (must have 'title' and 'content' keys).

        Returns:
            List of embedding vectors.
        """
        embeddings = []

        for doc in documents:
            title = doc.get('title', '')
            content = doc.get('content', '')
            full_text = f'Title: {title}\nContent: {content}'

            # Check if text needs chunking
            chunks = self._chunk_text(full_text)

            if len(chunks) == 1:
                # Single chunk - normal processing
                chunk_embeddings = await self.embed_texts(chunks)
                embeddings.append(chunk_embeddings[0])
            else:
                # Multiple chunks - generate embedding for each and average
                logger.warning(f'Document too large ({len(full_text)} chars), splitting into {len(chunks)} chunks')
                chunk_embeddings = await self.embed_texts(chunks)

                # Average the embeddings
                avg_embedding = [
                    sum(emb[i] for emb in chunk_embeddings) / len(chunk_embeddings)
                    for i in range(len(chunk_embeddings[0]))
                ]
                embeddings.append(avg_embedding)

        return embeddings

    async def embed_query(self, query: str) -> list[float]:
        """Generate embedding for a single search query.

        Args:
            query: Search query string.

        Returns:
            Single embedding vector.
        """
        embeddings = await self.embed_texts([query])
        return embeddings[0] if embeddings else []
