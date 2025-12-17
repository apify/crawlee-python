from openai import AsyncOpenAI
from typing import List, Optional
import logging
import os

logger = logging.getLogger(__name__)

class EmbeddingService:
    """Service for generating text embeddings using OpenAI's API"""

    def __init__(
        self,
        model_name: str = 'text-embedding-3-small',
        api_key: Optional[str] = None
    ):
        """
        Initialize OpenAI embedding service

        Args:
            model_name: OpenAI embedding model name
                - text-embedding-3-small: 1536 dimensions, cost-effective (default)
                - text-embedding-3-large: 3072 dimensions, higher quality
            api_key: OpenAI API key (if not provided, reads from OPENAI_API_KEY env var)

        Raises:
            ValueError: If API key is not provided or found in environment
        """
        self.model_name = model_name
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')

        if not self.api_key:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY environment variable.")

        self.client = AsyncOpenAI(api_key=self.api_key)
        self.vector_size = 1536  # text-embedding-3-small

        logger.info(f"Initialized OpenAI embeddings: {self.model_name}, {self.vector_size} dims")

    async def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding vectors (each is a list of floats)
        """
        if not texts:
            return []

        try:
            response = await self.client.embeddings.create(
                model=self.model_name,
                input=texts,
                encoding_format="float"
            )

            embeddings = [item.embedding for item in response.data]
            logger.info(f"Generated {len(embeddings)} embeddings, {response.usage.total_tokens} tokens")

            return embeddings
        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            raise

    async def embed_documents(self, documents: List[dict]) -> List[List[float]]:
        """
        Generate embeddings from document dictionaries

        Combines title and content for richer semantic representation

        Args:
            documents: List of document dicts (must have 'title' and 'content' keys)

        Returns:
            List of embedding vectors
        """
        # Combine title and content for better context
        # Format: "Title: <title>\nContent: <content>"
        texts = [
            f"Title: {doc.get('title', '')}\nContent: {doc.get('content', '')}"
            for doc in documents
        ]

        return await self.embed_texts(texts)

    async def embed_query(self, query: str) -> List[float]:
        """
        Generate embedding for a single search query

        Args:
            query: Search query string

        Returns:
            Single embedding vector
        """
        embeddings = await self.embed_texts([query])
        return embeddings[0] if embeddings else []
