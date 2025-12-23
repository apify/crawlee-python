"""Qdrant Cloud client for storing tax documentation with vector embeddings."""

import logging
import uuid
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

logger = logging.getLogger(__name__)


class TaxDataQdrantClient:
    """Client for interacting with Qdrant Cloud vector database.

    This client handles:
    - Collection creation and management
    - Document storage with vector embeddings
    - Similarity search
    - Document counting and deletion
    """

    def __init__(
        self,
        url: str,
        api_key: str,
        collection_name: str = "tax_documents",
        vector_size: int = 1536,
    ):
        """Initialize Qdrant Cloud client.

        Args:
            url: Qdrant Cloud URL (e.g., 'https://xyz.cloud.qdrant.io')
            api_key: API key for authentication
            collection_name: Name of the collection to use
            vector_size: Dimension of embedding vectors (default: 1536 for OpenAI text-embedding-3-small)
        """
        self.url = url
        self.api_key = api_key
        self.collection_name = collection_name
        self.vector_size = vector_size

        # Initialize Qdrant client
        self.client = QdrantClient(
            url=url,
            api_key=api_key,
        )

        # Create collection if it doesn't exist
        self._ensure_collection_exists()

    def _ensure_collection_exists(self):
        """Create collection if it doesn't exist."""
        try:
            # Check if collection exists
            collections = self.client.get_collections().collections
            collection_names = [c.name for c in collections]

            if self.collection_name not in collection_names:
                logger.info(f"Creating collection '{self.collection_name}' with vector size {self.vector_size}")
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.vector_size,
                        distance=Distance.COSINE,
                    ),
                )
                logger.info(f"✓ Collection '{self.collection_name}' created successfully")
            else:
                logger.info(f"✓ Collection '{self.collection_name}' already exists")

        except Exception as e:
            logger.error(f"Error ensuring collection exists: {e}")
            raise

    async def store_documents(self, documents: list[dict[str, Any]], embeddings: list[list[float]]):
        """Store documents with their embeddings in Qdrant.

        Args:
            documents: List of document dictionaries with metadata
            embeddings: List of embedding vectors (one per document)
        """
        if len(documents) != len(embeddings):
            raise ValueError(f"Document count ({len(documents)}) must match embedding count ({len(embeddings)})")

        try:
            points = []
            for doc, embedding in zip(documents, embeddings):
                # Generate unique ID for the document
                point_id = str(uuid.uuid4())

                # Create point with embedding and payload
                point = PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "title": doc.get("title", ""),
                        "content": doc.get("content", ""),
                        "url": doc.get("url", ""),
                        "source": doc.get("source", ""),
                        "doc_type": doc.get("doc_type", ""),
                        "scraped_at": doc.get("scraped_at", ""),
                    },
                )
                points.append(point)

            # Upload points to Qdrant
            self.client.upsert(
                collection_name=self.collection_name,
                points=points,
            )

            logger.info(f"✓ Stored {len(points)} documents in Qdrant collection '{self.collection_name}'")

        except Exception as e:
            logger.error(f"Error storing documents in Qdrant: {e}")
            raise

    def search(self, query_vector: list[float], limit: int = 5):
        """Search for similar documents using a query vector.

        Args:
            query_vector: Embedding vector for the search query
            limit: Maximum number of results to return

        Returns:
            List of search results with scores and payloads
        """
        try:
            # Updated to use query_points() which is the correct method name
            results = self.client.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                limit=limit,
            )
            return results.points

        except Exception as e:
            logger.error(f"Error searching Qdrant: {e}")
            raise

    def count_documents(self) -> int:
        """Count total documents in the collection.

        Returns:
            Number of documents in the collection
        """
        try:
            collection_info = self.client.get_collection(collection_name=self.collection_name)
            return collection_info.points_count or 0

        except Exception as e:
            logger.error(f"Error counting documents: {e}")
            return 0

    def delete_collection(self):
        """Delete the entire collection.

        Warning: This permanently deletes all documents in the collection.
        """
        try:
            self.client.delete_collection(collection_name=self.collection_name)
            logger.info(f"✓ Collection '{self.collection_name}' deleted")

        except Exception as e:
            logger.error(f"Error deleting collection: {e}")
            raise

    def get_collection_info(self) -> dict[str, Any]:
        """Get information about the collection.

        Returns:
            Dictionary with collection metadata
        """
        try:
            info = self.client.get_collection(collection_name=self.collection_name)
            return {
                "name": self.collection_name,
                "points_count": info.points_count,
                "vector_size": info.config.params.vectors.size,
                "distance": info.config.params.vectors.distance,
            }

        except Exception as e:
            logger.error(f"Error getting collection info: {e}")
            raise
