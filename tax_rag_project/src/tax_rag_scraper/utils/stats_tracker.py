"""Statistics tracking for crawling operations."""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class CrawlStats:
    """Track statistics for a crawling session."""

    start_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    urls_processed: int = 0
    urls_failed: int = 0
    documents_extracted: int = 0

    def record_success(self) -> None:
        """Record a successful URL processing with document extraction."""
        self.urls_processed += 1
        self.documents_extracted += 1

    def record_failure(self) -> None:
        """Record a failed URL processing."""
        self.urls_processed += 1
        self.urls_failed += 1

    def summary(self) -> dict:
        """Generate a summary of crawl statistics.

        Returns:
            Dictionary containing duration, URLs processed, failures, success rate, and documents extracted
        """
        elapsed = (datetime.now(UTC) - self.start_time).total_seconds()
        return {
            'duration_seconds': elapsed,
            'urls_processed': self.urls_processed,
            'urls_failed': self.urls_failed,
            'success_rate': (
                (self.urls_processed - self.urls_failed) / self.urls_processed * 100 if self.urls_processed > 0 else 0
            ),
            'documents_extracted': self.documents_extracted,
        }

    def to_jsonl(self, crawl_type: str = 'standard') -> str:
        """Export statistics as a JSONL line for persistent storage.

        Args:
            crawl_type: Type of crawl ('daily', 'weekly-deep', 'standard')

        Returns:
            A single-line JSON string with timestamp and all statistics
        """
        stats = self.summary()
        stats['timestamp'] = datetime.now(UTC).isoformat()
        stats['start_time'] = self.start_time.isoformat()
        stats['crawl_type'] = crawl_type
        return json.dumps(stats)
