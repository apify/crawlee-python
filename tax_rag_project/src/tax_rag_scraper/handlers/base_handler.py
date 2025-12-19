"""Base handler for processing crawled pages with error handling."""

import traceback
from typing import Optional

from crawlee.crawlers import BeautifulSoupCrawlingContext

from tax_rag_scraper.models.tax_document import TaxDocument


class BaseHandler:
    """Base handler with comprehensive error handling and logging."""

    async def handle(self, context: BeautifulSoupCrawlingContext) -> Optional[TaxDocument]:
        """Handle a crawled page with error handling.

        Args:
            context: Crawlee's BeautifulSoup crawling context

        Returns:
            TaxDocument if successful, None if failed

        Raises:
            Exception: Re-raises exceptions to trigger Crawlee's retry mechanism
        """
        try:
            context.log.info(f"Processing {context.request.url}")

            # Validate response
            if context.http_response.status_code != 200:
                context.log.warning(f"Non-200 status: {context.http_response.status_code}")
                return None

            # Extract data with error handling
            doc = await self._extract_data(context)

            if doc:
                await context.push_data(doc.model_dump())
                context.log.info(f"✓ Successfully processed {context.request.url}")

            return doc

        except Exception as e:
            context.log.error(f"Error processing {context.request.url}: {str(e)}\n{traceback.format_exc()}")
            # Re-raise to trigger Crawlee's retry mechanism
            raise

    async def _extract_data(self, context: BeautifulSoupCrawlingContext) -> Optional[TaxDocument]:
        """Extract data from the crawled page.

        Override in subclasses for specific extraction logic.

        Args:
            context: Crawlee's BeautifulSoup crawling context

        Returns:
            Extracted TaxDocument or None
        """
        raise NotImplementedError("Subclasses must implement _extract_data()")
