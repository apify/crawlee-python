"""Base handler for processing crawled pages with error handling."""

import traceback

from crawlee.crawlers import BeautifulSoupCrawlingContext
from tax_rag_scraper.models.tax_document import TaxDocument

HTTP_OK = 200


class BaseHandler:
    """Base handler with comprehensive error handling and logging."""

    async def handle(self, context: BeautifulSoupCrawlingContext) -> TaxDocument | None:
        """Handle a crawled page with error handling.

        Args:
            context: Crawlee's BeautifulSoup crawling context

        Returns:
            TaxDocument if successful, None if failed

        Raises:
            Exception: Re-raises exceptions to trigger Crawlee's retry mechanism
        """
        try:
            context.log.info(f'Processing {context.request.url}')

            # Validate response
            if context.http_response.status_code != HTTP_OK:
                context.log.warning(f'Non-200 status: {context.http_response.status_code}')
                return None

            # Extract data with error handling
            doc = await self._extract_data(context)

            if doc:
                await context.push_data(doc.model_dump())
                context.log.info(f'✓ Successfully processed {context.request.url}')
                return doc
            return None  # noqa: TRY300

        except Exception:
            context.log.exception(
                f'Error processing {context.request.url}\n{traceback.format_exc()}'
            )
            # Re-raise to trigger Crawlee's retry mechanism
            raise

    async def _extract_data(self, context: BeautifulSoupCrawlingContext) -> TaxDocument | None:
        """Extract data from the crawled page.

        Override in subclasses for specific extraction logic.

        Args:
            context: Crawlee's BeautifulSoup crawling context

        Returns:
            Extracted TaxDocument or None
        """
        raise NotImplementedError('Subclasses must implement _extract_data()')
