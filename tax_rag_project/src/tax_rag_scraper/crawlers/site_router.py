from typing import TYPE_CHECKING
from urllib.parse import urlparse

from tax_rag_scraper.handlers.base_handler import BaseHandler
from tax_rag_scraper.handlers.cra_handler import CRAHandler
from tax_rag_scraper.models.tax_document import TaxDocument

if TYPE_CHECKING:
    from crawlee.beautifulsoup_crawler import BeautifulSoupCrawlingContext


class SiteRouter:
    """Routes URLs to appropriate handlers based on domain."""

    def __init__(self) -> None:
        # Map domains to their specialized handlers
        self.handlers: dict[str, BaseHandler] = {
            'canada.ca': CRAHandler(),
            # Future: Add more site handlers here
            # 'irs.gov': IRSHandler(),
            # 'gov.uk': HMRCHandler(),
        }

        # Fallback handler for unknown domains
        # Note: BaseHandler._extract_data raises NotImplementedError
        # So we need a concrete implementation
        self.default_handler = self._create_default_handler()

    def _create_default_handler(self) -> BaseHandler:
        """Create a concrete default handler for unknown sites."""

        class DefaultExtractor(BaseHandler):
            async def _extract_data(
                self, context: 'BeautifulSoupCrawlingContext'
            ) -> TaxDocument | None:
                # Basic extraction for any site
                soup = context.soup
                title = soup.title.string if soup.title else 'No title'

                # Get first 1000 characters of text
                content = soup.get_text(strip=True)[:1000]

                return TaxDocument(
                    url=str(context.request.url),
                    title=title,
                    content=content,
                    document_type='Unknown',
                    metadata={'source': 'Unknown'},
                )

        return DefaultExtractor()

    def get_handler(self, url: str) -> BaseHandler:
        """Get appropriate handler for URL based on domain.

        Supports exact matches and subdomain matches.
        Example: www.canada.ca and sub.canada.ca both match 'canada.ca'.
        """
        domain = urlparse(url).netloc

        # Check for exact match or subdomain match
        for registered_domain, handler in self.handlers.items():
            if domain.endswith(registered_domain):
                return handler

        return self.default_handler
