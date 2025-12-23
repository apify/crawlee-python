import re

from crawlee.crawlers import BeautifulSoupCrawlingContext
from tax_rag_scraper.handlers.base_handler import BaseHandler
from tax_rag_scraper.models.tax_document import TaxDocument


class CRAHandler(BaseHandler):
    """Handler for Canada Revenue Agency (canada.ca) website."""

    async def _extract_data(self, context: BeautifulSoupCrawlingContext) -> TaxDocument | None:
        soup = context.soup

        # Extract CRA-specific data using their HTML structure
        title = soup.find('h1')
        title_text = title.get_text(strip=True) if title else 'No title'

        # Find main content area (CRA uses <main> tag)
        main_content = soup.find('main') or soup.find('div', class_='content')
        content = main_content.get_text(strip=True) if main_content else ''

        # Extract metadata specific to CRA
        metadata = {
            'source': 'CRA',
            'language': soup.find('html').get('lang', 'en') if soup.find('html') else 'en',
            'domain': 'canada.ca',
        }

        # Look for tax year indicators in the content
        tax_year = self._extract_tax_year(content)

        # Determine document type based on URL patterns
        document_type = self._determine_document_type(str(context.request.url))

        return TaxDocument(
            url=str(context.request.url),
            title=title_text,
            content=content,
            document_type=document_type,
            tax_year=tax_year,
            metadata=metadata,
        )

    def _extract_tax_year(self, text: str) -> str | None:
        """Extract tax year from content.

        Looks for patterns like: 2024, 2023-2024.
        """
        match = re.search(r'20\d{2}(?:\s*-\s*20\d{2})?', text)
        return match.group(0) if match else None

    def _determine_document_type(self, url: str) -> str:
        """Determine document type based on URL patterns."""
        url_lower = url.lower()

        if 'form' in url_lower:
            return 'CRA_Form'
        if 'guide' in url_lower:
            return 'CRA_Guide'
        if 'publication' in url_lower:
            return 'CRA_Publication'
        if 'notice' in url_lower or 'bulletin' in url_lower:
            return 'CRA_Bulletin'
        return 'CRA_General'
