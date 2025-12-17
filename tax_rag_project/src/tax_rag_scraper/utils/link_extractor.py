from typing import Set
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

class LinkExtractor:
    """Extract and filter links from HTML pages for deep crawling"""

    def __init__(self, allowed_domains: Set[str] = None, max_depth: int = 3):
        """
        Args:
            allowed_domains: Set of domains to crawl (None = same domain only)
            max_depth: Maximum crawl depth (0 = seed URLs only, 1 = seed + 1 level)
        """
        self.allowed_domains = allowed_domains or set()
        self.max_depth = max_depth

    def extract_links(
        self,
        soup: BeautifulSoup,
        base_url: str,
        current_depth: int = 0
    ) -> Set[str]:
        """
        Extract valid links from page

        Args:
            soup: BeautifulSoup parsed HTML
            base_url: URL of current page (for resolving relative links)
            current_depth: Current crawl depth

        Returns:
            Set of absolute URLs to crawl next
        """
        # Stop if at max depth
        if current_depth >= self.max_depth:
            return set()

        links = set()
        base_domain = urlparse(base_url).netloc

        # Find all <a> tags with href attribute
        for link in soup.find_all('a', href=True):
            href = link['href']

            # Skip empty hrefs
            if not href or href.strip() == '':
                continue

            # Convert relative URLs to absolute
            absolute_url = urljoin(base_url, href)
            parsed = urlparse(absolute_url)

            # Apply filters
            if self._is_valid_link(parsed, absolute_url, base_domain):
                # Remove fragments (anchors) but keep query strings
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                if parsed.query:
                    clean_url += f"?{parsed.query}"

                links.add(clean_url)

        return links

    def _is_valid_link(self, parsed, absolute_url: str, base_domain: str) -> bool:
        """
        Determine if a link should be followed

        Filters out:
        - Non-HTTP(S) schemes (mailto:, javascript:, etc.)
        - Different domains (unless in allowed_domains)
        - File downloads (PDF, ZIP, etc.)
        - Anchor-only links
        """
        # Must be HTTP or HTTPS
        if parsed.scheme not in ('http', 'https'):
            return False

        # Check domain restrictions
        if self.allowed_domains:
            # If allowed_domains specified, must be in the list
            if parsed.netloc not in self.allowed_domains:
                return False
        else:
            # If no allowed_domains, must be same domain
            if not parsed.netloc.endswith(base_domain):
                return False

        # Skip file downloads
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in ['.pdf', '.zip', '.doc', '.docx', '.xls', '.xlsx']):
            return False

        # Skip anchor-only links (fragments)
        if parsed.fragment and not parsed.path:
            return False

        return True
