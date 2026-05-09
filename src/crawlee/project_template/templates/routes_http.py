import re
from urllib.parse import urljoin, urlparse

from crawlee.crawlers import HttpCrawlingContext
from crawlee.router import Router

router = Router[HttpCrawlingContext]()


@router.default_handler
async def default_handler(context: HttpCrawlingContext) -> None:
    """Default request handler."""
    context.log.info(f'Processing {context.request.url} ...')
    body = (await context.http_response.read()).decode(errors='replace')

    title_match = re.search(r'<title[^>]*>(.*?)</title>', body, re.DOTALL | re.IGNORECASE)
    title = title_match.group(1).strip() if title_match else None
    await context.push_data(
        {
            'url': context.request.loaded_url,
            'title': title,
        }
    )

    # HttpCrawler has no HTML parser, so links are extracted with a regex below.
    # For real HTML scraping, prefer BeautifulSoupCrawler or ParselCrawler.
    base_url = context.request.loaded_url or context.request.url
    base_host = urlparse(base_url).hostname
    new_requests: list[str] = []
    for match in re.finditer(r'<a[^>]*\bhref=["\']([^"\']+)["\']', body, re.IGNORECASE):
        absolute = urljoin(base_url, match.group(1))
        parsed = urlparse(absolute)
        if parsed.scheme in ('http', 'https') and parsed.hostname == base_host:
            new_requests.append(absolute)
    await context.add_requests(new_requests)
