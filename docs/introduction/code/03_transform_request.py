from __future__ import annotations

from crawlee import HttpHeaders, Request
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=50)

    def transform_request(request: Request) -> Request | None:
        # Skip requests to PDF files
        if request.url.endswith('.pdf'):
            return None

        # Add custom headers to requests to specific sections
        if '/docs' in request.url:
            request.headers = HttpHeaders({'Custom-Header': 'value'})

        # Add label for certain URLs
        if '/blog' in request.url:
            request.user_data['label'] = 'BLOG'

        return request

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}.')

        # Transfor request befor enqueue
        await context.enqueue_links(transform_request_function=transform_request)

    @crawler.router.handler('BLOG')
    async def blog_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Blog Processing {context.request.url}.')

    await crawler.run(['https://crawlee.dev/'])
