from __future__ import annotations

from typing import Literal

from crawlee import HttpHeaders, RequestOptions
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


def transform_request(request_options: RequestOptions) -> RequestOptions | Literal['skip', 'unchanged']:
    # Skip requests to PDF files
    if request_options['url'].endswith('.pdf'):
        return 'skip'

    if '/docs' in request_options['url']:
        # Add custom headers to requests to specific URLs
        request_options['headers'] = HttpHeaders({'Custom-Header': 'value'})

    elif '/blog' in request_options['url']:
        # Add label for certain URLs
        request_options['label'] = 'BLOG'

    else:
        # Signal that the request should proceed without any transformation
        return 'unchanged'

    return request_options


async def main() -> None:
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=50)

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}.')

        # Transfor request befor enqueue
        await context.enqueue_links(transform_request_function=transform_request)

    @crawler.router.handler('BLOG')
    async def blog_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Blog Processing {context.request.url}.')

    await crawler.run(['https://crawlee.dev/'])
