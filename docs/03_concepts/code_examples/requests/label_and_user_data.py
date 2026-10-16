import asyncio

from crawlee import Request
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(
        max_requests_per_crawl=5,
        max_crawl_depth=1,
    )

    @crawler.router.default_handler
    async def default_handler(context: ParselCrawlingContext) -> None:
        source = context.request.user_data['source']
        context.log.info(f'Start page {context.request.url} (source: {source})')

        # Links enqueued here get the `DETAIL` label and carry the same user data.
        await context.enqueue_links(
            label='DETAIL',
            user_data={'source': source, 'parent_url': context.request.url},
        )

    @crawler.router.handler('DETAIL')
    async def detail_handler(context: ParselCrawlingContext) -> None:
        request = context.request
        context.log.info(
            f'Detail page {request.url} '
            f'(depth: {request.crawl_depth}, parent: {request.user_data["parent_url"]})'
        )
        await context.push_data(
            {
                'url': request.url,
                'title': context.selector.css('title::text').get(),
                'source': request.user_data['source'],
            }
        )

    start_request = Request.from_url(
        'https://crawlee.dev/',
        user_data={'source': 'homepage'},
    )

    await crawler.run([start_request])


if __name__ == '__main__':
    asyncio.run(main())
