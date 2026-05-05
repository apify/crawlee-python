import asyncio

from .selectolax_crawler import SelectolaxLexborContext, SelectolaxLexborCrawler


async def main() -> None:
    crawler = SelectolaxLexborCrawler(
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def handle_request(context: SelectolaxLexborContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        data = {
            'url': context.request.url,
            'title': context.parser.css_first('title').text(),
        }

        await context.push_data(data)
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
