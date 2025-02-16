import asyncio

from crawlee.crawlers import BasicCrawlingContext, ParselCrawler, ParselCrawlingContext

# Regex for identifying email addresses on a webpage.
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'


async def main() -> None:
    crawler = ParselCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.selector.xpath('//title/text()').get(),
            'email_address_list': context.selector.re(EMAIL_REGEX),
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

        # Enqueue all links found on the page.
        await context.enqueue_links()

    # Register pre navigation hook which will be called before each request.
    # This hook is optional and does not need to be defined at all.
    @crawler.pre_navigation_hook
    async def some_hook(context: BasicCrawlingContext) -> None:
        pass

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://github.com'])

    # Export the entire dataset to a JSON file.
    await crawler.export_data_json(path='results.json')


if __name__ == '__main__':
    asyncio.run(main())
