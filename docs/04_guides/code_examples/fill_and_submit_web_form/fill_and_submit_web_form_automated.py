import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler()

    # Fill in the form on the page and enqueue its submission.
    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info('Filling in the form on %s ...', context.request.url)
        requests = await context.extract_form_requests(
            fields={
                'custname': 'John Doe',
                'custtel': '1234567890',
                'custemail': 'johndoe@example.com',
                'size': 'large',
                'topping': ['bacon', 'cheese', 'mushroom'],
                'delivery': '13:00',
                'comments': 'Please ring the doorbell upon arrival.',
            },
            label='form-result',
        )
        await context.add_requests(requests)

    # Process the response to the form submission.
    @crawler.router.handler('form-result')
    async def form_result_handler(context: ParselCrawlingContext) -> None:
        context.log.info('Processing %s ...', context.request.url)
        response = (await context.http_response.read()).decode('utf-8')
        context.log.info('Response: %s', response)  # To see the response in the logs.

    # Run the crawler with the page containing the form.
    await crawler.run(['https://httpbin.org/forms/post'])


if __name__ == '__main__':
    asyncio.run(main())
