import asyncio
from urllib.parse import urlencode

from crawlee import Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    crawler = HttpCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        response = context.http_response.read().decode('utf-8')
        context.log.info(f'Response: {response}')  # To see the response in the logs.

    # Prepare a POST request to the form endpoint.
    request = Request.from_url(
        url='https://httpbin.org/post',
        method='POST',
        headers={'content-type': 'application/x-www-form-urlencoded'},
        payload=urlencode(
            {
                'custname': 'John Doe',
                'custtel': '1234567890',
                'custemail': 'johndoe@example.com',
                'size': 'large',
                'topping': ['bacon', 'cheese', 'mushroom'],
                'delivery': '13:00',
                'comments': 'Please ring the doorbell upon arrival.',
            }
        ).encode(),
    )

    # Run the crawler with the initial list of requests.
    await crawler.run([request])


if __name__ == '__main__':
    asyncio.run(main())
