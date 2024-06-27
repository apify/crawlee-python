```python
import asyncio

from crawlee.http_crawler import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    # Create a HttpCrawler instance and provide a starting requests
    crawler = HttpCrawler()

    # Define a handler for processing requests
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        # Crawler will provide a HttpCrawlingContext instance,
        # from which you can access the request and response data
        data = {
            'url': context.request.url,
            'status_code': context.http_response.status_code,
            'headers': dict(context.http_response.headers),
            'response': context.http_response.read().decode()[:1000],
        }
        # Extract the record and push it to the dataset
        await context.push_data(data)

    # Run the crawler
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```
