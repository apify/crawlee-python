import asyncio
import re

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    # Create an HttpCrawler instance - no automatic parsing
    crawler = HttpCrawler(
        # Limit the crawl to 10 requests
        max_requests_per_crawl=10,
    )

    # Define the default request handler
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # Get the raw response content
        response_body = await context.http_response.read()
        response_text = response_body.decode('utf-8')

        # Extract title manually using regex (since we don't have a parser)
        title_match = re.search(
            r'<title[^>]*>([^<]+)</title>', response_text, re.IGNORECASE
        )
        title = title_match.group(1).strip() if title_match else None

        # Extract basic information
        data = {
            'url': context.request.url,
            'title': title,
        }

        # Push extracted data to the dataset
        await context.push_data(data)

        # Simple link extraction for further crawling
        href_pattern = r'href=["\']([^"\']+)["\']'
        matches = re.findall(href_pattern, response_text, re.IGNORECASE)

        # Enqueue first few links found (limit to avoid too many requests)
        for href in matches[:3]:
            if href.startswith('http') and 'crawlee.dev' in href:
                await context.add_requests([href])

    # Run the crawler
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
