import asyncio
from collections.abc import Callable

from yarl import URL

from crawlee import RequestOptions, RequestTransformAction
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.http_clients import ImpitHttpClient
from crawlee.request_loaders import SitemapRequestLoader


# Create a transform_request_function that maps request options based on the host in
# the URL
def create_transform_request(
    data_mapper: dict[str, dict],
) -> Callable[[RequestOptions], RequestOptions | RequestTransformAction]:
    def transform_request(
        request_options: RequestOptions,
    ) -> RequestOptions | RequestTransformAction:
        # According to the Sitemap protocol, all URLs in a Sitemap must be from a single
        # host.
        request_host = URL(request_options['url']).host

        if request_host and (mapping_data := data_mapper.get(request_host)):
            # Set properties from the mapping data
            if 'label' in mapping_data:
                request_options['label'] = mapping_data['label']
            if 'user_data' in mapping_data:
                request_options['user_data'] = mapping_data['user_data']

            return request_options

        return 'unchanged'

    return transform_request


async def main() -> None:
    # Prepare data mapping for hosts
    apify_host = URL('https://apify.com/sitemap.xml').host
    crawlee_host = URL('https://crawlee.dev/sitemap.xml').host

    if not apify_host or not crawlee_host:
        raise ValueError('Unable to extract host from URLs')

    data_map = {
        apify_host: {
            'label': 'apify',
            'user_data': {'source': 'apify'},
        },
        crawlee_host: {
            'label': 'crawlee',
            'user_data': {'source': 'crawlee'},
        },
    }

    # Initialize the SitemapRequestLoader with the transform function
    async with SitemapRequestLoader(
        # Set the sitemap URLs and the HTTP client
        sitemap_urls=['https://crawlee.dev/sitemap.xml', 'https://apify.com/sitemap.xml'],
        http_client=ImpitHttpClient(),
        transform_request_function=create_transform_request(data_map),
    ) as sitemap_loader:
        # Convert the sitemap loader to a request manager
        request_manager = await sitemap_loader.to_tandem()

        # Create and configure the crawler
        crawler = BeautifulSoupCrawler(
            request_manager=request_manager,
            max_requests_per_crawl=10,
        )

        # Create default handler for requests without a specific label
        @crawler.router.default_handler
        async def handler(context: BeautifulSoupCrawlingContext) -> None:
            source = context.request.user_data.get('source', 'unknown')
            context.log.info(
                f'Processing request: {context.request.url} from source: {source}'
            )

        # Create handler for requests labeled 'apify'
        @crawler.router.handler('apify')
        async def apify_handler(context: BeautifulSoupCrawlingContext) -> None:
            source = context.request.user_data.get('source', 'unknown')
            context.log.info(
                f'Apify handler processing: {context.request.url} from source: {source}'
            )

        # Create handler for requests labeled 'crawlee'
        @crawler.router.handler('crawlee')
        async def crawlee_handler(context: BeautifulSoupCrawlingContext) -> None:
            source = context.request.user_data.get('source', 'unknown')
            context.log.info(
                f'Crawlee handler processing: {context.request.url} from source: {source}'
            )

        await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
