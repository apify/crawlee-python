from crawlee.crawlers import AdaptivePlaywrightCrawlingContext
from crawlee.router import Router

router = Router[AdaptivePlaywrightCrawlingContext]()


@router.default_handler
async def default_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
    """Default request handler."""
    context.log.info(f'Processing {context.request.url} ...')
    title = context.parsed_content.xpath('//title/text()').get()
    await context.push_data(
        {
            'url': context.request.loaded_url,
            'title': title,
        }
    )

    await context.enqueue_links()
