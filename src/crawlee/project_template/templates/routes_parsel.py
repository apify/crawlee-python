from crawlee.crawlers import ParselCrawlingContext
from crawlee.router import Router

router = Router[ParselCrawlingContext]()


@router.default_handler
async def default_handler(context: ParselCrawlingContext) -> None:
    """Default request handler."""
    context.log.info(f'Processing {context.request.url} ...')
    title = context.selector.xpath('//title/text()').get()
    await context.push_data(
        {
            'url': context.request.loaded_url,
            'title': title,
        }
    )

    await context.enqueue_links()
