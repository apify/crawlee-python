from crawlee.crawlers import StagehandCrawlingContext
from crawlee.router import Router

router = Router[StagehandCrawlingContext]()


@router.default_handler
async def default_handler(context: StagehandCrawlingContext) -> None:
    """Default request handler."""
    context.log.info(f'Processing {context.request.url} ...')

    data = await context.page.extract(instruction='Get the page title and main heading.')

    await context.push_data(
        {
            'url': context.request.loaded_url,
            'data': data.model_dump(),
        }
    )

    await context.enqueue_links()
