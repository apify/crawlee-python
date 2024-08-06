from crawlee.basic_crawler import Router
from crawlee.playwright_crawler import PlaywrightCrawlingContext

router = Router[PlaywrightCrawlingContext]()


@router.default_handler
async def default_handler(context: PlaywrightCrawlingContext) -> None:
    """Default request handler."""
    title = await context.page.query_selector('title')
    await context.push_data(
        {
            'url': context.request.loaded_url,
            'title': await title.inner_text() if title else None,
        }
    )

    await context.enqueue_links()
