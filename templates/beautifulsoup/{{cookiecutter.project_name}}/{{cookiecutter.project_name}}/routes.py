from crawlee.basic_crawler import Router
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawlingContext

router = Router[BeautifulSoupCrawlingContext]()


@router.default_handler
async def default_handler(context: BeautifulSoupCrawlingContext) -> None:
    """Default request handler."""
    title = context.soup.find('title')
    await context.push_data(
        {
            'url': context.request.loaded_url,
            'title': title.text if title else None,
        }
    )

    await context.enqueue_links()
