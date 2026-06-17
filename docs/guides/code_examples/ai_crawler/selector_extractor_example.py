import asyncio

from pydantic import BaseModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from crawlee import Glob
from crawlee.crawlers import (
    AiCrawler,
    AiCrawlingContext,
    AiDirectExtractor,
    AiSelectorExtractor,
)


class Article(BaseModel):
    """Model representing the extracted data for an article."""

    title: str
    main_text: str


async def main() -> None:
    model = OpenAIChatModel(
        'gpt-5.4-nano',
        provider=OpenAIProvider(api_key='your-openai-api-key'),
    )
    crawler = AiCrawler(
        extractor=AiSelectorExtractor(
            model=model,
            # Pages the cached selectors cannot handle fall back to direct extraction.
            fallback=AiDirectExtractor(model=model),
        ),
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def handler(context: AiCrawlingContext) -> None:
        # Enqueue blog article pages; the article handler extracts the data.
        await context.enqueue_links(
            include=[Glob('https://crawlee.dev/blog/*')],
            label='article',
        )

    @crawler.router.handler('article')
    async def article_handler(context: AiCrawlingContext) -> None:
        # The first page generates selectors; later pages reuse them with no LLM call.
        article = await context.extract(Article)

        await context.push_data(article.model_dump())

    await crawler.run(['https://crawlee.dev/blog'])


if __name__ == '__main__':
    asyncio.run(main())
