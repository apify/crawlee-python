import asyncio

from pydantic import BaseModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from crawlee.crawlers import PydanticAiCrawler, PydanticAiCrawlingContext


class Article(BaseModel):
    """Model representing the extracted data for an article."""

    title: str
    short_text: str


async def main() -> None:
    # A `Model` instance sets the API key explicitly. A provider-prefixed string such as
    # 'openai:gpt-5.4-nano' reads the key from the provider's env var like OPENAI_API_KEY.
    model = OpenAIChatModel(
        'gpt-5.4-nano',
        provider=OpenAIProvider(api_key='your-openai-api-key'),
    )

    # With only `model`, the crawler uses a PydanticAiDirectExtractor by default.
    crawler = PydanticAiCrawler(model=model, max_requests_per_crawl=5)

    @crawler.router.default_handler
    async def handler(context: PydanticAiCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Pass a Pydantic model and get a validated instance back.
        article = await context.extract(Article)

        await context.push_data(article.model_dump())

        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
