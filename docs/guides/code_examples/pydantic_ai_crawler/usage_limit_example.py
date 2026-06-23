import asyncio

from pydantic import BaseModel
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.usage import UsageLimits

from crawlee.crawlers import (
    PydanticAiCrawler,
    PydanticAiCrawlingContext,
    PydanticAiDirectExtractor,
)

# Stop the whole crawl once this many tokens have been spent.
TOKEN_BUDGET = 50_000


class Article(BaseModel):
    """Model representing the extracted data for an article."""

    title: str
    short_text: str


async def main() -> None:
    model = OpenAIChatModel(
        'gpt-5.4-nano',
        provider=OpenAIProvider(api_key='your-openai-api-key'),
    )
    crawler = PydanticAiCrawler(
        # Cap each extraction so an oversized page cannot consume LLM resources.
        extractor=PydanticAiDirectExtractor(
            model=model,
            usage_limits=UsageLimits(total_tokens_limit=10_000),
        ),
        max_requests_per_crawl=5,
    )

    @crawler.router.default_handler
    async def handler(context: PydanticAiCrawlingContext) -> None:
        # Stop the crawl once the cumulative token budget is exhausted.
        if context.ai_usage.total_tokens > TOKEN_BUDGET:
            context.log.info('Token budget exhausted, stopping the crawler.')
            crawler.stop()
            return

        try:
            article = await context.extract(Article)
        except UsageLimitExceeded:
            # The page needs more tokens than the per-extraction limit allows.
            context.log.warning(f'Content at {context.request.url} is too large.')
            return

        await context.push_data(article.model_dump())

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
