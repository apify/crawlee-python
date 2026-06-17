import asyncio

from pydantic import BaseModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from crawlee.crawlers import AiCrawler, AiCrawlingContext


class Post(BaseModel):
    """Model representing a single post."""

    title: str
    url: str


class Posts(BaseModel):
    """Model representing the extracted list of posts."""

    posts: list[Post]


async def main() -> None:
    model = OpenAIChatModel(
        'gpt-5.4-nano',
        provider=OpenAIProvider(api_key='your-openai-api-key'),
    )
    crawler = AiCrawler(model=model, max_requests_per_crawl=5)

    @crawler.router.default_handler
    async def handler(context: AiCrawlingContext) -> None:
        # The instruction narrows what the model returns from the page.
        posts = await context.extract(
            Posts,
            additional_instructions='Extract only the top five posts on the page.',
        )

        await context.push_data(posts.model_dump())

    await crawler.run(['https://news.ycombinator.com'])


if __name__ == '__main__':
    asyncio.run(main())
