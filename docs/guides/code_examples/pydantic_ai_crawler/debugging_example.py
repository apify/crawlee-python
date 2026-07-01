import asyncio

from pydantic import BaseModel
from pydantic_ai import capture_run_messages
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from crawlee import ConcurrencySettings
from crawlee.crawlers import (
    PydanticAiCleanHtmlDistiller,
    PydanticAiCrawler,
    PydanticAiCrawlingContext,
    PydanticAiDirectExtractor,
)


class Article(BaseModel):
    """Model representing the extracted data for an article."""

    title: str
    short_text: str


async def main() -> None:
    model = OpenAIChatModel(
        'gpt-5.4-nano',
        provider=OpenAIProvider(api_key='your-openai-api-key'),
    )
    # Build the distiller once so the extractor and the handler below share
    # the same instance.
    distiller = PydanticAiCleanHtmlDistiller()
    crawler = PydanticAiCrawler(
        max_requests_per_crawl=10,
        # Create a direct extractor with your distiller.
        extractor=PydanticAiDirectExtractor(
            model,
            distiller=distiller,
        ),
        # Set concurrency to 1, which ensures only one request is processed at a time.
        concurrency_settings=ConcurrencySettings(
            desired_concurrency=1, max_concurrency=1
        ),
        # Set abort_on_error to True to stop the crawl if an error occurs during
        # extraction.
        abort_on_error=True,
    )

    @crawler.router.default_handler
    async def handler(context: PydanticAiCrawlingContext) -> None:
        # Inspect the distilled document the model actually reads, using the same
        # distiller the extractor runs. On real pages this can be tens of KB.
        distilled = distiller.distill(context.selector.get())
        context.log.info(distilled)

        # Capture the prompts, responses, and retries exchanged with the model.
        with capture_run_messages() as messages:
            try:
                article = await context.extract(Article)
            except UnexpectedModelBehavior:
                context.log.exception(f'Extraction failed for {context.request.url}.')
                raise
            finally:
                # Log each exchanged message on its own line for readability.
                for message in messages:
                    context.log.info(f'{message}')

        await context.push_data(article.model_dump())

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
