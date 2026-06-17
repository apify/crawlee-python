import asyncio

from html_to_markdown import convert
from lxml_html_clean import Cleaner
from pydantic import BaseModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from crawlee.crawlers import (
    AiCrawler,
    AiCrawlingContext,
    AiDirectExtractor,
    BaseAiHtmlDistiller,
    get_basic_ai_cleaner,
)

# Notes appended to the model instructions so it knows the input format.
MARKDOWN_PROMPT_NOTES = 'The document is Markdown converted from the HTML page.'


class MarkdownDistiller(BaseAiHtmlDistiller):
    """Distiller that cleans the page HTML and converts it to Markdown."""

    def __init__(self, cleaner: Cleaner | None = None) -> None:
        super().__init__(prompt_notes=MARKDOWN_PROMPT_NOTES)

        # Strip scripts, styles, and other noise before the conversion.
        self._cleaner = cleaner or get_basic_ai_cleaner()

    def distill(self, html: str) -> str:
        return convert(self._cleaner.clean_html(html)).content or ''


class Article(BaseModel):
    """Model representing the extracted data for an article."""

    title: str
    short_text: str


async def main() -> None:
    model = OpenAIChatModel(
        'gpt-5.4-nano',
        # Set the provider with the API key explicitly.
        provider=OpenAIProvider(api_key='your-openai-api-key'),
    )
    crawler = AiCrawler(
        # Use the custom distiller to convert the page to Markdown before extraction.
        extractor=AiDirectExtractor(model=model, distiller=MarkdownDistiller()),
        max_requests_per_crawl=5,
    )

    @crawler.router.default_handler
    async def handler(context: AiCrawlingContext) -> None:
        # Pass a Pydantic model and get a validated instance back.
        article = await context.extract(Article)
        await context.push_data(article.model_dump())

        # Enqueue links as usual, the distillation and extraction don't affect
        # the rest of the crawling logic.
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
