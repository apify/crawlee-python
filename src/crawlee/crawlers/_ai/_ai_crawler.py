from __future__ import annotations

import warnings
from contextlib import AbstractAsyncContextManager
from logging import getLogger
from typing import TYPE_CHECKING

from parsel import Selector

from crawlee._utils.docs import docs_group
from crawlee.crawlers import AbstractHttpCrawler, HttpCrawlerOptions
from crawlee.crawlers._parsel._parsel_crawling_context import ParselCrawlingContext
from crawlee.crawlers._parsel._parsel_parser import ParselParser

from ._ai_crawling_context import AiCrawlingContext
from ._direct_extractor import AiDirectExtractor

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from pydantic_ai.models import Model
    from typing_extensions import Unpack

    from crawlee import Request
    from crawlee.crawlers._abstract_http import ParsedHttpCrawlingContext

    from ._types import AiHtmlExtractor, AiUsageStats, ExtractFunction, TSchema


logger = getLogger(__name__)

# Default model
_DEFAULT_AI_MODEL = 'openai:gpt-5.4-nano'


@docs_group('Crawlers')
class AiCrawler(AbstractHttpCrawler[AiCrawlingContext, Selector, Selector]):
    """A web crawler that extracts structured data from pages using an AI model.

    Builds on `AbstractHttpCrawler` and parses responses with Parsel, so the request handler has both the usual
    Parsel `selector` and the AI-powered `extract` helper: pass a Pydantic model and get a validated instance back.

    The model layer is Pydantic AI, so any provider it supports (OpenAI, Anthropic, Gemini, Ollama, ...) works
    through the `model` argument. The default extractor is an `AiDirectExtractor`: each page is distilled and sent
    to the model in one call. For cached CSS-selector extraction at near-zero LLM cost, pass an `AiSelectorExtractor`
    through the `extractor` argument.

    Warning:
        This is an experimental crawler. Its public API may change in future versions.

    ### Usage

    ```python
    from pydantic import BaseModel
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    from crawlee.crawlers import AiCrawler, AiCrawlingContext


    class Article(BaseModel):
        title: str
        author: str | None


    crawler = AiCrawler(model=OpenAIChatModel('gpt-5.4-nano', provider=OpenAIProvider(api_key='...')))


    @crawler.router.default_handler
    async def request_handler(context: AiCrawlingContext) -> None:
        article = await context.extract(Article)
        await context.push_data(article.model_dump())


    await crawler.run(['https://crawlee.dev/'])
    ```
    """

    def __init__(
        self,
        *,
        model: str | Model | None = None,
        extractor: AiHtmlExtractor | None = None,
        **kwargs: Unpack[HttpCrawlerOptions[AiCrawlingContext]],
    ) -> None:
        """Initialize a new instance.

        Args:
            model: The model used for extraction, given to the default extractor (`AiDirectExtractor`). A
                provider-prefixed name (e.g. `'openai:gpt-5.4-nano'`) or a Pydantic AI `Model` instance. When given
                as a string, the provider reads credentials from its environment variable (e.g. `OPENAI_API_KEY`).
                Pass a `Model` instance to supply them explicitly. Defaults to `'openai:gpt-5.4-nano'` when neither
                `model` nor `extractor` is given. Provide at most one of `model` or `extractor`.
            extractor: A pre-configured `AiHtmlExtractor`, for full control over the distiller, instructions,
                caching, usage limits, and model fallback. Pass an `AiSelectorExtractor` here for cached-selector
                extraction. Provide at most one of `model` or `extractor`.
            kwargs: Additional keyword arguments to pass to the underlying `AbstractHttpCrawler`.
        """
        if model is not None and extractor is not None:
            raise ValueError('Provide at most one of `model` or `extractor`.')

        if extractor is None:
            extractor = AiDirectExtractor(model if model is not None else _DEFAULT_AI_MODEL)

        # Call the notification only once.
        warnings.warn(
            'The AiCrawler is experimental and its public API may change in future releases.',
            category=UserWarning,
            stacklevel=2,
        )

        self._ai_usage = extractor.ai_usage
        self._extractor = extractor

        async def final_step(
            context: ParsedHttpCrawlingContext[Selector],
        ) -> AsyncGenerator[AiCrawlingContext, None]:
            """Enhance `ParsedHttpCrawlingContext[Selector]` with the `extract` helper and `ai_usage`."""
            parsel_context = ParselCrawlingContext.from_parsed_http_crawling_context(context)
            yield AiCrawlingContext.from_parsel_crawling_context(
                parsel_context,
                extract=self._create_extract_function(parsel_context.selector, parsel_context.request),
                ai_usage=self._ai_usage,
            )

        kwargs['_context_pipeline'] = self._create_static_content_crawler_pipeline().compose(final_step)

        # If the extractor is an async context manager, add it to the crawler's additional context managers so it's
        # properly entered and exited around the crawl.
        if isinstance(extractor, AbstractAsyncContextManager):
            kwargs['_additional_context_managers'] = [
                *kwargs.get('_additional_context_managers', []),
                extractor,
            ]
        super().__init__(
            parser=ParselParser(),
            **kwargs,
        )

    @property
    def extractor(self) -> AiHtmlExtractor:
        """The extractor used to turn pages into structured data."""
        return self._extractor

    @property
    def ai_usage(self) -> AiUsageStats:
        """Accumulated token usage across extraction calls."""
        return self._ai_usage

    def _create_extract_function(self, selector: Selector, request: Request) -> ExtractFunction:
        """Build an `extract` helper bound to the page's parsed tree.

        When the caller omits `cache_tag`, it defaults to `request.label` so an `AiSelectorExtractor` buckets
        selectors per route without extra wiring. An explicit `cache_tag` overrides this.
        """

        async def extract(
            schema: type[TSchema],
            *,
            scope: str | None = None,
            cache_tag: str | None = None,
            additional_instructions: str | None = None,
        ) -> TSchema:
            # `AiHtmlExtractor.extract` accepts a Selector directly, so the already-parsed tree is handed over
            # without a serialize round trip.
            return await self._extractor.extract(
                selector,
                schema,
                scope=scope,
                cache_tag=cache_tag if cache_tag is not None else request.label,
                additional_instructions=additional_instructions,
            )

        return extract
