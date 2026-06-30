from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING, cast

from pydantic_ai import Agent
from pydantic_ai.usage import RunUsage

from crawlee._utils.docs import docs_group

from ._base_extractor import BasePydanticAiHtmlExtractor
from ._clean_html_distiller import PydanticAiCleanHtmlDistiller
from ._prompts import _DIRECT_INSTRUCTIONS

if TYPE_CHECKING:
    from parsel import Selector
    from pydantic_ai.models import Model
    from pydantic_ai.usage import UsageLimits

    from ._types import PydanticAiHtmlDistiller, TSchema


logger = getLogger(__name__)


@docs_group('Other')
class PydanticAiDirectExtractor(BasePydanticAiHtmlExtractor):
    """Extractor that asks the LLM to read the page and return the data directly.

    The page is distilled to compact HTML and sent to the model in a single call. The user schema is the agent's
    output type, so pydantic-ai validates the result and feeds invalid output back to the model. This is the
    simplest extractor and works on any page, at the cost of one LLM call per page.

    See the `PydanticAiHtmlExtractor` protocol for the common extractor interface, and `PydanticAiSelectorExtractor`
    for a variant that learns reusable CSS selectors.

    ### Usage

    ```python
    from pydantic import BaseModel
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    from crawlee.crawlers import PydanticAiDirectExtractor


    class Product(BaseModel):
        name: str
        price: str | None


    model = OpenAIChatModel('gpt-5.4-nano', provider=OpenAIProvider(api_key='...'))
    extractor = PydanticAiDirectExtractor(model=model)
    product = await extractor.extract('<html>...</html>', Product)
    ```
    """

    def __init__(
        self,
        model: str | Model,
        *,
        distiller: PydanticAiHtmlDistiller | None = None,
        instructions: str = _DIRECT_INSTRUCTIONS,
        retries: int = 1,
        usage_limits: UsageLimits | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            model: A provider-prefixed name (e.g. `'openai:gpt-5.4-nano'`) or a pydantic-ai `Model`.
            distiller: The HTML distiller shaping the LLM input. Defaults to `PydanticAiCleanHtmlDistiller`.
            instructions: Base task instructions. The distiller's prompt notes are appended automatically.
            retries: How many times the model may fix output that fails schema validation within one run (pydantic-ai
                output retries).
            usage_limits: Optional pydantic-ai `UsageLimits` applied to every single run.
        """
        super().__init__(
            model,
            distiller=distiller or PydanticAiCleanHtmlDistiller(),
            instructions=instructions,
            usage_limits=usage_limits,
        )
        self._retries = retries

    async def extract(
        self,
        content: str | Selector,
        schema: type[TSchema],
        *,
        scope: str | None = None,
        cache_tag: str | None = None,  # noqa: ARG002 ignored in direct extraction (no caching)
        additional_instructions: str | None = None,
    ) -> TSchema:
        """Distill `content`, send it to the model, and return a validated `schema`.

        Args:
            content: Raw HTML or a parsed Parsel `Selector`.
            schema: The Pydantic model describing the desired output.
            scope: Optional CSS selector restricting extraction to the first matching subtree.
            cache_tag: Ignored in direct extraction.
            additional_instructions: Extra instructions appended for this call only.
        """
        if scope is not None:
            # Scope resolution requires a parsed tree. Serializing the matched subtree also keeps the distiller input
            # minimal.
            content = self._resolve_scope(self._as_selector(content), scope)
        html = self._as_selector(content).get()
        return await self._run(html, schema, additional_instructions)

    async def _run(
        self,
        html: str,
        schema: type[TSchema],
        additional_instructions: str | None,
    ) -> TSchema:
        distilled_html = await asyncio.to_thread(self._distiller.distill, html)

        # `cast` restores the static type pinned at runtime by `output_type`.
        agent: Agent[None, TSchema] = cast(
            'Agent[None, TSchema]',
            Agent(
                self._model,
                output_type=schema,
                instructions=self._base_instructions,
                retries=self._retries,
            ),
        )

        # The task framing names the fields explicitly: the output tool schema alone is not enough for smaller
        # models, which otherwise answer that no fields were requested or describe the page instead of extracting
        # from it. Types and descriptions already reach the model through the output tool schema, so they are not
        # repeated.
        field_names = ', '.join(schema.model_fields)
        prompt = f'Extract the following fields from the document below: {field_names}.\n\nDocument:\n{distilled_html}'

        run_usage = RunUsage()
        try:
            result = await agent.run(
                prompt,
                instructions=additional_instructions,
                usage_limits=self._usage_limits,
                usage=run_usage,
            )
        finally:
            self._ai_usage.add(run_usage)

        logger.debug(
            f'Direct extraction of {schema.__name__} used {run_usage.requests} request(s), '
            f'{run_usage.input_tokens} input + {run_usage.output_tokens} output tokens.'
        )
        return result.output
