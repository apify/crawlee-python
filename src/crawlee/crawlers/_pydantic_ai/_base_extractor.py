from __future__ import annotations

from abc import ABC, abstractmethod
from logging import getLogger
from typing import TYPE_CHECKING

from parsel import Selector
from pydantic_ai.models import infer_model

from crawlee._utils.docs import docs_group

from ._types import PydanticAiUsageStats

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Self

    from pydantic_ai.models import Model
    from pydantic_ai.usage import UsageLimits

    from ._types import PydanticAiHtmlDistiller, TSchema


logger = getLogger(__name__)


@docs_group('Other')
class BasePydanticAiHtmlExtractor(ABC):
    """Base class for the built-in HTML extractors.

    An HTML extractor turns a page into a validated Pydantic model with the help of an LLM. This abstract base
    implements the parts the built-in extractors share: resolving the model, composing the task instructions with
    the distiller's prompt notes, and accumulating token usage. It also provides a default `async with` lifecycle.

    The public interface is the `PydanticAiHtmlExtractor` protocol. The concrete extractors are
    `PydanticAiDirectExtractor` and `PydanticAiSelectorExtractor`.
    """

    def __init__(
        self,
        model: str | Model,
        *,
        distiller: PydanticAiHtmlDistiller,
        instructions: str,
        usage_limits: UsageLimits | None,
    ) -> None:
        """Initialize a new instance.

        Args:
            model: A provider-prefixed name (e.g. `'openai:gpt-5.4-nano'`) or a pydantic-ai `Model`. Credentials are
                read from the provider's environment variable (e.g. `OPENAI_API_KEY`) or passed explicitly through a
                `Model` instance.
            distiller: The HTML distiller shaping the LLM input.
            instructions: Base task instructions. The distiller's prompt notes are appended automatically.
            usage_limits: Optional pydantic-ai `UsageLimits` applied to every single run.
        """
        self._model = infer_model(model)
        self._distiller = distiller
        self._base_instructions = self._compose_instructions(instructions, distiller)
        self._usage_limits = usage_limits
        self._ai_usage = PydanticAiUsageStats()
        self._active = False

    @property
    def active(self) -> bool:
        """Whether the extractor is entered and not exited yet."""
        return self._active

    async def __aenter__(self) -> Self:
        """Prepare the extractor for extraction.

        Raises:
            RuntimeError: If the extractor is already active.
        """
        if self._active:
            raise RuntimeError(f'The {type(self).__name__} is already active.')

        self._active = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Release what the extractor holds.

        Raises:
            RuntimeError: If the extractor is not active.
        """
        if not self._active:
            raise RuntimeError(f'The {type(self).__name__} is not active.')

        self._active = False

    @property
    def ai_usage(self) -> PydanticAiUsageStats:
        """Accumulated token usage of this extractor's runs."""
        return self._ai_usage

    def set_ai_usage(self, value: PydanticAiUsageStats) -> None:
        """Replace the usage accumulator with `value`.

        Lets an external owner share one accumulator across a delegation chain.

        Args:
            value: The accumulator to adopt.
        """
        self._ai_usage = value

    @abstractmethod
    async def extract(
        self,
        content: str | Selector,
        schema: type[TSchema],
        *,
        scope: str | None = None,
        cache_tag: str | None = None,
        additional_instructions: str | None = None,
    ) -> TSchema:
        """Extract a structured instance of `schema` from `content`."""

    @staticmethod
    def _compose_instructions(instructions: str, distiller: PydanticAiHtmlDistiller) -> str:
        """Append the distiller's input-format notes to the task instructions.

        Args:
            instructions: The base task instructions.
            distiller: The distiller producing the LLM input.
        """
        notes = distiller.get_prompt_notes()
        return f'{instructions}\n\n{notes}' if notes else instructions

    @staticmethod
    def _resolve_scope(selector: Selector, scope: str) -> Selector:
        """Return the first subtree matching `scope`, or raise.

        Args:
            selector: The Parsel selector to query.
            scope: A CSS selector identifying the region of interest.

        Raises:
            ValueError: When the scope matches nothing on the page.
        """
        scoped = selector.css(scope)
        if not scoped:
            raise ValueError(f'Extraction scope {scope!r} matched nothing on the page.')
        if len(scoped) > 1:
            logger.warning(
                'Extraction scope %r matched %s elements on the page, using the first one.', scope, len(scoped)
            )
        return scoped[0]

    @staticmethod
    def _as_selector(content: str | Selector) -> Selector:
        """Wrap a raw HTML string in a `Selector`, or return the input unchanged."""
        return content if isinstance(content, Selector) else Selector(text=content)
